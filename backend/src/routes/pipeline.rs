//! `POST /api/pipeline/{ingest,enrich,discover,ingest-ownership}` and
//! `GET /api/pipeline/jobs/:id`, per Architecture.md's REST API section.
//!
//! `ingest` and `enrich` are wired end-to-end. `discover` has two modes:
//! pass `network_manifest_url` in the body to fetch+parse exactly that
//! one `cms-hpt.txt` (see `compliance_probe::manifest_parser` — format
//! confirmed live against a real, multi-location manifest), resolve its
//! `location-name` entries against hospitals by fuzzy name match (see
//! `crate::mrf_match`), cross-validate each match against CMS's
//! ownership-disclosure graph (`db::queries::ownership_reachable_ccns` —
//! populated by `ingest-ownership`, see below), and persist a
//! `mrf_discoveries` row for every match that clears *both* checks.
//! Without `network_manifest_url`: the general per-hospital-website mode
//! — every hospital with a non-null `website_url` (optionally narrowed by
//! `state`/`facility_ids`) is probed on its own site for its own
//! `cms-hpt.txt` (`compliance_probe::probe::Prober::probe`, bounded by
//! `Config::probe_concurrency` concurrent probes), and every probe
//! outcome — found, unreachable, no manifest, whatever — is recorded as
//! its own `mrf_discoveries` row via `db::queries::insert_discovery_result`.
//! Both modes capture an `mrf_metadata` baseline for every MRF URL they
//! confirm (see `crate::mrf_metadata`).
//!
//! `ingest-ownership` fetches CMS's "Hospital Enrollments" and "Hospital
//! All Owners" datasets (`cms_ingest::ownership`) and replaces
//! `hospital_enrollments` / `hospital_ownership_edges` — run this before
//! relying on `discover`'s ownership cross-validation actually finding
//! anything (an empty ownership graph just means every name match gets
//! rejected for lack of corroboration, not a crash).
//!
//! `enrich`'s cascade is `[census, nominatim, google_maps]`, each gated
//! by its own `GEOCODING_*_ENABLED` flag — Census first (free, generally
//! the best first-try hit rate), Nominatim second (free, 1 req/sec,
//! opportunistically also finds a website), Google Maps last (paid, off
//! by default, for the handful of addresses neither free provider can
//! resolve). Website discovery runs a geocode provider's opportunistic
//! find first (currently only Nominatim's `extratags.website`) and falls
//! back to Google Places only when that's empty — both to save the
//! Places API's per-call cost and because that's the priority order the
//! project owner chose. See `geo-enrich/src/enricher.rs`.
//!
//! `?retry_incomplete=true` on `POST /api/pipeline/enrich` opts into a
//! backfill pass: alongside the normal `enriched_at IS NULL` hospitals,
//! it also re-picks-up hospitals that already ran once but are still
//! missing coordinates or a website (see `EnrichQuery` and
//! `db::queries::list_unenriched_hospitals`'s doc comment for why this
//! is opt-in, not the default).

use std::collections::HashSet;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use compliance_probe::probe::{DiscoveryStatus, Prober};
use futures::stream::{self, StreamExt};
use serde::Deserialize;
use sqlx::SqlitePool;
use utoipa::{IntoParams, ToSchema};

use geo_enrich::enricher::EnrichmentConfig;
use geo_enrich::providers::{CensusProvider, GoogleMapsProvider, GooglePlacesClient, NominatimProvider};
use geo_enrich::{CascadingGeocoder, GeocodingProvider};

use crate::enrich_store::HospitalStore;
use crate::error::{ApiError, ErrorResponse};
use crate::jobs::{Job, JobStatus};
use crate::mrf_match::{self, MatchCandidate};
use crate::routes::AppState;

/// User-Agent shared by every outbound HTTP client this module builds —
/// enrich's geocoding cascade, and discover's manifest/MRF probing.
pub(crate) const USER_AGENT: &str =
    "cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)";

#[derive(Debug, Deserialize, Default, IntoParams)]
pub struct EnrichQuery {
    /// Opt-in backfill flag — see this module's doc comment. Missing from
    /// the query string defaults to `false` (a normal pass); an explicit
    /// `?retry_incomplete=true`/`false` is parsed as a bool by axum's
    /// `Query` extractor — an unparseable value (e.g. `?retry_incomplete=maybe`)
    /// is rejected with `400 Bad Request` before this handler runs, same
    /// as any other malformed query param in this API.
    #[serde(default)]
    retry_incomplete: bool,
}

/// Trigger CMS ingest
///
/// Fetches the "Hospital General Information" dataset from
/// data.cms.gov and upserts it into the `hospitals` table. Runs in the
/// background; poll `GET /api/pipeline/jobs/{id}` with the returned job
/// id for progress.
#[utoipa::path(
    post,
    path = "/api/pipeline/ingest",
    responses(
        (status = 202, description = "Ingest job accepted", body = Job),
    ),
    tag = "pipeline",
)]
pub async fn trigger_ingest(State(state): State<AppState>) -> Result<(StatusCode, Json<Job>), ApiError> {
    let job = state.jobs.create("ingest");
    let job_id = job.id.clone();

    let pool = state.pool.clone();
    let jobs = state.jobs.clone();

    // Runs in the background; the caller polls GET /api/pipeline/jobs/:id.
    tokio::spawn(async move {
        jobs.update(&job_id, |j| j.status = JobStatus::Running);

        let client = match cms_ingest::CmsClient::with_defaults() {
            Ok(c) => c,
            Err(e) => {
                fail_job(&jobs, &job_id, e.to_string());
                return;
            }
        };

        let summary = match cms_ingest::ingest_all(&client).await {
            Ok(s) => s,
            Err(e) => {
                fail_job(&jobs, &job_id, e.to_string());
                return;
            }
        };

        jobs.update(&job_id, |j| j.progress.total = summary.total_fetched as u64);

        let mut completed = 0u64;
        let mut failed = summary.skipped.len() as u64;
        for record in &summary.parsed {
            match crate::db::queries::upsert_hospital(&pool, record).await {
                Ok(()) => completed += 1,
                Err(e) => {
                    failed += 1;
                    tracing::warn!(facility_id = %record.facility_id, error = %e, "failed to upsert hospital");
                }
            }
        }

        jobs.update(&job_id, |j| {
            j.progress.completed = completed;
            j.progress.failed = failed;
            j.status = JobStatus::Completed;
            j.finished_at = Some(chrono::Utc::now());
        });
    });

    Ok((StatusCode::ACCEPTED, Json(job)))
}

pub(crate) fn fail_job(jobs: &crate::jobs::JobTracker, job_id: &str, error: String) {
    jobs.update(job_id, |j| {
        j.status = JobStatus::Failed;
        j.error = Some(error);
        j.finished_at = Some(chrono::Utc::now());
    });
}

/// Trigger geocoding enrichment
///
/// Runs the cascading geocoder (Census → Nominatim → Google Maps, each
/// gated by its own config flag) over up to `ENRICH_BATCH_LIMIT`
/// un-enriched hospitals. Runs in the background; poll
/// `GET /api/pipeline/jobs/{id}` with the returned job id for progress.
#[utoipa::path(
    post,
    path = "/api/pipeline/enrich",
    params(EnrichQuery),
    responses(
        (status = 202, description = "Enrich job accepted", body = Job),
    ),
    tag = "pipeline",
)]
pub async fn trigger_enrich(
    State(state): State<AppState>,
    Query(query): Query<EnrichQuery>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    let job = state.jobs.create("enrich");
    let job_id = job.id.clone();

    let pool = state.pool.clone();
    let jobs = state.jobs.clone();
    let config = state.config.clone();
    let retry_incomplete = query.retry_incomplete;

    tokio::spawn(async move {
        jobs.update(&job_id, |j| j.status = JobStatus::Running);

        // Shared by Nominatim (usage-policy requirement), Census, Places,
        // and Google Maps alike — one client, one User-Agent, reused.
        let http = match reqwest::Client::builder().user_agent(USER_AGENT).build() {
            Ok(c) => c,
            Err(e) => {
                fail_job(&jobs, &job_id, format!("failed to build HTTP client: {e}"));
                return;
            }
        };

        // Cascade order: census (free, best first-try hit rate) →
        // nominatim (free, 1 req/sec, also opportunistically finds a
        // website) → google_maps (paid, off by default). Each gated by
        // its own GEOCODING_*_ENABLED flag.
        let mut providers: Vec<Box<dyn GeocodingProvider>> = Vec::new();
        if config.geocoding_census_enabled {
            providers.push(Box::new(CensusProvider::new(http.clone())));
        }
        if config.geocoding_nominatim_enabled {
            providers.push(Box::new(NominatimProvider::new(http.clone())));
        }
        if let Some(google_maps) = GoogleMapsProvider::from_config(
            http.clone(),
            config.geocoding_google_maps_enabled,
            config.geocoding_google_maps_api_key.clone(),
        ) {
            providers.push(Box::new(google_maps));
        }

        if providers.is_empty() {
            fail_job(
                &jobs,
                &job_id,
                "no geocoding providers enabled — set at least one of GEOCODING_CENSUS_ENABLED, \
                 GEOCODING_NOMINATIM_ENABLED, or GEOCODING_GOOGLE_MAPS_ENABLED (with an API key) to true"
                    .to_string(),
            );
            return;
        }

        let geocoder = Arc::new(CascadingGeocoder::new(providers));

        let places = GooglePlacesClient::from_config(
            http.clone(),
            config.google_places_enabled,
            config.google_places_api_key.clone(),
        )
        .map(Arc::new);
        if places.is_none() {
            tracing::info!(
                "GOOGLE_PLACES_ENABLED is false or GOOGLE_PLACES_API_KEY is unset — website discovery \
                 will only use Nominatim's opportunistic extratags.website, no fallback"
            );
        }

        let store = HospitalStore::new(pool);
        let enrich_config = EnrichmentConfig {
            concurrency: config.enrich_concurrency,
        };

        let jobs_for_total = jobs.clone();
        let job_id_for_total = job_id.clone();
        let jobs_for_progress = jobs.clone();
        let job_id_for_progress = job_id.clone();

        let result = geo_enrich::enricher::enrich_batch(
            &store,
            geocoder,
            places,
            enrich_config,
            config.enrich_batch_limit,
            retry_incomplete,
            move |total| {
                jobs_for_total.update(&job_id_for_total, |j| j.progress.total = total);
            },
            move |outcome, saved| {
                jobs_for_progress.update(&job_id_for_progress, |j| {
                    if saved {
                        j.progress.completed += 1;
                    } else {
                        j.progress.failed += 1;
                    }
                });
                tracing::debug!(
                    facility_id = %outcome.facility_id,
                    geocoded = outcome.geocode.is_some(),
                    website_found = outcome.website_url.is_some(),
                    website_source = outcome.website_source.as_deref().unwrap_or("none"),
                    saved,
                    "enriched one hospital"
                );
            },
        )
        .await;

        match result {
            Ok(_summary) => {
                jobs.update(&job_id, |j| {
                    j.status = JobStatus::Completed;
                    j.finished_at = Some(chrono::Utc::now());
                });
            }
            Err(e) => fail_job(&jobs, &job_id, e.to_string()),
        }
    });

    Ok((StatusCode::ACCEPTED, Json(job)))
}

/// Body for `POST /api/pipeline/discover`. All fields optional.
#[derive(Debug, Deserialize, Default, ToSchema)]
pub struct DiscoverRequest {
    /// When set, runs the **network-manifest mode**: fetch and parse
    /// exactly this `cms-hpt.txt` URL, resolve its entries against
    /// hospitals, cross-validate against the ownership graph, and tag
    /// every confirmed match. When unset, runs the **per-hospital-website
    /// mode** instead: every hospital with a `website_url` on file is
    /// probed on its own site for its own `cms-hpt.txt`.
    #[schema(example = "https://www.sentara.com/cms-hpt.txt")]
    pub network_manifest_url: Option<String>,
    /// Narrows which hospitals are considered — as fuzzy-match candidates
    /// in network-manifest mode, or as the hospitals actually probed in
    /// per-hospital-website mode — to one state. Optional; recommended
    /// for a nationwide health system's manifest, to cut down on
    /// incidental name collisions before the ownership-graph check even
    /// runs.
    pub state: Option<String>,
    /// Narrows to an explicit facility_id list, instead of (or in
    /// addition to) `state`. In per-hospital-website mode, this is how to
    /// run discovery for one or a few specific hospitals rather than
    /// every hospital nationwide with a website.
    pub facility_ids: Option<Vec<String>>,
}

/// Trigger MRF discovery
///
/// **With `network_manifest_url`** in the body: fetches and parses that
/// one `cms-hpt.txt`, resolves each entry's `location-name` to a hospital
/// by fuzzy name match (`state`/`facility_ids` optionally narrow the
/// candidate pool), keeps only matches CMS's ownership-disclosure data
/// also connects to the manifest's best-matching (anchor) hospital, and
/// records a `mrf_discoveries` row for each. Run
/// `POST /api/pipeline/ingest-ownership` at least once first, or every
/// match gets rejected for lack of ownership corroboration.
///
/// **Without `network_manifest_url`**: probes every hospital with a
/// non-null `website_url` (`state`/`facility_ids` optionally narrow which
/// hospitals — pass `facility_ids: ["050454"]` to discover just one) on
/// its own site for its own `cms-hpt.txt`, up to `PROBE_CONCURRENCY`
/// concurrently. Every probe outcome is recorded as its own
/// `mrf_discoveries` row, whether or not an MRF was actually found —
/// `website_unreachable`/`no_manifest` are legitimate, common outcomes,
/// not failures of the job.
///
/// Either mode captures an `mrf_metadata` baseline for every MRF URL it
/// confirms. Runs in the background; poll `GET /api/pipeline/jobs/{id}`
/// for progress.
#[utoipa::path(
    post,
    path = "/api/pipeline/discover",
    request_body = DiscoverRequest,
    responses(
        (status = 202, description = "Discover job accepted", body = Job),
        (status = 400, description = "Malformed JSON body", body = ErrorResponse),
    ),
    tag = "pipeline",
)]
pub async fn trigger_discover(State(state): State<AppState>, body: Bytes) -> Result<(StatusCode, Json<Job>), ApiError> {
    let request: DiscoverRequest = if body.is_empty() {
        DiscoverRequest::default()
    } else {
        serde_json::from_slice(&body).map_err(|e| ApiError::BadRequest(format!("invalid JSON body: {e}")))?
    };

    let job = state.jobs.create("discover");
    let job_id = job.id.clone();

    let pool = state.pool.clone();
    let jobs = state.jobs.clone();
    let config = state.config.clone();

    tokio::spawn(async move {
        jobs.update(&job_id, |j| j.status = JobStatus::Running);

        let http = match reqwest::Client::builder().user_agent(USER_AGENT).build() {
            Ok(c) => c,
            Err(e) => {
                fail_job(&jobs, &job_id, format!("failed to build HTTP client: {e}"));
                return;
            }
        };
        let prober = Prober::new(http, config.probe_rate_limit_per_sec);

        match request.network_manifest_url {
            Some(manifest_url) => {
                run_network_manifest_discover(
                    &pool,
                    &jobs,
                    &job_id,
                    &prober,
                    &manifest_url,
                    request.state.as_deref(),
                    request.facility_ids.as_deref(),
                )
                .await;
            }
            None => {
                run_per_hospital_discover(
                    &pool,
                    &jobs,
                    &job_id,
                    &prober,
                    config.probe_concurrency,
                    request.state.as_deref(),
                    request.facility_ids.as_deref(),
                )
                .await;
            }
        }
    });

    Ok((StatusCode::ACCEPTED, Json(job)))
}

/// Network-manifest mode: fetch+parse one `cms-hpt.txt` covering a whole
/// network, resolve its entries against hospitals, cross-validate against
/// the ownership graph, and tag every confirmed match. See
/// `trigger_discover`'s doc comment.
#[allow(clippy::too_many_arguments)]
async fn run_network_manifest_discover(
    pool: &SqlitePool,
    jobs: &crate::jobs::JobTracker,
    job_id: &str,
    prober: &Prober,
    manifest_url: &str,
    state_filter: Option<&str>,
    facility_ids: Option<&[String]>,
) {
    let manifest = match prober.fetch_manifest(manifest_url).await {
        Ok(m) => m,
        Err(e) => {
            fail_job(jobs, job_id, format!("failed to fetch {manifest_url}: {e}"));
            return;
        }
    };
    if !manifest.found {
        fail_job(jobs, job_id, format!("{manifest_url} did not return a manifest (HTTP {:?})", manifest.status));
        return;
    }
    if manifest.entries.is_empty() {
        // Fetched fine, just nothing usable in it — not an error.
        jobs.update(job_id, |j| {
            j.status = JobStatus::Completed;
            j.finished_at = Some(chrono::Utc::now());
        });
        return;
    }

    jobs.update(job_id, |j| j.progress.total = manifest.entries.len() as u64);

    let candidate_rows = match crate::db::queries::list_match_candidates(pool, state_filter, facility_ids).await {
        Ok(rows) => rows,
        Err(e) => {
            fail_job(jobs, job_id, e.to_string());
            return;
        }
    };
    let candidates: Vec<MatchCandidate> = candidate_rows
        .into_iter()
        .map(|(facility_id, facility_name)| MatchCandidate { facility_id, facility_name })
        .collect();

    let resolved = mrf_match::resolve_entries(&manifest.entries, &candidates, mrf_match::DEFAULT_MATCH_THRESHOLD);

    if resolved.is_empty() {
        jobs.update(job_id, |j| {
            j.progress.failed = manifest.entries.len() as u64;
            j.status = JobStatus::Completed;
            j.finished_at = Some(chrono::Utc::now());
        });
        tracing::info!(manifest_url, "no manifest entries matched any candidate hospital by name");
        return;
    }

    // Union the ownership-reachable set across *every* name match, not
    // just the highest-scoring one. A real-world system can have more
    // than one PECOS owner-of-record entity for the same network —
    // confirmed live 2026-09-15 against Sentara: most of its hospitals
    // list "SENTARA HEALTH" (5%+ direct ownership) as their controlling
    // owner, but at least one lists "SENTARA HOSPITALS"
    // (operational/managerial control) instead. Anchoring on a single
    // hospital's reachable set alone would silently drop every hospital
    // connected only through the other owner. Taking the union costs
    // nothing in safety — each entry in `resolved` already independently
    // cleared the name-similarity bar to seed its own corroboration, so
    // this only ever *adds* legitimate reachable hospitals, never trusts
    // an unvetted one.
    let mut reachable: HashSet<String> = HashSet::new();
    for m in &resolved {
        match crate::db::queries::ownership_reachable_ccns(pool, &m.facility_id).await {
            Ok(set) => reachable.extend(set),
            Err(e) => {
                fail_job(jobs, job_id, e.to_string());
                return;
            }
        }
    }

    let mut completed = 0u64;
    let mut failed = (manifest.entries.len() - resolved.len()) as u64; // entries with no name match at all

    for m in &resolved {
        if !reachable.contains(&m.facility_id) {
            failed += 1;
            jobs.update(job_id, |j| j.progress.failed = failed);
            tracing::info!(
                facility_id = %m.facility_id,
                facility_name = %m.facility_name,
                score = m.score,
                "name-matched but not confirmed by the ownership graph — skipped"
            );
            continue;
        }

        let entry = &manifest.entries[m.entry_index];
        let reach_check = prober.check_mrf_reachable(&entry.mrf_url).await;
        let status = if reach_check.reachable { DiscoveryStatus::MrfFound } else { DiscoveryStatus::ManifestOnly };

        match crate::db::queries::insert_mrf_discovery(
            pool,
            &m.facility_id,
            &manifest.manifest_url,
            &entry.mrf_url,
            status.as_str(),
            entry.contact_name.as_deref(),
            entry.contact_email.as_deref(),
        )
        .await
        {
            Ok(discovery_id) => {
                completed += 1;
                // Initial conditional-caching capture for this MRF URL
                // (ETag/Last-Modified/Cache-Control/Content-Length/
                // Content-Type) — always a "baseline" since this
                // discovery row is brand new, so there's nothing to
                // compare against yet. A later recheck
                // (POST /api/mrf-discoveries/{id}/metadata/recheck) is
                // what actually detects a change. Only reachable MRFs are
                // worth probing headers for.
                if reach_check.reachable {
                    let outcome = crate::mrf_metadata::check_mrf_metadata(prober, &entry.mrf_url, None).await;
                    if let Err(e) = crate::db::queries::insert_mrf_metadata(
                        pool,
                        &discovery_id,
                        &entry.mrf_url,
                        outcome.sha1_hash.as_deref(),
                        outcome.headers.last_modified.as_deref(),
                        outcome.headers.etag.as_deref(),
                        outcome.headers.cache_control.as_deref(),
                        outcome.headers.content_length,
                        outcome.headers.content_type.as_deref(),
                        outcome.change_detection_method,
                        outcome.changed_from_previous,
                    )
                    .await
                    {
                        tracing::warn!(facility_id = %m.facility_id, discovery_id, error = %e, "failed to save mrf_metadata baseline row");
                    }
                }
            }
            Err(e) => {
                failed += 1;
                tracing::warn!(facility_id = %m.facility_id, error = %e, "failed to save mrf_discoveries row");
            }
        }
        jobs.update(job_id, |j| {
            j.progress.completed = completed;
            j.progress.failed = failed;
        });
    }

    jobs.update(job_id, |j| {
        j.status = JobStatus::Completed;
        j.finished_at = Some(chrono::Utc::now());
    });
}

/// Per-hospital-website mode: every hospital with a non-null
/// `website_url` (narrowed by `state_filter`/`facility_ids`) is probed on
/// its own site for its own `cms-hpt.txt`, up to `concurrency` probes at
/// once. Unlike the network-manifest mode, every probed hospital gets a
/// `mrf_discoveries` row regardless of outcome — `website_unreachable`
/// and `no_manifest` are recorded results, not skipped or treated as job
/// failures. `completed`/`failed` here track whether the *probe itself*
/// could be run and recorded, not whether it found an MRF.
async fn run_per_hospital_discover(
    pool: &SqlitePool,
    jobs: &crate::jobs::JobTracker,
    job_id: &str,
    prober: &Prober,
    concurrency: usize,
    state_filter: Option<&str>,
    facility_ids: Option<&[String]>,
) {
    let candidates = match crate::db::queries::list_discoverable_hospitals(pool, state_filter, facility_ids).await {
        Ok(rows) => rows,
        Err(e) => {
            fail_job(jobs, job_id, e.to_string());
            return;
        }
    };

    if candidates.is_empty() {
        jobs.update(job_id, |j| {
            j.status = JobStatus::Completed;
            j.finished_at = Some(chrono::Utc::now());
        });
        return;
    }

    jobs.update(job_id, |j| j.progress.total = candidates.len() as u64);

    let mut probes = stream::iter(candidates)
        .map(|(facility_id, website_url)| async move {
            let result = prober.probe(&facility_id, Some(&website_url)).await;
            (facility_id, result)
        })
        .buffer_unordered(concurrency.max(1));

    let mut completed = 0u64;
    let mut failed = 0u64;

    while let Some((facility_id, result)) = probes.next().await {
        let discovery = match result {
            Ok(d) => d,
            Err(e) => {
                failed += 1;
                jobs.update(job_id, |j| j.progress.failed = failed);
                tracing::warn!(facility_id = %facility_id, error = %e, "probe failed (transport-level)");
                continue;
            }
        };

        let mrf_urls: Vec<String> = discovery.mrf_urls.iter().map(|e| e.mrf_url.clone()).collect();
        // A hospital's own cms-hpt.txt normally has exactly one entry
        // (itself); on the rare manifest with more than one, the first
        // entry's contact wins — see insert_discovery_result's doc comment.
        let contact_name = discovery.mrf_urls.first().and_then(|e| e.contact_name.as_deref());
        let contact_email = discovery.mrf_urls.first().and_then(|e| e.contact_email.as_deref());

        let insert_result = crate::db::queries::insert_discovery_result(
            pool,
            &discovery.facility_id,
            discovery.website_url.as_deref(),
            discovery.website_reachable,
            discovery.cms_hpt_txt_found,
            discovery.cms_hpt_txt_url.as_deref(),
            &mrf_urls,
            discovery.status.as_str(),
            contact_name,
            contact_email,
        )
        .await;

        match insert_result {
            Ok(discovery_id) => {
                completed += 1;
                // Baseline mrf_metadata capture for every MRF URL this
                // hospital's own manifest listed — see the equivalent
                // step in `run_network_manifest_discover`. There's
                // usually at most one for a single hospital's own
                // cms-hpt.txt, so probing each again here (headers only,
                // not a full download) is cheap.
                for mrf_url in &mrf_urls {
                    let outcome = crate::mrf_metadata::check_mrf_metadata(prober, mrf_url, None).await;
                    if let Err(e) = crate::db::queries::insert_mrf_metadata(
                        pool,
                        &discovery_id,
                        mrf_url,
                        outcome.sha1_hash.as_deref(),
                        outcome.headers.last_modified.as_deref(),
                        outcome.headers.etag.as_deref(),
                        outcome.headers.cache_control.as_deref(),
                        outcome.headers.content_length,
                        outcome.headers.content_type.as_deref(),
                        outcome.change_detection_method,
                        outcome.changed_from_previous,
                    )
                    .await
                    {
                        tracing::warn!(facility_id = %facility_id, discovery_id, error = %e, "failed to save mrf_metadata baseline row");
                    }
                }
            }
            Err(e) => {
                failed += 1;
                tracing::warn!(facility_id = %facility_id, error = %e, "failed to save mrf_discoveries row");
            }
        }

        jobs.update(job_id, |j| {
            j.progress.completed = completed;
            j.progress.failed = failed;
        });
    }

    jobs.update(job_id, |j| {
        j.status = JobStatus::Completed;
        j.finished_at = Some(chrono::Utc::now());
    });
}

/// Trigger ownership-graph ingest
///
/// Fetches CMS's "Hospital Enrollments" and "Hospital All Owners"
/// datasets and replaces `hospital_enrollments` / `hospital_ownership_edges`
/// outright (see `cms_ingest::ownership` and
/// `db::queries::replace_hospital_enrollments`/`replace_ownership_edges`
/// for why a full replace, not an upsert). Run this before
/// `POST /api/pipeline/discover`'s `network_manifest_url` mode — it's
/// what lets that endpoint cross-validate a name match against CMS's own
/// ownership disclosures instead of trusting name similarity alone. Runs
/// in the background (the "Hospital All Owners" dataset alone is
/// ~145K rows, paginated 5,000 at a time); poll
/// `GET /api/pipeline/jobs/{id}` for progress.
#[utoipa::path(
    post,
    path = "/api/pipeline/ingest-ownership",
    responses(
        (status = 202, description = "Ownership-graph ingest job accepted", body = Job),
    ),
    tag = "pipeline",
)]
pub async fn trigger_ingest_ownership(State(state): State<AppState>) -> Result<(StatusCode, Json<Job>), ApiError> {
    let job = state.jobs.create("ingest-ownership");
    let job_id = job.id.clone();

    let pool = state.pool.clone();
    let jobs = state.jobs.clone();

    tokio::spawn(async move {
        jobs.update(&job_id, |j| j.status = JobStatus::Running);

        let client = match cms_ingest::OwnershipClient::with_defaults() {
            Ok(c) => c,
            Err(e) => {
                fail_job(&jobs, &job_id, e.to_string());
                return;
            }
        };

        let mut enrollments = Vec::new();
        let enroll_result = client
            .fetch_all_enrollments(|rows| {
                enrollments.extend(rows.iter().filter_map(cms_ingest::ownership::parse_enrollment));
            })
            .await;
        if let Err(e) = enroll_result {
            fail_job(&jobs, &job_id, format!("failed to fetch Hospital Enrollments: {e}"));
            return;
        }

        let mut edges = Vec::new();
        let edges_result = client
            .fetch_all_ownership_edges(|rows| {
                edges.extend(rows.iter().filter_map(cms_ingest::ownership::parse_ownership_edge));
            })
            .await;
        if let Err(e) = edges_result {
            fail_job(&jobs, &job_id, format!("failed to fetch Hospital All Owners: {e}"));
            return;
        }

        jobs.update(&job_id, |j| j.progress.total = (enrollments.len() + edges.len()) as u64);

        if let Err(e) = crate::db::queries::replace_hospital_enrollments(&pool, &enrollments).await {
            fail_job(&jobs, &job_id, format!("failed to save hospital_enrollments: {e}"));
            return;
        }
        jobs.update(&job_id, |j| j.progress.completed = enrollments.len() as u64);

        if let Err(e) = crate::db::queries::replace_ownership_edges(&pool, &edges).await {
            fail_job(&jobs, &job_id, format!("failed to save hospital_ownership_edges: {e}"));
            return;
        }

        jobs.update(&job_id, |j| {
            j.progress.completed = (enrollments.len() + edges.len()) as u64;
            j.status = JobStatus::Completed;
            j.finished_at = Some(chrono::Utc::now());
        });
    });

    Ok((StatusCode::ACCEPTED, Json(job)))
}

/// Get job status
///
/// Poll this with the job id returned by any `POST /api/pipeline/*`
/// trigger endpoint to track progress.
#[utoipa::path(
    get,
    path = "/api/pipeline/jobs/{id}",
    params(
        ("id" = String, Path, description = "Job id returned by a trigger endpoint"),
    ),
    responses(
        (status = 200, description = "The job's current state", body = Job),
        (status = 404, description = "No job with that id (never existed, or the process restarted — jobs aren't persisted)", body = ErrorResponse),
    ),
    tag = "pipeline",
)]
pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Job>, ApiError> {
    state.jobs.get(&id).map(Json).ok_or(ApiError::NotFound)
}
