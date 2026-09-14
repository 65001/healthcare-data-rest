//! `POST /api/pipeline/{ingest,enrich,discover}` and
//! `GET /api/pipeline/jobs/:id`, per Architecture.md's REST API section.
//!
//! `ingest` and `enrich` are wired end-to-end now; `discover` still
//! returns `501 Not Implemented`, since `compliance-probe`'s probing
//! sequence and `cms-hpt.txt` parser remain scaffolds (see
//! IMPLEMENTATION_NOTES.md).
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

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::Json;
use serde::Deserialize;
use utoipa::IntoParams;

use geo_enrich::enricher::EnrichmentConfig;
use geo_enrich::providers::{CensusProvider, GoogleMapsProvider, GooglePlacesClient, NominatimProvider};
use geo_enrich::{CascadingGeocoder, GeocodingProvider};

use crate::enrich_store::HospitalStore;
use crate::error::{ApiError, ErrorResponse};
use crate::jobs::{Job, JobStatus};
use crate::routes::AppState;

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

fn fail_job(jobs: &crate::jobs::JobTracker, job_id: &str, error: String) {
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
        let http = match reqwest::Client::builder()
            .user_agent("cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)")
            .build()
        {
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

/// Trigger MRF discovery
///
/// Probes each enriched hospital's website for a `cms-hpt.txt` manifest
/// and its listed MRF URLs. **Not implemented yet** — always returns
/// `501`; see `compliance-probe/src/` and `IMPLEMENTATION_NOTES.md`.
#[utoipa::path(
    post,
    path = "/api/pipeline/discover",
    responses(
        (status = 501, description = "Not implemented yet", body = ErrorResponse),
    ),
    tag = "pipeline",
)]
pub async fn trigger_discover() -> ApiError {
    ApiError::NotImplemented {
        stage: "discover",
        detail: "compliance-probe's rate limiter is implemented but the \
                 website-probing sequence and cms-hpt.txt parser are stubs \
                 — see compliance-probe/src/ and IMPLEMENTATION_NOTES.md."
            .to_string(),
    }
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
