//! `POST /api/pipeline/{ingest,enrich,discover}` and
//! `GET /api/pipeline/jobs/:id`, per Architecture.md's REST API section.
//!
//! `ingest` and `enrich` are wired end-to-end now; `discover` still
//! returns `501 Not Implemented`, since `compliance-probe`'s probing
//! sequence and `cms-hpt.txt` parser remain scaffolds (see
//! IMPLEMENTATION_NOTES.md).
//!
//! `enrich`'s cascade is `[nominatim]` only in this pass — Census and
//! Google Maps geocoding are still stubs, so they're deliberately left
//! out rather than added and immediately erroring (or silently
//! no-op'ing) on every hospital. Website discovery runs Nominatim's
//! opportunistic `extratags.website` first and falls back to Google
//! Places only when that's empty — both to save the Places API's
//! per-call cost and because that's the priority order the project owner
//! chose. See `geo-enrich/src/enricher.rs`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

use geo_enrich::enricher::EnrichmentConfig;
use geo_enrich::providers::{GooglePlacesClient, NominatimProvider};
use geo_enrich::{CascadingGeocoder, GeocodingProvider};

use crate::enrich_store::HospitalStore;
use crate::error::ApiError;
use crate::jobs::{Job, JobStatus};
use crate::routes::AppState;

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

pub async fn trigger_enrich(State(state): State<AppState>) -> Result<(StatusCode, Json<Job>), ApiError> {
    let job = state.jobs.create("enrich");
    let job_id = job.id.clone();

    let pool = state.pool.clone();
    let jobs = state.jobs.clone();
    let config = state.config.clone();

    tokio::spawn(async move {
        jobs.update(&job_id, |j| j.status = JobStatus::Running);

        // Shared by both Nominatim (usage-policy requirement) and Places.
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

        let mut providers: Vec<Box<dyn GeocodingProvider>> = Vec::new();
        if config.geocoding_nominatim_enabled {
            providers.push(Box::new(NominatimProvider::new(http.clone())));
        }
        // Census and Google Maps geocoding are still stubs (see
        // IMPLEMENTATION_NOTES.md) — deliberately not added here even
        // though `geocoding_census_enabled` / `geocoding_google_maps_enabled`
        // exist as config flags. Add them once their providers are real;
        // `GeoEnrichError::NotImplemented` is treated as transient by the
        // cascade already, so adding a still-stubbed one back wouldn't
        // break anything, just waste a cascade step on every hospital.

        if providers.is_empty() {
            fail_job(
                &jobs,
                &job_id,
                "no geocoding providers enabled — set GEOCODING_NOMINATIM_ENABLED=true (Census and \
                 Google Maps geocoding aren't implemented yet)"
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

pub async fn trigger_discover() -> ApiError {
    ApiError::NotImplemented {
        stage: "discover",
        detail: "compliance-probe's rate limiter is implemented but the \
                 website-probing sequence and cms-hpt.txt parser are stubs \
                 — see compliance-probe/src/ and IMPLEMENTATION_NOTES.md."
            .to_string(),
    }
}

pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Job>, ApiError> {
    state.jobs.get(&id).map(Json).ok_or(ApiError::NotFound)
}
