//! `POST /api/pipeline/{ingest,enrich,discover}` and
//! `GET /api/pipeline/jobs/:id`, per Architecture.md's REST API section.
//!
//! Only `ingest` actually runs anything — it's wired end-to-end to
//! `cms-ingest` and the `hospitals` table. `enrich` and `discover` return
//! `501 Not Implemented` with an explanation, since `geo-enrich` and
//! `compliance-probe` are scaffolds, not working implementations, in
//! this pass (see IMPLEMENTATION_NOTES.md).

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

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

pub async fn trigger_enrich() -> ApiError {
    ApiError::NotImplemented {
        stage: "enrich",
        detail: "geo-enrich's cascade orchestrator is implemented but its \
                 Census/Nominatim/Google Maps providers are stubs — see \
                 geo-enrich/src/providers/ and IMPLEMENTATION_NOTES.md."
            .to_string(),
    }
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
