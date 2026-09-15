//! `GET /api/mrf-discoveries/{id}/metadata` and
//! `POST /api/mrf-discoveries/{id}/metadata/recheck` — conditional-caching
//! change history for one `mrf_discoveries` row (see
//! `crate::mrf_metadata` and Architecture.md's `mrf_metadata` section).
//!
//! The `discover` job (`routes::pipeline::trigger_discover`) already
//! writes the first ("baseline") row for every MRF it finds. This module
//! is what lets a caller later ask "has this file changed since?" —
//! `recheck` re-probes the URL, compares against the latest stored row,
//! and (only when `ETag`/`Last-Modified` can't answer that) downloads the
//! full file and hashes it. Runs as a background job like every other
//! `POST /api/pipeline/*` trigger, since that download can be large.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::Json;

use crate::db::models::MrfMetadata;
use crate::error::{ApiError, ErrorResponse};
use crate::jobs::{Job, JobStatus};
use crate::routes::pipeline::USER_AGENT;
use crate::routes::AppState;

/// Pulls the one MRF URL a discovery row tracks out of its
/// `mrf_urls` JSON array (see `db::queries::insert_mrf_discovery` — always
/// a single-element array in practice, since each discovery row is
/// created per-entry).
fn discovery_mrf_url(discovery: &crate::db::models::MrfDiscovery) -> Option<String> {
    let raw = discovery.mrf_urls.as_deref()?;
    let urls: Vec<String> = serde_json::from_str(raw).ok()?;
    urls.into_iter().next()
}

/// MRF metadata history
///
/// Every conditional-caching probe recorded for one `mrf_discoveries` row,
/// most recent first. The first (oldest) entry is always the `"baseline"`
/// capture from when the discovery was made; later entries are
/// `POST .../recheck` runs, each carrying whether that check found the
/// file changed since the previous one.
#[utoipa::path(
    get,
    path = "/api/mrf-discoveries/{id}/metadata",
    params(
        ("id" = String, Path, description = "mrf_discoveries row id"),
    ),
    responses(
        (status = 200, description = "Metadata history, most recent first", body = [MrfMetadata]),
        (status = 404, description = "No mrf_discoveries row with that id", body = ErrorResponse),
    ),
    tag = "mrf-metadata",
)]
pub async fn list_metadata(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Vec<MrfMetadata>>, ApiError> {
    crate::db::queries::get_mrf_discovery(&state.pool, &id)
        .await?
        .ok_or(ApiError::NotFound)?;

    let history = crate::db::queries::list_mrf_metadata(&state.pool, &id).await?;
    Ok(Json(history))
}

/// Recheck MRF metadata
///
/// Re-probes the discovery's MRF URL and compares the result against the
/// latest stored `mrf_metadata` row: if `ETag` or `Last-Modified` is
/// present on both sides, that alone decides whether the file changed —
/// no download. Otherwise the full file is downloaded and hashed (SHA-1)
/// to compare against the previous hash. Runs in the background; poll
/// `GET /api/pipeline/jobs/{id}` with the returned job id.
#[utoipa::path(
    post,
    path = "/api/mrf-discoveries/{id}/metadata/recheck",
    params(
        ("id" = String, Path, description = "mrf_discoveries row id"),
    ),
    responses(
        (status = 202, description = "Recheck job accepted", body = Job),
        (status = 404, description = "No mrf_discoveries row with that id", body = ErrorResponse),
    ),
    tag = "mrf-metadata",
)]
pub async fn recheck_metadata(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Job>), ApiError> {
    let discovery = crate::db::queries::get_mrf_discovery(&state.pool, &id)
        .await?
        .ok_or(ApiError::NotFound)?;
    let mrf_url = discovery_mrf_url(&discovery).ok_or_else(|| {
        ApiError::BadRequest(format!("mrf_discoveries row {id} has no mrf_url to recheck"))
    })?;

    let job = state.jobs.create("mrf-metadata-recheck");
    let job_id = job.id.clone();

    let pool = state.pool.clone();
    let jobs = state.jobs.clone();
    let config = state.config.clone();

    tokio::spawn(async move {
        jobs.update(&job_id, |j| j.status = JobStatus::Running);

        let http = match reqwest::Client::builder().user_agent(USER_AGENT).build() {
            Ok(c) => c,
            Err(e) => {
                super::pipeline::fail_job(&jobs, &job_id, format!("failed to build HTTP client: {e}"));
                return;
            }
        };
        let prober = compliance_probe::probe::Prober::new(http, config.probe_rate_limit_per_sec);

        let previous = match crate::db::queries::latest_mrf_metadata(&pool, &id).await {
            Ok(row) => row,
            Err(e) => {
                super::pipeline::fail_job(&jobs, &job_id, e.to_string());
                return;
            }
        };

        let outcome = crate::mrf_metadata::check_mrf_metadata(&prober, &mrf_url, previous.as_ref()).await;

        let saved = crate::db::queries::insert_mrf_metadata(
            &pool,
            &id,
            &mrf_url,
            outcome.sha1_hash.as_deref(),
            outcome.headers.last_modified.as_deref(),
            outcome.headers.etag.as_deref(),
            outcome.headers.cache_control.as_deref(),
            outcome.headers.content_length,
            outcome.headers.content_type.as_deref(),
            outcome.change_detection_method,
            outcome.changed_from_previous,
        )
        .await;

        match saved {
            Ok(_) => {
                jobs.update(&job_id, |j| {
                    j.progress.total = 1;
                    j.progress.completed = 1;
                    j.status = JobStatus::Completed;
                    j.finished_at = Some(chrono::Utc::now());
                });
                tracing::info!(
                    discovery_id = %id,
                    mrf_url,
                    changed = ?outcome.changed_from_previous,
                    method = ?outcome.change_detection_method,
                    "mrf metadata recheck complete"
                );
            }
            Err(e) => super::pipeline::fail_job(&jobs, &job_id, e.to_string()),
        }
    });

    Ok((StatusCode::ACCEPTED, Json(job)))
}
