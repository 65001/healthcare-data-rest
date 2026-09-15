pub mod hospitals;
pub mod mrf_metadata;
pub mod pipeline;
pub mod stats;

use std::sync::Arc;

use axum::routing::{get, patch, post};
use axum::Router;
use sqlx::SqlitePool;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::config::Config;
use crate::jobs::JobTracker;
use crate::openapi::ApiDoc;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<Config>,
    pub jobs: JobTracker,
    /// Shared client for `url_check`'s manual-`website_url` verification
    /// (see `hospitals::patch_enrichment`) — short timeouts, reused
    /// across requests rather than built fresh each time.
    pub http: reqwest::Client,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/hospitals", get(hospitals::list))
        .route("/api/hospitals/needs-enrichment", get(hospitals::needs_enrichment))
        .route("/api/hospitals/:facility_id", get(hospitals::get_one))
        .route("/api/hospitals/:facility_id/ownership", get(hospitals::get_ownership))
        .route(
            "/api/hospitals/:facility_id/enrichment",
            patch(hospitals::patch_enrichment),
        )
        .route("/api/stats", get(stats::get_stats))
        .route("/api/pipeline/ingest", post(pipeline::trigger_ingest))
        .route("/api/pipeline/enrich", post(pipeline::trigger_enrich))
        .route("/api/pipeline/discover", post(pipeline::trigger_discover))
        .route("/api/pipeline/ingest-ownership", post(pipeline::trigger_ingest_ownership))
        .route("/api/pipeline/jobs/:id", get(pipeline::get_job))
        .route("/api/mrf-discoveries/:id/metadata", get(mrf_metadata::list_metadata))
        .route(
            "/api/mrf-discoveries/:id/metadata/recheck",
            post(mrf_metadata::recheck_metadata),
        )
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}
