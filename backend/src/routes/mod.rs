pub mod hospitals;
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
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/hospitals", get(hospitals::list))
        .route("/api/hospitals/needs-enrichment", get(hospitals::needs_enrichment))
        .route("/api/hospitals/:facility_id", get(hospitals::get_one))
        .route(
            "/api/hospitals/:facility_id/enrichment",
            patch(hospitals::patch_enrichment),
        )
        .route("/api/stats", get(stats::get_stats))
        .route("/api/pipeline/ingest", post(pipeline::trigger_ingest))
        .route("/api/pipeline/enrich", post(pipeline::trigger_enrich))
        .route("/api/pipeline/discover", post(pipeline::trigger_discover))
        .route("/api/pipeline/jobs/:id", get(pipeline::get_job))
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .with_state(state)
}
