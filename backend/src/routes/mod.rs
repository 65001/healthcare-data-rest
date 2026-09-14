pub mod hospitals;
pub mod pipeline;
pub mod stats;

use std::sync::Arc;

use axum::routing::{get, post};
use axum::Router;
use sqlx::SqlitePool;

use crate::config::Config;
use crate::jobs::JobTracker;

#[derive(Clone)]
pub struct AppState {
    pub pool: SqlitePool,
    pub config: Arc<Config>,
    pub jobs: JobTracker,
}

pub fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/api/hospitals", get(hospitals::list))
        .route("/api/hospitals/:facility_id", get(hospitals::get_one))
        .route("/api/stats", get(stats::get_stats))
        .route("/api/pipeline/ingest", post(pipeline::trigger_ingest))
        .route("/api/pipeline/enrich", post(pipeline::trigger_enrich))
        .route("/api/pipeline/discover", post(pipeline::trigger_discover))
        .route("/api/pipeline/jobs/:id", get(pipeline::get_job))
        .with_state(state)
}
