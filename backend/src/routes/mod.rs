use axum::{
    routing::get,
    Router,
};
use common::state::AppState;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use crate::{handlers, openapi::ApiDoc};

pub fn create_router(state: AppState) -> Router {
    let api_routes = Router::new()
        .route("/hospitals", get(handlers::hospitals::search_hospitals))
        .route("/hospitals/{id}", get(handlers::hospitals::get_hospital))
        .route("/procedures", get(handlers::procedures::search_procedures))
        .route("/prices/compare", get(handlers::prices::compare_prices))
        .route("/stats", get(handlers::stats::get_stats))
        .with_state(state);

    Router::new()
        .nest("/api/v1", api_routes)
        .merge(SwaggerUi::new("/swagger-ui").url("/api/v1/openapi.json", ApiDoc::openapi()))
}
