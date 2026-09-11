use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use common::{
    model::{ApiErrorResponse, PriceComparisonItem, PriceComparisonParams},
    repository::HealthcareRepository,
    state::AppState,
};

#[utoipa::path(
    get,
    path = "/api/v1/prices/compare",
    params(PriceComparisonParams),
    responses(
        (status = 200, description = "Comparative procedure prices across facilities and insurance plans", body = Vec<PriceComparisonItem>),
        (status = 500, description = "Database error", body = ApiErrorResponse)
    ),
    tag = "Pricing"
)]
pub async fn compare_prices(
    State(state): State<AppState>,
    Query(params): Query<PriceComparisonParams>,
) -> Result<Json<Vec<PriceComparisonItem>>, (StatusCode, Json<ApiErrorResponse>)> {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("Pool error: {e}"))?;
        HealthcareRepository::compare_procedure_prices(&conn, &params).map_err(|e| format!("{e}"))
    })
    .await
    .map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "TaskJoinError".to_string(),
                message: format!("Blocking task failed: {e}"),
            }),
        )
    })?;

    match result {
        Ok(data) => Ok(Json(data)),
        Err(err_msg) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "DatabaseError".to_string(),
                message: err_msg,
            }),
        )),
    }
}
