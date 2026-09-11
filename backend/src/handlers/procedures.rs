use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use common::{
    model::{ApiErrorResponse, PaginatedResponse, ProcedureSearchParams, StandardCharge},
    repository::HealthcareRepository,
    state::AppState,
};

#[utoipa::path(
    get,
    path = "/api/v1/procedures",
    params(ProcedureSearchParams),
    responses(
        (status = 200, description = "List of procedure charges matching filters", body = PaginatedResponse<StandardCharge>),
        (status = 500, description = "Database error", body = ApiErrorResponse)
    ),
    tag = "Procedures"
)]
pub async fn search_procedures(
    State(state): State<AppState>,
    Query(params): Query<ProcedureSearchParams>,
) -> Result<Json<PaginatedResponse<StandardCharge>>, (StatusCode, Json<ApiErrorResponse>)> {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("Pool error: {e}"))?;
        HealthcareRepository::search_procedures(&conn, &params).map_err(|e| format!("{e}"))
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
