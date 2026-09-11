use axum::{extract::State, http::StatusCode, Json};
use common::{
    model::{ApiErrorResponse, DatasetStats},
    repository::HealthcareRepository,
    state::AppState,
};

#[utoipa::path(
    get,
    path = "/api/v1/stats",
    responses(
        (status = 200, description = "Dataset overview and connection metrics", body = DatasetStats),
        (status = 500, description = "Database error", body = ApiErrorResponse)
    ),
    tag = "Stats"
)]
pub async fn get_stats(
    State(state): State<AppState>,
) -> Result<Json<DatasetStats>, (StatusCode, Json<ApiErrorResponse>)> {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("Pool error: {e}"))?;
        HealthcareRepository::get_dataset_stats(&conn).map_err(|e| format!("{e}"))
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
