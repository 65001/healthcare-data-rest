use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use common::{
    model::{ApiErrorResponse, HospitalDetail, HospitalSearchParams, HospitalSummary, PaginatedResponse},
    repository::HealthcareRepository,
    state::AppState,
};

#[utoipa::path(
    get,
    path = "/api/v1/hospitals",
    params(HospitalSearchParams),
    responses(
        (status = 200, description = "List of hospitals matching query filters", body = PaginatedResponse<HospitalSummary>),
        (status = 500, description = "Database error", body = ApiErrorResponse)
    ),
    tag = "Hospitals"
)]
pub async fn search_hospitals(
    State(state): State<AppState>,
    Query(params): Query<HospitalSearchParams>,
) -> Result<Json<PaginatedResponse<HospitalSummary>>, (StatusCode, Json<ApiErrorResponse>)> {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("Pool error: {e}"))?;
        HealthcareRepository::search_hospitals(&conn, &params).map_err(|e| format!("{e}"))
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

#[utoipa::path(
    get,
    path = "/api/v1/hospitals/{id}",
    params(
        ("id" = i64, Path, description = "Unique Hospital Identifier")
    ),
    responses(
        (status = 200, description = "Hospital details retrieved successfully", body = HospitalDetail),
        (status = 404, description = "Hospital not found", body = ApiErrorResponse),
        (status = 500, description = "Database error", body = ApiErrorResponse)
    ),
    tag = "Hospitals"
)]
pub async fn get_hospital(
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<HospitalDetail>, (StatusCode, Json<ApiErrorResponse>)> {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| format!("Pool error: {e}"))?;
        HealthcareRepository::get_hospital_by_id(&conn, id).map_err(|e| format!("{e}"))
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
        Ok(Some(hospital)) => Ok(Json(hospital)),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiErrorResponse {
                error: "NotFound".to_string(),
                message: format!("Hospital with ID {id} not found"),
            }),
        )),
        Err(err_msg) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiErrorResponse {
                error: "DatabaseError".to_string(),
                message: err_msg,
            }),
        )),
    }
}
