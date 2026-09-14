//! Backend-wide API error type. Every route handler returns
//! `Result<_, ApiError>`; this maps errors to HTTP status codes per
//! Architecture.md's "Error Handling Strategy" note ("The `backend` crate
//! maps all errors to appropriate HTTP status codes via an `IntoResponse`
//! implementation").

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;
use serde_json::json;
use utoipa::ToSchema;

/// Documents the `{ "error": "..." }` body every non-2xx response returns.
/// Not the type actually constructed on the error path (that's still
/// `serde_json::json!` below) — this exists purely so `#[utoipa::path]`
/// responses have a concrete schema to point at.
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ApiError {
    #[error("not found")]
    NotFound,

    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("{0}")]
    BadRequest(String),

    #[error("{stage} is not implemented yet: {detail}")]
    NotImplemented { stage: &'static str, detail: String },

    #[error("CMS ingest failed: {0}")]
    Ingest(#[from] cms_ingest::IngestError),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiError::NotFound => StatusCode::NOT_FOUND,
            ApiError::Database(_) => StatusCode::INTERNAL_SERVER_ERROR,
            ApiError::BadRequest(_) => StatusCode::BAD_REQUEST,
            ApiError::NotImplemented { .. } => StatusCode::NOT_IMPLEMENTED,
            ApiError::Ingest(_) => StatusCode::BAD_GATEWAY,
        };
        let body = Json(json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}
