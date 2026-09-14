//! `GET /api/hospitals`, `GET /api/hospitals/:facility_id`,
//! `GET /api/hospitals/needs-enrichment`, and
//! `PATCH /api/hospitals/:facility_id/enrichment`, per Architecture.md's
//! REST API section.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::db::models::{Hospital, HospitalListItem, MrfDiscovery, NeedsEnrichmentItem};
use crate::db::queries::{self, HospitalFilters};
use crate::error::{ApiError, ErrorResponse};
use crate::routes::AppState;

#[derive(Debug, Deserialize, IntoParams)]
pub struct ListParams {
    /// Two-letter state code, e.g. `CA`.
    pub state: Option<String>,
    pub hospital_type: Option<String>,
    pub discovery_status: Option<String>,
    /// Filter to enriched (`true`) or un-enriched (`false`) hospitals.
    pub enriched: Option<bool>,
    #[param(minimum = 1)]
    pub page: Option<u32>,
    #[param(minimum = 1, maximum = 200)]
    pub per_page: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Pagination {
    pub page: u32,
    pub per_page: u32,
    pub total_count: i64,
    pub total_pages: i64,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ListResponse {
    pub data: Vec<HospitalListItem>,
    pub pagination: Pagination,
}

/// List hospitals
///
/// Paginated, filterable list of hospitals with their latest MRF
/// discovery status joined in.
#[utoipa::path(
    get,
    path = "/api/hospitals",
    params(ListParams),
    responses(
        (status = 200, description = "Page of hospitals", body = ListResponse),
        (status = 400, description = "Malformed query parameter", body = ErrorResponse),
    ),
    tag = "hospitals",
)]
pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<ListResponse>, ApiError> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);

    let filters = HospitalFilters {
        state: params.state,
        hospital_type: params.hospital_type,
        discovery_status: params.discovery_status,
        enriched: params.enriched,
    };

    let (data, total_count) = queries::list_hospitals(&state.pool, &filters, page, per_page).await?;
    let total_pages = if total_count == 0 {
        0
    } else {
        (total_count + per_page as i64 - 1) / per_page as i64
    };

    Ok(Json(ListResponse {
        data,
        pagination: Pagination {
            page,
            per_page,
            total_count,
            total_pages,
        },
    }))
}

#[derive(Debug, Serialize, ToSchema)]
pub struct HospitalDetail {
    #[serde(flatten)]
    #[schema(inline)]
    pub hospital: Hospital,
    pub latest_discovery: Option<MrfDiscovery>,
}

/// Get one hospital
///
/// Full detail for a single hospital including enrichment and latest MRF
/// discovery.
#[utoipa::path(
    get,
    path = "/api/hospitals/{facility_id}",
    params(
        ("facility_id" = String, Path, description = "CMS Certification Number (primary key)"),
    ),
    responses(
        (status = 200, description = "The hospital", body = HospitalDetail),
        (status = 404, description = "No hospital with that facility_id", body = ErrorResponse),
    ),
    tag = "hospitals",
)]
pub async fn get_one(
    State(state): State<AppState>,
    Path(facility_id): Path<String>,
) -> Result<Json<HospitalDetail>, ApiError> {
    let (hospital, latest_discovery) = queries::get_hospital(&state.pool, &facility_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(Json(HospitalDetail {
        hospital,
        latest_discovery,
    }))
}

#[derive(Debug, Deserialize, IntoParams)]
pub struct NeedsEnrichmentParams {
    /// `"coordinates"` or `"website"`. Anything else is a `400`. Omitted
    /// means either (the default — see `queries::list_needs_enrichment`).
    #[param(pattern = "coordinates|website")]
    pub missing: Option<String>,
    /// Two-letter state code, e.g. `CA`.
    pub state: Option<String>,
    #[param(minimum = 1)]
    pub page: Option<u32>,
    #[param(minimum = 1, maximum = 200)]
    pub per_page: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct NeedsEnrichmentResponse {
    pub data: Vec<NeedsEnrichmentItem>,
    pub pagination: Pagination,
}

/// Manual-enrichment queue
///
/// Hospitals the automated pipeline has already run on
/// (`enriched_at IS NOT NULL`) but couldn't fully resolve — still missing
/// coordinates, a website, or both. A hospital the pipeline hasn't reached
/// at all belongs in `POST /api/pipeline/enrich` instead, not here.
#[utoipa::path(
    get,
    path = "/api/hospitals/needs-enrichment",
    params(NeedsEnrichmentParams),
    responses(
        (status = 200, description = "Page of incompletely-enriched hospitals", body = NeedsEnrichmentResponse),
        (status = 400, description = "Invalid `missing` value or other malformed query parameter", body = ErrorResponse),
    ),
    tag = "hospitals",
)]
pub async fn needs_enrichment(
    State(state): State<AppState>,
    Query(params): Query<NeedsEnrichmentParams>,
) -> Result<Json<NeedsEnrichmentResponse>, ApiError> {
    let missing = match params.missing.as_deref() {
        None => None,
        Some("coordinates") => Some("coordinates"),
        Some("website") => Some("website"),
        Some(other) => {
            return Err(ApiError::BadRequest(format!(
                "invalid `missing` value {other:?}: expected \"coordinates\" or \"website\""
            )))
        }
    };

    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);

    let (data, total_count) =
        queries::list_needs_enrichment(&state.pool, missing, params.state.as_deref(), page, per_page).await?;
    let total_pages = if total_count == 0 {
        0
    } else {
        (total_count + per_page as i64 - 1) / per_page as i64
    };

    Ok(Json(NeedsEnrichmentResponse {
        data,
        pagination: Pagination {
            page,
            per_page,
            total_count,
            total_pages,
        },
    }))
}

/// Body for `PATCH /api/hospitals/:facility_id/enrichment`. All fields
/// optional; at least one must be provided (validated in the handler).
#[derive(Debug, Deserialize, ToSchema)]
pub struct ManualEnrichmentRequest {
    #[schema(minimum = -90.0, maximum = 90.0, example = 31.451773)]
    pub latitude: Option<f64>,
    #[schema(minimum = -180.0, maximum = 180.0, example = -85.631010)]
    pub longitude: Option<f64>,
    #[schema(example = "https://example-hospital.org")]
    pub website_url: Option<String>,
}

/// Manual enrichment
///
/// Sets coordinates and/or a website by hand for one hospital — for
/// whatever `POST /api/pipeline/enrich` (every provider in the cascade,
/// including the `retry_incomplete` backfill pass) couldn't resolve.
/// Typically used against the queue `GET /api/hospitals/needs-enrichment`
/// surfaces. `latitude`/`longitude` must be provided together (not just
/// one) and in-range; `website_url` must start with `http://` or
/// `https://`. Whatever's provided **overwrites** the existing value
/// outright — a manual correction here is assumed intentional. A
/// successful update stamps `geo_provider = "manual"` and
/// `geo_confidence = 1.0` when coordinates are set, and sets
/// `enriched_at` if it wasn't already.
#[utoipa::path(
    patch,
    path = "/api/hospitals/{facility_id}/enrichment",
    params(
        ("facility_id" = String, Path, description = "CMS Certification Number (primary key)"),
    ),
    request_body = ManualEnrichmentRequest,
    responses(
        (status = 200, description = "The updated hospital row", body = Hospital),
        (status = 400, description = "Validation failure — see the `error` message", body = ErrorResponse),
        (status = 404, description = "No hospital with that facility_id", body = ErrorResponse),
    ),
    tag = "hospitals",
)]
pub async fn patch_enrichment(
    State(state): State<AppState>,
    Path(facility_id): Path<String>,
    Json(body): Json<ManualEnrichmentRequest>,
) -> Result<Json<Hospital>, ApiError> {
    if body.latitude.is_none() && body.longitude.is_none() && body.website_url.is_none() {
        return Err(ApiError::BadRequest(
            "at least one of latitude/longitude or website_url must be provided".to_string(),
        ));
    }

    match (body.latitude, body.longitude) {
        (Some(_), None) | (None, Some(_)) => {
            return Err(ApiError::BadRequest(
                "latitude and longitude must be provided together".to_string(),
            ));
        }
        (Some(lat), Some(lon)) => {
            if !(-90.0..=90.0).contains(&lat) {
                return Err(ApiError::BadRequest(format!("latitude {lat} out of range [-90, 90]")));
            }
            if !(-180.0..=180.0).contains(&lon) {
                return Err(ApiError::BadRequest(format!("longitude {lon} out of range [-180, 180]")));
            }
        }
        (None, None) => {}
    }

    if let Some(url) = &body.website_url {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(ApiError::BadRequest(
                "website_url must start with http:// or https://".to_string(),
            ));
        }
    }

    let rows_affected = queries::manual_enrich_hospital(
        &state.pool,
        &facility_id,
        body.latitude,
        body.longitude,
        body.website_url.as_deref(),
    )
    .await?;

    if rows_affected == 0 {
        return Err(ApiError::NotFound);
    }

    let (hospital, _latest_discovery) = queries::get_hospital(&state.pool, &facility_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(Json(hospital))
}
