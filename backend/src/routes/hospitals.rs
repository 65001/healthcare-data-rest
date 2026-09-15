//! `GET /api/hospitals`, `GET /api/hospitals/:facility_id`,
//! `GET /api/hospitals/needs-enrichment`, and
//! `PATCH /api/hospitals/:facility_id/enrichment`, per Architecture.md's
//! REST API section.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use crate::db::models::{Hospital, HospitalListItem, HospitalOwner, MrfDiscovery, NeedsEnrichmentItem};
use crate::db::queries::{self, HospitalFilters};
use crate::error::{ApiError, ErrorResponse};
use crate::routes::AppState;
use crate::url_check::{self, UrlCheckResult};

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

/// Response for `GET /api/hospitals/:facility_id/ownership`.
#[derive(Debug, Serialize, ToSchema)]
pub struct HospitalOwnershipResponse {
    pub owners: Vec<HospitalOwner>,
    /// True when this hospital's `hospital_ownership` category is one
    /// CMS's PECOS ownership-disclosure data structurally carries no
    /// owner for (there's no private ownership stake to disclose for a
    /// federal facility — see
    /// `db::queries::ownership_category_has_no_pecos_stake`). When this
    /// is `true`, an empty `owners` list is expected and does **not**
    /// mean `POST /api/pipeline/ingest-ownership` needs to be run.
    pub no_stake_expected: bool,
}

/// Ownership disclosures
///
/// Every owner/controller CMS's ownership-disclosure data has on file for
/// this hospital — organizations before individuals, then by name. Empty
/// `owners` is not necessarily an error: either
/// `POST /api/pipeline/ingest-ownership` hasn't been run yet, this
/// hospital has no PECOS enrollment on file, it has one with no disclosed
/// owner, or (see `no_stake_expected`) this hospital's ownership category
/// structurally has no PECOS disclosure at all. This is the same data
/// `POST /api/pipeline/discover`'s ownership-graph cross-validation uses
/// internally — this endpoint just exposes it per-hospital instead of
/// walking it into a reachable-CCN set.
#[utoipa::path(
    get,
    path = "/api/hospitals/{facility_id}/ownership",
    params(
        ("facility_id" = String, Path, description = "CMS Certification Number (primary key)"),
    ),
    responses(
        (status = 200, description = "Disclosed owners, possibly empty", body = HospitalOwnershipResponse),
        (status = 404, description = "No hospital with that facility_id", body = ErrorResponse),
    ),
    tag = "hospitals",
)]
pub async fn get_ownership(
    State(state): State<AppState>,
    Path(facility_id): Path<String>,
) -> Result<Json<HospitalOwnershipResponse>, ApiError> {
    let (hospital, _) = queries::get_hospital(&state.pool, &facility_id).await?.ok_or(ApiError::NotFound)?;

    let owners = queries::list_hospital_owners(&state.pool, &facility_id).await?;
    let no_stake_expected = queries::ownership_category_has_no_pecos_stake(&hospital.hospital_ownership);
    Ok(Json(HospitalOwnershipResponse { owners, no_stake_expected }))
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

/// Response for `PATCH /api/hospitals/:facility_id/enrichment`: the
/// updated hospital row, plus — only when `website_url` was part of the
/// request — the verification that was run on it.
#[derive(Debug, Serialize, ToSchema)]
pub struct ManualEnrichmentResponse {
    #[serde(flatten)]
    #[schema(inline)]
    pub hospital: Hospital,
    pub website_check: Option<UrlCheckResult>,
}

/// Manual enrichment
///
/// Sets coordinates and/or a website by hand for one hospital — for
/// whatever `POST /api/pipeline/enrich` (every provider in the cascade,
/// including the `retry_incomplete` backfill pass) couldn't resolve.
/// Typically used against the queue `GET /api/hospitals/needs-enrichment`
/// surfaces. `latitude`/`longitude` must be provided together (not just
/// one) and in-range; `website_url` must start with `http://` or
/// `https://` **and be independently verified to exist** (a `HEAD`,
/// falling back to a capped `GET`, run inline before saving — see
/// `url_check`) — an unreachable URL is rejected with `400`. Its content
/// type, size, and a best-effort guess at whether it looks like a CMS
/// machine-readable file are returned in `website_check` but never block
/// the save: `website_url` is a hospital's homepage, not necessarily an
/// MRF link, so failing that sniff is the expected common case, not an
/// error. Whatever's provided **overwrites** the existing value outright
/// — a manual correction here is assumed intentional. A successful
/// update stamps `geo_provider = "manual"` and `geo_confidence = 1.0`
/// when coordinates are set, and sets `enriched_at` if it wasn't already.
#[utoipa::path(
    patch,
    path = "/api/hospitals/{facility_id}/enrichment",
    params(
        ("facility_id" = String, Path, description = "CMS Certification Number (primary key)"),
    ),
    request_body = ManualEnrichmentRequest,
    responses(
        (status = 200, description = "The updated hospital row", body = ManualEnrichmentResponse),
        (status = 400, description = "Validation failure, or website_url is unreachable — see the `error` message", body = ErrorResponse),
        (status = 404, description = "No hospital with that facility_id", body = ErrorResponse),
    ),
    tag = "hospitals",
)]
pub async fn patch_enrichment(
    State(state): State<AppState>,
    Path(facility_id): Path<String>,
    Json(body): Json<ManualEnrichmentRequest>,
) -> Result<Json<ManualEnrichmentResponse>, ApiError> {
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

    // `website_check` stays `None` unless a website_url was actually
    // submitted (no URL to verify otherwise).
    let mut website_check: Option<UrlCheckResult> = None;

    if let Some(url) = &body.website_url {
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(ApiError::BadRequest(
                "website_url must start with http:// or https://".to_string(),
            ));
        }

        let check = url_check::check_url(&state.http, url).await;
        if !check.reachable {
            let reason = check
                .error
                .clone()
                .or_else(|| check.status.map(|s| format!("HTTP {s}")))
                .unwrap_or_else(|| "unknown error".to_string());
            return Err(ApiError::BadRequest(format!("website_url is not reachable: {reason}")));
        }
        website_check = Some(check);
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

    Ok(Json(ManualEnrichmentResponse { hospital, website_check }))
}
