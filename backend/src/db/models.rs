//! `sqlx::FromRow` structs mirroring the tables in
//! `migrations/0001_init.sql`, which in turn mirror Architecture.md's
//! "Database Schema" section.

use serde::Serialize;
use sqlx::FromRow;
use utoipa::ToSchema;

/// Full row from the `hospitals` table.
#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct Hospital {
    pub facility_id: String,
    pub facility_name: String,
    pub address: String,
    pub city: String,
    pub state: String,
    pub zip_code: String,
    pub county_name: Option<String>,
    pub phone_number: Option<String>,
    pub hospital_type: String,
    pub hospital_ownership: String,
    pub emergency_services: bool,
    pub overall_rating: Option<i64>,

    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub formatted_address: Option<String>,
    pub website_url: Option<String>,
    pub geo_provider: Option<String>,
    pub geo_confidence: Option<f64>,
    pub enriched_at: Option<String>,

    pub ingested_at: String,
    pub updated_at: String,
}

/// Projection used by `GET /api/hospitals` — Architecture.md's example
/// response is a subset of the full row, plus the hospital's *latest*
/// MRF discovery status joined in.
#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct HospitalListItem {
    pub facility_id: String,
    pub facility_name: String,
    pub address: String,
    pub city: String,
    pub state: String,
    pub zip_code: String,
    pub hospital_type: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub website_url: Option<String>,
    pub discovery_status: Option<String>,
}

/// Full row from the `mrf_discoveries` table.
#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct MrfDiscovery {
    pub id: String,
    pub facility_id: String,
    pub website_url: Option<String>,
    pub website_reachable: Option<bool>,
    pub cms_hpt_txt_found: Option<bool>,
    pub cms_hpt_txt_url: Option<String>,
    /// JSON-encoded array of MRF references (kept as raw text here;
    /// callers that need structured access can
    /// `serde_json::from_str::<Vec<_>>(...)` it).
    pub mrf_urls: Option<String>,
    /// From the manifest entry's `contact-name`/`contact-email` fields
    /// (`compliance_probe::manifest_parser::MrfEntry`) — CMS's `cms-hpt.txt`
    /// spec lists these alongside `mrf-url` but they weren't persisted
    /// anywhere until this column was added (2026-09-15).
    pub contact_name: Option<String>,
    pub contact_email: Option<String>,
    pub discovery_status: String,
    pub checked_at: String,
}

/// Full row from the `mrf_metadata` table — one conditional-caching probe
/// of one MRF URL. Multiple rows can share a `mrf_discovery_id` (append-
/// only, same pattern as `mrf_discoveries`): the first is always the
/// `"baseline"` capture, later ones are re-checks, each recording whether
/// the file changed since the previous row for that same discovery.
#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct MrfMetadata {
    pub id: String,
    pub mrf_discovery_id: String,
    pub mrf_url: String,
    pub sha1_hash: Option<String>,
    pub last_modified: Option<String>,
    pub etag: Option<String>,
    pub cache_control: Option<String>,
    pub content_length: Option<i64>,
    pub content_type: Option<String>,
    pub schema_valid: Option<bool>,
    /// `"baseline"` | `"etag"` | `"last_modified"` | `"sha1"` | `NULL`
    /// (unreachable) — see `crate::mrf_metadata::check_mrf_metadata`.
    pub change_detection_method: Option<String>,
    pub changed_from_previous: Option<bool>,
    pub checked_at: String,
}

/// One row of `GET /api/hospitals/needs-enrichment` — hospitals the
/// automated pipeline already ran on (`enriched_at IS NOT NULL`) but
/// couldn't fully resolve.
#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct NeedsEnrichmentItem {
    pub facility_id: String,
    pub facility_name: String,
    pub address: String,
    pub city: String,
    pub state: String,
    pub zip_code: String,
    pub missing_coordinates: bool,
    pub missing_website: bool,
}

/// One disclosed owner/controller of one hospital's own PECOS enrollment
/// — a hospital typically has several. Joined from `hospital_enrollments`
/// (by `ccn = facility_id`) and `hospital_ownership_edges` (by
/// `enrollment_id`) in `db::queries::list_hospital_owners`. Empty for a
/// hospital CMS's ownership-disclosure data hasn't been ingested for yet
/// (see `POST /api/pipeline/ingest-ownership`) or one with no disclosed
/// organizational/individual owner at all — both legitimate, not errors.
#[derive(Debug, Clone, FromRow, Serialize, ToSchema)]
pub struct HospitalOwner {
    pub enrollment_id: String,
    /// `"I"` (individual) or `"O"` (organization).
    pub owner_type: Option<String>,
    pub owner_role_text: Option<String>,
    pub owner_organization_name: Option<String>,
    /// First + last name joined, for an individual owner. `None` for an
    /// organization owner.
    pub owner_person_name: Option<String>,
    /// Raw PECOS value (e.g. `"100"`) — not guaranteed numeric-clean
    /// across all rows, so kept as text rather than parsed.
    pub percentage_ownership: Option<String>,
}

/// One row of the `discovery` breakdown in `GET /api/stats`.
#[derive(Debug, Clone)]
pub struct StatusCount {
    pub state: String,
    /// `"not_checked"` when a hospital has no `mrf_discoveries` row yet.
    pub status: String,
    pub count: i64,
}
