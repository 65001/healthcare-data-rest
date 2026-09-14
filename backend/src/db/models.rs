//! `sqlx::FromRow` structs mirroring the tables in
//! `migrations/0001_init.sql`, which in turn mirror Architecture.md's
//! "Database Schema" section.

use serde::Serialize;
use sqlx::FromRow;

/// Full row from the `hospitals` table.
#[derive(Debug, Clone, FromRow, Serialize)]
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
#[derive(Debug, Clone, FromRow, Serialize)]
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
#[derive(Debug, Clone, FromRow, Serialize)]
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
    pub discovery_status: String,
    pub checked_at: String,
}

/// One row of the `discovery` breakdown in `GET /api/stats`.
#[derive(Debug, Clone)]
pub struct StatusCount {
    pub state: String,
    /// `"not_checked"` when a hospital has no `mrf_discoveries` row yet.
    pub status: String,
    pub count: i64,
}
