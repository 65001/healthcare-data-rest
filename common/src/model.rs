use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

/// Summary information for a hospital, as returned in search and listing endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HospitalSummary {
    pub hospital_id: i64,
    pub hospital_name: String,
    pub hospital_address: Option<String>,
    pub hospital_city: Option<String>,
    pub hospital_state: Option<String>,
    pub license_number: Option<String>,
    pub last_updated_on: Option<NaiveDate>,
    pub version: Option<String>,
}

/// Comprehensive detail for a single hospital.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct HospitalDetail {
    pub hospital_id: i64,
    pub hospital_name: String,
    pub hospital_address: Option<String>,
    pub hospital_city: Option<String>,
    pub hospital_state: Option<String>,
    pub license_number: Option<String>,
    pub last_updated_on: Option<NaiveDate>,
    pub version: Option<String>,
    pub attestation: Option<String>,
    pub confirm_attestation: Option<bool>,
    pub attester_name: Option<String>,
    pub financial_aid_policy: Option<String>,
    pub general_contract_provisions: Option<String>,
}

/// Service-level charge data for procedures and items.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StandardCharge {
    pub charge_id: i64,
    pub charge_seq: Option<i32>,
    pub hospital_id: i64,
    pub hospital_name: Option<String>,
    pub description: String,
    pub gross_charge: Option<f64>,
    pub discounted_cash: Option<f64>,
    pub minimum: Option<f64>,
    pub maximum: Option<f64>,
    pub setting: Option<String>,
    pub billing_class: Option<String>,
    pub cpt: Option<String>,
    pub hcpcs: Option<String>,
    pub ms_drg: Option<String>,
    pub rc: Option<String>,
    pub cdm: Option<String>,
    pub ndc: Option<String>,
    pub payer_count: Option<i32>,
    pub distinct_payer_count: Option<i32>,
    pub avg_negotiated_rate: Option<f64>,
    pub min_negotiated_rate: Option<f64>,
    pub max_negotiated_rate: Option<f64>,
}

/// Payer-specific negotiated rate details for a procedure.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StandardChargeDetail {
    pub detail_id: i64,
    pub charge_id: i64,
    pub hospital_id: i64,
    pub hospital_name: Option<String>,
    pub description: String,
    pub payer_name: String,
    pub plan_name: Option<String>,
    pub standard_charge_dollar: Option<f64>,
    pub standard_charge_percentage: Option<f64>,
    pub estimated_amount: Option<f64>,
    pub methodology: Option<String>,
    pub payer_group: Option<String>,
    pub billing_class: Option<String>,
    pub setting: Option<String>,
    pub cpt: Option<String>,
    pub hcpcs: Option<String>,
    pub ms_drg: Option<String>,
}

/// Query parameters for searching hospitals.
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct HospitalSearchParams {
    /// Full-text query on hospital name or address
    pub q: Option<String>,
    /// Two-letter state code filter (e.g. "CA", "NY", "TX")
    pub state: Option<String>,
    /// City name filter
    pub city: Option<String>,
    /// License or CMS certification identifier
    pub license: Option<String>,
    /// Number of items to return (default: 20, max: 100)
    pub limit: Option<u32>,
    /// Number of items to skip for pagination
    pub offset: Option<u32>,
}

/// Query parameters for searching procedures / charges.
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct ProcedureSearchParams {
    /// Text search across procedure descriptions
    pub q: Option<String>,
    /// Specific code value (e.g., "99213", "470")
    pub code: Option<String>,
    /// Code system filter: "cpt", "hcpcs", or "ms_drg"
    pub code_type: Option<String>,
    /// Filter to a specific hospital ID
    pub hospital_id: Option<i64>,
    /// Number of items to return (default: 20, max: 100)
    pub limit: Option<u32>,
    /// Number of items to skip for pagination
    pub offset: Option<u32>,
}

/// Query parameters for comparing procedure prices across facilities and payers.
#[derive(Debug, Clone, Default, Deserialize, IntoParams)]
pub struct PriceComparisonParams {
    /// Procedure code (e.g. CPT "99213" or MS-DRG "470")
    pub code: String,
    /// Code system: "cpt", "hcpcs", or "ms_drg"
    pub code_type: Option<String>,
    /// Two-letter state code to limit geographic comparison (e.g. "MA", "TX")
    pub state: Option<String>,
    /// Filter to a specific insurance payer (e.g. "Blue Cross", "Aetna", "UnitedHealthcare")
    pub payer: Option<String>,
    /// Specific plan name filter
    pub plan: Option<String>,
    /// Number of comparative results to return (default: 50, max: 200)
    pub limit: Option<u32>,
}

/// Comparative pricing entry for a procedure at a hospital for a specific payer/plan.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PriceComparisonItem {
    pub hospital_id: i64,
    pub hospital_name: String,
    pub hospital_city: Option<String>,
    pub hospital_state: Option<String>,
    pub description: String,
    pub payer_name: Option<String>,
    pub plan_name: Option<String>,
    pub negotiated_dollar: Option<f64>,
    pub discounted_cash: Option<f64>,
    pub gross_charge: Option<f64>,
    pub estimated_amount: Option<f64>,
    pub methodology: Option<String>,
    pub setting: Option<String>,
}

/// Aggregate dataset statistics.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DatasetStats {
    pub total_hospitals: i64,
    pub states_covered: i64,
    pub lake_version: String,
    pub is_lake_attached: bool,
}

/// Pagination metadata discriminator ensuring either exact count or cursor continuation is returned.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum PaginationMeta {
    Exact { total: u64 },
    Cursor { has_more: bool },
}

/// Generic paginated response wrapper.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PaginatedResponse<T> {
    pub items: Vec<T>,
    pub limit: u32,
    pub offset: u32,
    #[serde(flatten)]
    pub pagination: PaginationMeta,
}

impl<T> PaginatedResponse<T> {
    pub fn exact(items: Vec<T>, total: u64, limit: u32, offset: u32) -> Self {
        Self {
            items,
            limit,
            offset,
            pagination: PaginationMeta::Exact { total },
        }
    }

    pub fn cursor(items: Vec<T>, has_more: bool, limit: u32, offset: u32) -> Self {
        Self {
            items,
            limit,
            offset,
            pagination: PaginationMeta::Cursor { has_more },
        }
    }

    pub fn total(&self) -> Option<u64> {
        match self.pagination {
            PaginationMeta::Exact { total } => Some(total),
            PaginationMeta::Cursor { .. } => None,
        }
    }

    pub fn has_more(&self) -> bool {
        match self.pagination {
            PaginationMeta::Exact { total } => (self.offset as u64 + self.limit as u64) < total,
            PaginationMeta::Cursor { has_more } => has_more,
        }
    }
}

/// Standard API error payload.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiErrorResponse {
    pub error: String,
    pub message: String,
}
