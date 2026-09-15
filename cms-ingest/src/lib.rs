//! Stage 1 of the pipeline: fetch and normalize CMS's "Hospital General
//! Information" dataset. See `client` for the HTTP layer and `parser` for
//! CMS-field-name → internal-field-name normalization.

pub mod client;
pub mod error;
pub mod ownership;
pub mod parser;

pub use client::{CmsClient, CmsClientConfig, HOSPITAL_GENERAL_INFO_DATASET_ID};
pub use error::IngestError;
pub use ownership::{
    EnrollmentRecord, OwnershipClient, OwnershipClientConfig, OwnershipEdgeRecord, HOSPITAL_ALL_OWNERS_DATASET_ID,
    HOSPITAL_ENROLLMENTS_DATASET_ID,
};

/// A single normalized hospital record, ready to be upserted into the
/// `hospitals` table by the `backend` crate. Field names match
/// Architecture.md's `hospitals` schema, *not* CMS's raw column names —
/// see `parser` for the mapping.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct HospitalRecord {
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
    pub overall_rating: Option<i32>,
}

/// Outcome of a full ingest run: how many rows CMS returned, how many
/// parsed cleanly, and which rows were skipped (with why) so the caller
/// can log or surface them without the whole run failing over a handful
/// of malformed records.
#[derive(Debug, Default)]
pub struct IngestSummary {
    pub total_fetched: usize,
    pub parsed: Vec<HospitalRecord>,
    pub skipped: Vec<(String, String)>,
}

/// Fetch every page of the Hospital General Information dataset and parse
/// each row, collecting everything in memory. Fine for ~5-6k hospital
/// records; if this dataset grows substantially, switch callers to
/// `CmsClient::fetch_all` directly and stream batches into the database
/// instead of collecting.
pub async fn ingest_all(client: &CmsClient) -> Result<IngestSummary, IngestError> {
    let mut summary = IngestSummary::default();

    let total = client
        .fetch_all(|rows| {
            for row in rows {
                match parser::parse_record(&row) {
                    Ok(record) => summary.parsed.push(record),
                    Err(e) => {
                        let facility_id = row
                            .get("facility_id")
                            .and_then(serde_json::Value::as_str)
                            .unwrap_or("<unknown>")
                            .to_string();
                        tracing::warn!(facility_id, error = %e, "skipping unparseable hospital row");
                        summary.skipped.push((facility_id, e.to_string()));
                    }
                }
            }
        })
        .await?;

    summary.total_fetched = total;
    Ok(summary)
}
