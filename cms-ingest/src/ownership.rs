//! Fetches CMS's "Hospital Enrollments" and "Hospital All Owners"
//! datasets — a different, newer CMS API (`data-api/v1/dataset`) than
//! `client.rs`'s Provider Data Catalog `datastore/query` endpoint.
//!
//! These feed the CCN ↔ PECOS-ownership graph that
//! `backend::db::queries::ownership_reachable_ccns` walks to restrict
//! automatic cms-hpt.txt-driven MRF tagging (see
//! `routes::pipeline::trigger_discover`'s `network_manifest_url` mode)
//! to hospitals actually connected through CMS's own ownership
//! disclosures — not just ones whose name happens to look similar to
//! something in a network's manifest.
//!
//! Endpoint, pagination, and field names confirmed live 2026-09-15
//! against:
//! - Hospital Enrollments: <https://data.cms.gov/data-api/v1/dataset/f6f6505c-e8b0-4d57-b258-e2b94133aaf2/data>
//!   (~5-10K rows; CCN ↔ ENROLLMENT ID ↔ ASSOCIATE ID crosswalk, one row
//!   per hospital's own PECOS enrollment)
//! - Hospital All Owners: <https://data.cms.gov/data-api/v1/dataset/029c119f-f79c-49be-9100-344d31d10344/data>
//!   (~145K rows; one row per disclosed owner/controller of a hospital's
//!   enrollment — a hospital typically has several)
//! - API docs: <https://data.cms.gov/api-docs>
//!
//! Unlike `client.rs`'s API, this one returns a **bare JSON array** (no
//! `{results, count}` envelope) and has **no documented total-row-count**
//! — pagination continues until a page comes back shorter than the
//! requested `size` (CMS's docs cap `size` at 5,000).

use std::time::Duration;

use serde_json::{Map, Value};

use crate::error::IngestError;

pub const HOSPITAL_ENROLLMENTS_DATASET_ID: &str = "f6f6505c-e8b0-4d57-b258-e2b94133aaf2";
pub const HOSPITAL_ALL_OWNERS_DATASET_ID: &str = "029c119f-f79c-49be-9100-344d31d10344";

const DEFAULT_BASE_URL: &str = "https://data.cms.gov/data-api/v1/dataset";
const DEFAULT_PAGE_SIZE: u32 = 5000;

#[derive(Debug, Clone)]
pub struct OwnershipClientConfig {
    pub base_url: String,
    pub page_size: u32,
    pub request_timeout: Duration,
    pub user_agent: String,
}

impl Default for OwnershipClientConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            page_size: DEFAULT_PAGE_SIZE,
            request_timeout: Duration::from_secs(30),
            user_agent: "cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)"
                .to_string(),
        }
    }
}

pub struct OwnershipClient {
    http: reqwest::Client,
    config: OwnershipClientConfig,
}

impl OwnershipClient {
    pub fn new(config: OwnershipClientConfig) -> Result<Self, IngestError> {
        let http = reqwest::Client::builder()
            .user_agent(config.user_agent.clone())
            .timeout(config.request_timeout)
            .build()?;
        Ok(Self { http, config })
    }

    pub fn with_defaults() -> Result<Self, IngestError> {
        Self::new(OwnershipClientConfig::default())
    }

    /// Base URL override, primarily so tests can point this at a
    /// `wiremock` server instead of `data.cms.gov`.
    #[cfg(test)]
    pub(crate) fn with_base_url(mut config: OwnershipClientConfig, base_url: String) -> Result<Self, IngestError> {
        config.base_url = base_url;
        Self::new(config)
    }

    async fn fetch_page(&self, dataset_id: &str, offset: u32) -> Result<Vec<Map<String, Value>>, IngestError> {
        let url = format!("{}/{}/data", self.config.base_url, dataset_id);
        let resp = self
            .http
            .get(&url)
            .query(&[("size", self.config.page_size.to_string()), ("offset", offset.to_string())])
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(IngestError::UnexpectedStatus {
                status: status.as_u16(),
                body,
            });
        }

        let bytes = resp.bytes().await?;
        let rows: Vec<Map<String, Value>> = serde_json::from_slice(&bytes)?;
        Ok(rows)
    }

    /// Pages `dataset_id` until a short page signals the end (see this
    /// module's doc comment — there's no total-count field to page
    /// against), handing each page's raw rows to `on_page`. Returns the
    /// total row count seen.
    async fn fetch_all<F>(&self, dataset_id: &str, mut on_page: F) -> Result<usize, IngestError>
    where
        F: FnMut(Vec<Map<String, Value>>),
    {
        let mut offset = 0u32;
        let mut total = 0usize;
        loop {
            let page = self.fetch_page(dataset_id, offset).await?;
            let n = page.len();
            total += n;
            let is_last_page = n < self.config.page_size as usize;
            on_page(page);
            if n == 0 || is_last_page {
                break;
            }
            offset += self.config.page_size;
        }
        Ok(total)
    }

    /// Streams every row of the Hospital Enrollments dataset to
    /// `on_page`, page by page (not buffered — this and the owners
    /// dataset are large enough that a caller should upsert per page
    /// rather than collect).
    pub async fn fetch_all_enrollments<F>(&self, on_page: F) -> Result<usize, IngestError>
    where
        F: FnMut(Vec<Map<String, Value>>),
    {
        self.fetch_all(HOSPITAL_ENROLLMENTS_DATASET_ID, on_page).await
    }

    /// Streams every row of the Hospital All Owners dataset to `on_page`.
    pub async fn fetch_all_ownership_edges<F>(&self, on_page: F) -> Result<usize, IngestError>
    where
        F: FnMut(Vec<Map<String, Value>>),
    {
        self.fetch_all(HOSPITAL_ALL_OWNERS_DATASET_ID, on_page).await
    }
}

/// One row of the Hospital Enrollments dataset — the CCN ↔ PECOS
/// associate-id crosswalk `backend` joins the `hospitals` table against.
#[derive(Debug, Clone, PartialEq)]
pub struct EnrollmentRecord {
    pub enrollment_id: String,
    /// Blank on some non-hospital-subtype PECOS enrollments; `None` in
    /// that case (that enrollment just can't be joined to a `hospitals`
    /// row).
    pub ccn: Option<String>,
    pub associate_id: String,
    pub organization_name: String,
    pub state: Option<String>,
}

/// One row of the Hospital All Owners dataset — one (subject, owner)
/// edge. A hospital typically has several of these, one per disclosed
/// owner/controller.
#[derive(Debug, Clone, PartialEq)]
pub struct OwnershipEdgeRecord {
    pub enrollment_id: String,
    pub associate_id: String,
    pub owner_associate_id: Option<String>,
    /// `"I"` (individual) or `"O"` (organization). `ownership_reachable_ccns`
    /// only trusts `"O"` edges with a controlling role — an individual
    /// physician's minority stake doesn't make two hospitals "the same
    /// network".
    pub owner_type: Option<String>,
    pub owner_role_code: Option<String>,
    pub owner_role_text: Option<String>,
    pub owner_organization_name: Option<String>,
    /// First + last name joined, for an individual owner. `None` for an
    /// organization owner.
    pub owner_person_name: Option<String>,
    pub percentage_ownership: Option<String>,
}

fn str_field(row: &Map<String, Value>, field: &str) -> Option<String> {
    row.get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

/// `None` only when the row is missing its own identifying fields
/// (`ENROLLMENT ID` / `ASSOCIATE ID`) — malformed beyond use, so the
/// caller skips it rather than erroring the whole ingest run.
pub fn parse_enrollment(row: &Map<String, Value>) -> Option<EnrollmentRecord> {
    Some(EnrollmentRecord {
        enrollment_id: str_field(row, "ENROLLMENT ID")?,
        ccn: str_field(row, "CCN"),
        associate_id: str_field(row, "ASSOCIATE ID")?,
        organization_name: str_field(row, "ORGANIZATION NAME").unwrap_or_default(),
        state: str_field(row, "STATE"),
    })
}

/// `None` only when the row is missing its own identifying fields.
pub fn parse_ownership_edge(row: &Map<String, Value>) -> Option<OwnershipEdgeRecord> {
    let owner_person_name = match (str_field(row, "FIRST NAME - OWNER"), str_field(row, "LAST NAME - OWNER")) {
        (None, None) => None,
        (first, last) => {
            let joined = [first, last].into_iter().flatten().collect::<Vec<_>>().join(" ");
            (!joined.is_empty()).then_some(joined)
        }
    };
    Some(OwnershipEdgeRecord {
        enrollment_id: str_field(row, "ENROLLMENT ID")?,
        associate_id: str_field(row, "ASSOCIATE ID")?,
        owner_associate_id: str_field(row, "ASSOCIATE ID - OWNER"),
        owner_type: str_field(row, "TYPE - OWNER"),
        owner_role_code: str_field(row, "ROLE CODE - OWNER"),
        owner_role_text: str_field(row, "ROLE TEXT - OWNER"),
        owner_organization_name: str_field(row, "ORGANIZATION NAME - OWNER"),
        owner_person_name,
        percentage_ownership: str_field(row, "PERCENTAGE OWNERSHIP"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path_regex, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn enrollment_row() -> Map<String, Value> {
        // Trimmed from a live response, 2026-09-15.
        json!({
            "ENROLLMENT ID": "O20020814000009",
            "ENROLLMENT STATE": "ME",
            "NPI": "1689653487",
            "CCN": "200024",
            "ASSOCIATE ID": "2567379563",
            "ORGANIZATION NAME": "CENTRAL MAINE MEDICAL CENTER",
            "STATE": "ME"
        })
        .as_object()
        .unwrap()
        .clone()
    }

    fn ownership_row() -> Map<String, Value> {
        // Shaped like a live Sentara row, 2026-09-15 (associate ids
        // replaced — the real ones aren't needed for parser coverage).
        json!({
            "ENROLLMENT ID": "O20150127002358",
            "ASSOCIATE ID": "1111111111",
            "ORGANIZATION NAME": "SENTARA PRINCESS ANNE HOSPITAL",
            "ASSOCIATE ID - OWNER": "2222222222",
            "TYPE - OWNER": "O",
            "ROLE CODE - OWNER": "35",
            "ROLE TEXT - OWNER": "5% OR GREATER DIRECT OWNERSHIP INTEREST",
            "ORGANIZATION NAME - OWNER": "SENTARA HEALTH",
            "PERCENTAGE OWNERSHIP": "100"
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn parses_enrollment_row() {
        let rec = parse_enrollment(&enrollment_row()).unwrap();
        assert_eq!(rec.ccn.as_deref(), Some("200024"));
        assert_eq!(rec.associate_id, "2567379563");
        assert_eq!(rec.organization_name, "CENTRAL MAINE MEDICAL CENTER");
        assert_eq!(rec.state.as_deref(), Some("ME"));
    }

    #[test]
    fn blank_ccn_becomes_none() {
        let mut row = enrollment_row();
        row.insert("CCN".into(), json!(""));
        let rec = parse_enrollment(&row).unwrap();
        assert_eq!(rec.ccn, None);
    }

    #[test]
    fn parses_organization_owner_edge() {
        let rec = parse_ownership_edge(&ownership_row()).unwrap();
        assert_eq!(rec.owner_type.as_deref(), Some("O"));
        assert_eq!(rec.owner_organization_name.as_deref(), Some("SENTARA HEALTH"));
        assert_eq!(rec.owner_role_text.as_deref(), Some("5% OR GREATER DIRECT OWNERSHIP INTEREST"));
        assert_eq!(rec.owner_person_name, None);
    }

    #[test]
    fn parses_individual_owner_name() {
        let mut row = ownership_row();
        row.insert("TYPE - OWNER".into(), json!("I"));
        row.remove("ORGANIZATION NAME - OWNER");
        row.insert("FIRST NAME - OWNER".into(), json!("Melinda"));
        row.insert("LAST NAME - OWNER".into(), json!("Hancock"));
        let rec = parse_ownership_edge(&row).unwrap();
        assert_eq!(rec.owner_person_name.as_deref(), Some("Melinda Hancock"));
        assert_eq!(rec.owner_organization_name, None);
    }

    #[tokio::test]
    async fn paginates_until_a_short_page() {
        let server = MockServer::start().await;
        let mut config = OwnershipClientConfig::default();
        config.page_size = 2;

        Mock::given(method("GET"))
            .and(path_regex(r"^/f6f6505c-e8b0-4d57-b258-e2b94133aaf2/data$"))
            .and(query_param("offset", "0"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!([Value::Object(enrollment_row()), Value::Object(enrollment_row())])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/f6f6505c-e8b0-4d57-b258-e2b94133aaf2/data$"))
            .and(query_param("offset", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([Value::Object(enrollment_row())])))
            .mount(&server)
            .await;

        let client = OwnershipClient::with_base_url(config, server.uri()).unwrap();
        let mut seen = 0usize;
        let total = client.fetch_all_enrollments(|rows| seen += rows.len()).await.unwrap();
        assert_eq!(total, 3);
        assert_eq!(seen, 3);
    }

    #[tokio::test]
    async fn a_full_page_stops_after_the_next_empty_one() {
        // page_size rows exactly on page 1 means the loop can't yet know
        // it was the last page from length alone — confirms it correctly
        // issues one more (empty) request rather than assuming.
        let server = MockServer::start().await;
        let mut config = OwnershipClientConfig::default();
        config.page_size = 1;

        Mock::given(method("GET"))
            .and(path_regex(r"^/029c119f-f79c-49be-9100-344d31d10344/data$"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([Value::Object(ownership_row())])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/029c119f-f79c-49be-9100-344d31d10344/data$"))
            .and(query_param("offset", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;

        let client = OwnershipClient::with_base_url(config, server.uri()).unwrap();
        let total = client.fetch_all_ownership_edges(|_| {}).await.unwrap();
        assert_eq!(total, 1);
    }
}
