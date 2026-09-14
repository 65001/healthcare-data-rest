//! HTTP client for the CMS Provider Data Catalog "datastore query" API.
//!
//! Endpoint, pagination behavior, and field names below were confirmed
//! live on 2026-09-14 against:
//!   - Dataset page: <https://data.cms.gov/provider-data/dataset/xubh-q36u>
//!   - Query API:    <https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0>
//!   - API docs:     <https://data.cms.gov/api-docs>, <https://data.cms.gov/provider-data/docs>
//!
//! Architecture.md describes this as `datastore/query/{distribution_id}`;
//! in practice the dataset identifier *is* the distribution identifier for
//! this dataset (it has exactly one CSV distribution), and the query path
//! addresses it by index (`/0`) rather than by a separate distribution
//! UUID.
//!
//! Note for local testing: this container's egress policy blocks
//! `data.cms.gov` directly (confirmed via the agent proxy — see
//! IMPLEMENTATION_NOTES.md), so this client was validated by hand via the
//! web-fetch tool rather than `cargo run` in this environment. Point
//! `cargo test -p cms-ingest` at the `wiremock`-backed tests for offline
//! coverage, and do a live run once this is on a host that can reach
//! data.cms.gov.

use std::time::Duration;

use serde::Deserialize;

use crate::error::IngestError;

/// The CMS Provider Data Catalog dataset identifier for the "Hospital
/// General Information" dataset.
pub const HOSPITAL_GENERAL_INFO_DATASET_ID: &str = "xubh-q36u";

const DEFAULT_BASE_URL: &str = "https://data.cms.gov/provider-data/api/1/datastore/query";

/// CMS's datastore query endpoint 400s above roughly 1,000-5,000 rows per
/// request depending on the day; 500 (Architecture.md's number) is a
/// comfortably safe page size and keeps individual requests small enough
/// to retry cheaply.
const DEFAULT_PAGE_SIZE: u32 = 500;

#[derive(Debug, Clone)]
pub struct CmsClientConfig {
    pub base_url: String,
    pub dataset_id: String,
    pub page_size: u32,
    pub request_timeout: Duration,
    pub user_agent: String,
}

impl Default for CmsClientConfig {
    fn default() -> Self {
        Self {
            base_url: DEFAULT_BASE_URL.to_string(),
            dataset_id: HOSPITAL_GENERAL_INFO_DATASET_ID.to_string(),
            page_size: DEFAULT_PAGE_SIZE,
            request_timeout: Duration::from_secs(30),
            user_agent: "cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)"
                .to_string(),
        }
    }
}

pub struct CmsClient {
    http: reqwest::Client,
    config: CmsClientConfig,
}

#[derive(Debug, Deserialize)]
pub struct DatastoreQueryResponse {
    pub results: Vec<serde_json::Map<String, serde_json::Value>>,
    pub count: u64,
}

impl CmsClient {
    pub fn new(config: CmsClientConfig) -> Result<Self, IngestError> {
        let http = reqwest::Client::builder()
            .user_agent(config.user_agent.clone())
            .timeout(config.request_timeout)
            .build()?;
        Ok(Self { http, config })
    }

    pub fn with_defaults() -> Result<Self, IngestError> {
        Self::new(CmsClientConfig::default())
    }

    /// Base URL override, primarily so tests can point this at a
    /// `wiremock` server instead of `data.cms.gov`.
    #[cfg(test)]
    pub(crate) fn with_base_url(mut config: CmsClientConfig, base_url: String) -> Result<Self, IngestError> {
        config.base_url = base_url;
        Self::new(config)
    }

    /// Fetch a single page of raw records, offset-based.
    pub async fn fetch_page(&self, offset: u32) -> Result<DatastoreQueryResponse, IngestError> {
        let url = format!("{}/{}/0", self.config.base_url, self.config.dataset_id);
        let resp = self
            .http
            .get(&url)
            .query(&[
                ("limit", self.config.page_size.to_string()),
                ("offset", offset.to_string()),
            ])
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
        let parsed: DatastoreQueryResponse = serde_json::from_slice(&bytes)?;
        Ok(parsed)
    }

    /// Drive `offset` forward, handing each page's raw rows to `on_page`
    /// as they arrive (rather than buffering the whole ~5-6k row dataset).
    /// Returns the total number of rows seen.
    pub async fn fetch_all<F>(&self, mut on_page: F) -> Result<usize, IngestError>
    where
        F: FnMut(Vec<serde_json::Map<String, serde_json::Value>>),
    {
        let mut offset = 0u32;
        let mut total_seen = 0usize;
        loop {
            let page = self.fetch_page(offset).await?;
            let page_count = page.count;
            let n = page.results.len();
            total_seen += n;
            on_page(page.results);

            if n == 0 || total_seen as u64 >= page_count {
                break;
            }
            offset += self.config.page_size;
        }
        Ok(total_seen)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path_regex, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn paginates_until_count_is_reached() {
        let server = MockServer::start().await;

        let page1 = json!({
            "results": [{"facility_id": "1"}, {"facility_id": "2"}],
            "count": 3
        });
        let page2 = json!({
            "results": [{"facility_id": "3"}],
            "count": 3
        });

        Mock::given(method("GET"))
            .and(path_regex(r"^/xubh-q36u/0$"))
            .and(query_param("offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(page1))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path_regex(r"^/xubh-q36u/0$"))
            .and(query_param("offset", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(page2))
            .mount(&server)
            .await;

        let mut config = CmsClientConfig::default();
        config.page_size = 2;
        let client = CmsClient::with_base_url(config, server.uri()).unwrap();

        let mut seen = Vec::new();
        let total = client
            .fetch_all(|rows| {
                for row in rows {
                    seen.push(row.get("facility_id").unwrap().as_str().unwrap().to_string());
                }
            })
            .await
            .unwrap();

        assert_eq!(total, 3);
        assert_eq!(seen, vec!["1", "2", "3"]);
    }

    #[tokio::test]
    async fn surfaces_non_2xx_as_unexpected_status() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/xubh-q36u/0$"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream hiccup"))
            .mount(&server)
            .await;

        let client = CmsClient::with_base_url(CmsClientConfig::default(), server.uri()).unwrap();
        let err = client.fetch_page(0).await.unwrap_err();
        assert!(err.is_transient());
        assert!(matches!(err, IngestError::UnexpectedStatus { status: 503, .. }));
    }
}
