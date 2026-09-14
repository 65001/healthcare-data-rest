//! Nominatim / OpenStreetMap — priority 2 (free, 1 req/sec).
//!
//! Endpoint: `GET https://nominatim.openstreetmap.org/search`. Not
//! verified live against the real host this session (this container's
//! egress policy blocks arbitrary hosts — see IMPLEMENTATION_NOTES.md),
//! but this is Nominatim's long-stable, documented `search` endpoint —
//! lower risk than the still-unconfirmed `cms-hpt.txt` format.
//!
//! Two things Nominatim's usage policy
//! (<https://operations.osmfoundation.org/policies/nominatim/>) requires
//! and this implementation follows:
//! - A descriptive `User-Agent` identifying the project (set on the
//!   shared `reqwest::Client` passed in — see `backend`'s enrich job).
//! - No more than 1 request/second against the public instance. This
//!   provider enforces that itself, internally, with a shared
//!   last-request timestamp guarded by a `tokio::sync::Mutex` — so it
//!   stays true regardless of how much concurrency `enricher::enrich_batch`
//!   uses across hospitals; each `NominatimProvider` instance should be
//!   constructed once and shared (it's `Clone`, cheap to hand around).
//!
//! Website discovery: `extratags=1` pulls OSM's `extratags` map, which
//! sometimes carries a `website` or `contact:website` tag. This is
//! opportunistic, not authoritative — `providers::google_places` is the
//! dedicated fallback for when this comes back empty, per the project
//! owner's decision (see IMPLEMENTATION_NOTES.md).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::error::GeoEnrichError;
use crate::{GeocodingProvider, GeocodingResult};

const DEFAULT_BASE_URL: &str = "https://nominatim.openstreetmap.org";

#[derive(Clone)]
pub struct NominatimProvider {
    http: reqwest::Client,
    base_url: String,
    min_interval: Duration,
    next_allowed: Arc<Mutex<Instant>>,
}

impl NominatimProvider {
    /// `http` should carry a descriptive `User-Agent` per Nominatim's
    /// usage policy — build it with `.user_agent(...)` before passing it
    /// in, same client the rest of this pipeline uses is fine.
    pub fn new(http: reqwest::Client) -> Self {
        Self::with_base_url(http, DEFAULT_BASE_URL.to_string())
    }

    /// Base URL override, primarily for tests (point at a `wiremock`
    /// server) — also usable for a self-hosted Nominatim instance, which
    /// wouldn't need the 1 req/sec throttle, though this always applies
    /// it regardless for simplicity.
    pub fn with_base_url(http: reqwest::Client, base_url: String) -> Self {
        Self {
            http,
            base_url,
            min_interval: Duration::from_secs(1),
            next_allowed: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Blocks until it's been at least `min_interval` since the last
    /// request *this provider instance* made, then reserves the next
    /// slot. Guards concurrent callers via the shared mutex so bounded
    /// concurrency upstream (in `enricher::enrich_batch`) can't cause a
    /// burst against Nominatim.
    async fn throttle(&self) {
        let mut next_allowed = self.next_allowed.lock().await;
        let now = Instant::now();
        if *next_allowed > now {
            tokio::time::sleep(*next_allowed - now).await;
        }
        *next_allowed = Instant::now() + self.min_interval;
    }
}

#[derive(Debug, Deserialize)]
struct NominatimResult {
    lat: String,
    lon: String,
    display_name: Option<String>,
    /// Nominatim's own relevance score, roughly `[0, 1]` — close enough
    /// to `GeocodingResult::confidence`'s documented range to pass
    /// through directly.
    importance: Option<f64>,
    extratags: Option<HashMap<String, String>>,
}

fn extract_website(extratags: &Option<HashMap<String, String>>) -> Option<String> {
    let tags = extratags.as_ref()?;
    tags.get("website")
        .or_else(|| tags.get("contact:website"))
        .cloned()
        .filter(|s| !s.trim().is_empty())
}

#[async_trait::async_trait]
impl GeocodingProvider for NominatimProvider {
    fn name(&self) -> &str {
        "nominatim"
    }

    async fn geocode(
        &self,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        self.throttle().await;

        let query = format!("{address}, {city}, {state} {zip}");
        let url = format!("{}/search", self.base_url);

        let resp = self
            .http
            .get(&url)
            .query(&[
                ("q", query.as_str()),
                ("format", "jsonv2"),
                ("addressdetails", "0"),
                ("extratags", "1"),
                ("limit", "1"),
            ])
            .send()
            .await?;

        let status = resp.status();
        if status.as_u16() == 429 || status.as_u16() == 503 {
            return Err(GeoEnrichError::RateLimited {
                provider: "nominatim".to_string(),
            });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GeoEnrichError::InvalidResponse(format!(
                "nominatim returned HTTP {status}: {body}"
            )));
        }

        let results: Vec<NominatimResult> = resp
            .json()
            .await
            .map_err(|e| GeoEnrichError::InvalidResponse(format!("bad nominatim JSON: {e}")))?;

        let Some(top) = results.into_iter().next() else {
            return Ok(None);
        };

        let latitude: f64 = top.lat.parse().map_err(|_| {
            GeoEnrichError::InvalidResponse(format!("nominatim returned unparseable lat: {}", top.lat))
        })?;
        let longitude: f64 = top.lon.parse().map_err(|_| {
            GeoEnrichError::InvalidResponse(format!("nominatim returned unparseable lon: {}", top.lon))
        })?;

        Ok(Some(GeocodingResult {
            latitude,
            longitude,
            formatted_address: top.display_name,
            confidence: top.importance,
            website_url: extract_website(&top.extratags),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn parses_a_result_with_website_extratag() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {
                    "lat": "34.0762",
                    "lon": "-118.3805",
                    "display_name": "Cedars-Sinai Medical Center, Beverly Blvd, Los Angeles, CA",
                    "importance": 0.71,
                    "extratags": { "website": "https://www.cedars-sinai.org" }
                }
            ])))
            .mount(&server)
            .await;

        let provider = NominatimProvider::with_base_url(reqwest::Client::new(), server.uri());
        let result = provider
            .geocode("8700 Beverly Blvd", "Los Angeles", "CA", "90048")
            .await
            .unwrap()
            .expect("should have geocoded");

        assert_eq!(result.latitude, 34.0762);
        assert_eq!(result.longitude, -118.3805);
        assert_eq!(result.website_url.as_deref(), Some("https://www.cedars-sinai.org"));
    }

    #[tokio::test]
    async fn returns_none_for_empty_results() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;

        let provider = NominatimProvider::with_base_url(reqwest::Client::new(), server.uri());
        let result = provider.geocode("1 Nowhere Rd", "Nowhere", "ZZ", "00000").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn missing_website_extratag_yields_none_not_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                { "lat": "1.0", "lon": "2.0", "display_name": "Somewhere", "importance": 0.5 }
            ])))
            .mount(&server)
            .await;

        let provider = NominatimProvider::with_base_url(reqwest::Client::new(), server.uri());
        let result = provider.geocode("x", "y", "z", "0").await.unwrap().unwrap();
        assert_eq!(result.website_url, None);
    }

    #[tokio::test]
    async fn maps_429_to_rate_limited() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let provider = NominatimProvider::with_base_url(reqwest::Client::new(), server.uri());
        let err = provider.geocode("x", "y", "z", "0").await.unwrap_err();
        assert!(err.is_transient());
        assert!(matches!(err, GeoEnrichError::RateLimited { .. }));
    }

    #[tokio::test]
    async fn throttles_consecutive_calls_to_roughly_one_per_second() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/search"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;

        let provider = NominatimProvider::with_base_url(reqwest::Client::new(), server.uri());
        let start = std::time::Instant::now();
        provider.geocode("a", "b", "c", "0").await.unwrap();
        provider.geocode("a", "b", "c", "0").await.unwrap();
        // Two calls must be spaced >= ~1s apart; allow a little slack for
        // scheduling jitter in CI.
        assert!(start.elapsed() >= Duration::from_millis(950));
    }

    #[tokio::test]
    #[ignore]
    async fn live_nominatim_test() {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .user_agent("cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)")
            .build()
            .unwrap();
        let provider = NominatimProvider::new(http);
        let res = provider.geocode("101 SIVLEY RD", "HUNTSVILLE", "AL", "35801").await;
        println!("LIVE RES: {:?}", res);
    }
}
