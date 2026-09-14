//! Google Places API (New) — dedicated website lookup, used as the
//! fallback when Nominatim doesn't turn up a `website_url`. Intentionally
//! separate from `providers::google_maps` (Maps *Geocoding*) — different
//! API, different key, different pricing SKU. Don't conflate the two.
//!
//! Confirmed live 2026-09-14 against
//! <https://developers.google.com/maps/documentation/places/web-service/text-search>:
//! - `POST https://places.googleapis.com/v1/places:searchText`
//! - Headers: `Content-Type: application/json`, `X-Goog-Api-Key: <key>`,
//!   `X-Goog-FieldMask: places.displayName,places.formattedAddress,places.websiteUri`
//! - Body: `{"textQuery": "<free text>"}`
//! - `websiteUri` is a "Text Search Pro" SKU field — priced accordingly
//!   (~$17 per 1,000 calls beyond the free monthly allotment as of this
//!   writing; see IMPLEMENTATION_NOTES.md). This is why it's only called
//!   as a fallback after the free Nominatim `extratags` check comes up
//!   empty, not for every hospital unconditionally.

use serde::{Deserialize, Serialize};

use crate::error::GeoEnrichError;

const DEFAULT_BASE_URL: &str = "https://places.googleapis.com/v1/places:searchText";

pub struct GooglePlacesClient {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl GooglePlacesClient {
    /// Returns `None` when disabled or keyless (mirrors
    /// `providers::google_maps::GoogleMapsProvider::from_config`), so
    /// callers can `if let Some(client) = GooglePlacesClient::from_config(...)`
    /// and simply skip the fallback step when it isn't configured.
    pub fn from_config(http: reqwest::Client, enabled: bool, api_key: Option<String>) -> Option<Self> {
        match (enabled, api_key) {
            (true, Some(api_key)) if !api_key.is_empty() => Some(Self {
                http,
                base_url: DEFAULT_BASE_URL.to_string(),
                api_key,
            }),
            _ => None,
        }
    }

    #[cfg(test)]
    fn with_base_url(http: reqwest::Client, base_url: String, api_key: String) -> Self {
        Self { http, base_url, api_key }
    }

    /// Looks up a hospital by name + address text and returns its
    /// website, if Google has one on file. `Ok(None)` means Places
    /// genuinely has no match (or no website for the match) — not an
    /// error condition, same convention as `GeocodingProvider::geocode`.
    pub async fn find_website(
        &self,
        facility_name: &str,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<String>, GeoEnrichError> {
        let text_query = format!("{facility_name}, {address}, {city}, {state} {zip}");

        let resp = self
            .http
            .post(&self.base_url)
            .header("Content-Type", "application/json")
            .header("X-Goog-Api-Key", &self.api_key)
            .header(
                "X-Goog-FieldMask",
                "places.displayName,places.formattedAddress,places.websiteUri",
            )
            .json(&SearchTextRequest { text_query })
            .send()
            .await?;

        let status = resp.status();
        if status.as_u16() == 429 {
            return Err(GeoEnrichError::RateLimited {
                provider: "google_places".to_string(),
            });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GeoEnrichError::InvalidResponse(format!(
                "google places returned HTTP {status}: {body}"
            )));
        }

        let parsed: SearchTextResponse = resp
            .json()
            .await
            .map_err(|e| GeoEnrichError::InvalidResponse(format!("bad google places JSON: {e}")))?;

        Ok(parsed
            .places
            .into_iter()
            .next()
            .and_then(|p| p.website_uri)
            .filter(|s| !s.trim().is_empty()))
    }
}

#[derive(Debug, Serialize)]
struct SearchTextRequest {
    #[serde(rename = "textQuery")]
    text_query: String,
}

#[derive(Debug, Deserialize, Default)]
struct SearchTextResponse {
    #[serde(default)]
    places: Vec<PlaceResult>,
}

#[derive(Debug, Deserialize)]
struct PlaceResult {
    #[serde(rename = "websiteUri")]
    website_uri: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn from_config_is_none_when_disabled_or_keyless() {
        let http = reqwest::Client::new();
        assert!(GooglePlacesClient::from_config(http.clone(), false, Some("key".into())).is_none());
        assert!(GooglePlacesClient::from_config(http.clone(), true, None).is_none());
        assert!(GooglePlacesClient::from_config(http, true, Some(String::new())).is_none());
    }

    #[tokio::test]
    async fn returns_website_uri_from_first_place() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .and(header("X-Goog-Api-Key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "places": [
                    { "displayName": { "text": "Cedars-Sinai" }, "websiteUri": "https://www.cedars-sinai.org" }
                ]
            })))
            .mount(&server)
            .await;

        let client = GooglePlacesClient::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let website = client
            .find_website("Cedars-Sinai Medical Center", "8700 Beverly Blvd", "Los Angeles", "CA", "90048")
            .await
            .unwrap();

        assert_eq!(website.as_deref(), Some("https://www.cedars-sinai.org"));
    }

    #[tokio::test]
    async fn returns_none_when_no_places_match() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "places": [] })))
            .mount(&server)
            .await;

        let client = GooglePlacesClient::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let website = client.find_website("Nobody Hospital", "1 Nowhere Rd", "Nowhere", "ZZ", "00000").await.unwrap();
        assert_eq!(website, None);
    }

    #[tokio::test]
    async fn maps_429_to_rate_limited() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;

        let client = GooglePlacesClient::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let err = client.find_website("x", "y", "z", "s", "0").await.unwrap_err();
        assert!(matches!(err, GeoEnrichError::RateLimited { .. }));
    }

    #[tokio::test]
    #[ignore]
    async fn live_places_test() {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .unwrap();
        let client = GooglePlacesClient::from_config(http, true, Some("AIzaSyCO9Hf-YsMMTSFfKmVqylVoJZtyhBBgJKA".into())).unwrap();
        let res = client.find_website("SOUTHEAST HEALTH MEDICAL CENTER", "1108 ROSS CLARK CIRCLE", "DOTHAN", "AL", "36301").await;
        println!("LIVE PLACES RES: {:?}", res);
    }
}
