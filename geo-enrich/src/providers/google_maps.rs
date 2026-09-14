//! Google Maps Geocoding API — priority 3 (paid, disabled by default).
//!
//! Endpoint: `GET https://maps.googleapis.com/maps/api/geocode/json?address={...}&key={api_key}`.
//! Confirmed live 2026-09-14 against Google's current docs
//! (`developers.google.com/maps/documentation/geocoding/requests-geocoding`).
//! Like everything else in this pass, the request/response shape was
//! confirmed via web-fetch, not exercised against a live key or compiled
//! in this container.
//!
//! This is the last step in the cascade, behind the free `census` and
//! `nominatim` providers — it exists to pick up the handful of addresses
//! neither of those can resolve. Google's geocoder is considerably more
//! tolerant of messy input (PO boxes, multi-line addresses, "HWY 9 PO BOX
//! 1270" style strings) since it isn't limited to TIGER/Line reference
//! data or OSM's community-maintained street network, but it's a paid
//! API — see `GEOCODING_GOOGLE_MAPS_ENABLED` / `.env.example` for why
//! it's off by default.
//!
//! `status` maps to `GeoEnrichError` as follows: `OK` → `Some(result)`;
//! `ZERO_RESULTS` → `Ok(None)` (not an error — same "nothing found"
//! semantics as every other provider); `OVER_QUERY_LIMIT` /
//! `OVER_DAILY_LIMIT` → `RateLimited` (transient, cascade moves on);
//! `REQUEST_DENIED` / `INVALID_REQUEST` / `UNKNOWN_ERROR` / anything else
//! → `InvalidResponse` (also transient per `is_transient()`, but worth
//! surfacing in logs since these usually mean a config problem, not "try
//! again later").
//!
//! `geometry.location_type` is mapped to `GeocodingResult::confidence` on
//! a best-effort 0–1 scale (`ROOFTOP` = 1.0 down to `APPROXIMATE` = 0.5)
//! — Google doesn't publish an exact confidence score, this is a
//! reasonable ordering, not a calibrated probability.
//!
//! Separately: the project owner also asked for hospital **website**
//! discovery via Google **Places**, which is a different API and a
//! different key from Maps Geocoding — see `providers::google_places`.
//! That lookup doesn't belong in this file; it's its own enrichment step,
//! not part of the geocoding cascade.

use serde::Deserialize;

use crate::error::GeoEnrichError;
use crate::{GeocodingProvider, GeocodingResult};

const DEFAULT_BASE_URL: &str = "https://maps.googleapis.com/maps/api/geocode/json";

pub struct GoogleMapsProvider {
    http: reqwest::Client,
    base_url: String,
    api_key: String,
}

impl GoogleMapsProvider {
    /// Returns `None` (rather than a provider you'd have to remember not
    /// to enable) when no key is configured, so callers building the
    /// cascade can simply `if let Some(p) = GoogleMapsProvider::from_config(...)`.
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

    /// Base URL override, for tests (point at a `wiremock` server).
    #[cfg(test)]
    fn with_base_url(http: reqwest::Client, base_url: String, api_key: String) -> Self {
        Self { http, base_url, api_key }
    }
}

#[derive(Debug, Deserialize)]
struct GeocodeResponse {
    status: String,
    #[serde(default)]
    results: Vec<GeocodeResult>,
    /// Present on some error statuses (`REQUEST_DENIED`, `INVALID_REQUEST`);
    /// absent on `OK`/`ZERO_RESULTS`. Purely for the `InvalidResponse`
    /// error message, so callers see *why* rather than just the status.
    #[serde(default)]
    error_message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GeocodeResult {
    formatted_address: Option<String>,
    geometry: Geometry,
}

#[derive(Debug, Deserialize)]
struct Geometry {
    location: Location,
    #[serde(default)]
    location_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Location {
    lat: f64,
    lng: f64,
}

/// Google doesn't publish a numeric confidence score — this maps
/// `geometry.location_type` onto the same rough `[0, 1]` scale the other
/// providers use, most-precise first. Unrecognized/missing values fall
/// back to `None` rather than guessing.
fn confidence_for(location_type: Option<&str>) -> Option<f64> {
    match location_type {
        Some("ROOFTOP") => Some(1.0),
        Some("RANGE_INTERPOLATED") => Some(0.9),
        Some("GEOMETRIC_CENTER") => Some(0.7),
        Some("APPROXIMATE") => Some(0.5),
        _ => None,
    }
}

#[async_trait::async_trait]
impl GeocodingProvider for GoogleMapsProvider {
    fn name(&self) -> &str {
        "google_maps"
    }

    async fn geocode(
        &self,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        let full_address = format!("{address}, {city}, {state} {zip}");

        let resp = self
            .http
            .get(&self.base_url)
            .query(&[("address", full_address.as_str()), ("key", self.api_key.as_str())])
            .send()
            .await?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GeoEnrichError::InvalidResponse(format!(
                "google_maps returned HTTP {status}: {body}"
            )));
        }

        let parsed: GeocodeResponse = resp
            .json()
            .await
            .map_err(|e| GeoEnrichError::InvalidResponse(format!("bad google_maps JSON: {e}")))?;

        match parsed.status.as_str() {
            "OK" => {
                let Some(top) = parsed.results.into_iter().next() else {
                    // Shouldn't happen per the API's contract (OK implies
                    // >=1 result), but don't panic on a malformed response.
                    return Ok(None);
                };
                Ok(Some(GeocodingResult {
                    latitude: top.geometry.location.lat,
                    longitude: top.geometry.location.lng,
                    formatted_address: top.formatted_address,
                    confidence: confidence_for(top.geometry.location_type.as_deref()),
                    website_url: None,
                }))
            }
            "ZERO_RESULTS" => Ok(None),
            "OVER_QUERY_LIMIT" | "OVER_DAILY_LIMIT" => Err(GeoEnrichError::RateLimited {
                provider: "google_maps".to_string(),
            }),
            other => Err(GeoEnrichError::InvalidResponse(format!(
                "google_maps status {other}: {}",
                parsed.error_message.unwrap_or_default()
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn parses_a_rooftop_match() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .and(query_param("key", "test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "OK",
                "results": [{
                    "formatted_address": "1108 Ross Clark Cir, Dothan, AL 36301, USA",
                    "geometry": {
                        "location": { "lat": 31.2156, "lng": -85.3614 },
                        "location_type": "ROOFTOP"
                    }
                }]
            })))
            .mount(&server)
            .await;

        let provider =
            GoogleMapsProvider::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let result = provider
            .geocode("1108 Ross Clark Cir", "Dothan", "AL", "36301")
            .await
            .unwrap()
            .expect("should have geocoded");

        assert_eq!(result.latitude, 31.2156);
        assert_eq!(result.longitude, -85.3614);
        assert_eq!(result.confidence, Some(1.0));
    }

    #[tokio::test]
    async fn zero_results_is_ok_none_not_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "ZERO_RESULTS",
                "results": []
            })))
            .mount(&server)
            .await;

        let provider =
            GoogleMapsProvider::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let result = provider.geocode("nowhere", "nowhere", "ZZ", "00000").await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn over_query_limit_maps_to_rate_limited() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "OVER_QUERY_LIMIT",
                "results": []
            })))
            .mount(&server)
            .await;

        let provider =
            GoogleMapsProvider::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let err = provider.geocode("x", "y", "z", "0").await.unwrap_err();
        assert!(err.is_transient());
        assert!(matches!(err, GeoEnrichError::RateLimited { .. }));
    }

    #[tokio::test]
    async fn request_denied_is_invalid_response() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "status": "REQUEST_DENIED",
                "error_message": "The provided API key is invalid.",
                "results": []
            })))
            .mount(&server)
            .await;

        let provider =
            GoogleMapsProvider::with_base_url(reqwest::Client::new(), format!("{}/", server.uri()), "test-key".to_string());
        let err = provider.geocode("x", "y", "z", "0").await.unwrap_err();
        assert!(matches!(err, GeoEnrichError::InvalidResponse(_)));
    }

    #[test]
    fn from_config_returns_none_when_disabled_or_keyless() {
        let http = reqwest::Client::new();
        assert!(GoogleMapsProvider::from_config(http.clone(), false, Some("key".to_string())).is_none());
        assert!(GoogleMapsProvider::from_config(http.clone(), true, None).is_none());
        assert!(GoogleMapsProvider::from_config(http.clone(), true, Some(String::new())).is_none());
        assert!(GoogleMapsProvider::from_config(http, true, Some("key".to_string())).is_some());
    }
}
