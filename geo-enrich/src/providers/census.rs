//! US Census Bureau Geocoder — priority 1 (free, no API key).
//!
//! Implements single-address lookup via the `GeocodingProvider` trait:
//! `GET https://geocoding.geo.census.gov/geocoder/locations/address` — this
//! is what `backend/src/routes/pipeline.rs` wires into the cascade,
//! ahead of `nominatim`, since it's free and generally the highest
//! first-try success rate of the three providers.
//!
//! Also provides bulk batch geocoding via:
//! `POST https://geocoding.geo.census.gov/geocoder/locations/addressbatch`
//! which processes up to 10,000 addresses per HTTP request in seconds.
//! **This batch path is not currently called from anywhere** —
//! `enricher::enrich_batch` processes hospitals one at a time through
//! `CascadingGeocoder` (bounded concurrency, not true request batching),
//! so `geocode_batch`/`geocode_batch_chunk` are unused outside their own
//! unit test. It's a legitimate future optimization (one HTTP call per
//! 1,000 hospitals instead of one per hospital) but wiring it in would
//! mean a separate code path from the rest of the cascade — worth doing
//! deliberately, not as a drive-by change.

use std::collections::HashMap;
use std::time::Duration;

use reqwest::multipart::{Form, Part};
use serde::Deserialize;

use crate::enricher::EnrichmentTarget;
use crate::error::GeoEnrichError;
use crate::{GeocodingProvider, GeocodingResult};

const DEFAULT_SINGLE_URL: &str = "https://geocoding.geo.census.gov/geocoder/locations/address";
const DEFAULT_BATCH_URL: &str = "https://geocoding.geo.census.gov/geocoder/locations/addressbatch";
const DEFAULT_BENCHMARK: &str = "Public_AR_Current";

pub struct CensusProvider {
    http: reqwest::Client,
    single_url: String,
    batch_url: String,
    benchmark: String,
}

#[derive(Debug, Deserialize)]
struct CensusJsonResponse {
    result: CensusJsonResult,
}

#[derive(Debug, Deserialize)]
struct CensusJsonResult {
    #[serde(rename = "addressMatches", default)]
    address_matches: Vec<CensusAddressMatch>,
}

#[derive(Debug, Deserialize)]
struct CensusAddressMatch {
    coordinates: CensusCoordinates,
    #[serde(rename = "matchedAddress")]
    matched_address: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CensusCoordinates {
    x: f64, // longitude
    y: f64, // latitude
}

impl CensusProvider {
    pub fn new(http: reqwest::Client) -> Self {
        Self {
            http,
            single_url: DEFAULT_SINGLE_URL.to_string(),
            batch_url: DEFAULT_BATCH_URL.to_string(),
            benchmark: DEFAULT_BENCHMARK.to_string(),
        }
    }

    pub fn with_defaults() -> Result<Self, GeoEnrichError> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .user_agent("cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)")
            .build()?;
        Ok(Self::new(http))
    }

    #[cfg(test)]
    pub(crate) fn with_single_url(mut self, url: String) -> Self {
        self.single_url = url;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_batch_url(mut self, url: String) -> Self {
        self.batch_url = url;
        self
    }

    /// Performs batch geocoding of addresses in chunks (up to 1,000 per request)
    /// using the Census `/locations/addressbatch` endpoint.
    /// Returns a map of `facility_id -> GeocodingResult` for all matched addresses.
    pub async fn geocode_batch(
        &self,
        targets: &[EnrichmentTarget],
    ) -> Result<HashMap<String, GeocodingResult>, GeoEnrichError> {
        let mut results = HashMap::new();
        if targets.is_empty() {
            return Ok(results);
        }

        // Census supports up to 10,000, but chunks of 1,000 ensure reliable HTTP transfers
        const CHUNK_SIZE: usize = 1000;

        for chunk in targets.chunks(CHUNK_SIZE) {
            let chunk_results = self.geocode_batch_chunk(chunk).await?;
            results.extend(chunk_results);
        }

        Ok(results)
    }

    async fn geocode_batch_chunk(
        &self,
        targets: &[EnrichmentTarget],
    ) -> Result<HashMap<String, GeocodingResult>, GeoEnrichError> {
        let mut csv_payload = String::with_capacity(targets.len() * 64);
        for t in targets {
            // Census batch format: ID,Street,City,State,Zip
            // Punctuation like commas in street address must be stripped/replaced
            let clean_street = t.address.replace(',', " ");
            let clean_city = t.city.replace(',', " ");
            csv_payload.push_str(&format!(
                "{},{},{},{},{}\n",
                t.facility_id.trim(),
                clean_street.trim(),
                clean_city.trim(),
                t.state.trim(),
                t.zip_code.trim()
            ));
        }

        let form = Form::new()
            .text("benchmark", self.benchmark.clone())
            .part(
                "addressFile",
                Part::bytes(csv_payload.into_bytes())
                    .file_name("addresses.csv")
                    .mime_str("text/csv")
                    .map_err(|e| GeoEnrichError::InvalidResponse(e.to_string()))?,
            );

        let resp = self
            .http
            .post(&self.batch_url)
            .multipart(form)
            .send()
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(GeoEnrichError::RateLimited {
                provider: "us_census".to_string(),
            });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GeoEnrichError::InvalidResponse(format!(
                "Census batch HTTP {}: {}",
                status, body
            )));
        }

        let body = resp.text().await?;
        Ok(parse_census_batch_response(&body))
    }
}

fn parse_census_batch_response(csv_body: &str) -> HashMap<String, GeocodingResult> {
    let mut map = HashMap::new();

    for line in csv_body.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }

        // The Census batch response is CSV format with quoted fields:
        // "ID","Original Address","Match/No_Match","Exact/Non_Exact","Matched Address","lon,lat","TIGER/Line ID","Side"
        let fields = parse_csv_line(line);
        if fields.len() < 6 {
            continue;
        }

        let id = fields[0].trim();
        let match_indicator = fields[2].trim();
        if !match_indicator.eq_ignore_ascii_case("Match") {
            continue;
        }

        let match_type = fields[3].trim();
        let matched_address = fields[4].trim();
        let coords_str = fields[5].trim();

        // coords_str is "lon,lat"
        let mut coords = coords_str.split(',');
        let lon = coords.next().and_then(|s| s.trim().parse::<f64>().ok());
        let lat = coords.next().and_then(|s| s.trim().parse::<f64>().ok());

        if let (Some(lon), Some(lat)) = (lon, lat) {
            let confidence = if match_type.eq_ignore_ascii_case("Exact") {
                1.0
            } else {
                0.8
            };

            map.insert(
                id.to_string(),
                GeocodingResult {
                    latitude: lat,
                    longitude: lon,
                    formatted_address: if matched_address.is_empty() {
                        None
                    } else {
                        Some(matched_address.to_string())
                    },
                    confidence: Some(confidence),
                    website_url: None,
                },
            );
        }
    }

    map
}

/// Simple CSV line splitter that respects quoted strings.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;

    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes && chars.peek() == Some(&'"') {
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = !in_quotes;
                }
            }
            ',' if !in_quotes => {
                fields.push(current.trim().to_string());
                current.clear();
            }
            _ => {
                current.push(c);
            }
        }
    }
    fields.push(current.trim().to_string());
    fields
}

#[async_trait::async_trait]
impl GeocodingProvider for CensusProvider {
    fn name(&self) -> &str {
        "us_census"
    }

    async fn geocode(
        &self,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        let resp = self
            .http
            .get(&self.single_url)
            .query(&[
                ("street", address),
                ("city", city),
                ("state", state),
                ("zip", zip),
                ("benchmark", &self.benchmark),
                ("format", "json"),
            ])
            .send()
            .await?;

        let status = resp.status();
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            return Err(GeoEnrichError::RateLimited {
                provider: "us_census".to_string(),
            });
        }
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            return Err(GeoEnrichError::InvalidResponse(format!(
                "Census single HTTP {}: {}",
                status, body
            )));
        }

        let parsed: CensusJsonResponse = resp.json().await?;
        let Some(first_match) = parsed.result.address_matches.into_iter().next() else {
            return Ok(None);
        };

        Ok(Some(GeocodingResult {
            latitude: first_match.coordinates.y,
            longitude: first_match.coordinates.x,
            formatted_address: first_match.matched_address,
            confidence: Some(1.0),
            website_url: None,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn geocodes_single_address_success() {
        let server = MockServer::start().await;
        let json_body = serde_json::json!({
            "result": {
                "addressMatches": [{
                    "coordinates": {
                        "x": -85.3614,
                        "y": 31.2156
                    },
                    "matchedAddress": "1108 ROSS CLARK CIR, DOTHAN, AL, 36301"
                }]
            }
        });

        Mock::given(method("GET"))
            .and(path("/geocoder/locations/address"))
            .and(query_param("street", "1108 ROSS CLARK CIRCLE"))
            .and(query_param("city", "DOTHAN"))
            .and(query_param("state", "AL"))
            .and(query_param("zip", "36301"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json_body))
            .mount(&server)
            .await;

        let provider = CensusProvider::with_defaults()
            .unwrap()
            .with_single_url(format!("{}/geocoder/locations/address", server.uri()));

        let res = provider
            .geocode("1108 ROSS CLARK CIRCLE", "DOTHAN", "AL", "36301")
            .await
            .unwrap()
            .expect("should find match");

        assert_eq!(res.latitude, 31.2156);
        assert_eq!(res.longitude, -85.3614);
        assert_eq!(
            res.formatted_address.as_deref(),
            Some("1108 ROSS CLARK CIR, DOTHAN, AL, 36301")
        );
        assert_eq!(res.confidence, Some(1.0));
    }

    #[tokio::test]
    async fn returns_none_when_no_match() {
        let server = MockServer::start().await;
        let json_body = serde_json::json!({
            "result": {
                "addressMatches": []
            }
        });

        Mock::given(method("GET"))
            .and(path("/geocoder/locations/address"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json_body))
            .mount(&server)
            .await;

        let provider = CensusProvider::with_defaults()
            .unwrap()
            .with_single_url(format!("{}/geocoder/locations/address", server.uri()));

        let res = provider
            .geocode("999 Unknown", "Nowhere", "ZZ", "00000")
            .await
            .unwrap();

        assert!(res.is_none());
    }

    #[test]
    fn parses_batch_csv_response() {
        let sample = r#"
"010001","1108 ROSS CLARK CIRCLE, DOTHAN, AL, 36301","Match","Exact","1108 ROSS CLARK CIR, DOTHAN, AL, 36301","-85.361446991439,31.21566695385","101238843","R"
"010005","2505 U S HIGHWAY 431 NORTH, BOAZ, AL, 35957","Match","Non_Exact","2505 US HWY 431, BOAZ, AL, 35957","-86.159366335505,34.221221654827","640319453","R"
"010009","UNKNOWN ST, NOWHERE, AL, 00000","No_Match"
"#;

        let map = parse_census_batch_response(sample);
        assert_eq!(map.len(), 2);

        let h1 = map.get("010001").unwrap();
        assert_eq!(h1.confidence, Some(1.0));
        assert_eq!(h1.latitude, 31.21566695385);
        assert_eq!(h1.longitude, -85.361446991439);

        let h2 = map.get("010005").unwrap();
        assert_eq!(h2.confidence, Some(0.8));

        assert!(!map.contains_key("010009"));
    }
}
