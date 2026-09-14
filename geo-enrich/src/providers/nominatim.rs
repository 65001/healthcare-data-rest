//! Nominatim / OpenStreetMap — priority 2 (free, 1 req/sec).
//!
//! **Stub — not yet implemented.** Not verified live this session; confirm
//! before implementing:
//! - `GET https://nominatim.openstreetmap.org/search?q={street},{city},{state}
//!   {zip}&format=jsonv2&addressdetails=1&extratags=1`
//! - Nominatim's usage policy (<https://operations.osmfoundation.org/policies/nominatim/>)
//!   requires a descriptive `User-Agent` identifying this project and caps
//!   the public instance at 1 request/second — self-hosting is the stated
//!   option for higher throughput (Architecture.md's provider table
//!   mentions this). Don't parallelize calls to the public instance.
//! - `extratags=1` is what the earlier project decision (see
//!   IMPLEMENTATION_NOTES.md) meant by "best-effort via Nominatim" for
//!   `website_url` before that plan was superseded by the Google Places
//!   decision — that field is present often enough to be worth reading
//!   opportunistically here even though it's no longer the primary path.

use crate::error::GeoEnrichError;
use crate::{GeocodingProvider, GeocodingResult};

pub struct NominatimProvider {
    http: reqwest::Client,
}

impl NominatimProvider {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

#[async_trait::async_trait]
impl GeocodingProvider for NominatimProvider {
    fn name(&self) -> &str {
        "nominatim"
    }

    async fn geocode(
        &self,
        _address: &str,
        _city: &str,
        _state: &str,
        _zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        let _ = &self.http;
        Err(GeoEnrichError::NotImplemented("nominatim"))
    }
}
