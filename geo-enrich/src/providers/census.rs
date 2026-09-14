//! US Census Bureau Geocoder — priority 1 (free, no API key).
//!
//! **Stub — not yet implemented.** This is the recommended next provider
//! to finish: it's free, keyless, and (per Architecture.md) covers an
//! estimated 80-90% of US hospital addresses on its own.
//!
//! Not verified live in this session (unlike the `cms-ingest` endpoint) —
//! confirm against current docs before implementing:
//! - Single-address lookup: `GET https://geocoding.geo.census.gov/geocoder/locations/address`
//!   with `street`, `city`, `state`, `zip`, `benchmark=Public_AR_Current`,
//!   `format=json`.
//! - Batch lookup (up to 10,000 addresses per request, matching
//!   Architecture.md's "Multi-Pass Strategy" note about batch mode
//!   reducing HTTP calls): `POST
//!   https://geocoding.geo.census.gov/geocoder/locations/addressbatch`
//!   with a CSV file body (`id,street,city,state,zip` columns) — worth
//!   using instead of one request per hospital once this crate is wired
//!   into `backend`.

use crate::error::GeoEnrichError;
use crate::{GeocodingProvider, GeocodingResult};

pub struct CensusProvider {
    http: reqwest::Client,
}

impl CensusProvider {
    pub fn new(http: reqwest::Client) -> Self {
        Self { http }
    }
}

#[async_trait::async_trait]
impl GeocodingProvider for CensusProvider {
    fn name(&self) -> &str {
        "us_census"
    }

    async fn geocode(
        &self,
        _address: &str,
        _city: &str,
        _state: &str,
        _zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        let _ = &self.http;
        Err(GeoEnrichError::NotImplemented("us_census"))
    }
}
