//! Google Maps Geocoding API — priority 3 (paid, disabled by default).
//!
//! **Stub — not yet implemented.** Per the project owner's decision,
//! implement this fully so it's ready to enable, but leave
//! `GEOCODING_GOOGLE_MAPS_ENABLED=false` in `.env` until a key is
//! supplied — `backend`'s config layer should simply not construct this
//! provider (and not add it to the cascade) when the flag is off, rather
//! than constructing-but-skipping it.
//!
//! Not verified live this session; confirm before implementing:
//! `GET https://maps.googleapis.com/maps/api/geocode/json?address={...}&key={api_key}`.
//!
//! Separately: the project owner also asked for hospital **website**
//! discovery via Google **Places** (Find Place / Place Details), which is
//! a different API and a different key from Maps Geocoding — don't
//! conflate the two when that gets built. That lookup doesn't belong in
//! this file; it's its own enrichment step, not part of the geocoding
//! cascade.

use crate::error::GeoEnrichError;
use crate::{GeocodingProvider, GeocodingResult};

pub struct GoogleMapsProvider {
    http: reqwest::Client,
    api_key: String,
}

impl GoogleMapsProvider {
    /// Returns `None` (rather than a provider you'd have to remember not
    /// to enable) when no key is configured, so callers building the
    /// cascade can simply `if let Some(p) = GoogleMapsProvider::from_config(...)`.
    pub fn from_config(http: reqwest::Client, enabled: bool, api_key: Option<String>) -> Option<Self> {
        match (enabled, api_key) {
            (true, Some(api_key)) if !api_key.is_empty() => Some(Self { http, api_key }),
            _ => None,
        }
    }
}

#[async_trait::async_trait]
impl GeocodingProvider for GoogleMapsProvider {
    fn name(&self) -> &str {
        "google_maps"
    }

    async fn geocode(
        &self,
        _address: &str,
        _city: &str,
        _state: &str,
        _zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        let _ = (&self.http, &self.api_key);
        Err(GeoEnrichError::NotImplemented("google_maps"))
    }
}
