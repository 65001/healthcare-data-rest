//! Stage 2 of the pipeline: cascading geocoding.
//!
//! **Status: skeleton.** The [`GeocodingProvider`] trait, [`GeocodingResult`],
//! and [`crate::cascade::CascadingGeocoder`] orchestrator below are fully
//! implemented per Architecture.md. The three provider bodies
//! (`providers::census`, `providers::nominatim`, `providers::google_maps`)
//! and the batch `enricher` loop are stubs that compile and document what
//! they need — see each module's doc comment and IMPLEMENTATION_NOTES.md
//! for the plan. This crate is not wired into `backend` yet: the
//! `POST /api/pipeline/enrich` route returns 501 until it is.
//!
//! Two decisions from the project owner that the next implementation pass
//! should follow:
//! - Google Maps geocoding: implement fully, but leave
//!   `GEOCODING_GOOGLE_MAPS_ENABLED=false` by default (no key configured
//!   yet).
//! - Website URL discovery: Architecture.md's plan to pull `website_url`
//!   out of geocoder "map/place data" doesn't hold up — Census and
//!   Nominatim's geocoding endpoints don't reliably return a business
//!   website. The decision was to add a dedicated Google **Places** API
//!   lookup for this (a different API/key from Google Maps Geocoding),
//!   as its own step, rather than trying to scrape it out of the geocode
//!   response. That step isn't implemented in this pass either.

pub mod cascade;
pub mod enricher;
pub mod error;
pub mod providers;

pub use cascade::CascadingGeocoder;
pub use error::GeoEnrichError;

/// Result of successfully geocoding one address.
#[derive(Debug, Clone, PartialEq)]
pub struct GeocodingResult {
    pub latitude: f64,
    pub longitude: f64,
    pub formatted_address: Option<String>,
    /// Best-effort confidence score in `[0, 1]`, when the provider gives one.
    pub confidence: Option<f64>,
}

/// A single provider in the geocoding cascade.
///
/// Implementors return:
/// - `Ok(Some(result))` — successfully geocoded, stop the cascade.
/// - `Ok(None)`         — this provider can't resolve the address, try the next.
/// - `Err(e)` where `e.is_transient()` — temporary failure, try the next.
/// - `Err(e)` otherwise — stop the cascade and surface the error.
#[async_trait::async_trait]
pub trait GeocodingProvider: Send + Sync {
    /// Human-readable name (e.g. "us_census", "nominatim", "google_maps"),
    /// used for logging and for the `geo_provider` column.
    fn name(&self) -> &str;

    async fn geocode(
        &self,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError>;
}
