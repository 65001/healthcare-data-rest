//! Stage 2 of the pipeline: cascading geocoding, plus website discovery.
//!
//! **Status:**
//! - `providers::census`, `providers::nominatim`, `providers::google_maps`
//!   — **all fully implemented**, forming the geocoding cascade (in that
//!   priority order — free/best-hit-rate first, paid/last-resort last).
//!   Nominatim self-throttles to 1 req/sec per its usage policy and
//!   opportunistically reads a website out of OSM's `extratags` when
//!   present; Census also has an unused (not wired in) bulk batch path,
//!   see its doc comment.
//! - `providers::google_places` — **fully implemented.** A dedicated
//!   website lookup (Places API (New), Text Search), used as the fallback
//!   when no geocode provider turned up a website. Separate from
//!   `providers::google_maps` (Maps *Geocoding*, a different API/key) —
//!   don't conflate the two.
//! - `enricher` — implemented: bounded-concurrency batch loop wiring the
//!   cascade + Places fallback together against a small `UnenrichedHospitalStore`
//!   trait, so this crate still doesn't depend on `backend`/`sqlx` directly
//!   (Architecture.md's dependency graph stays one-directional). Also
//!   supports an opt-in `retry_incomplete` backfill pass for hospitals
//!   that already ran once but are still missing a field.
//!
//! `backend`'s `POST /api/pipeline/enrich` route now runs this for real —
//! see `backend/src/routes/pipeline.rs` and `backend/src/enrich_store.rs`.

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
    /// Best-effort website URL, when the provider's response happened to
    /// carry one (Nominatim's OSM `extratags`, e.g.). `None` from
    /// providers that don't supply this (Census) or when it just wasn't
    /// tagged. This is opportunistic only — `providers::google_places` is
    /// the dedicated fallback for when this comes back `None`.
    pub website_url: Option<String>,
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
