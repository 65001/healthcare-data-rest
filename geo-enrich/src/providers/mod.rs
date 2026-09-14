//! Provider implementations for the geocoding cascade, plus website
//! discovery.
//!
//! Geocoding cascade priority order per Architecture.md: `census` (free,
//! single-address lookup — its `geocode_batch` method is a faster bulk
//! path that exists but isn't wired into the cascade, see its doc
//! comment) → `nominatim` (free, 1 req/sec) → `google_maps` (paid,
//! disabled by default). All three are fully implemented as of this
//! pass; `backend/src/routes/pipeline.rs` is what actually assembles
//! them into a `CascadingGeocoder`, gated by each `GEOCODING_*_ENABLED`
//! flag.
//!
//! `google_places` is *not* part of the geocoding cascade — it's a
//! separate website-discovery fallback, used when a geocode provider's
//! opportunistic website read (currently only `nominatim`'s
//! `extratags.website`) comes up empty. See its doc comment and
//! `enricher::enrich_batch`.

pub mod census;
pub mod google_maps;
pub mod google_places;
pub mod nominatim;

pub use census::CensusProvider;
pub use google_maps::GoogleMapsProvider;
pub use google_places::GooglePlacesClient;
pub use nominatim::NominatimProvider;
