//! Provider implementations for the geocoding cascade.
//!
//! Priority order per Architecture.md: `census` (free, batch) →
//! `nominatim` (free, 1 req/sec) → `google_maps` (paid, disabled by
//! default). All three are stubs in this pass — see each module.

pub mod census;
pub mod google_maps;
pub mod nominatim;

pub use census::CensusProvider;
pub use google_maps::GoogleMapsProvider;
pub use nominatim::NominatimProvider;
