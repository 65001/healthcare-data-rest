//! Provider implementations for the geocoding cascade, plus website
//! discovery.
//!
//! Geocoding cascade priority order per Architecture.md: `census` (free,
//! batch) → `nominatim` (free, 1 req/sec) → `google_maps` (paid, disabled
//! by default). `nominatim` is fully implemented this pass; `census` and
//! `google_maps` are still stubs — see each module's doc comment.
//!
//! `google_places` is *not* part of the geocoding cascade — it's a
//! separate website-discovery fallback, used when `nominatim`'s
//! opportunistic `extratags.website` read comes up empty. See its doc
//! comment and `enricher::enrich_batch`.

pub mod census;
pub mod google_maps;
pub mod google_places;
pub mod nominatim;

pub use census::CensusProvider;
pub use google_maps::GoogleMapsProvider;
pub use google_places::GooglePlacesClient;
pub use nominatim::NominatimProvider;
