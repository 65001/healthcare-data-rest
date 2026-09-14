//! Batch enrichment loop: pulls un-enriched hospitals, geocodes them
//! through a [`CascadingGeocoder`] with bounded concurrency, and would
//! persist results back to the `hospitals` table.
//!
//! **Stub.** The concurrency shape (`tokio::sync::Semaphore`, per
//! Architecture.md) is here; the actual DB read/write is not, because that
//! needs the `backend` crate's connection pool (`sqlx::SqlitePool`), which
//! would make `geo-enrich` depend on `backend` — backwards from the
//! dependency graph in Architecture.md. The real version of this should
//! take a small trait (e.g. `UnenrichedHospitalStore`) that `backend`
//! implements against its pool, so `geo-enrich` stays DB-agnostic. Wiring
//! that up, plus finishing at least the Census provider, is the next pass.

use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::{CascadingGeocoder, GeoEnrichError};

pub struct EnrichmentConfig {
    /// Max concurrent in-flight geocode requests. Architecture.md's
    /// default is 10.
    pub concurrency: usize,
}

impl Default for EnrichmentConfig {
    fn default() -> Self {
        Self { concurrency: 10 }
    }
}

/// One hospital record's worth of the fields a geocoder needs.
pub struct EnrichmentTarget {
    pub facility_id: String,
    pub address: String,
    pub city: String,
    pub state: String,
    pub zip_code: String,
}

/// Would geocode `targets` with bounded concurrency and return
/// `(facility_id, provider_name, GeocodingResult)` for everything that
/// resolved. Left unimplemented: this needs the DB-agnostic store trait
/// described above before it can do anything useful.
pub async fn enrich_batch(
    _geocoder: &CascadingGeocoder,
    _targets: Vec<EnrichmentTarget>,
    _config: EnrichmentConfig,
) -> Result<Vec<(String, String, crate::GeocodingResult)>, GeoEnrichError> {
    let _semaphore = Arc::new(Semaphore::new(_config.concurrency));
    Err(GeoEnrichError::NotImplemented("enrich_batch"))
}
