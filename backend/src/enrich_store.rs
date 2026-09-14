//! `geo_enrich::enricher::UnenrichedHospitalStore` implemented against
//! this crate's `SqlitePool`. This is the seam `geo-enrich`'s doc
//! comments point to: it lets that crate stay database-agnostic while
//! `backend` still gets a real, persisted enrichment loop.

use async_trait::async_trait;
use sqlx::SqlitePool;

use geo_enrich::enricher::{EnrichmentOutcome, EnrichmentTarget, UnenrichedHospitalStore};
use geo_enrich::GeoEnrichError;

use crate::db::queries;

pub struct HospitalStore {
    pool: SqlitePool,
}

impl HospitalStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UnenrichedHospitalStore for HospitalStore {
    async fn list_unenriched(&self, limit: u32) -> Result<Vec<EnrichmentTarget>, GeoEnrichError> {
        let rows = queries::list_unenriched_hospitals(&self.pool, limit)
            .await
            .map_err(|e| GeoEnrichError::Store(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(|(facility_id, facility_name, address, city, state, zip_code)| EnrichmentTarget {
                facility_id,
                facility_name,
                address,
                city,
                state,
                zip_code,
            })
            .collect())
    }

    async fn save_enrichment(&self, outcome: &EnrichmentOutcome) -> Result<(), GeoEnrichError> {
        let geocode = outcome.geocode.as_ref().map(|g| {
            (
                g.provider.as_str(),
                g.latitude,
                g.longitude,
                g.formatted_address.as_deref(),
                g.confidence,
            )
        });

        queries::save_enrichment(&self.pool, &outcome.facility_id, geocode, outcome.website_url.as_deref())
            .await
            .map_err(|e| GeoEnrichError::Store(e.to_string()))
    }
}
