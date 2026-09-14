//! Batch enrichment loop: pulls un-enriched hospitals from a small
//! DB-agnostic store trait, geocodes them through a [`CascadingGeocoder`]
//! with bounded concurrency (`tokio::sync::Semaphore`, per
//! Architecture.md), opportunistically picks up a website from the
//! geocode result, falls back to a Google Places lookup when the website
//! is still missing, and persists everything back through the same store
//! trait.
//!
//! [`UnenrichedHospitalStore`] is what keeps this crate from depending on
//! `backend`/`sqlx` — Architecture.md's dependency graph is
//! one-directional (`backend` depends on `geo-enrich`, not the reverse).
//! `backend::enrich_store::HospitalStore` is the real implementation,
//! against its `SqlitePool`.

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Semaphore;

use crate::providers::GooglePlacesClient;
use crate::{CascadingGeocoder, GeoEnrichError};

pub struct EnrichmentConfig {
    /// Max concurrent in-flight enrichment tasks. Each task may make one
    /// geocode call (self-throttled to 1 req/sec if it lands on
    /// Nominatim, regardless of this concurrency) and, on a website
    /// cache miss, one Google Places call. Architecture.md's default for
    /// the probe stage is 10; reused here.
    pub concurrency: usize,
}

impl Default for EnrichmentConfig {
    fn default() -> Self {
        Self { concurrency: 10 }
    }
}

/// One hospital's worth of the fields enrichment needs to read.
#[derive(Debug, Clone)]
pub struct EnrichmentTarget {
    pub facility_id: String,
    pub facility_name: String,
    pub address: String,
    pub city: String,
    pub state: String,
    pub zip_code: String,
}

#[derive(Debug, Clone)]
pub struct GeocodeOutcome {
    /// Which provider in the cascade resolved this (`"nominatim"`, once
    /// Census is implemented also `"us_census"` / `"google_maps"`).
    pub provider: String,
    pub latitude: f64,
    pub longitude: f64,
    pub formatted_address: Option<String>,
    pub confidence: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct EnrichmentOutcome {
    pub facility_id: String,
    /// `None` when no provider in the cascade could geocode this address
    /// — still worth persisting (see `UnenrichedHospitalStore::save_enrichment`'s
    /// doc comment) so this hospital doesn't get re-tried forever on
    /// every enrich run.
    pub geocode: Option<GeocodeOutcome>,
    pub website_url: Option<String>,
    /// Which source actually supplied `website_url` — the geocode
    /// provider's name (when it came from e.g. Nominatim's `extratags`)
    /// or `"google_places"`. `None` alongside `website_url: None` means
    /// nothing found a website at all. Architecture.md's schema has no
    /// column for this; it's informational (logged), not persisted.
    pub website_source: Option<String>,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct EnrichmentSummary {
    pub total: u64,
    /// Outcomes successfully computed AND persisted via the store.
    pub completed: u64,
    /// Persist failures + panicked tasks.
    pub failed: u64,
}

/// Implemented by `backend` against its `SqlitePool`. Kept minimal and
/// database-agnostic on purpose — see this module's doc comment.
#[async_trait]
pub trait UnenrichedHospitalStore: Send + Sync {
    /// Up to `limit` hospitals with `enriched_at IS NULL`, per
    /// Architecture.md's "skips hospitals that already have `enriched_at`
    /// set" note.
    async fn list_unenriched(&self, limit: u32) -> Result<Vec<EnrichmentTarget>, GeoEnrichError>;

    /// Persist one outcome. Implementations should set `enriched_at`
    /// even when `outcome.geocode` is `None` — otherwise a hospital every
    /// provider fails on gets re-fetched and re-attempted on every future
    /// enrich run instead of being skipped like Architecture.md intends.
    async fn save_enrichment(&self, outcome: &EnrichmentOutcome) -> Result<(), GeoEnrichError>;
}

/// Geocodes and resolves a website for one hospital. Degrades gracefully
/// rather than propagating errors: a geocode or Places failure for one
/// hospital is logged and treated as "not found" for that hospital,
/// rather than aborting the whole batch — consistent with `cms-ingest`'s
/// per-row error handling.
async fn enrich_one(
    target: &EnrichmentTarget,
    geocoder: &CascadingGeocoder,
    places: Option<&GooglePlacesClient>,
) -> EnrichmentOutcome {
    let geocode = match geocoder
        .geocode(&target.address, &target.city, &target.state, &target.zip_code)
        .await
    {
        Ok(found) => found,
        Err(e) => {
            tracing::warn!(
                facility_id = %target.facility_id,
                error = %e,
                "geocoding cascade failed for this hospital"
            );
            None
        }
    };

    let mut website_url = None;
    let mut website_source = None;

    if let Some((provider, result)) = &geocode {
        if let Some(url) = &result.website_url {
            website_url = Some(url.clone());
            website_source = Some(provider.clone());
        }
    }

    if website_url.is_none() {
        if let Some(places) = places {
            match places
                .find_website(
                    &target.facility_name,
                    &target.address,
                    &target.city,
                    &target.state,
                    &target.zip_code,
                )
                .await
            {
                Ok(Some(url)) => {
                    website_url = Some(url);
                    website_source = Some("google_places".to_string());
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(
                        facility_id = %target.facility_id,
                        error = %e,
                        "google places lookup failed for this hospital"
                    );
                }
            }
        }
    }

    EnrichmentOutcome {
        facility_id: target.facility_id.clone(),
        geocode: geocode.map(|(provider, r)| GeocodeOutcome {
            provider,
            latitude: r.latitude,
            longitude: r.longitude,
            formatted_address: r.formatted_address,
            confidence: r.confidence,
        }),
        website_url,
        website_source,
    }
}

/// Fetches up to `limit` un-enriched hospitals from `store`, enriches
/// them with bounded concurrency, and persists each result back through
/// `store` as it completes. `on_total` fires once, right after the
/// initial fetch, with how many hospitals this run will process;
/// `on_progress` fires after each one (saved or not) — together these
/// let a caller with a job tracker (`backend`) report progress
/// incrementally instead of only once at the end. This matters here more
/// than in `cms-ingest`: when Nominatim is in the cascade, every geocode
/// call is serialized to roughly 1/sec regardless of `concurrency`, so a
/// few hundred hospitals can take minutes — see `EnrichmentConfig` and
/// `backend`'s `ENRICH_BATCH_LIMIT`.
///
/// Progress callback ordering follows task-spawn order, not true
/// completion order (no `FuturesUnordered`) — the running totals are
/// still accurate, just not reported in strict wall-clock-completion
/// order. Kept simple deliberately; revisit if that ordering matters.
pub async fn enrich_batch<OnTotal, OnProgress>(
    store: &dyn UnenrichedHospitalStore,
    geocoder: Arc<CascadingGeocoder>,
    places: Option<Arc<GooglePlacesClient>>,
    config: EnrichmentConfig,
    limit: u32,
    on_total: OnTotal,
    mut on_progress: OnProgress,
) -> Result<EnrichmentSummary, GeoEnrichError>
where
    OnTotal: FnOnce(u64),
    OnProgress: FnMut(&EnrichmentOutcome, bool),
{
    let targets = store.list_unenriched(limit).await?;
    let total = targets.len() as u64;
    on_total(total);

    let semaphore = Arc::new(Semaphore::new(config.concurrency.max(1)));
    let mut handles = Vec::with_capacity(targets.len());

    for target in targets {
        let semaphore = semaphore.clone();
        let geocoder = geocoder.clone();
        let places = places.clone();

        handles.push(tokio::spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("semaphore is never closed while handles are outstanding");
            enrich_one(&target, &geocoder, places.as_deref()).await
        }));
    }

    let mut completed = 0u64;
    let mut failed = 0u64;

    for handle in handles {
        let outcome = match handle.await {
            Ok(outcome) => outcome,
            Err(join_err) => {
                tracing::warn!(error = %join_err, "enrichment task panicked");
                failed += 1;
                continue;
            }
        };

        let saved = match store.save_enrichment(&outcome).await {
            Ok(()) => {
                completed += 1;
                true
            }
            Err(e) => {
                tracing::warn!(
                    facility_id = %outcome.facility_id,
                    error = %e,
                    "failed to persist enrichment result"
                );
                failed += 1;
                false
            }
        };

        on_progress(&outcome, saved);
    }

    Ok(EnrichmentSummary { total, completed, failed })
}
