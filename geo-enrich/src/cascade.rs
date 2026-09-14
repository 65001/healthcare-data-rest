//! The fallback-chain orchestrator. Fully implemented per Architecture.md
//! — this part doesn't depend on any provider being finished, since it
//! just walks whatever providers it's given.

use crate::{error::GeoEnrichError, GeocodingProvider, GeocodingResult};

pub struct CascadingGeocoder {
    providers: Vec<Box<dyn GeocodingProvider>>,
}

impl CascadingGeocoder {
    pub fn new(providers: Vec<Box<dyn GeocodingProvider>>) -> Self {
        Self { providers }
    }

    /// Try each provider in order. Stops on first success (`Ok(Some(_))`),
    /// a fatal error, or the list running out (`Ok(None)`).
    pub async fn geocode(
        &self,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<(String, GeocodingResult)>, GeoEnrichError> {
        for provider in &self.providers {
            match provider.geocode(address, city, state, zip).await {
                Ok(Some(result)) => return Ok(Some((provider.name().to_string(), result))),
                Ok(None) => continue,
                Err(e) if e.is_transient() => {
                    tracing::warn!(provider = provider.name(), error = %e, "transient failure, trying next provider");
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    struct AlwaysNone;
    #[async_trait]
    impl GeocodingProvider for AlwaysNone {
        fn name(&self) -> &str {
            "always_none"
        }
        async fn geocode(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<GeocodingResult>, GeoEnrichError> {
            Ok(None)
        }
    }

    struct AlwaysTransientThenNone;
    #[async_trait]
    impl GeocodingProvider for AlwaysTransientThenNone {
        fn name(&self) -> &str {
            "flaky"
        }
        async fn geocode(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<GeocodingResult>, GeoEnrichError> {
            Err(GeoEnrichError::RateLimited { provider: "flaky".into() })
        }
    }

    struct AlwaysSucceeds;
    #[async_trait]
    impl GeocodingProvider for AlwaysSucceeds {
        fn name(&self) -> &str {
            "reliable"
        }
        async fn geocode(&self, _: &str, _: &str, _: &str, _: &str) -> Result<Option<GeocodingResult>, GeoEnrichError> {
            Ok(Some(GeocodingResult {
                latitude: 1.0,
                longitude: 2.0,
                formatted_address: None,
                confidence: None,
            }))
        }
    }

    #[tokio::test]
    async fn falls_through_to_a_later_provider_on_none_and_transient_error() {
        let cascade = CascadingGeocoder::new(vec![
            Box::new(AlwaysNone),
            Box::new(AlwaysTransientThenNone),
            Box::new(AlwaysSucceeds),
        ]);
        let (provider, _) = cascade
            .geocode("1 Main St", "Anytown", "CA", "90210")
            .await
            .unwrap()
            .expect("should have found a result");
        assert_eq!(provider, "reliable");
    }

    #[tokio::test]
    async fn returns_none_when_every_provider_passes() {
        let cascade = CascadingGeocoder::new(vec![Box::new(AlwaysNone), Box::new(AlwaysNone)]);
        let result = cascade.geocode("1 Main St", "Anytown", "CA", "90210").await.unwrap();
        assert!(result.is_none());
    }
}
