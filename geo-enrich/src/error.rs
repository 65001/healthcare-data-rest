//! Error type for the `geo-enrich` crate, as specified in Architecture.md.

#[derive(Debug, thiserror::Error)]
pub enum GeoEnrichError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("rate limited by provider: {provider}")]
    RateLimited { provider: String },

    #[error("invalid API response: {0}")]
    InvalidResponse(String),

    #[error("API key missing for provider: {0}")]
    MissingApiKey(String),

    /// Every geocoding provider in this crate (`census`, `nominatim`,
    /// `google_maps`) is now real — this variant is unused today but kept
    /// (and still treated as transient below) in case a future provider
    /// ships as a stub first. Not part of the original Architecture.md
    /// error set.
    #[error("provider `{0}` is not implemented yet")]
    NotImplemented(&'static str),

    /// Bubbled up from `enricher::UnenrichedHospitalStore` — kept as a
    /// plain string rather than a real DB error type so this crate stays
    /// database-agnostic (see `enricher.rs`'s doc comment). The concrete
    /// implementation (`backend::enrich_store::HospitalStore`) converts
    /// its `sqlx::Error`s into this via `.to_string()`.
    #[error("hospital store error: {0}")]
    Store(String),
}

impl GeoEnrichError {
    /// Whether this error is transient (retryable / fallback-eligible).
    ///
    /// `NotImplemented` counts as transient too: a still-stubbed provider
    /// should be skipped by the cascade like any other provider that
    /// can't help, not treated as fatal for the whole batch — relevant if
    /// a future provider is added stub-first, the way `nominatim` and
    /// `google_maps` briefly were.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Http(_) | Self::RateLimited { .. } | Self::NotImplemented(_)
        )
    }
}
