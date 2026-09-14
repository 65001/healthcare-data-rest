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

    /// Provider stubs in this pass raise this rather than making a real
    /// call — see IMPLEMENTATION_NOTES.md. Not part of the original
    /// Architecture.md error set; remove once every provider is wired up.
    #[error("provider `{0}` is not implemented yet")]
    NotImplemented(&'static str),
}

impl GeoEnrichError {
    /// Whether this error is transient (retryable / fallback-eligible).
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Http(_) | Self::RateLimited { .. })
    }
}
