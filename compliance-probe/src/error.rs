//! Error type for the `compliance-probe` crate.

#[derive(Debug, thiserror::Error)]
pub enum ProbeError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("hospital website is unreachable: {0}")]
    WebsiteUnreachable(String),

    #[error("failed to parse cms-hpt.txt manifest: {0}")]
    ManifestParse(String),

    #[error("this stage is not implemented yet: {0}")]
    NotImplemented(&'static str),
}
