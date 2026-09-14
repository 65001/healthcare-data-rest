//! Stage 3 core logic: probe a hospital's website for `cms-hpt.txt` and
//! check whether its MRF URLs actually resolve.
//!
//! **Stub.** The five-value status enum below matches Architecture.md's
//! table exactly and is safe to build against today. The actual HTTP
//! sequence (HEAD website → GET /cms-hpt.txt → parse → HEAD each MRF URL)
//! is not implemented — it's blocked on `manifest_parser` (see that
//! module) and should use `RateLimiter`/`DomainBackoff` from
//! `rate_limiter` once it is.

use serde::{Deserialize, Serialize};

use crate::error::ProbeError;
use crate::manifest_parser::MrfEntry;
use crate::rate_limiter::{DomainBackoff, RateLimiter};

/// Mirrors the `discovery_status` column values from Architecture.md's
/// `mrf_discoveries` table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DiscoveryStatus {
    MrfFound,
    ManifestOnly,
    NoManifest,
    WebsiteUnreachable,
    NoWebsite,
}

impl DiscoveryStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MrfFound => "mrf_found",
            Self::ManifestOnly => "manifest_only",
            Self::NoManifest => "no_manifest",
            Self::WebsiteUnreachable => "website_unreachable",
            Self::NoWebsite => "no_website",
        }
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    pub facility_id: String,
    pub website_url: Option<String>,
    pub website_reachable: Option<bool>,
    pub cms_hpt_txt_found: bool,
    pub cms_hpt_txt_url: Option<String>,
    pub mrf_urls: Vec<MrfEntry>,
    pub status: DiscoveryStatus,
}

pub struct Prober {
    http: reqwest::Client,
    rate_limiter: RateLimiter,
    backoff: DomainBackoff,
}

impl Prober {
    pub fn new(http: reqwest::Client, rate_limit_per_sec: f64) -> Self {
        Self {
            http,
            rate_limiter: RateLimiter::new(rate_limit_per_sec),
            backoff: DomainBackoff::default(),
        }
    }

    /// Would run the HEAD → GET /cms-hpt.txt → parse → HEAD-each-MRF
    /// sequence for one hospital and classify the result into a
    /// [`DiscoveryStatus`]. Left unimplemented pending `manifest_parser`.
    pub async fn probe(&self, facility_id: &str, website_url: Option<&str>) -> Result<DiscoveryResult, ProbeError> {
        let Some(_url) = website_url else {
            return Ok(DiscoveryResult {
                facility_id: facility_id.to_string(),
                website_url: None,
                website_reachable: None,
                cms_hpt_txt_found: false,
                cms_hpt_txt_url: None,
                mrf_urls: Vec::new(),
                status: DiscoveryStatus::NoWebsite,
            });
        };

        let _ = (&self.http, &self.rate_limiter, &self.backoff);
        Err(ProbeError::NotImplemented("Prober::probe"))
    }
}
