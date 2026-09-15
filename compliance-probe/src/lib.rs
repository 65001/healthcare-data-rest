//! Stage 3 of the pipeline: discover whether a hospital publishes its
//! CMS-required machine-readable pricing file.
//!
//! **Fully implemented as of 2026-09-15.** `rate_limiter` (token bucket +
//! per-domain exponential backoff), `manifest_parser` (block-format
//! `cms-hpt.txt` parser, format confirmed against a real, live,
//! multi-location manifest), and `probe::Prober` (the real HTTP
//! sequence: `HEAD` website → `GET`/parse `cms-hpt.txt` → `HEAD` each MRF
//! URL) are all real and tested. What lives outside this crate on
//! purpose (kept database-agnostic, same seam `geo-enrich` uses):
//! resolving a multi-location manifest's `location-name` entries to
//! specific hospitals, and restricting that to hospitals connected
//! through CMS's ownership-disclosure data — see `backend::mrf_match`
//! and `backend::db::queries::ownership_reachable_ccns`. Wired into
//! `POST /api/pipeline/discover`'s `network_manifest_url` mode.

pub mod error;
pub mod manifest_parser;
pub mod probe;
pub mod rate_limiter;

pub use error::ProbeError;
pub use probe::{
    detect_change_from_headers, ChangeDetectionMethod, DiscoveryResult, DiscoveryStatus, HeaderChangeCheck,
    MrfHeaderProbe, Prober,
};
