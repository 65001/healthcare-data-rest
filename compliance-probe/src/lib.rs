//! Stage 3 of the pipeline: discover whether a hospital publishes its
//! CMS-required machine-readable pricing file.
//!
//! **Status: partial skeleton.** `rate_limiter` (token bucket + per-domain
//! exponential backoff) is fully implemented and tested. `probe` defines
//! the real `DiscoveryStatus` enum and result shape but stubs the actual
//! HTTP sequence. `manifest_parser` is a stub pending confirmation of the
//! `cms-hpt.txt` wire format — see that module's doc comment. Not wired
//! into `backend` yet: `POST /api/pipeline/discover` returns 501.

pub mod error;
pub mod manifest_parser;
pub mod probe;
pub mod rate_limiter;

pub use error::ProbeError;
pub use probe::{DiscoveryResult, DiscoveryStatus, Prober};
