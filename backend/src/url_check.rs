//! Verifies a manually-entered `website_url` (see
//! `routes::hospitals::patch_enrichment`) before it's accepted: that the
//! URL exists, its `Content-Type`, its `Content-Length`, and a
//! best-effort guess at whether it looks like a CMS machine-readable
//! file (MRF) rather than an ordinary web page.
//!
//! Deliberately lightweight — this reads at most `SNIFF_CAP_BYTES` of
//! the body, never the whole thing. Architecture.md already lists *full*
//! MRF schema validation as an explicit future/out-of-scope item
//! precisely because these files can be multi-gigabyte; a manual-entry
//! field that might download one on every keystroke's worth of retries
//! would be a real cost, not just an implementation shortcut.
//!
//! Only "does it exist" is treated as a hard failure by the caller —
//! `Content-Type` / size / `looks_like_mrf` are informational. That's
//! because `website_url` is a hospital's *homepage*, not necessarily an
//! MRF link (that's what Stage 3's `mrf_discoveries` table is for, once
//! it's implemented — see `compliance-probe`); a homepage failing the
//! MRF sniff is the expected, common case, not an error.

use std::time::Duration;

use reqwest::{Client, StatusCode};
use serde::Serialize;
use utoipa::ToSchema;

/// How much of the body we're willing to read to sniff its format —
/// enough to see JSON's top-level keys or a CSV header row, nowhere near
/// enough to download an actual MRF file.
const SNIFF_CAP_BYTES: usize = 64 * 1024;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Heuristic markers of a CMS hospital price-transparency
/// machine-readable file (see cms.gov/hospital-price-transparency).
/// Not a schema validator: a real MRF is expected to contain at least
/// one of these field names; a page that happens to contain one of
/// these words isn't guaranteed to *be* one. That imprecision is the
/// deliberate tradeoff for not downloading the whole file.
const JSON_MRF_MARKERS: [&str; 4] = [
    "hospital_name",
    "standard_charge_information",
    "last_updated_on",
    "affirmation",
];
const CSV_MRF_MARKERS: [&str; 5] = [
    "hospital_name",
    "standard_charge",
    "drug_unit_of_measurement",
    "setting",
    "description",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MrfFormat {
    Json,
    Csv,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct UrlCheckResult {
    /// The server responded with a `2xx` status. `false` covers both a
    /// transport failure (DNS, connection refused, timeout — see
    /// `error`) and an HTTP error status (`status` is still populated
    /// for the latter).
    pub reachable: bool,
    pub status: Option<u16>,
    pub content_type: Option<String>,
    pub content_length: Option<u64>,
    /// Best-effort sniff of the first `SNIFF_CAP_BYTES` of the body
    /// against `JSON_MRF_MARKERS` / `CSV_MRF_MARKERS`. `None` when the
    /// body wasn't fetched (URL unreachable, or a non-2xx status).
    pub looks_like_mrf: Option<bool>,
    pub mrf_format: Option<MrfFormat>,
    /// Set only for a transport-level failure (`reachable: false` with
    /// no `status`) — e.g. "operation timed out", "dns error".
    pub error: Option<String>,
}

impl UrlCheckResult {
    fn unreachable(error: String) -> Self {
        Self {
            reachable: false,
            status: None,
            content_type: None,
            content_length: None,
            looks_like_mrf: None,
            mrf_format: None,
            error: Some(error),
        }
    }
}

/// Builds the shared `reqwest::Client` used for URL verification —
/// short timeouts (this runs inline in a request handler, not a
/// background job) and a distinct, identifying User-Agent, per the same
/// politeness convention `compliance-probe` and the enrich pipeline use.
pub fn http_client() -> reqwest::Result<Client> {
    Client::builder()
        .user_agent("cms-hpt-checker/0.1 (compliance-audit; +https://github.com/asathiabalan/healthcare-data-rest)")
        .connect_timeout(CONNECT_TIMEOUT)
        .timeout(REQUEST_TIMEOUT)
        .build()
}

/// Verifies `url`. Never returns `Err` — every failure mode (DNS,
/// timeout, 404, ...) is represented in the returned `UrlCheckResult` so
/// the caller (and the human who submitted the URL) can see *why* it
/// failed, not just that it did.
pub async fn check_url(client: &Client, url: &str) -> UrlCheckResult {
    match client.head(url).send().await {
        // Some servers (small hospital sites especially) don't support
        // HEAD at all — fall back to GET rather than treating that as
        // "doesn't exist".
        Ok(resp) if resp.status() == StatusCode::METHOD_NOT_ALLOWED || resp.status() == StatusCode::NOT_IMPLEMENTED => {
            get_and_finish(client, url).await
        }
        // HEAD says it exists — a capped GET both confirms that and
        // gives us a body to sniff.
        Ok(resp) if resp.status().is_success() => get_and_finish(client, url).await,
        // Any other status (404, 403, 500, ...) — trust it, no need to
        // spend a GET confirming what HEAD already told us.
        Ok(resp) => UrlCheckResult {
            reachable: false,
            status: Some(resp.status().as_u16()),
            content_type: header_str(resp.headers(), reqwest::header::CONTENT_TYPE),
            content_length: header_num(resp.headers(), reqwest::header::CONTENT_LENGTH),
            looks_like_mrf: None,
            mrf_format: None,
            error: None,
        },
        // Transport-level failure on HEAD doesn't necessarily mean the
        // URL is dead (some servers reject HEAD at the connection level
        // too) — try GET once more before giving up.
        Err(head_err) => match client.get(url).send().await {
            Ok(resp) => finish_with_body(resp).await,
            Err(get_err) => {
                UrlCheckResult::unreachable(format!("HEAD failed ({head_err}); GET also failed ({get_err})"))
            }
        },
    }
}

async fn get_and_finish(client: &Client, url: &str) -> UrlCheckResult {
    match client.get(url).send().await {
        Ok(resp) => finish_with_body(resp).await,
        Err(e) => UrlCheckResult::unreachable(format!("GET failed: {e}")),
    }
}

async fn finish_with_body(mut resp: reqwest::Response) -> UrlCheckResult {
    let status = resp.status();
    let content_type = header_str(resp.headers(), reqwest::header::CONTENT_TYPE);
    let content_length = header_num(resp.headers(), reqwest::header::CONTENT_LENGTH);

    if !status.is_success() {
        return UrlCheckResult {
            reachable: false,
            status: Some(status.as_u16()),
            content_type,
            content_length,
            looks_like_mrf: None,
            mrf_format: None,
            error: None,
        };
    }

    let mut buf: Vec<u8> = Vec::with_capacity(SNIFF_CAP_BYTES.min(8192));
    while buf.len() < SNIFF_CAP_BYTES {
        match resp.chunk().await {
            Ok(Some(chunk)) => buf.extend_from_slice(&chunk),
            Ok(None) => break,
            // A body read error past this point doesn't invalidate the
            // "it exists" verdict we've already established — just sniff
            // whatever we did get.
            Err(_) => break,
        }
    }
    let (looks_like_mrf, mrf_format) = sniff_mrf(&buf);

    UrlCheckResult {
        reachable: true,
        status: Some(status.as_u16()),
        content_type,
        content_length,
        looks_like_mrf: Some(looks_like_mrf),
        mrf_format,
        error: None,
    }
}

fn sniff_mrf(buf: &[u8]) -> (bool, Option<MrfFormat>) {
    let sample = String::from_utf8_lossy(buf).to_lowercase();
    let trimmed = sample.trim_start();

    if trimmed.starts_with('{') && JSON_MRF_MARKERS.iter().any(|m| sample.contains(m)) {
        (true, Some(MrfFormat::Json))
    } else if sample.contains(',') && CSV_MRF_MARKERS.iter().any(|m| sample.contains(m)) {
        (true, Some(MrfFormat::Csv))
    } else {
        (false, None)
    }
}

fn header_str(headers: &reqwest::header::HeaderMap, name: reqwest::header::HeaderName) -> Option<String> {
    headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string)
}

fn header_num(headers: &reqwest::header::HeaderMap, name: reqwest::header::HeaderName) -> Option<u64> {
    headers.get(name).and_then(|v| v.to_str().ok()).and_then(|v| v.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniffs_cms_json_mrf() {
        let body = br#"{"hospital_name": "Test Hospital", "last_updated_on": "2026-01-01", "standard_charge_information": []}"#;
        let (looks_like_mrf, format) = sniff_mrf(body);
        assert!(looks_like_mrf);
        assert_eq!(format, Some(MrfFormat::Json));
    }

    #[test]
    fn sniffs_cms_csv_mrf() {
        let body = b"description,setting,code|1,code|1|type,standard_charge|gross\nMRI,outpatient,71045,CPT,450.00\n";
        let (looks_like_mrf, format) = sniff_mrf(body);
        assert!(looks_like_mrf);
        assert_eq!(format, Some(MrfFormat::Csv));
    }

    #[test]
    fn ordinary_homepage_does_not_look_like_mrf() {
        let body = b"<!doctype html><html><head><title>Anytown General Hospital</title></head><body>Welcome</body></html>";
        let (looks_like_mrf, format) = sniff_mrf(body);
        assert!(!looks_like_mrf);
        assert_eq!(format, None);
    }
}
