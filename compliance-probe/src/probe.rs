//! Stage 3 core logic: probe a hospital's website for `cms-hpt.txt` and
//! check whether its MRF URLs actually resolve.
//!
//! The five-value status enum below matches Architecture.md's table
//! exactly. `Prober` implements the real HTTP sequence (GET a manifest
//! URL → parse → HEAD each MRF URL), rate-limited and backed off
//! per-domain via `rate_limiter`. What it does *not* do is decide which
//! hospital a manifest's location-name entries belong to, or restrict
//! that to a corporate-ownership-verified set — that's
//! `backend::mrf_match` and `backend::db::queries::ownership_reachable_ccns`,
//! kept out of this crate so it stays database-agnostic (same seam
//! `geo-enrich` uses).

use serde::{Deserialize, Serialize};

use crate::error::ProbeError;
use crate::manifest_parser::{self, MrfEntry};
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

/// Result of fetching and parsing one manifest URL, independent of any
/// particular hospital — a network manifest like Sentara's covers many.
#[derive(Debug, Clone)]
pub struct ManifestFetch {
    pub manifest_url: String,
    pub found: bool,
    /// `None` when `found` is `false`.
    pub status: Option<u16>,
    pub entries: Vec<MrfEntry>,
}

/// Whether one MRF URL, checked with `HEAD`, actually resolves.
#[derive(Debug, Clone)]
pub struct MrfReachability {
    pub mrf_url: String,
    pub reachable: bool,
    pub status: Option<u16>,
}

/// The caching-relevant response headers from a `HEAD` on one MRF URL —
/// everything needed to tell, on a later re-check, whether the file
/// behind it has changed without downloading it again. Mirrors
/// Architecture.md's `mrf_metadata` table columns of the same names.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MrfHeaderProbe {
    pub mrf_url: String,
    pub reachable: bool,
    pub status: Option<u16>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub cache_control: Option<String>,
    pub content_length: Option<i64>,
    pub content_type: Option<String>,
}

/// Which signal a change decision was made on. `Etag` and `LastModified`
/// mean the decision came from `HEAD` response headers alone — no
/// download needed. `Sha1` means neither header was usable (missing on
/// the previous check, the current one, or both), so the full file was
/// downloaded and hashed instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeDetectionMethod {
    Etag,
    LastModified,
}

/// Result of comparing a fresh [`MrfHeaderProbe`] against a previously
/// recorded ETag/Last-Modified, when at least one of those was usable.
#[derive(Debug, Clone)]
pub struct HeaderChangeCheck {
    pub changed: bool,
    pub method: ChangeDetectionMethod,
}

/// Compares freshly probed headers against the previously recorded
/// `etag`/`last_modified` and decides whether that's enough to tell if
/// the file changed. Prefers `ETag` (a stronger, content-derived signal)
/// over `Last-Modified` when both are available. Returns `None` when
/// neither can be compared (absent on either side) — the caller must
/// fall back to downloading the file and comparing a SHA-1 hash instead.
pub fn detect_change_from_headers(
    previous_etag: Option<&str>,
    previous_last_modified: Option<&str>,
    fresh: &MrfHeaderProbe,
) -> Option<HeaderChangeCheck> {
    if let (Some(prev), Some(new)) = (previous_etag, fresh.etag.as_deref()) {
        return Some(HeaderChangeCheck {
            changed: prev != new,
            method: ChangeDetectionMethod::Etag,
        });
    }
    if let (Some(prev), Some(new)) = (previous_last_modified, fresh.last_modified.as_deref()) {
        return Some(HeaderChangeCheck {
            changed: prev != new,
            method: ChangeDetectionMethod::LastModified,
        });
    }
    None
}

pub struct Prober {
    http: reqwest::Client,
    rate_limiter: RateLimiter,
    backoff: DomainBackoff,
}

/// Extracts the registrable-ish host (for rate-limiter/backoff keying)
/// from a URL. Falls back to the whole URL string on a parse failure —
/// still a stable, if coarser, key.
fn host_key(url: &str) -> String {
    // `reqwest::Url` re-exports the `url` crate's type — no separate
    // dependency needed just for this.
    reqwest::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(str::to_string))
        .unwrap_or_else(|| url.to_string())
}

/// Strips a website URL down to its origin (`scheme://host[:port]`),
/// dropping any path/query/fragment. `hospitals.website_url` is often
/// sourced from a geocoding provider (Nominatim/Google Places) that can
/// return a specific location-profile subpage rather than the site root
/// — see `Prober::probe`'s doc comment for why probing that exact path
/// instead of the origin can incorrectly mark a perfectly reachable
/// hospital site as unreachable. Falls back to the original string
/// unchanged on a parse failure (trimming a trailing slash instead) — an
/// unparseable URL will fail the reachability check on its own merits
/// either way, so there's nothing to gain from erroring here specifically.
fn base_url(url: &str) -> String {
    // `Url::host_str()` alone drops a non-default port (matters for
    // tests against a local `wiremock` server; real hospital sites are
    // almost always default-port https, so this is a no-op there).
    reqwest::Url::parse(url)
        .map(|u| match u.port() {
            Some(port) => format!("{}://{}:{}", u.scheme(), u.host_str().unwrap_or_default(), port),
            None => format!("{}://{}", u.scheme(), u.host_str().unwrap_or_default()),
        })
        .unwrap_or_else(|_| url.trim_end_matches('/').to_string())
}

impl Prober {
    pub fn new(http: reqwest::Client, rate_limit_per_sec: f64) -> Self {
        Self {
            http,
            rate_limiter: RateLimiter::new(rate_limit_per_sec),
            backoff: DomainBackoff::default(),
        }
    }

    /// Rate-limited, backoff-respecting `GET`. `Ok(None)` means "don't
    /// retry right now" was already handled by waiting; this always
    /// either waits out a domain's backoff or (for a 429/503 response)
    /// records a new one and returns that response as-is for the caller
    /// to classify.
    async fn polite_get(&self, url: &str) -> Result<reqwest::Response, ProbeError> {
        let host = host_key(url);
        let wait = self.backoff.wait_for(&host);
        if wait > std::time::Duration::ZERO {
            tokio::time::sleep(wait).await;
        }
        self.rate_limiter.acquire().await;

        let resp = self.http.get(url).send().await?;
        if resp.status().as_u16() == 429 || resp.status().as_u16() == 503 {
            self.backoff.record_failure(&host);
        } else {
            self.backoff.record_success(&host);
        }
        Ok(resp)
    }

    async fn polite_head(&self, url: &str) -> Result<reqwest::Response, ProbeError> {
        let host = host_key(url);
        let wait = self.backoff.wait_for(&host);
        if wait > std::time::Duration::ZERO {
            tokio::time::sleep(wait).await;
        }
        self.rate_limiter.acquire().await;

        let resp = self.http.head(url).send().await?;
        if resp.status().as_u16() == 429 || resp.status().as_u16() == 503 {
            self.backoff.record_failure(&host);
        } else {
            self.backoff.record_success(&host);
        }
        Ok(resp)
    }

    /// Fetches and parses exactly the manifest at `manifest_url` (the
    /// caller derives this — typically `{origin}/cms-hpt.txt` — or
    /// passes one directly, e.g. a network's manifest URL a human
    /// supplied). Never returns `Err` for an ordinary "not found";
    /// only a transport-level failure does.
    pub async fn fetch_manifest(&self, manifest_url: &str) -> Result<ManifestFetch, ProbeError> {
        let resp = self.polite_get(manifest_url).await?;
        let status = resp.status();

        if !status.is_success() {
            return Ok(ManifestFetch {
                manifest_url: manifest_url.to_string(),
                found: false,
                status: Some(status.as_u16()),
                entries: Vec::new(),
            });
        }

        let body = resp.text().await?;
        let entries = manifest_parser::parse_manifest(&body)?;
        Ok(ManifestFetch {
            manifest_url: manifest_url.to_string(),
            found: true,
            status: Some(status.as_u16()),
            entries,
        })
    }

    /// `HEAD`s one MRF URL to confirm it actually resolves. A
    /// transport-level failure is reported as unreachable rather than
    /// propagated — one dead link in a manifest shouldn't fail a whole
    /// discovery run.
    pub async fn check_mrf_reachable(&self, mrf_url: &str) -> MrfReachability {
        match self.polite_head(mrf_url).await {
            Ok(resp) => MrfReachability {
                mrf_url: mrf_url.to_string(),
                reachable: resp.status().is_success(),
                status: Some(resp.status().as_u16()),
            },
            Err(_) => MrfReachability {
                mrf_url: mrf_url.to_string(),
                reachable: false,
                status: None,
            },
        }
    }

    /// `HEAD`s one MRF URL and captures the caching-relevant response
    /// headers (`ETag`, `Last-Modified`, `Cache-Control`,
    /// `Content-Length`, `Content-Type`) instead of just a reachability
    /// bool, per Architecture.md's `mrf_metadata` table. A transport-level
    /// failure is reported as unreachable, same as `check_mrf_reachable`.
    pub async fn probe_mrf_headers(&self, mrf_url: &str) -> MrfHeaderProbe {
        match self.polite_head(mrf_url).await {
            Ok(resp) => {
                let headers = resp.headers();
                let header_str = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).map(str::to_string);

                MrfHeaderProbe {
                    mrf_url: mrf_url.to_string(),
                    reachable: resp.status().is_success(),
                    status: Some(resp.status().as_u16()),
                    etag: header_str("etag"),
                    last_modified: header_str("last-modified"),
                    cache_control: header_str("cache-control"),
                    content_length: headers
                        .get("content-length")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|s| s.parse::<i64>().ok()),
                    content_type: header_str("content-type"),
                }
            }
            Err(_) => MrfHeaderProbe {
                mrf_url: mrf_url.to_string(),
                reachable: false,
                status: None,
                etag: None,
                last_modified: None,
                cache_control: None,
                content_length: None,
                content_type: None,
            },
        }
    }

    /// Downloads the full MRF file and computes its SHA-1 hash — the
    /// fallback for when `detect_change_from_headers` can't tell whether
    /// the file changed from `ETag`/`Last-Modified` alone (per
    /// Architecture.md's deferred `mrf_metadata` design and the CMS MRF
    /// files this targets, these can be multi-GB, so the body is streamed
    /// and hashed incrementally rather than buffered whole in memory).
    pub async fn hash_mrf_content(&self, mrf_url: &str) -> Result<String, ProbeError> {
        use futures::StreamExt;
        use sha1::{Digest, Sha1};

        let host = host_key(mrf_url);
        let wait = self.backoff.wait_for(&host);
        if wait > std::time::Duration::ZERO {
            tokio::time::sleep(wait).await;
        }
        self.rate_limiter.acquire().await;

        let resp = self.http.get(mrf_url).send().await?;
        if resp.status().as_u16() == 429 || resp.status().as_u16() == 503 {
            self.backoff.record_failure(&host);
        } else {
            self.backoff.record_success(&host);
        }
        let resp = resp.error_for_status()?;

        let mut hasher = Sha1::new();
        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            hasher.update(&chunk?);
        }
        Ok(format!("{:x}", hasher.finalize()))
    }

    /// Runs the full per-hospital sequence: normalize `website_url` down
    /// to its origin (`base_url` — dropping whatever path a geocoding
    /// provider may have returned, e.g. a location-profile subpage), then
    /// `HEAD` that origin, `GET {origin}/cms-hpt.txt`, parse it, and
    /// classify. Normalizing *before* the `HEAD` (not after) matters: a
    /// deep subpage can 404 under a plain `HEAD` on a JS-routed site even
    /// when the site itself is fully reachable, which used to incorrectly
    /// mark the whole hospital `WebsiteUnreachable` before discovery ever
    /// got to try `/cms-hpt.txt`.
    /// Does **not** attempt to match a multi-location manifest's entries
    /// to a specific hospital by name — a caller with one hospital in
    /// hand and a manifest that turns out to cover a whole network
    /// should use `fetch_manifest` directly plus `backend::mrf_match`
    /// instead of this convenience method, which just takes the first
    /// (or only) entry.
    pub async fn probe(&self, facility_id: &str, website_url: Option<&str>) -> Result<DiscoveryResult, ProbeError> {
        let Some(url) = website_url else {
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

        // Normalize to the origin *before* the reachability check, not
        // after — `website_url` often comes from a geocoding provider
        // (Nominatim/Google Places) that can return a specific
        // location-profile subpage rather than the site root (e.g.
        // `.../our-locations/profile/warren-memorial-hospital`). HEADing
        // that exact path can 404 on a JS-routed site even when the site
        // itself is fully up, which used to incorrectly short-circuit the
        // whole probe at `WebsiteUnreachable` before it ever tried
        // `/cms-hpt.txt`.
        let url = base_url(url);

        let head = self.polite_head(&url).await;
        let website_reachable = matches!(&head, Ok(r) if r.status().is_success());
        if !website_reachable {
            return Ok(DiscoveryResult {
                facility_id: facility_id.to_string(),
                website_url: Some(url),
                website_reachable: Some(false),
                cms_hpt_txt_found: false,
                cms_hpt_txt_url: None,
                mrf_urls: Vec::new(),
                status: DiscoveryStatus::WebsiteUnreachable,
            });
        }

        let manifest_url = format!("{url}/cms-hpt.txt");

        let manifest = self.fetch_manifest(&manifest_url).await?;
        if !manifest.found || manifest.entries.is_empty() {
            return Ok(DiscoveryResult {
                facility_id: facility_id.to_string(),
                website_url: Some(url),
                website_reachable: Some(true),
                cms_hpt_txt_found: false,
                cms_hpt_txt_url: None,
                mrf_urls: Vec::new(),
                status: DiscoveryStatus::NoManifest,
            });
        }

        // Single-hospital convenience path: check the first entry's MRF
        // (a multi-location manifest should go through
        // `backend::mrf_match` instead, per this method's doc comment).
        let mut any_reachable = false;
        for entry in &manifest.entries {
            if self.check_mrf_reachable(&entry.mrf_url).await.reachable {
                any_reachable = true;
                break;
            }
        }

        Ok(DiscoveryResult {
            facility_id: facility_id.to_string(),
            website_url: Some(url),
            website_reachable: Some(true),
            cms_hpt_txt_found: true,
            cms_hpt_txt_url: Some(manifest_url),
            mrf_urls: manifest.entries,
            status: if any_reachable { DiscoveryStatus::MrfFound } else { DiscoveryStatus::ManifestOnly },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn prober() -> Prober {
        Prober::new(reqwest::Client::new(), 50.0)
    }

    #[tokio::test]
    async fn fetch_manifest_parses_a_real_shaped_body() {
        let server = MockServer::start().await;
        let body = "location-name: Test Hospital\nmrf-url: https://example.com/mrf.json\n";
        Mock::given(method("GET"))
            .and(path("/cms-hpt.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(body))
            .mount(&server)
            .await;

        let result = prober().fetch_manifest(&format!("{}/cms-hpt.txt", server.uri())).await.unwrap();
        assert!(result.found);
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].mrf_url, "https://example.com/mrf.json");
    }

    #[tokio::test]
    async fn fetch_manifest_404_is_not_found_not_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/cms-hpt.txt"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let result = prober().fetch_manifest(&format!("{}/cms-hpt.txt", server.uri())).await.unwrap();
        assert!(!result.found);
        assert_eq!(result.status, Some(404));
        assert!(result.entries.is_empty());
    }

    #[tokio::test]
    async fn check_mrf_reachable_true_on_200() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&server)
            .await;

        let result = prober().check_mrf_reachable(&format!("{}/mrf.json", server.uri())).await;
        assert!(result.reachable);
        assert_eq!(result.status, Some(200));
    }

    #[tokio::test]
    async fn check_mrf_reachable_false_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&server)
            .await;

        let result = prober().check_mrf_reachable(&format!("{}/mrf.json", server.uri())).await;
        assert!(!result.reachable);
    }

    #[tokio::test]
    async fn probe_no_website_short_circuits() {
        let result = prober().probe("F1", None).await.unwrap();
        assert_eq!(result.status, DiscoveryStatus::NoWebsite);
    }

    #[tokio::test]
    async fn probe_full_sequence_finds_mrf() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/cms-hpt.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "location-name: Test\nmrf-url: {}/mrf.json\n",
                server.uri()
            )))
            .mount(&server)
            .await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let result = prober().probe("F1", Some(&server.uri())).await.unwrap();
        assert_eq!(result.status, DiscoveryStatus::MrfFound);
        assert!(result.cms_hpt_txt_found);
        assert_eq!(result.mrf_urls.len(), 1);
    }

    #[tokio::test]
    async fn probe_website_unreachable() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/")).respond_with(ResponseTemplate::new(500)).mount(&server).await;

        let result = prober().probe("F1", Some(&server.uri())).await.unwrap();
        assert_eq!(result.status, DiscoveryStatus::WebsiteUnreachable);
    }

    #[tokio::test]
    async fn probe_normalizes_a_deep_profile_page_url_to_the_origin_before_heading_it() {
        // Mirrors a real geocoding-sourced website_url shape, e.g.
        // "https://www.valleyhealthlink.com/our-locations/profile/warren-memorial-hospital"
        // — a location-profile subpage, not the site root. Only "/" is
        // mocked as reachable; if `probe` HEADed the deep path as given
        // instead of normalizing first, this would 404 and wrongly
        // short-circuit at WebsiteUnreachable.
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/cms-hpt.txt"))
            .respond_with(ResponseTemplate::new(200).set_body_string(format!(
                "location-name: Test\nmrf-url: {}/mrf.json\n",
                server.uri()
            )))
            .mount(&server)
            .await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let deep_url = format!("{}/our-locations/profile/warren-memorial-hospital", server.uri());
        let result = prober().probe("F1", Some(&deep_url)).await.unwrap();

        assert_eq!(result.status, DiscoveryStatus::MrfFound);
        assert_eq!(result.website_url.as_deref(), Some(server.uri().as_str()));
    }

    #[tokio::test]
    async fn probe_website_unreachable_still_uses_the_normalized_origin() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/")).respond_with(ResponseTemplate::new(500)).mount(&server).await;

        let deep_url = format!("{}/our-locations/profile/warren-memorial-hospital", server.uri());
        let result = prober().probe("F1", Some(&deep_url)).await.unwrap();

        assert_eq!(result.status, DiscoveryStatus::WebsiteUnreachable);
        assert_eq!(result.website_url.as_deref(), Some(server.uri().as_str()));
    }

    #[test]
    fn base_url_strips_path_query_and_fragment() {
        assert_eq!(
            base_url("https://www.valleyhealthlink.com/our-locations/profile/warren-memorial-hospital"),
            "https://www.valleyhealthlink.com"
        );
        assert_eq!(base_url("https://example.com/a/b?x=1#frag"), "https://example.com");
        assert_eq!(base_url("https://example.com"), "https://example.com");
        assert_eq!(base_url("https://example.com:8443/a/b"), "https://example.com:8443");
    }

    #[test]
    fn base_url_falls_back_to_trimmed_input_on_parse_failure() {
        assert_eq!(base_url("not a url"), "not a url");
    }

    #[tokio::test]
    async fn probe_mrf_headers_captures_cache_headers() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/mrf.json"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("etag", "\"abc123\"")
                    .insert_header("last-modified", "Wed, 01 Jan 2026 00:00:00 GMT")
                    .insert_header("cache-control", "max-age=3600")
                    .insert_header("content-length", "12345")
                    .insert_header("content-type", "application/json"),
            )
            .mount(&server)
            .await;

        let result = prober().probe_mrf_headers(&format!("{}/mrf.json", server.uri())).await;
        assert!(result.reachable);
        assert_eq!(result.etag.as_deref(), Some("\"abc123\""));
        assert_eq!(result.last_modified.as_deref(), Some("Wed, 01 Jan 2026 00:00:00 GMT"));
        assert_eq!(result.cache_control.as_deref(), Some("max-age=3600"));
        assert_eq!(result.content_length, Some(12345));
        assert_eq!(result.content_type.as_deref(), Some("application/json"));
    }

    #[tokio::test]
    async fn probe_mrf_headers_missing_headers_are_none() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(200)).mount(&server).await;

        let result = prober().probe_mrf_headers(&format!("{}/mrf.json", server.uri())).await;
        assert!(result.reachable);
        assert!(result.etag.is_none());
        assert!(result.last_modified.is_none());
    }

    #[tokio::test]
    async fn probe_mrf_headers_unreachable_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let result = prober().probe_mrf_headers(&format!("{}/mrf.json", server.uri())).await;
        assert!(!result.reachable);
        assert_eq!(result.status, Some(404));
    }

    #[tokio::test]
    async fn hash_mrf_content_computes_a_stable_sha1() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world"))
            .mount(&server)
            .await;

        let hash = prober().hash_mrf_content(&format!("{}/mrf.json", server.uri())).await.unwrap();
        // Known SHA-1 of "hello world".
        assert_eq!(hash, "2aae6c35c94fcfb415dbe95f408b9ce91ee846ed");
    }

    #[tokio::test]
    async fn hash_mrf_content_errors_on_404() {
        let server = MockServer::start().await;
        Mock::given(method("GET")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let result = prober().hash_mrf_content(&format!("{}/mrf.json", server.uri())).await;
        assert!(result.is_err());
    }

    #[test]
    fn detect_change_prefers_etag_over_last_modified() {
        let fresh = MrfHeaderProbe {
            mrf_url: "https://example.com/mrf.json".to_string(),
            reachable: true,
            status: Some(200),
            etag: Some("\"new\"".to_string()),
            last_modified: Some("same-date".to_string()),
            cache_control: None,
            content_length: None,
            content_type: None,
        };
        let check = detect_change_from_headers(Some("\"old\""), Some("same-date"), &fresh).unwrap();
        assert!(check.changed);
        assert_eq!(check.method, ChangeDetectionMethod::Etag);
    }

    #[test]
    fn detect_change_falls_back_to_last_modified_when_no_etag() {
        let fresh = MrfHeaderProbe {
            mrf_url: "https://example.com/mrf.json".to_string(),
            reachable: true,
            status: Some(200),
            etag: None,
            last_modified: Some("new-date".to_string()),
            cache_control: None,
            content_length: None,
            content_type: None,
        };
        let check = detect_change_from_headers(None, Some("old-date"), &fresh).unwrap();
        assert!(check.changed);
        assert_eq!(check.method, ChangeDetectionMethod::LastModified);
    }

    #[test]
    fn detect_change_none_when_headers_insufficient() {
        let fresh = MrfHeaderProbe {
            mrf_url: "https://example.com/mrf.json".to_string(),
            reachable: true,
            status: Some(200),
            etag: None,
            last_modified: None,
            cache_control: None,
            content_length: None,
            content_type: None,
        };
        assert!(detect_change_from_headers(None, None, &fresh).is_none());
        // Previous had an etag but this check couldn't fetch a new one.
        assert!(detect_change_from_headers(Some("\"old\""), None, &fresh).is_none());
    }

    #[test]
    fn detect_change_unchanged_when_etag_matches() {
        let fresh = MrfHeaderProbe {
            mrf_url: "https://example.com/mrf.json".to_string(),
            reachable: true,
            status: Some(200),
            etag: Some("\"same\"".to_string()),
            last_modified: None,
            cache_control: None,
            content_length: None,
            content_type: None,
        };
        let check = detect_change_from_headers(Some("\"same\""), None, &fresh).unwrap();
        assert!(!check.changed);
    }
}
