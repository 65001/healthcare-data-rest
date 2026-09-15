//! Orchestrates conditional-caching change detection for one MRF URL:
//! probe its `HEAD` response headers, and decide whether the file
//! changed since the last recorded `mrf_metadata` row for the same
//! discovery using `ETag`/`Last-Modified` alone when possible — falling
//! back to downloading the full file and comparing a SHA-1 hash only
//! when neither header is usable (missing on the previous check, the
//! fresh one, or both). Database-agnostic: takes the previous row as a
//! plain argument rather than querying for it, so it's usable from both
//! the `discover` job (baseline capture, `previous = None`) and a
//! standalone recheck (`previous` from `db::queries::latest_mrf_metadata`).

use compliance_probe::probe::{self, ChangeDetectionMethod, MrfHeaderProbe, Prober};

use crate::db::models::MrfMetadata;

/// What one probe found, ready to hand to `db::queries::insert_mrf_metadata`.
pub struct MetadataCheckOutcome {
    pub headers: MrfHeaderProbe,
    pub sha1_hash: Option<String>,
    /// `"baseline"` | `"etag"` | `"last_modified"` | `"sha1"` | `None`
    /// (URL was unreachable this check).
    pub change_detection_method: Option<&'static str>,
    pub changed_from_previous: Option<bool>,
}

fn method_str(method: ChangeDetectionMethod) -> &'static str {
    match method {
        ChangeDetectionMethod::Etag => "etag",
        ChangeDetectionMethod::LastModified => "last_modified",
    }
}

/// Runs one conditional-caching check against `mrf_url`. `previous` is
/// the latest `mrf_metadata` row already recorded for the same
/// `mrf_discovery_id`, or `None` for a first-ever ("baseline") check.
pub async fn check_mrf_metadata(prober: &Prober, mrf_url: &str, previous: Option<&MrfMetadata>) -> MetadataCheckOutcome {
    let headers = prober.probe_mrf_headers(mrf_url).await;

    if !headers.reachable {
        return MetadataCheckOutcome {
            headers,
            sha1_hash: None,
            change_detection_method: None,
            changed_from_previous: None,
        };
    }

    let Some(prev) = previous else {
        // Baseline: nothing to compare against yet. Only pay for a full
        // download now if neither header would be usable to compare
        // against on the *next* check either — otherwise there's nothing
        // to gain from hashing today.
        let sha1_hash = if headers.etag.is_none() && headers.last_modified.is_none() {
            prober.hash_mrf_content(mrf_url).await.ok()
        } else {
            None
        };
        return MetadataCheckOutcome {
            headers,
            sha1_hash,
            change_detection_method: Some("baseline"),
            changed_from_previous: None,
        };
    };

    if let Some(check) = probe::detect_change_from_headers(prev.etag.as_deref(), prev.last_modified.as_deref(), &headers) {
        return MetadataCheckOutcome {
            headers,
            sha1_hash: None,
            change_detection_method: Some(method_str(check.method)),
            changed_from_previous: Some(check.changed),
        };
    }

    // Headers alone weren't enough — download the file and hash it.
    let sha1_hash = prober.hash_mrf_content(mrf_url).await.ok();
    let changed_from_previous = match (prev.sha1_hash.as_deref(), sha1_hash.as_deref()) {
        (Some(old), Some(new)) => Some(old != new),
        _ => None, // no prior hash to compare against, or this download failed
    };
    MetadataCheckOutcome {
        headers,
        sha1_hash,
        change_detection_method: Some("sha1"),
        changed_from_previous,
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

    fn prev_row(etag: Option<&str>, last_modified: Option<&str>, sha1_hash: Option<&str>) -> MrfMetadata {
        MrfMetadata {
            id: "prev-id".to_string(),
            mrf_discovery_id: "disc-1".to_string(),
            mrf_url: "https://example.com/mrf.json".to_string(),
            sha1_hash: sha1_hash.map(str::to_string),
            last_modified: last_modified.map(str::to_string),
            etag: etag.map(str::to_string),
            cache_control: None,
            content_length: None,
            content_type: None,
            schema_valid: None,
            change_detection_method: None,
            changed_from_previous: None,
            checked_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    #[tokio::test]
    async fn baseline_with_etag_skips_download() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"v1\""))
            .mount(&server)
            .await;
        // No GET mock registered — if the code tried to download, the
        // request would 404/fail against wiremock's default response.

        let outcome = check_mrf_metadata(&prober(), &format!("{}/mrf.json", server.uri()), None).await;
        assert_eq!(outcome.change_detection_method, Some("baseline"));
        assert!(outcome.changed_from_previous.is_none());
        assert!(outcome.sha1_hash.is_none());
        assert_eq!(outcome.headers.etag.as_deref(), Some("\"v1\""));
    }

    #[tokio::test]
    async fn baseline_without_cache_headers_downloads_and_hashes() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world"))
            .mount(&server)
            .await;

        let outcome = check_mrf_metadata(&prober(), &format!("{}/mrf.json", server.uri()), None).await;
        assert_eq!(outcome.change_detection_method, Some("baseline"));
        assert_eq!(outcome.sha1_hash.as_deref(), Some("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"));
    }

    #[tokio::test]
    async fn recheck_etag_match_is_unchanged_without_download() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"v1\""))
            .mount(&server)
            .await;

        let prev = prev_row(Some("\"v1\""), None, None);
        let outcome = check_mrf_metadata(&prober(), &format!("{}/mrf.json", server.uri()), Some(&prev)).await;
        assert_eq!(outcome.change_detection_method, Some("etag"));
        assert_eq!(outcome.changed_from_previous, Some(false));
        assert!(outcome.sha1_hash.is_none());
    }

    #[tokio::test]
    async fn recheck_etag_mismatch_is_changed_without_download() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200).insert_header("etag", "\"v2\""))
            .mount(&server)
            .await;

        let prev = prev_row(Some("\"v1\""), None, None);
        let outcome = check_mrf_metadata(&prober(), &format!("{}/mrf.json", server.uri()), Some(&prev)).await;
        assert_eq!(outcome.change_detection_method, Some("etag"));
        assert_eq!(outcome.changed_from_previous, Some(true));
    }

    #[tokio::test]
    async fn recheck_headers_insufficient_falls_back_to_sha1() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(200)).mount(&server).await;
        Mock::given(method("GET"))
            .and(path("/mrf.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("hello world"))
            .mount(&server)
            .await;

        // Previous row has no etag/last_modified either (e.g. baseline
        // couldn't get any), but does have a prior hash to compare.
        let prev = prev_row(None, None, Some("deadbeef"));
        let outcome = check_mrf_metadata(&prober(), &format!("{}/mrf.json", server.uri()), Some(&prev)).await;
        assert_eq!(outcome.change_detection_method, Some("sha1"));
        assert_eq!(outcome.changed_from_previous, Some(true)); // deadbeef != real hash
        assert_eq!(outcome.sha1_hash.as_deref(), Some("2aae6c35c94fcfb415dbe95f408b9ce91ee846ed"));
    }

    #[tokio::test]
    async fn unreachable_url_records_no_method_or_decision() {
        let server = MockServer::start().await;
        Mock::given(method("HEAD")).and(path("/mrf.json")).respond_with(ResponseTemplate::new(404)).mount(&server).await;

        let prev = prev_row(Some("\"v1\""), None, None);
        let outcome = check_mrf_metadata(&prober(), &format!("{}/mrf.json", server.uri()), Some(&prev)).await;
        assert!(!outcome.headers.reachable);
        assert!(outcome.change_detection_method.is_none());
        assert!(outcome.changed_from_previous.is_none());
    }
}
