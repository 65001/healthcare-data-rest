//! Parses a fetched `cms-hpt.txt` manifest into a list of MRF references.
//!
//! **Format confirmed live 2026-09-15** against a real, multi-location
//! manifest (<https://www.sentara.com/cms-hpt.txt>, 18 entries) — this
//! was previously an open question (see IMPLEMENTATION_NOTES.md's "pull
//! a handful of real `cms-hpt.txt` files" note). The format is a
//! **block format**: entries are separated by one or more blank lines,
//! and each entry is a set of `key: value` lines (field set per CMS's
//! FAQ PDF and the Drupal module docs):
//!
//! ```text
//! location-name: Sentara Norfolk General Hospital
//! source-page-url: https://www.sentara.com/billing/estimating-hospital-charges
//! mrf-url: https://www.sentara.com/Documents/s/sentaranorfolkgeneralhospitalstandardchargesxlsx-1395284
//! contact-name: Casey Simpkins
//! contact-email: payerdropbox@sentara.com
//!
//! location-name: Sentara Leigh Hospital
//! ...
//! ```
//!
//! A single-hospital system's manifest is just one such block. A
//! multi-location health system (Sentara's included both real CCN-bearing
//! hospitals and satellite/outpatient locations reusing a parent
//! hospital's MRF) lists one block per named location — see
//! `backend::mrf_match` for how those location names get resolved back
//! to specific hospitals.
//!
//! Parsing is deliberately lenient: an unrecognized line (extra fields
//! some hospital's generator added, stray text) is ignored rather than
//! failing the whole file, and a block missing the one truly required
//! field (`mrf-url`) is skipped rather than aborting every other block
//! in the same manifest.

use std::collections::HashMap;

use crate::error::ProbeError;

#[derive(Debug, Clone, PartialEq)]
pub struct MrfEntry {
    pub location_name: Option<String>,
    pub mrf_url: String,
    pub source_page_url: Option<String>,
    pub contact_name: Option<String>,
    pub contact_email: Option<String>,
}

fn finalize_entry(current: &mut HashMap<String, String>, entries: &mut Vec<MrfEntry>) {
    if current.is_empty() {
        return;
    }
    // `mrf-url` is the one field this parser treats as load-bearing —
    // an entry without it isn't usable for discovery regardless of what
    // else it carries. Skip it (don't fail the whole manifest) and move
    // on to the next block.
    if let Some(mrf_url) = current.remove("mrf-url") {
        entries.push(MrfEntry {
            location_name: current.remove("location-name"),
            mrf_url,
            source_page_url: current.remove("source-page-url"),
            contact_name: current.remove("contact-name"),
            contact_email: current.remove("contact-email"),
        });
    }
    current.clear();
}

/// Parses a raw `cms-hpt.txt` body into its entries. Never fails on
/// malformed input — a manifest with zero valid blocks just yields an
/// empty `Vec` (the caller treats that the same as "no manifest" for
/// discovery-status purposes).
pub fn parse_manifest(raw: &str) -> Result<Vec<MrfEntry>, ProbeError> {
    let mut entries = Vec::new();
    let mut current: HashMap<String, String> = HashMap::new();

    for line in raw.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            finalize_entry(&mut current, &mut entries);
            continue;
        }
        if let Some((key, value)) = trimmed.split_once(':') {
            let key = key.trim().to_ascii_lowercase();
            let value = value.trim().to_string();
            if !value.is_empty() {
                current.insert(key, value);
            }
        }
        // A line with no `:` at all is ignored rather than erroring —
        // real-world generators occasionally emit stray blank-ish or
        // comment-like lines.
    }
    // The last block has no trailing blank line to trigger on.
    finalize_entry(&mut current, &mut entries);

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Trimmed from the real, live Sentara manifest fetched 2026-09-15.
    const SENTARA_SAMPLE: &str = "location-name: Sentara Albemarle Regional Medical Center\nsource-page-url: https://www.sentara.com/billing/estimating-hospital-charges\nmrf-url: https://www.sentara.com/Documents/s/sentaraalbemarlemedicalcenterstandardchargesxlsx-1395240\ncontact-name: Casey Simpkins\ncontact-email: payerdropbox@sentara.com\n\nlocation-name: Sentara Norfolk General Hospital\nsource-page-url: https://www.sentara.com/billing/estimating-hospital-charges\nmrf-url: https://www.sentara.com/Documents/s/sentaranorfolkgeneralhospitalstandardchargesxlsx-1395284\ncontact-name: Casey Simpkins\ncontact-email: payerdropbox@sentara.com";

    #[test]
    fn parses_multiple_blocks() {
        let entries = parse_manifest(SENTARA_SAMPLE).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].location_name.as_deref(), Some("Sentara Albemarle Regional Medical Center"));
        assert!(entries[0].mrf_url.ends_with("sentaraalbemarlemedicalcenterstandardchargesxlsx-1395240"));
        assert_eq!(entries[0].contact_email.as_deref(), Some("payerdropbox@sentara.com"));
        assert_eq!(entries[1].location_name.as_deref(), Some("Sentara Norfolk General Hospital"));
    }

    #[test]
    fn single_entry_no_trailing_blank_line() {
        let raw = "location-name: Example Hospital\nmrf-url: https://example.com/mrf.json";
        let entries = parse_manifest(raw).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].mrf_url, "https://example.com/mrf.json");
        assert_eq!(entries[0].source_page_url, None);
    }

    #[test]
    fn multiple_blank_lines_between_blocks_are_fine() {
        let raw = "mrf-url: https://a.example/mrf.json\n\n\n\nmrf-url: https://b.example/mrf.json\n";
        let entries = parse_manifest(raw).unwrap();
        assert_eq!(entries.len(), 2);
    }

    #[test]
    fn block_missing_mrf_url_is_skipped_not_fatal() {
        let raw = "location-name: No MRF Here\ncontact-email: someone@example.com\n\nlocation-name: Has MRF\nmrf-url: https://example.com/mrf.json";
        let entries = parse_manifest(raw).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].location_name.as_deref(), Some("Has MRF"));
    }

    #[test]
    fn empty_input_yields_no_entries() {
        assert_eq!(parse_manifest("").unwrap(), Vec::new());
        assert_eq!(parse_manifest("\n\n\n").unwrap(), Vec::new());
    }

    #[test]
    fn field_names_are_case_insensitive_and_crlf_safe() {
        let raw = "Location-Name: Example\r\nMRF-URL: https://example.com/mrf.json\r\n";
        let entries = parse_manifest(raw).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].mrf_url, "https://example.com/mrf.json");
    }
}
