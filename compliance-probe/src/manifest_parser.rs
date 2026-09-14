//! Parses a fetched `cms-hpt.txt` manifest into a list of MRF references.
//!
//! **Stub — not yet implemented, and the exact wire format is still an
//! open question.** What's confirmed (via CMS's own FAQ PDF and the
//! Drupal module docs for hospitals publishing this file — see
//! IMPLEMENTATION_NOTES.md for links) is the *field set* each entry
//! carries:
//! - `mrf-url` — direct link to the machine-readable file
//! - `source-page-url` — the page a human would find the MRF from
//! - `location-name` — which hospital location this MRF covers (for
//!   systems with multiple facilities under one manifest)
//! - `contact-name` / `contact-email` — technical point of contact
//!
//! What's **not** confirmed: the exact serialization — delimiter between
//! fields on a line, how multiple entries are separated, whether it's
//! one-entry-per-line or a block format. CMS's own generator tool
//! (<https://cmsgov.github.io/hpt-tool/txt-generator/>) is what hospitals
//! use to produce this file and is the most reliable place to get an
//! example, but it's a JS-rendered SPA this session couldn't extract
//! example output from. Before implementing this parser: pull a handful
//! of real `cms-hpt.txt` files from actual hospital sites (once network
//! access allows it) and reverse-engineer the format from those directly
//! — that's likely faster and more reliable than chasing the spec doc
//! further.

use crate::error::ProbeError;

#[derive(Debug, Clone, PartialEq)]
pub struct MrfEntry {
    pub location_name: Option<String>,
    pub mrf_url: String,
    pub source_page_url: Option<String>,
    pub contact_name: Option<String>,
    pub contact_email: Option<String>,
}

pub fn parse_manifest(_raw: &str) -> Result<Vec<MrfEntry>, ProbeError> {
    Err(ProbeError::NotImplemented("manifest_parser::parse_manifest"))
}
