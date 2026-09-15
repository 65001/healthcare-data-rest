//! Resolves a `cms-hpt.txt` manifest's `location-name` entries (see
//! `compliance_probe::manifest_parser`) to specific hospitals in the
//! `hospitals` table by fuzzy name matching.
//!
//! Deliberately **not** the only safeguard: a name-only match can be
//! fooled by two unrelated hospitals with similar names, or by a
//! network's manifest being pointed at by mistake/malice. See
//! `db::queries::ownership_reachable_ccns` — `routes::pipeline`'s
//! `network_manifest_url` discover mode requires a match to pass *both*
//! this name-similarity check *and* be present in the CMS
//! ownership-disclosure-derived reachable set before it's persisted.

use std::collections::HashSet;

use compliance_probe::manifest_parser::MrfEntry;

/// One hospital candidate to match manifest entries against — just the
/// two fields matching needs, kept independent of `db::models::Hospital`
/// so this module has no `sqlx` dependency.
#[derive(Debug, Clone)]
pub struct MatchCandidate {
    pub facility_id: String,
    pub facility_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMatch {
    pub entry_index: usize,
    pub facility_id: String,
    pub facility_name: String,
    pub score: f64,
}

/// Minimum Jaro-Winkler similarity (on normalized names) to accept a
/// match at all. Calibrated against a real, live manifest
/// (sentara.com/cms-hpt.txt, 2026-09-15) cross-checked against CMS's own
/// "Hospital General Information" facility names for the same system:
/// every genuine hospital location-name scored well above this bar
/// (exact matches after normalization, or one word's difference scoring
/// ~0.96), while every satellite/outpatient location-name with no real
/// CCN — the majority-shared-prefix case Jaro-Winkler is most at risk of
/// over-scoring — scored well below it. See this module's tests for the
/// exact cases.
pub const DEFAULT_MATCH_THRESHOLD: f64 = 0.90;

/// Uppercases, replaces every run of non-alphanumeric characters with a
/// single space, and trims — so `"Sentara Norfolk General Hospital"` and
/// `"SENTARA NORFOLK GENERAL HOSPITAL"` compare identically regardless
/// of case or punctuation differences between a network's own branding
/// and CMS's on-file name.
pub fn normalize_name(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_space = true; // swallows leading separators
    for ch in s.chars() {
        if ch.is_alphanumeric() {
            out.push(ch.to_ascii_uppercase());
            last_was_space = false;
        } else if !last_was_space {
            out.push(' ');
            last_was_space = true;
        }
    }
    if out.ends_with(' ') {
        out.pop();
    }
    out
}

pub fn similarity(a: &str, b: &str) -> f64 {
    strsim::jaro_winkler(&normalize_name(a), &normalize_name(b))
}

/// Resolves each of `entries` (that has a `location_name`) to at most
/// one candidate, and each candidate to at most one entry — a greedy
/// highest-score-first bipartite assignment, so two entries competing
/// for the same hospital doesn't double-assign it. Only pairs scoring
/// `>= threshold` are considered at all. Returned matches are sorted by
/// `entry_index`.
pub fn resolve_entries(entries: &[MrfEntry], candidates: &[MatchCandidate], threshold: f64) -> Vec<ResolvedMatch> {
    let mut scored: Vec<(usize, usize, f64)> = Vec::new();
    for (entry_idx, entry) in entries.iter().enumerate() {
        let Some(location_name) = &entry.location_name else {
            continue;
        };
        for (cand_idx, candidate) in candidates.iter().enumerate() {
            let score = similarity(location_name, &candidate.facility_name);
            if score >= threshold {
                scored.push((entry_idx, cand_idx, score));
            }
        }
    }
    // Highest-confidence pairs claim their entry/candidate first.
    scored.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

    let mut used_entries = HashSet::new();
    let mut used_candidates = HashSet::new();
    let mut results = Vec::new();
    for (entry_idx, cand_idx, score) in scored {
        if used_entries.contains(&entry_idx) || used_candidates.contains(&cand_idx) {
            continue;
        }
        used_entries.insert(entry_idx);
        used_candidates.insert(cand_idx);
        results.push(ResolvedMatch {
            entry_index: entry_idx,
            facility_id: candidates[cand_idx].facility_id.clone(),
            facility_name: candidates[cand_idx].facility_name.clone(),
            score,
        });
    }
    results.sort_by_key(|r| r.entry_index);
    results
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(location_name: &str) -> MrfEntry {
        MrfEntry {
            location_name: Some(location_name.to_string()),
            mrf_url: "https://example.com/mrf.json".to_string(),
            source_page_url: None,
            contact_name: None,
            contact_email: None,
        }
    }

    fn candidate(facility_id: &str, facility_name: &str) -> MatchCandidate {
        MatchCandidate {
            facility_id: facility_id.to_string(),
            facility_name: facility_name.to_string(),
        }
    }

    #[test]
    fn normalize_collapses_case_and_punctuation() {
        assert_eq!(normalize_name("Sentara CarePlex Hospital"), "SENTARA CAREPLEX HOSPITAL");
        assert_eq!(normalize_name("St. Mary's Hospital  -  Downtown"), "ST MARY S HOSPITAL DOWNTOWN");
    }

    // Real (location-name, CMS facility_name) pairs, live 2026-09-15:
    // sentara.com/cms-hpt.txt vs CMS's "Hospital General Information".

    #[test]
    fn exact_up_to_case_scores_near_one() {
        assert!(similarity("Sentara Norfolk General Hospital", "SENTARA NORFOLK GENERAL HOSPITAL") > 0.99);
        assert!(similarity("Sentara Leigh Hospital", "SENTARA LEIGH HOSPITAL") > 0.99);
        assert!(similarity("Sentara CarePlex Hospital", "SENTARA CAREPLEX HOSPITAL") > 0.99);
    }

    #[test]
    fn one_word_difference_still_clears_the_default_threshold() {
        // CMS's on-file name omits "Regional" that the network's own
        // manifest includes.
        let score = similarity("Sentara Albemarle Regional Medical Center", "SENTARA ALBEMARLE MEDICAL CENTER");
        assert!(score >= DEFAULT_MATCH_THRESHOLD, "score was {score}");
    }

    #[test]
    fn satellite_location_sharing_a_long_prefix_does_not_cross_the_threshold() {
        // A real trap for Jaro-Winkler's common-prefix bonus: an
        // outpatient satellite name that shares "Sentara Martha
        // Jefferson" with its parent hospital, but isn't that hospital.
        let score = similarity(
            "Sentara Martha Jefferson Outpatient Care Center at Proffit Road",
            "SENTARA MARTHA JEFFERSON HOSPITAL",
        );
        assert!(score < DEFAULT_MATCH_THRESHOLD, "score was {score}, expected a clear non-match");
    }

    #[test]
    fn generic_rebranded_satellite_name_does_not_match_anything() {
        // "Sentara Independence" (a freestanding ER reusing Virginia
        // Beach General's MRF) has no CCN of its own and shares almost
        // no substring with any real Sentara hospital name.
        let candidates = [
            candidate("490057", "SENTARA VIRGINIA BEACH GENERAL HOSPITAL"),
            candidate("490007", "SENTARA NORFOLK GENERAL HOSPITAL"),
        ];
        for c in &candidates {
            let score = similarity("Sentara Independence", &c.facility_name);
            assert!(score < DEFAULT_MATCH_THRESHOLD, "unexpected match against {}: {score}", c.facility_name);
        }
    }

    #[test]
    fn resolve_entries_matches_real_sentara_manifest_against_real_cms_names() {
        // Trimmed, real shape: a handful of genuine hospitals plus two
        // satellites reusing a parent's MRF and one entry for a hospital
        // not in the candidate list at all.
        let entries = vec![
            entry("Sentara Albemarle Regional Medical Center"),
            entry("Sentara Norfolk General Hospital"),
            entry("Sentara Leigh Hospital"),
            entry("Sentara Independence"), // satellite — no CCN, must not match
            entry("Sentara Martha Jefferson Outpatient Care Center at Proffit Road"), // satellite
        ];
        let candidates = vec![
            candidate("340109", "SENTARA ALBEMARLE MEDICAL CENTER"),
            candidate("490007", "SENTARA NORFOLK GENERAL HOSPITAL"),
            candidate("490046", "SENTARA LEIGH HOSPITAL"),
            candidate("490077", "SENTARA MARTHA JEFFERSON HOSPITAL"),
            candidate("490057", "SENTARA VIRGINIA BEACH GENERAL HOSPITAL"),
        ];

        let matches = resolve_entries(&entries, &candidates, DEFAULT_MATCH_THRESHOLD);

        let matched_entries: Vec<usize> = matches.iter().map(|m| m.entry_index).collect();
        assert_eq!(matched_entries, vec![0, 1, 2]); // only the three real hospitals matched

        assert_eq!(matches[0].facility_id, "340109");
        assert_eq!(matches[1].facility_id, "490007");
        assert_eq!(matches[2].facility_id, "490046");
    }

    #[test]
    fn resolve_entries_never_double_assigns_a_candidate() {
        let entries = vec![entry("Sentara Leigh Hospital"), entry("Sentara Leigh Hosptial")]; // typo duplicate
        let candidates = vec![candidate("490046", "SENTARA LEIGH HOSPITAL")];

        let matches = resolve_entries(&entries, &candidates, 0.85);
        assert_eq!(matches.len(), 1); // the single candidate goes to only one entry
    }

    #[test]
    fn entry_without_a_location_name_is_skipped() {
        let mut e = entry("placeholder");
        e.location_name = None;
        let candidates = vec![candidate("490046", "SENTARA LEIGH HOSPITAL")];
        assert!(resolve_entries(&[e], &candidates, 0.5).is_empty());
    }
}
