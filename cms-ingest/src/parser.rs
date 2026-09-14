//! Normalizes raw CMS "Hospital General Information" datastore rows into
//! [`crate::HospitalRecord`].
//!
//! The live schema (confirmed 2026-09-14 against
//! `https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0`)
//! does **not** match the column names implied by Architecture.md's table
//! definition — CMS uses `citytown`, `countyparish`, and `telephone_number`
//! rather than `city`, `county_name`, `phone_number`. This module is the
//! seam that translates between the two, so the rest of the codebase can
//! use the friendlier internal names.

use serde_json::{Map, Value};

use crate::error::IngestError;
use crate::HospitalRecord;

fn required_str(
    row: &Map<String, Value>,
    cms_field: &'static str,
    facility_id: &str,
) -> Result<String, IngestError> {
    row.get(cms_field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| IngestError::MissingField {
            facility_id: facility_id.to_string(),
            field: cms_field,
        })
}

fn optional_str(row: &Map<String, Value>, cms_field: &'static str) -> Option<String> {
    row.get(cms_field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("not available"))
        .map(str::to_string)
}

/// CMS renders booleans as the strings "Yes" / "No" (and, in some
/// datasets, leaves the field blank when unknown). Blank/unrecognized
/// values are treated as `false` rather than erroring, since emergency
/// services status is not load-bearing for the rest of the pipeline.
fn parse_yes_no(row: &Map<String, Value>, cms_field: &'static str) -> bool {
    row.get(cms_field)
        .and_then(Value::as_str)
        .map(|s| s.trim().eq_ignore_ascii_case("yes"))
        .unwrap_or(false)
}

fn parse_rating(
    row: &Map<String, Value>,
    cms_field: &'static str,
    facility_id: &str,
) -> Result<Option<i32>, IngestError> {
    match row.get(cms_field).and_then(Value::as_str).map(str::trim) {
        None | Some("") => Ok(None),
        Some(s) if s.eq_ignore_ascii_case("not available") => Ok(None),
        Some(s) => s.parse::<i32>().map(Some).map_err(|_| IngestError::InvalidFieldValue {
            facility_id: facility_id.to_string(),
            field: cms_field,
            value: s.to_string(),
        }),
    }
}

/// Parse one raw datastore row into a [`HospitalRecord`].
///
/// Returns `Err` only for rows missing a field the rest of the system
/// treats as non-negotiable (facility_id, name, address, city, state,
/// zip, type, ownership). Callers ingesting a full page should log and
/// skip individual row errors rather than aborting the whole batch — CMS
/// data has occasional partial rows.
pub fn parse_record(row: &Map<String, Value>) -> Result<HospitalRecord, IngestError> {
    // facility_id doubles as the primary key, so it has to come out first
    // (everything else's error messages reference it).
    let facility_id = row
        .get("facility_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| IngestError::MissingField {
            facility_id: "<unknown>".to_string(),
            field: "facility_id",
        })?
        .to_string();

    Ok(HospitalRecord {
        facility_id: facility_id.clone(),
        facility_name: required_str(row, "facility_name", &facility_id)?,
        address: required_str(row, "address", &facility_id)?,
        city: required_str(row, "citytown", &facility_id)?,
        state: required_str(row, "state", &facility_id)?,
        zip_code: required_str(row, "zip_code", &facility_id)?,
        county_name: optional_str(row, "countyparish"),
        phone_number: optional_str(row, "telephone_number"),
        hospital_type: required_str(row, "hospital_type", &facility_id)?,
        hospital_ownership: required_str(row, "hospital_ownership", &facility_id)?,
        emergency_services: parse_yes_no(row, "emergency_services"),
        overall_rating: parse_rating(row, "hospital_overall_rating", &facility_id)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample_row() -> Map<String, Value> {
        // Trimmed from a live response for facility 010001.
        json!({
            "facility_id": "010001",
            "facility_name": "SOUTHEAST HEALTH MEDICAL CENTER",
            "address": "1108 ROSS CLARK CIRCLE",
            "citytown": "DOTHAN",
            "state": "AL",
            "zip_code": "36301",
            "countyparish": "HOUSTON",
            "telephone_number": "(334) 793-8701",
            "hospital_type": "Acute Care Hospitals",
            "hospital_ownership": "Government - Hospital District or Authority",
            "emergency_services": "Yes",
            "hospital_overall_rating": "4",
            "hospital_overall_rating_footnote": ""
        })
        .as_object()
        .unwrap()
        .clone()
    }

    #[test]
    fn parses_a_complete_row() {
        let record = parse_record(&sample_row()).expect("should parse");
        assert_eq!(record.facility_id, "010001");
        assert_eq!(record.city, "DOTHAN");
        assert_eq!(record.county_name.as_deref(), Some("HOUSTON"));
        assert!(record.emergency_services);
        assert_eq!(record.overall_rating, Some(4));
    }

    #[test]
    fn treats_not_available_rating_as_none() {
        let mut row = sample_row();
        row.insert("hospital_overall_rating".into(), json!("Not Available"));
        let record = parse_record(&row).expect("should parse");
        assert_eq!(record.overall_rating, None);
    }

    #[test]
    fn errors_on_missing_required_field() {
        let mut row = sample_row();
        row.remove("citytown");
        let err = parse_record(&row).unwrap_err();
        assert!(matches!(err, IngestError::MissingField { field: "citytown", .. }));
    }

    #[test]
    fn rejects_unparseable_rating() {
        let mut row = sample_row();
        row.insert("hospital_overall_rating".into(), json!("N/A-ish"));
        let err = parse_record(&row).unwrap_err();
        assert!(matches!(err, IngestError::InvalidFieldValue { field: "hospital_overall_rating", .. }));
    }
}
