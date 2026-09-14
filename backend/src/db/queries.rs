//! Typed query functions used by the route handlers.
//!
//! Uses `sqlx`'s runtime (non-macro) query API throughout —
//! deliberately, since `query!`/`query_as!` need a live database (or a
//! checked-in `.sqlx` query cache) at *compile* time, and this workspace
//! is built in an environment that can't necessarily reach a dev
//! database. `sqlx::query_as::<_, T>()` still gets compile-time-checked
//! column *mapping* via `FromRow`, just not compile-time-checked SQL.

use chrono::Utc;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};

use crate::db::models::{Hospital, HospitalListItem, MrfDiscovery, NeedsEnrichmentItem, StatusCount};

/// A hospital record's `facility_id` may not have a matching latest-row
/// in `mrf_discoveries` yet — this join condition is reused by both the
/// list and stats queries so "latest discovery per hospital" means the
/// same thing everywhere.
const LATEST_DISCOVERY_JOIN: &str = r#"
    LEFT JOIN mrf_discoveries d
        ON d.facility_id = h.facility_id
       AND d.checked_at = (
             SELECT MAX(checked_at) FROM mrf_discoveries WHERE facility_id = h.facility_id
           )
"#;

#[derive(Debug, Default, Clone)]
pub struct HospitalFilters {
    pub state: Option<String>,
    pub hospital_type: Option<String>,
    pub discovery_status: Option<String>,
    pub enriched: Option<bool>,
}

fn push_filters(qb: &mut QueryBuilder<'_, Sqlite>, filters: &HospitalFilters) {
    let mut first = true;

    if let Some(state) = &filters.state {
        qb.push(if first { " WHERE " } else { " AND " });
        first = false;
        qb.push("h.state = ").push_bind(state.clone());
    }
    if let Some(hospital_type) = &filters.hospital_type {
        qb.push(if first { " WHERE " } else { " AND " });
        first = false;
        qb.push("h.hospital_type = ").push_bind(hospital_type.clone());
    }
    if let Some(discovery_status) = &filters.discovery_status {
        qb.push(if first { " WHERE " } else { " AND " });
        first = false;
        qb.push("d.discovery_status = ").push_bind(discovery_status.clone());
    }
    if let Some(enriched) = filters.enriched {
        qb.push(if first { " WHERE " } else { " AND " });
        first = false;
        if enriched {
            qb.push("h.enriched_at IS NOT NULL");
        } else {
            qb.push("h.enriched_at IS NULL");
        }
    }
    let _ = first;
}

/// Upserts one hospital record from `cms-ingest`. `ingested_at` is only
/// set on first insert (kept on conflict); `updated_at` always moves
/// forward.
pub async fn upsert_hospital(
    pool: &SqlitePool,
    record: &cms_ingest::HospitalRecord,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        r#"
        INSERT INTO hospitals (
            facility_id, facility_name, address, city, state, zip_code,
            county_name, phone_number, hospital_type, hospital_ownership,
            emergency_services, overall_rating, ingested_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(facility_id) DO UPDATE SET
            facility_name = excluded.facility_name,
            address = excluded.address,
            city = excluded.city,
            state = excluded.state,
            zip_code = excluded.zip_code,
            county_name = excluded.county_name,
            phone_number = excluded.phone_number,
            hospital_type = excluded.hospital_type,
            hospital_ownership = excluded.hospital_ownership,
            emergency_services = excluded.emergency_services,
            overall_rating = excluded.overall_rating,
            updated_at = excluded.updated_at
        "#,
    )
    .bind(&record.facility_id)
    .bind(&record.facility_name)
    .bind(&record.address)
    .bind(&record.city)
    .bind(&record.state)
    .bind(&record.zip_code)
    .bind(&record.county_name)
    .bind(&record.phone_number)
    .bind(&record.hospital_type)
    .bind(&record.hospital_ownership)
    .bind(record.emergency_services)
    .bind(record.overall_rating)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn list_hospitals(
    pool: &SqlitePool,
    filters: &HospitalFilters,
    page: u32,
    per_page: u32,
) -> Result<(Vec<HospitalListItem>, i64), sqlx::Error> {
    let page = page.max(1);
    let offset = (page - 1) as i64 * per_page as i64;

    let mut count_qb: QueryBuilder<Sqlite> = QueryBuilder::new("SELECT COUNT(*) FROM hospitals h");
    count_qb.push(LATEST_DISCOVERY_JOIN);
    push_filters(&mut count_qb, filters);
    let total_count: i64 = count_qb.build_query_scalar().fetch_one(pool).await?;

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        r#"SELECT h.facility_id, h.facility_name, h.address, h.city, h.state, h.zip_code,
                  h.hospital_type, h.latitude, h.longitude, h.website_url,
                  d.discovery_status
           FROM hospitals h"#,
    );
    qb.push(LATEST_DISCOVERY_JOIN);
    push_filters(&mut qb, filters);
    qb.push(" ORDER BY h.facility_id LIMIT ");
    qb.push_bind(per_page as i64);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb
        .build_query_as::<HospitalListItem>()
        .fetch_all(pool)
        .await?;

    Ok((rows, total_count))
}

pub async fn get_hospital(
    pool: &SqlitePool,
    facility_id: &str,
) -> Result<Option<(Hospital, Option<MrfDiscovery>)>, sqlx::Error> {
    let hospital = sqlx::query_as::<_, Hospital>("SELECT * FROM hospitals WHERE facility_id = ?")
        .bind(facility_id)
        .fetch_optional(pool)
        .await?;

    let Some(hospital) = hospital else {
        return Ok(None);
    };

    let discovery = sqlx::query_as::<_, MrfDiscovery>(
        "SELECT * FROM mrf_discoveries WHERE facility_id = ? ORDER BY checked_at DESC LIMIT 1",
    )
    .bind(facility_id)
    .fetch_optional(pool)
    .await?;

    Ok(Some((hospital, discovery)))
}

pub async fn total_hospitals(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM hospitals")
        .fetch_one(pool)
        .await
}

pub async fn enriched_count(pool: &SqlitePool) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT COUNT(*) FROM hospitals WHERE enriched_at IS NOT NULL")
        .fetch_one(pool)
        .await
}

/// `(state, discovery_status, count)` for every state × status
/// combination present, with `discovery_status` normalized to
/// `"not_checked"` when a hospital has no `mrf_discoveries` row. Callers
/// build both the overall `discovery` breakdown and the `by_state`
/// breakdown in `GET /api/stats` out of this one query's results.
pub async fn status_counts_by_state(pool: &SqlitePool) -> Result<Vec<StatusCount>, sqlx::Error> {
    let rows: Vec<(String, Option<String>, i64)> = sqlx::query_as(
        r#"
        SELECT h.state, d.discovery_status, COUNT(*) as n
        FROM hospitals h
        LEFT JOIN mrf_discoveries d
            ON d.facility_id = h.facility_id
           AND d.checked_at = (
                 SELECT MAX(checked_at) FROM mrf_discoveries WHERE facility_id = h.facility_id
               )
        GROUP BY h.state, d.discovery_status
        "#,
    )
    .fetch_all(pool)
    .await?;

    Ok(rows
        .into_iter()
        .map(|(state, status, count)| StatusCount {
            state,
            status: status.unwrap_or_else(|| "not_checked".to_string()),
            count,
        })
        .collect())
}

/// One row of what `enrich_store::HospitalStore::list_unenriched` needs
/// to build a `geo_enrich::enricher::EnrichmentTarget`: the address
/// fields plus the two columns that tell it what's already on the row
/// (`latitude` — `Some` means this hospital already has coordinates;
/// `website_url` — carried through as-is). Returned as a plain tuple
/// rather than a `FromRow` struct on purpose — the target shape belongs
/// to `geo_enrich`, a crate this one depends on (not the reverse), so it
/// can't derive `sqlx::FromRow` (that would pull `sqlx` into `geo-enrich`,
/// which is meant to stay database-agnostic — see that crate's
/// `enricher.rs`).
pub type UnenrichedRow = (String, String, String, String, String, String, Option<f64>, Option<String>);

/// Up to `limit` hospitals to (re-)process.
///
/// `retry_incomplete: false` selects only `enriched_at IS NULL` rows —
/// the normal, default-cost path. `retry_incomplete: true` additionally
/// selects rows that already have `enriched_at` set but are still
/// missing `latitude` or `website_url` — see
/// `geo_enrich::enricher::UnenrichedHospitalStore::list_unenriched`'s
/// doc comment for why that's opt-in rather than always-on.
pub async fn list_unenriched_hospitals(
    pool: &SqlitePool,
    limit: u32,
    retry_incomplete: bool,
) -> Result<Vec<UnenrichedRow>, sqlx::Error> {
    let where_clause = if retry_incomplete {
        "enriched_at IS NULL \
         OR latitude IS NULL \
         OR website_url IS NULL OR website_url = ''"
    } else {
        "enriched_at IS NULL"
    };
    let sql = format!(
        "SELECT facility_id, facility_name, address, city, state, zip_code, latitude, website_url \
         FROM hospitals WHERE {where_clause} LIMIT ?"
    );
    sqlx::query_as(&sql).bind(limit as i64).fetch_all(pool).await
}

/// One hospital's geocode result, in the shape `save_enrichment` needs:
/// `(provider, latitude, longitude, formatted_address, confidence)`.
pub type GeocodeRow<'a> = (&'a str, f64, f64, Option<&'a str>, Option<f64>);

/// Persists one enrichment outcome. `geocode: None` means no provider
/// could resolve this address — `enriched_at` is still stamped (so this
/// hospital isn't re-attempted on every future enrich run, per
/// Architecture.md's "skips hospitals that already have `enriched_at`
/// set"), but `latitude`/`longitude`/etc. are left untouched (`NULL` on
/// a first run). `website_url: None` leaves any existing value alone
/// (`COALESCE`) rather than clobbering a previously-found website with
/// nothing.
pub async fn save_enrichment(
    pool: &SqlitePool,
    facility_id: &str,
    geocode: Option<GeocodeRow<'_>>,
    website_url: Option<&str>,
) -> Result<(), sqlx::Error> {
    let now = Utc::now().to_rfc3339();

    match geocode {
        Some((provider, lat, lon, formatted_address, confidence)) => {
            sqlx::query(
                r#"
                UPDATE hospitals SET
                    latitude = ?,
                    longitude = ?,
                    formatted_address = ?,
                    geo_provider = ?,
                    geo_confidence = ?,
                    website_url = COALESCE(?, website_url),
                    enriched_at = ?,
                    updated_at = ?
                WHERE facility_id = ?
                "#,
            )
            .bind(lat)
            .bind(lon)
            .bind(formatted_address)
            .bind(provider)
            .bind(confidence)
            .bind(website_url)
            .bind(&now)
            .bind(&now)
            .bind(facility_id)
            .execute(pool)
            .await?;
        }
        None => {
            sqlx::query(
                r#"
                UPDATE hospitals SET
                    website_url = COALESCE(?, website_url),
                    enriched_at = ?,
                    updated_at = ?
                WHERE facility_id = ?
                "#,
            )
            .bind(website_url)
            .bind(&now)
            .bind(&now)
            .bind(facility_id)
            .execute(pool)
            .await?;
        }
    }

    Ok(())
}

/// `missing` narrows the "still incomplete" set to just `"coordinates"` or
/// `"website"`; `None` (or any other value the caller might pass — callers
/// are expected to have already validated it, see
/// `routes::hospitals::needs_enrichment`) keeps the default "either" filter.
fn push_needs_enrichment_filters(qb: &mut QueryBuilder<'_, Sqlite>, missing: Option<&str>, state: Option<&str>) {
    match missing {
        Some("coordinates") => {
            qb.push(" AND latitude IS NULL");
        }
        Some("website") => {
            qb.push(" AND (website_url IS NULL OR website_url = '')");
        }
        _ => {
            qb.push(" AND (latitude IS NULL OR website_url IS NULL OR website_url = '')");
        }
    }
    if let Some(state) = state {
        qb.push(" AND state = ").push_bind(state.to_string());
    }
}

/// Backs `GET /api/hospitals/needs-enrichment` — hospitals the automated
/// pipeline already ran on (`enriched_at IS NOT NULL`) but is still
/// missing coordinates and/or a website for. See
/// `routes::hospitals::needs_enrichment` for query-param validation.
pub async fn list_needs_enrichment(
    pool: &SqlitePool,
    missing: Option<&str>,
    state: Option<&str>,
    page: u32,
    per_page: u32,
) -> Result<(Vec<NeedsEnrichmentItem>, i64), sqlx::Error> {
    let page = page.max(1);
    let offset = (page - 1) as i64 * per_page as i64;

    let mut count_qb: QueryBuilder<Sqlite> =
        QueryBuilder::new("SELECT COUNT(*) FROM hospitals WHERE enriched_at IS NOT NULL");
    push_needs_enrichment_filters(&mut count_qb, missing, state);
    let total_count: i64 = count_qb.build_query_scalar().fetch_one(pool).await?;

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        r#"SELECT facility_id, facility_name, address, city, state, zip_code,
                  (latitude IS NULL) AS missing_coordinates,
                  (website_url IS NULL OR website_url = '') AS missing_website
           FROM hospitals
           WHERE enriched_at IS NOT NULL"#,
    );
    push_needs_enrichment_filters(&mut qb, missing, state);
    qb.push(" ORDER BY facility_id LIMIT ");
    qb.push_bind(per_page as i64);
    qb.push(" OFFSET ");
    qb.push_bind(offset);

    let rows = qb.build_query_as::<NeedsEnrichmentItem>().fetch_all(pool).await?;

    Ok((rows, total_count))
}

/// Backs `PATCH /api/hospitals/:facility_id/enrichment`. Only the fields
/// that are `Some` are written — unlike `save_enrichment` (the automated
/// pipeline's save path), a value passed here **overwrites** whatever was
/// there before, per Architecture.md's manual-enrichment spec ("a manual
/// correction here is assumed intentional"). `latitude`/`longitude` are
/// expected to arrive together or not at all — the route handler
/// validates that (and the lat/lon range, and the website URL scheme)
/// before calling this. Stamps `geo_provider = "manual"` and
/// `geo_confidence = 1.0` when coordinates are set, and only fills in
/// `enriched_at` if it wasn't already set (`COALESCE`), same as the
/// automated path.
///
/// Returns the number of rows affected — `0` means `facility_id` doesn't
/// exist, which the caller maps to `404`.
pub async fn manual_enrich_hospital(
    pool: &SqlitePool,
    facility_id: &str,
    latitude: Option<f64>,
    longitude: Option<f64>,
    website_url: Option<&str>,
) -> Result<u64, sqlx::Error> {
    let now = Utc::now().to_rfc3339();

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("UPDATE hospitals SET ");
    let mut first = true;

    if let (Some(lat), Some(lon)) = (latitude, longitude) {
        qb.push("latitude = ").push_bind(lat);
        qb.push(", longitude = ").push_bind(lon);
        qb.push(", geo_provider = ").push_bind("manual");
        qb.push(", geo_confidence = ").push_bind(1.0_f64);
        first = false;
    }
    if let Some(url) = website_url {
        if !first {
            qb.push(", ");
        }
        qb.push("website_url = ").push_bind(url.to_string());
        first = false;
    }
    if !first {
        qb.push(", ");
    }
    qb.push("enriched_at = COALESCE(enriched_at, ");
    qb.push_bind(now.clone());
    qb.push(")");
    qb.push(", updated_at = ");
    qb.push_bind(now);
    qb.push(" WHERE facility_id = ");
    qb.push_bind(facility_id.to_string());

    let result = qb.build().execute(pool).await?;
    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    /// A fresh, fully-migrated in-memory database. `max_connections(1)`
    /// matters here — SQLite's `:memory:` database is per-connection, so
    /// a pool that hands out more than one connection would make the
    /// migration invisible to whichever connection a later query lands
    /// on.
    async fn test_pool() -> SqlitePool {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .expect("open in-memory sqlite db");
        sqlx::migrate!("./migrations").run(&pool).await.expect("run migrations");
        pool
    }

    /// Inserts a minimal hospital row with the given enrichment state.
    #[allow(clippy::too_many_arguments)]
    async fn insert_hospital(
        pool: &SqlitePool,
        facility_id: &str,
        state: &str,
        latitude: Option<f64>,
        website_url: Option<&str>,
        enriched: bool,
    ) {
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            r#"
            INSERT INTO hospitals (
                facility_id, facility_name, address, city, state, zip_code,
                hospital_type, hospital_ownership, emergency_services,
                latitude, longitude, website_url, enriched_at,
                ingested_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            "#,
        )
        .bind(facility_id)
        .bind(format!("Test Hospital {facility_id}"))
        .bind("123 Main St")
        .bind("Anytown")
        .bind(state)
        .bind("00000")
        .bind("Acute Care Hospitals")
        .bind("Voluntary non-profit")
        .bind(false)
        .bind(latitude)
        .bind(latitude.map(|_| -70.0))
        .bind(website_url)
        .bind(enriched.then(|| now.clone()))
        .bind(&now)
        .bind(&now)
        .execute(pool)
        .await
        .expect("insert test hospital");
    }

    #[tokio::test]
    async fn needs_enrichment_only_surfaces_incomplete_enriched_hospitals() {
        let pool = test_pool().await;

        // Enriched, missing coordinates only.
        insert_hospital(&pool, "H1", "CA", None, Some("https://h1.example"), true).await;
        // Enriched, missing website only.
        insert_hospital(&pool, "H2", "CA", Some(34.0), None, true).await;
        // Enriched, missing both.
        insert_hospital(&pool, "H3", "TX", None, None, true).await;
        // Never run by the pipeline at all — must NOT show up here.
        insert_hospital(&pool, "H4", "CA", None, None, false).await;
        // Fully resolved — must NOT show up here.
        insert_hospital(&pool, "H5", "CA", Some(34.0), Some("https://h5.example"), true).await;

        let (either, total) = list_needs_enrichment(&pool, None, None, 1, 50).await.unwrap();
        let mut ids: Vec<_> = either.iter().map(|r| r.facility_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["H1", "H2", "H3"]);
        assert_eq!(total, 3);

        let (coords_only, _) = list_needs_enrichment(&pool, Some("coordinates"), None, 1, 50)
            .await
            .unwrap();
        let mut ids: Vec<_> = coords_only.iter().map(|r| r.facility_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["H1", "H3"]);
        assert!(coords_only.iter().all(|r| r.missing_coordinates));

        let (website_only, _) = list_needs_enrichment(&pool, Some("website"), None, 1, 50)
            .await
            .unwrap();
        let mut ids: Vec<_> = website_only.iter().map(|r| r.facility_id.clone()).collect();
        ids.sort();
        assert_eq!(ids, vec!["H2", "H3"]);

        let (tx_only, tx_total) = list_needs_enrichment(&pool, None, Some("TX"), 1, 50).await.unwrap();
        assert_eq!(tx_total, 1);
        assert_eq!(tx_only[0].facility_id, "H3");
        assert!(tx_only[0].missing_coordinates && tx_only[0].missing_website);
    }

    #[tokio::test]
    async fn manual_enrich_sets_coordinates_and_marks_provider_manual() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "CA", None, None, false).await;

        let rows = manual_enrich_hospital(&pool, "H1", Some(31.451773), Some(-85.63101), Some("https://example-hospital.org"))
            .await
            .unwrap();
        assert_eq!(rows, 1);

        let (hospital, _) = get_hospital(&pool, "H1").await.unwrap().unwrap();
        assert_eq!(hospital.latitude, Some(31.451773));
        assert_eq!(hospital.longitude, Some(-85.63101));
        assert_eq!(hospital.geo_provider.as_deref(), Some("manual"));
        assert_eq!(hospital.geo_confidence, Some(1.0));
        assert_eq!(hospital.website_url.as_deref(), Some("https://example-hospital.org"));
        assert!(hospital.enriched_at.is_some());
        let first_enriched_at = hospital.enriched_at.clone().unwrap();

        // A later website-only correction overwrites the website but
        // doesn't touch the coordinates or clobber `enriched_at`.
        let rows = manual_enrich_hospital(&pool, "H1", None, None, Some("https://new-site.example")).await.unwrap();
        assert_eq!(rows, 1);
        let (hospital, _) = get_hospital(&pool, "H1").await.unwrap().unwrap();
        assert_eq!(hospital.website_url.as_deref(), Some("https://new-site.example"));
        assert_eq!(hospital.latitude, Some(31.451773));
        assert_eq!(hospital.geo_provider.as_deref(), Some("manual"));
        assert_eq!(hospital.enriched_at, Some(first_enriched_at));
    }

    #[tokio::test]
    async fn manual_enrich_unknown_facility_affects_no_rows() {
        let pool = test_pool().await;
        let rows = manual_enrich_hospital(&pool, "does-not-exist", Some(1.0), Some(2.0), None)
            .await
            .unwrap();
        assert_eq!(rows, 0);
    }
}
