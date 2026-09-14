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

use crate::db::models::{Hospital, HospitalListItem, MrfDiscovery, StatusCount};

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
