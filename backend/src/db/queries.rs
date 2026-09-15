//! Typed query functions used by the route handlers.
//!
//! Uses `sqlx`'s runtime (non-macro) query API throughout —
//! deliberately, since `query!`/`query_as!` need a live database (or a
//! checked-in `.sqlx` query cache) at *compile* time, and this workspace
//! is built in an environment that can't necessarily reach a dev
//! database. `sqlx::query_as::<_, T>()` still gets compile-time-checked
//! column *mapping* via `FromRow`, just not compile-time-checked SQL.

use std::collections::HashSet;

use chrono::Utc;
use sqlx::{QueryBuilder, Sqlite, SqlitePool};
use uuid::Uuid;

use crate::db::models::{
    Hospital, HospitalListItem, HospitalOwner, MrfDiscovery, MrfMetadata, NeedsEnrichmentItem, StatusCount,
};

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

// --- Ownership graph (`hospital_enrollments` / `hospital_ownership_edges`) ---
//
// Fed by `cms_ingest::ownership`. Unlike `hospitals`, these two tables
// have no locally-edited fields (nothing here is ever hand-corrected the
// way `geo_provider = "manual"` corrections are) — so each full ingest
// run **replaces** the tables outright rather than upserting row by row.
// That keeps re-ingesting simple and correct: no stale rows from a
// hospital that changed owners since the last run, no need for a
// generated-vs-natural primary key reconciliation story.

/// Replaces the entire `hospital_enrollments` table with `records`, in
/// one transaction (so a reader never sees a half-populated table
/// mid-refresh).
pub async fn replace_hospital_enrollments(
    pool: &SqlitePool,
    records: &[cms_ingest::EnrollmentRecord],
) -> Result<(), sqlx::Error> {
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM hospital_enrollments").execute(&mut *tx).await?;

    for chunk in records.chunks(500) {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
            "INSERT INTO hospital_enrollments (enrollment_id, ccn, associate_id, organization_name, state, ingested_at) ",
        );
        qb.push_values(chunk, |mut b, rec| {
            b.push_bind(&rec.enrollment_id)
                .push_bind(&rec.ccn)
                .push_bind(&rec.associate_id)
                .push_bind(&rec.organization_name)
                .push_bind(&rec.state)
                .push_bind(&now);
        });
        qb.build().execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Replaces the entire `hospital_ownership_edges` table with `records`.
/// See `replace_hospital_enrollments`'s doc comment for why a full
/// replace, not an upsert.
pub async fn replace_ownership_edges(pool: &SqlitePool, records: &[cms_ingest::OwnershipEdgeRecord]) -> Result<(), sqlx::Error> {
    let now = Utc::now().to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM hospital_ownership_edges").execute(&mut *tx).await?;

    for chunk in records.chunks(500) {
        let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
            "INSERT INTO hospital_ownership_edges (
                id, enrollment_id, associate_id, owner_associate_id, owner_type,
                owner_role_code, owner_role_text, owner_organization_name,
                owner_person_name, percentage_ownership, ingested_at
            ) ",
        );
        qb.push_values(chunk, |mut b, rec| {
            b.push_bind(Uuid::new_v4().to_string())
                .push_bind(&rec.enrollment_id)
                .push_bind(&rec.associate_id)
                .push_bind(&rec.owner_associate_id)
                .push_bind(&rec.owner_type)
                .push_bind(&rec.owner_role_code)
                .push_bind(&rec.owner_role_text)
                .push_bind(&rec.owner_organization_name)
                .push_bind(&rec.owner_person_name)
                .push_bind(&rec.percentage_ownership)
                .push_bind(&now);
        });
        qb.build().execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(())
}

/// Ownership-disclosure roles this graph treats as "same network" — an
/// individual physician's minority stake, or a shared corporate officer
/// (`CORPORATE OFFICER`/`CORPORATE DIRECTOR`/`W-2 MANAGING EMPLOYEE` —
/// almost always `owner_type = 'I'` anyway, but excluded explicitly here
/// too) doesn't make two hospitals "the same network"; a controlling
/// organizational owner does. Role strings confirmed live against CMS's
/// "Hospital All Owners" dataset 2026-09-15 (see `cms_ingest::ownership`).
const CONTROLLING_OWNER_ROLES: [&str; 3] = [
    "5% OR GREATER DIRECT OWNERSHIP INTEREST",
    "5% OR GREATER INDIRECT OWNERSHIP INTEREST",
    "OPERATIONAL/MANAGERIAL CONTROL",
];

fn push_in_clause<'a, I, T>(qb: &mut QueryBuilder<'a, Sqlite>, values: I)
where
    I: IntoIterator<Item = T>,
    T: sqlx::Encode<'a, Sqlite> + sqlx::Type<Sqlite> + Send + 'a,
{
    qb.push("(");
    let mut sep = qb.separated(", ");
    for v in values {
        sep.push_bind(v);
    }
    qb.push(")");
}

/// Given one hospital's CCN, returns every CCN CMS's ownership-disclosure
/// data connects it to (including the seed's own) — hospitals that share
/// at least one organizational owner with a controlling role (see
/// `CONTROLLING_OWNER_ROLES`) on their own PECOS enrollment.
///
/// **Single-hop only** — this doesn't walk "my owner's owner's owner"
/// chains. Real data (Sentara, sampled live 2026-09-15) shows a
/// system-wide parent ("SENTARA HEALTH") listed directly as a
/// controlling owner on every one of its hospitals' own enrollment
/// records, so one hop already captures the whole network for that
/// (common) shape. A structure with an intermediate holding company not
/// itself a controlling owner of record on the hospital's own enrollment
/// would need a second hop to connect — not implemented, to avoid an
/// unbounded graph walk over real-world data that can have surprising
/// cycles (an owner that is itself, transitively, owned by the entity it
/// owns — data-quality artifacts do happen in self-reported PECOS data).
///
/// `hospitals.hospital_ownership` categories the reachability graph
/// (`ownership_reachable_ccns`, below) treats as one unified system for
/// cms-hpt.txt cross-validation purposes — a hospital in one of these
/// categories is reachable from every *other* hospital sharing it, even
/// with zero PECOS ownership data connecting them. **Deliberately
/// narrow, and a different question from
/// `ownership_category_has_no_pecos_stake` below**: this list is about
/// whether hospitals sharing a category are actually the *same
/// real-world organization* (so cross-validating a name match between
/// them is safe), not merely about whether PECOS happens to have no
/// ownership data for them. Only a category that really is one coherent
/// system nationwide belongs here — currently just Department of Defense
/// (Military Health System / TRICARE). "Government - Hospital District
/// or Authority" is explicitly excluded despite also having sparse PECOS
/// data (see `ownership_category_has_no_pecos_stake`, which *does* cover
/// it): it covers ~500 real, independent, unrelated local hospital
/// districts, and blanket-connecting all of them here would be exactly
/// the false-positive cross-hospital linking this mechanism exists to
/// prevent.
const OWNERSHIP_GRAPH_UNIFIED_CATEGORIES: [&str; 1] = ["Department of Defense"];

/// Whether `hospital_ownership_category` (a `hospitals.hospital_ownership`
/// value) is one CMS's PECOS ownership-disclosure data structurally never
/// carries a stake for — a government-owned or military facility has no
/// *private* ownership stake to disclose, so an empty owner list from
/// `list_hospital_owners` is expected, not evidence that
/// `POST /api/pipeline/ingest-ownership` needs to run. Exposed so
/// `routes::hospitals::get_ownership` can tell the frontend this
/// directly, rather than the frontend guessing from the category string
/// itself (or worse, showing a "go run ingest" nudge that would be
/// actively misleading for these hospitals).
///
/// **Deliberately broader than `OWNERSHIP_GRAPH_UNIFIED_CATEGORIES`
/// above, and answering a different question.** That list decides
/// whether hospitals sharing a category should be treated as the same
/// real-world network for MRF cross-validation — getting that wrong
/// risks actually corrupting discovery data, so it stays narrow. This
/// function only decides whether the *absence* of ownership data is
/// expected — getting that wrong just shows a slightly wrong hint on a
/// detail page, so it's safe to be broad: every "Government - *"
/// category (Federal, State, Local, Hospital District or Authority — all
/// confirmed present in the real dataset) plus Department of Defense all
/// have no private ownership stake to disclose, even though the district
/// authorities among them are absolutely not one network with each
/// other.
pub fn ownership_category_has_no_pecos_stake(hospital_ownership_category: &str) -> bool {
    hospital_ownership_category == "Department of Defense" || hospital_ownership_category.starts_with("Government - ")
}

/// Never errors on "seed not found" — a CCN with no enrollment row, or
/// with an enrollment but no qualifying owner, just yields `{seed_ccn}`
/// alone (the caller then has no ownership corroboration for that
/// hospital, which is a legitimate, common outcome — e.g. a hospital
/// with no CMS-disclosed organizational owner at all — not an error).
pub async fn ownership_reachable_ccns(pool: &SqlitePool, seed_ccn: &str) -> Result<HashSet<String>, sqlx::Error> {
    let mut reachable = HashSet::new();
    reachable.insert(seed_ccn.to_string());

    let seed_ownership: Option<String> = sqlx::query_scalar("SELECT hospital_ownership FROM hospitals WHERE facility_id = ?")
        .bind(seed_ccn)
        .fetch_optional(pool)
        .await?;
    if let Some(category) = seed_ownership.as_deref() {
        if OWNERSHIP_GRAPH_UNIFIED_CATEGORIES.contains(&category) {
            let siblings: Vec<String> = sqlx::query_scalar("SELECT facility_id FROM hospitals WHERE hospital_ownership = ?")
                .bind(category)
                .fetch_all(pool)
                .await?;
            reachable.extend(siblings);
            return Ok(reachable);
        }
    }

    let seed_enrollment_ids: Vec<String> = sqlx::query_scalar("SELECT enrollment_id FROM hospital_enrollments WHERE ccn = ?")
        .bind(seed_ccn)
        .fetch_all(pool)
        .await?;
    if seed_enrollment_ids.is_empty() {
        return Ok(reachable);
    }

    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT DISTINCT owner_associate_id FROM hospital_ownership_edges \
         WHERE owner_type = 'O' AND owner_associate_id IS NOT NULL AND owner_role_text IN ",
    );
    push_in_clause(&mut qb, CONTROLLING_OWNER_ROLES);
    qb.push(" AND enrollment_id IN ");
    push_in_clause(&mut qb, seed_enrollment_ids.iter().map(String::as_str));
    let owner_associate_ids: Vec<String> = qb.build_query_scalar().fetch_all(pool).await?;

    if owner_associate_ids.is_empty() {
        return Ok(reachable);
    }

    let mut qb2: QueryBuilder<Sqlite> = QueryBuilder::new(
        "SELECT DISTINCT enrollment_id FROM hospital_ownership_edges WHERE owner_type = 'O' AND owner_role_text IN ",
    );
    push_in_clause(&mut qb2, CONTROLLING_OWNER_ROLES);
    qb2.push(" AND owner_associate_id IN ");
    push_in_clause(&mut qb2, owner_associate_ids.iter().map(String::as_str));
    let sibling_enrollment_ids: Vec<String> = qb2.build_query_scalar().fetch_all(pool).await?;

    if sibling_enrollment_ids.is_empty() {
        return Ok(reachable);
    }

    let mut qb3: QueryBuilder<Sqlite> =
        QueryBuilder::new("SELECT DISTINCT ccn FROM hospital_enrollments WHERE ccn IS NOT NULL AND enrollment_id IN ");
    push_in_clause(&mut qb3, sibling_enrollment_ids.iter().map(String::as_str));
    let sibling_ccns: Vec<String> = qb3.build_query_scalar().fetch_all(pool).await?;

    reachable.extend(sibling_ccns);
    Ok(reachable)
}

/// Every disclosed owner/controller across every PECOS enrollment CMS's
/// ownership data has on file for `facility_id` (CCN) — usually one
/// enrollment, but the schema doesn't assume it. Empty (not an error)
/// when ownership data hasn't been ingested yet, or this hospital has no
/// enrollment row, or it has one with no disclosed owner. Ordered
/// organizations before individuals, then by name, so a network's
/// corporate parent surfaces first.
pub async fn list_hospital_owners(pool: &SqlitePool, facility_id: &str) -> Result<Vec<HospitalOwner>, sqlx::Error> {
    sqlx::query_as::<_, HospitalOwner>(
        r#"
        SELECT o.enrollment_id, o.owner_type, o.owner_role_text,
               o.owner_organization_name, o.owner_person_name, o.percentage_ownership
        FROM hospital_enrollments e
        JOIN hospital_ownership_edges o ON o.enrollment_id = e.enrollment_id
        WHERE e.ccn = ?
        ORDER BY (o.owner_type = 'I'), o.owner_organization_name, o.owner_person_name
        "#,
    )
    .bind(facility_id)
    .fetch_all(pool)
    .await
}

// --- MRF discovery / manifest-driven tagging ---

/// Hospitals to consider as match targets for
/// `mrf_match::resolve_entries`, optionally narrowed by state and/or an
/// explicit `facility_id` list (both optional; when neither is given,
/// every hospital nationwide is a candidate — safe because name
/// similarity alone never writes anything, see
/// `routes::pipeline::trigger_discover`'s ownership-graph
/// cross-validation).
pub async fn list_match_candidates(
    pool: &SqlitePool,
    state_filter: Option<&str>,
    facility_ids: Option<&[String]>,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("SELECT facility_id, facility_name FROM hospitals");
    let mut first = true;

    if let Some(state) = state_filter {
        qb.push(" WHERE state = ").push_bind(state.to_string());
        first = false;
    }
    if let Some(ids) = facility_ids {
        if !ids.is_empty() {
            qb.push(if first { " WHERE facility_id IN " } else { " AND facility_id IN " });
            push_in_clause(&mut qb, ids.iter().map(String::as_str));
            first = false;
        }
    }
    let _ = first;

    qb.build_query_as::<(String, String)>().fetch_all(pool).await
}

/// Hospitals with a non-null `website_url` — the candidate pool for
/// `POST /api/pipeline/discover`'s general per-hospital-website mode (an
/// unset `network_manifest_url`), which probes each one's own site for
/// its own `cms-hpt.txt` rather than resolving entries out of someone
/// else's network-wide manifest. `state_filter`/`facility_ids` narrow the
/// pool the same way `list_match_candidates` does — both optional.
pub async fn list_discoverable_hospitals(
    pool: &SqlitePool,
    state_filter: Option<&str>,
    facility_ids: Option<&[String]>,
) -> Result<Vec<(String, String)>, sqlx::Error> {
    let mut qb: QueryBuilder<Sqlite> =
        QueryBuilder::new("SELECT facility_id, website_url FROM hospitals WHERE website_url IS NOT NULL");

    if let Some(state) = state_filter {
        qb.push(" AND state = ").push_bind(state.to_string());
    }
    if let Some(ids) = facility_ids {
        if !ids.is_empty() {
            qb.push(" AND facility_id IN ");
            push_in_clause(&mut qb, ids.iter().map(String::as_str));
        }
    }

    qb.build_query_as::<(String, String)>().fetch_all(pool).await
}

/// Persists one MRF-tagging outcome for one hospital — a fresh row, per
/// `mrf_discoveries`' existing append-only-per-check design (`get_hospital`
/// reads back only the *latest* row per facility, so re-running discovery
/// naturally supersedes a stale result without needing an update/upsert
/// here).
/// `contact_name`/`contact_email` come straight off the manifest entry's
/// own `contact-name`/`contact-email` fields
/// (`compliance_probe::manifest_parser::MrfEntry`) — part of CMS's
/// `cms-hpt.txt` spec but not persisted anywhere until this parameter was
/// added (2026-09-15).
/// Returns the newly generated `mrf_discoveries.id` so the caller (the
/// `discover` job) can chain an initial `mrf_metadata` capture onto the
/// same row without a re-query.
#[allow(clippy::too_many_arguments)]
pub async fn insert_mrf_discovery(
    pool: &SqlitePool,
    facility_id: &str,
    cms_hpt_txt_url: &str,
    mrf_url: &str,
    discovery_status: &str,
    contact_name: Option<&str>,
    contact_email: Option<&str>,
) -> Result<String, sqlx::Error> {
    let now = Utc::now().to_rfc3339();
    let id = Uuid::new_v4().to_string();
    let mrf_urls_json = serde_json::to_string(&[mrf_url]).unwrap_or_else(|_| "[]".to_string());

    sqlx::query(
        r#"
        INSERT INTO mrf_discoveries (
            id, facility_id, website_url, website_reachable, cms_hpt_txt_found,
            cms_hpt_txt_url, mrf_urls, discovery_status, contact_name, contact_email, checked_at
        ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(facility_id)
    .bind(true) // the manifest itself was fetched successfully to get here
    .bind(true)
    .bind(cms_hpt_txt_url)
    .bind(mrf_urls_json)
    .bind(discovery_status)
    .bind(contact_name)
    .bind(contact_email)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(id)
}

/// Persists one full `compliance_probe::probe::DiscoveryResult` from the
/// general per-hospital-website discover mode — unlike
/// `insert_mrf_discovery` (built for the network-manifest mode's one
/// confirmed-match-per-entry shape), this records the *whole* probe
/// outcome for one hospital in a single row, `mrf_urls` holding every MRF
/// reference the hospital's own `cms-hpt.txt` listed (typically zero or
/// one, but the column has always been a JSON array — see
/// `migrations/0001_init.sql`). `website_reachable`/`cms_hpt_txt_url` are
/// genuinely nullable here (unlike the network-manifest mode, where a
/// confirmed match implies both), since a probe can end at any stage
/// (`website_unreachable`, `no_manifest`, etc.). `contact_name`/
/// `contact_email` are the first manifest entry's contact fields — a
/// hospital's own `cms-hpt.txt` normally has exactly one entry (itself),
/// so there's usually nothing to disambiguate; on the rare manifest with
/// more than one entry for the same hospital, the first entry's contact
/// wins rather than trying to merge or pick among several. Returns the
/// new row's id so the caller can chain `mrf_metadata` baseline captures
/// onto it.
#[allow(clippy::too_many_arguments)]
pub async fn insert_discovery_result(
    pool: &SqlitePool,
    facility_id: &str,
    website_url: Option<&str>,
    website_reachable: Option<bool>,
    cms_hpt_txt_found: bool,
    cms_hpt_txt_url: Option<&str>,
    mrf_urls: &[String],
    discovery_status: &str,
    contact_name: Option<&str>,
    contact_email: Option<&str>,
) -> Result<String, sqlx::Error> {
    let now = Utc::now().to_rfc3339();
    let id = Uuid::new_v4().to_string();
    let mrf_urls_json =
        (!mrf_urls.is_empty()).then(|| serde_json::to_string(mrf_urls).unwrap_or_else(|_| "[]".to_string()));

    sqlx::query(
        r#"
        INSERT INTO mrf_discoveries (
            id, facility_id, website_url, website_reachable, cms_hpt_txt_found,
            cms_hpt_txt_url, mrf_urls, discovery_status, contact_name, contact_email, checked_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(facility_id)
    .bind(website_url)
    .bind(website_reachable)
    .bind(cms_hpt_txt_found)
    .bind(cms_hpt_txt_url)
    .bind(mrf_urls_json)
    .bind(discovery_status)
    .bind(contact_name)
    .bind(contact_email)
    .bind(now)
    .execute(pool)
    .await?;

    Ok(id)
}

pub async fn get_mrf_discovery(pool: &SqlitePool, id: &str) -> Result<Option<MrfDiscovery>, sqlx::Error> {
    sqlx::query_as::<_, MrfDiscovery>("SELECT * FROM mrf_discoveries WHERE id = ?")
        .bind(id)
        .fetch_optional(pool)
        .await
}

/// The most recent `mrf_metadata` row for one discovery — the "previous
/// check" a fresh probe is compared against in `crate::mrf_metadata`.
/// `None` means no metadata has been captured yet, i.e. the next probe is
/// a baseline.
pub async fn latest_mrf_metadata(pool: &SqlitePool, mrf_discovery_id: &str) -> Result<Option<MrfMetadata>, sqlx::Error> {
    sqlx::query_as::<_, MrfMetadata>(
        "SELECT * FROM mrf_metadata WHERE mrf_discovery_id = ? ORDER BY checked_at DESC LIMIT 1",
    )
    .bind(mrf_discovery_id)
    .fetch_optional(pool)
    .await
}

/// Full change-over-time history for one discovery, most recent first.
pub async fn list_mrf_metadata(pool: &SqlitePool, mrf_discovery_id: &str) -> Result<Vec<MrfMetadata>, sqlx::Error> {
    sqlx::query_as::<_, MrfMetadata>("SELECT * FROM mrf_metadata WHERE mrf_discovery_id = ? ORDER BY checked_at DESC")
        .bind(mrf_discovery_id)
        .fetch_all(pool)
        .await
}

/// Persists one `crate::mrf_metadata::MetadataCheckOutcome` as a new
/// `mrf_metadata` row (append-only, same pattern as `mrf_discoveries` —
/// each probe is its own row, never an update) and returns it.
#[allow(clippy::too_many_arguments)]
pub async fn insert_mrf_metadata(
    pool: &SqlitePool,
    mrf_discovery_id: &str,
    mrf_url: &str,
    sha1_hash: Option<&str>,
    last_modified: Option<&str>,
    etag: Option<&str>,
    cache_control: Option<&str>,
    content_length: Option<i64>,
    content_type: Option<&str>,
    change_detection_method: Option<&str>,
    changed_from_previous: Option<bool>,
) -> Result<MrfMetadata, sqlx::Error> {
    let now = Utc::now().to_rfc3339();
    let id = Uuid::new_v4().to_string();

    sqlx::query(
        r#"
        INSERT INTO mrf_metadata (
            id, mrf_discovery_id, mrf_url, sha1_hash, last_modified, etag,
            cache_control, content_length, content_type, schema_valid,
            change_detection_method, changed_from_previous, checked_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?, ?, ?)
        "#,
    )
    .bind(&id)
    .bind(mrf_discovery_id)
    .bind(mrf_url)
    .bind(sha1_hash)
    .bind(last_modified)
    .bind(etag)
    .bind(cache_control)
    .bind(content_length)
    .bind(content_type)
    .bind(change_detection_method)
    .bind(changed_from_previous)
    .bind(&now)
    .execute(pool)
    .await?;

    Ok(MrfMetadata {
        id,
        mrf_discovery_id: mrf_discovery_id.to_string(),
        mrf_url: mrf_url.to_string(),
        sha1_hash: sha1_hash.map(str::to_string),
        last_modified: last_modified.map(str::to_string),
        etag: etag.map(str::to_string),
        cache_control: cache_control.map(str::to_string),
        content_length,
        content_type: content_type.map(str::to_string),
        schema_valid: None,
        change_detection_method: change_detection_method.map(str::to_string),
        changed_from_previous,
        checked_at: now,
    })
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

    async fn set_hospital_ownership(pool: &SqlitePool, facility_id: &str, ownership: &str) {
        sqlx::query("UPDATE hospitals SET hospital_ownership = ? WHERE facility_id = ?")
            .bind(ownership)
            .bind(facility_id)
            .execute(pool)
            .await
            .expect("set hospital_ownership");
    }

    #[tokio::test]
    async fn ownership_reachable_ccns_connects_department_of_defense_hospitals_with_no_pecos_data() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "TX", None, None, false).await;
        insert_hospital(&pool, "H2", "VA", None, None, false).await;
        insert_hospital(&pool, "H3", "CA", None, None, false).await; // not DoD — must not connect
        set_hospital_ownership(&pool, "H1", "Department of Defense").await;
        set_hospital_ownership(&pool, "H2", "Department of Defense").await;
        // H3 keeps the default "Voluntary non-profit" from insert_hospital.
        // No hospital_enrollments/hospital_ownership_edges rows at all —
        // mirrors real PECOS data, which has none for DoD facilities.

        let reachable = ownership_reachable_ccns(&pool, "H1").await.unwrap();
        assert_eq!(reachable, HashSet::from(["H1".to_string(), "H2".to_string()]));
    }

    #[tokio::test]
    async fn ownership_reachable_ccns_non_dod_hospital_with_no_pecos_data_stays_isolated() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "CA", None, None, false).await;
        insert_hospital(&pool, "H2", "CA", None, None, false).await;
        // Both default to "Voluntary non-profit" — not a member of
        // OWNERSHIP_GRAPH_UNIFIED_CATEGORIES, so the ordinary PECOS walk
        // applies, and with no enrollment data it finds nothing.

        let reachable = ownership_reachable_ccns(&pool, "H1").await.unwrap();
        assert_eq!(reachable, HashSet::from(["H1".to_string()]));
    }

    #[tokio::test]
    async fn ownership_reachable_ccns_district_authority_hospitals_stay_isolated_from_each_other() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "TX", None, None, false).await;
        insert_hospital(&pool, "H2", "OR", None, None, false).await;
        set_hospital_ownership(&pool, "H1", "Government - Hospital District or Authority").await;
        set_hospital_ownership(&pool, "H2", "Government - Hospital District or Authority").await;
        // Same category as each other, no PECOS data either — but unlike
        // Department of Defense, sharing this category does NOT mean
        // "same network" (there are ~500 independent local districts), so
        // OWNERSHIP_GRAPH_UNIFIED_CATEGORIES deliberately excludes it.

        let reachable = ownership_reachable_ccns(&pool, "H1").await.unwrap();
        assert_eq!(reachable, HashSet::from(["H1".to_string()]));
    }

    #[test]
    fn ownership_category_has_no_pecos_stake_covers_dod_and_every_government_category() {
        assert!(ownership_category_has_no_pecos_stake("Department of Defense"));
        assert!(ownership_category_has_no_pecos_stake("Government - Federal"));
        assert!(ownership_category_has_no_pecos_stake("Government - State"));
        assert!(ownership_category_has_no_pecos_stake("Government - Local"));
        assert!(ownership_category_has_no_pecos_stake("Government - Hospital District or Authority"));

        assert!(!ownership_category_has_no_pecos_stake("Voluntary non-profit - Private"));
        assert!(!ownership_category_has_no_pecos_stake("Proprietary"));
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

    fn enrollment(enrollment_id: &str, ccn: &str, associate_id: &str, org_name: &str) -> cms_ingest::EnrollmentRecord {
        cms_ingest::EnrollmentRecord {
            enrollment_id: enrollment_id.to_string(),
            ccn: Some(ccn.to_string()),
            associate_id: associate_id.to_string(),
            organization_name: org_name.to_string(),
            state: Some("VA".to_string()),
        }
    }

    fn org_owner_edge(enrollment_id: &str, associate_id: &str, owner_associate_id: &str, role: &str) -> cms_ingest::OwnershipEdgeRecord {
        cms_ingest::OwnershipEdgeRecord {
            enrollment_id: enrollment_id.to_string(),
            associate_id: associate_id.to_string(),
            owner_associate_id: Some(owner_associate_id.to_string()),
            owner_type: Some("O".to_string()),
            owner_role_code: Some("35".to_string()),
            owner_role_text: Some(role.to_string()),
            owner_organization_name: Some("SENTARA HEALTH".to_string()),
            owner_person_name: None,
            percentage_ownership: Some("100".to_string()),
        }
    }

    fn individual_owner_edge(enrollment_id: &str, associate_id: &str) -> cms_ingest::OwnershipEdgeRecord {
        cms_ingest::OwnershipEdgeRecord {
            enrollment_id: enrollment_id.to_string(),
            associate_id: associate_id.to_string(),
            owner_associate_id: Some("SHARED-PERSON".to_string()),
            owner_type: Some("I".to_string()),
            owner_role_code: Some("41".to_string()),
            owner_role_text: Some("CORPORATE DIRECTOR".to_string()),
            owner_organization_name: None,
            owner_person_name: Some("Melinda Hancock".to_string()),
            percentage_ownership: None,
        }
    }

    #[tokio::test]
    async fn ownership_reachable_ccns_finds_siblings_under_a_shared_controlling_owner() {
        let pool = test_pool().await;
        let records = vec![
            enrollment("E1", "010001", "A1", "SENTARA NORFOLK GENERAL HOSPITAL"),
            enrollment("E2", "010002", "A2", "SENTARA LEIGH HOSPITAL"),
            enrollment("E3", "010003", "A3", "UNRELATED HOSPITAL"),
        ];
        replace_hospital_enrollments(&pool, &records).await.unwrap();

        let edges = vec![
            org_owner_edge("E1", "A1", "SENTARA-HEALTH", "5% OR GREATER DIRECT OWNERSHIP INTEREST"),
            org_owner_edge("E2", "A2", "SENTARA-HEALTH", "5% OR GREATER DIRECT OWNERSHIP INTEREST"),
            org_owner_edge("E3", "A3", "SOME-OTHER-PARENT", "5% OR GREATER DIRECT OWNERSHIP INTEREST"),
        ];
        replace_ownership_edges(&pool, &edges).await.unwrap();

        let reachable = ownership_reachable_ccns(&pool, "010001").await.unwrap();
        assert_eq!(reachable, HashSet::from(["010001".to_string(), "010002".to_string()]));
    }

    #[tokio::test]
    async fn ownership_reachable_ccns_ignores_individual_owners() {
        let pool = test_pool().await;
        let records = vec![
            enrollment("E1", "010001", "A1", "HOSPITAL ONE"),
            enrollment("E2", "010002", "A2", "HOSPITAL TWO"),
        ];
        replace_hospital_enrollments(&pool, &records).await.unwrap();

        // Both hospitals share the same individual corporate director —
        // that alone must NOT connect them.
        let edges = vec![individual_owner_edge("E1", "A1"), individual_owner_edge("E2", "A2")];
        replace_ownership_edges(&pool, &edges).await.unwrap();

        let reachable = ownership_reachable_ccns(&pool, "010001").await.unwrap();
        assert_eq!(reachable, HashSet::from(["010001".to_string()]));
    }

    #[tokio::test]
    async fn ownership_reachable_ccns_unknown_seed_yields_just_itself() {
        let pool = test_pool().await;
        let reachable = ownership_reachable_ccns(&pool, "999999").await.unwrap();
        assert_eq!(reachable, HashSet::from(["999999".to_string()]));
    }

    #[tokio::test]
    async fn replace_hospital_enrollments_clears_previous_rows() {
        let pool = test_pool().await;
        replace_hospital_enrollments(&pool, &[enrollment("E1", "010001", "A1", "OLD ORG")]).await.unwrap();
        replace_hospital_enrollments(&pool, &[enrollment("E2", "010002", "A2", "NEW ORG")]).await.unwrap();

        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM hospital_enrollments").fetch_one(&pool).await.unwrap();
        assert_eq!(count, 1);
        let ccn: Option<String> = sqlx::query_scalar("SELECT ccn FROM hospital_enrollments WHERE enrollment_id = 'E2'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(ccn.as_deref(), Some("010002"));
    }

    #[tokio::test]
    async fn list_hospital_owners_joins_enrollment_to_edges() {
        let pool = test_pool().await;
        replace_hospital_enrollments(&pool, &[enrollment("E1", "010001", "A1", "SENTARA NORFOLK GENERAL HOSPITAL")])
            .await
            .unwrap();
        let edges = vec![
            org_owner_edge("E1", "A1", "SENTARA-HEALTH", "5% OR GREATER DIRECT OWNERSHIP INTEREST"),
            individual_owner_edge("E1", "A1"),
        ];
        replace_ownership_edges(&pool, &edges).await.unwrap();

        let owners = list_hospital_owners(&pool, "010001").await.unwrap();
        assert_eq!(owners.len(), 2);
        // Organizations sort before individuals.
        assert_eq!(owners[0].owner_type.as_deref(), Some("O"));
        assert_eq!(owners[0].owner_organization_name.as_deref(), Some("SENTARA HEALTH"));
        assert_eq!(owners[0].percentage_ownership.as_deref(), Some("100"));
        assert_eq!(owners[1].owner_type.as_deref(), Some("I"));
        assert_eq!(owners[1].owner_person_name.as_deref(), Some("Melinda Hancock"));
    }

    #[tokio::test]
    async fn list_hospital_owners_empty_when_not_ingested() {
        let pool = test_pool().await;
        let owners = list_hospital_owners(&pool, "999999").await.unwrap();
        assert!(owners.is_empty());
    }

    #[tokio::test]
    async fn list_match_candidates_filters_by_state_and_facility_ids() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "VA", None, None, false).await;
        insert_hospital(&pool, "H2", "VA", None, None, false).await;
        insert_hospital(&pool, "H3", "NC", None, None, false).await;

        let all = list_match_candidates(&pool, None, None).await.unwrap();
        assert_eq!(all.len(), 3);

        let va_only = list_match_candidates(&pool, Some("VA"), None).await.unwrap();
        let mut va_ids: Vec<_> = va_only.into_iter().map(|(id, _)| id).collect();
        va_ids.sort();
        assert_eq!(va_ids, vec!["H1", "H2"]);

        let by_ids = list_match_candidates(&pool, None, Some(&["H3".to_string()])).await.unwrap();
        assert_eq!(by_ids, vec![("H3".to_string(), "Test Hospital H3".to_string())]);
    }

    #[tokio::test]
    async fn list_discoverable_hospitals_excludes_no_website_and_filters() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "VA", None, Some("https://h1.example.com"), false).await;
        insert_hospital(&pool, "H2", "VA", None, None, false).await; // no website — excluded
        insert_hospital(&pool, "H3", "NC", None, Some("https://h3.example.com"), false).await;

        let all = list_discoverable_hospitals(&pool, None, None).await.unwrap();
        let mut all_ids: Vec<_> = all.into_iter().map(|(id, _)| id).collect();
        all_ids.sort();
        assert_eq!(all_ids, vec!["H1", "H3"]);

        let va_only = list_discoverable_hospitals(&pool, Some("VA"), None).await.unwrap();
        assert_eq!(va_only, vec![("H1".to_string(), "https://h1.example.com".to_string())]);

        let by_ids = list_discoverable_hospitals(&pool, None, Some(&["H3".to_string()])).await.unwrap();
        assert_eq!(by_ids, vec![("H3".to_string(), "https://h3.example.com".to_string())]);
    }

    #[tokio::test]
    async fn insert_discovery_result_persists_full_probe_outcome() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "VA", None, Some("https://h1.example.com"), false).await;

        let mrf_urls = vec!["https://h1.example.com/mrf.json".to_string()];
        let discovery_id = insert_discovery_result(
            &pool,
            "H1",
            Some("https://h1.example.com"),
            Some(true),
            true,
            Some("https://h1.example.com/cms-hpt.txt"),
            &mrf_urls,
            "mrf_found",
            Some("Jane Doe"),
            Some("jane@h1.example.com"),
        )
        .await
        .unwrap();

        let discovery = get_mrf_discovery(&pool, &discovery_id).await.unwrap().unwrap();
        assert_eq!(discovery.facility_id, "H1");
        assert_eq!(discovery.website_reachable, Some(true));
        assert_eq!(discovery.mrf_urls.as_deref(), Some(r#"["https://h1.example.com/mrf.json"]"#));
        assert_eq!(discovery.discovery_status, "mrf_found");
        assert_eq!(discovery.contact_name.as_deref(), Some("Jane Doe"));
        assert_eq!(discovery.contact_email.as_deref(), Some("jane@h1.example.com"));
    }

    #[tokio::test]
    async fn insert_discovery_result_handles_website_unreachable() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "VA", None, Some("https://h1.example.com"), false).await;

        let discovery_id = insert_discovery_result(
            &pool,
            "H1",
            Some("https://h1.example.com"),
            Some(false),
            false,
            None,
            &[],
            "website_unreachable",
            None,
            None,
        )
        .await
        .unwrap();

        let discovery = get_mrf_discovery(&pool, &discovery_id).await.unwrap().unwrap();
        assert_eq!(discovery.website_reachable, Some(false));
        assert_eq!(discovery.cms_hpt_txt_url, None);
        assert_eq!(discovery.mrf_urls, None);
        assert_eq!(discovery.contact_name, None);
        assert_eq!(discovery.contact_email, None);
    }

    #[tokio::test]
    async fn insert_mrf_discovery_persists_a_row() {
        let pool = test_pool().await;
        insert_hospital(&pool, "H1", "VA", None, None, false).await;

        insert_mrf_discovery(
            &pool,
            "H1",
            "https://example.com/cms-hpt.txt",
            "https://example.com/mrf.json",
            "mrf_found",
            Some("Casey Simpkins"),
            Some("payerdropbox@example.com"),
        )
        .await
        .unwrap();

        let (hospital, discovery) = get_hospital(&pool, "H1").await.unwrap().unwrap();
        assert_eq!(hospital.facility_id, "H1");
        let discovery = discovery.expect("expected a discovery row");
        assert_eq!(discovery.discovery_status, "mrf_found");
        assert_eq!(discovery.cms_hpt_txt_url.as_deref(), Some("https://example.com/cms-hpt.txt"));
        assert_eq!(discovery.mrf_urls.as_deref(), Some(r#"["https://example.com/mrf.json"]"#));
        assert_eq!(discovery.contact_name.as_deref(), Some("Casey Simpkins"));
        assert_eq!(discovery.contact_email.as_deref(), Some("payerdropbox@example.com"));
    }
}
