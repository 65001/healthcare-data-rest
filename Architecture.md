# Architecture

Detailed backend architecture for the CMS Hospital Price Transparency Compliance Checker.

---

## Table of Contents

- [System Overview](#system-overview)
- [Pipeline Stages](#pipeline-stages)
- [Crate Architecture](#crate-architecture)
  - [cms-ingest](#cms-ingest)
  - [geo-enrich](#geo-enrich)
  - [compliance-probe](#compliance-probe)
  - [backend](#backend)
- [Cascading Geocoding Design](#cascading-geocoding-design)
- [Database Schema](#database-schema)
- [REST API](#rest-api)
- [Error Handling Strategy](#error-handling-strategy)
- [Concurrency & Rate Limiting](#concurrency--rate-limiting)
- [Configuration](#configuration)
- [Future Extensions](#future-extensions)

---

## System Overview

The system is a **four-stage data pipeline** backed by an Axum REST API. Each stage is an independent operation triggered on-demand via the API.

```
┌──────────────────────────────────────────────────────────────────────────┐
│                          Rust Backend (Axum)                            │
│                                                                          │
│   ┌───────────┐    ┌───────────────┐    ┌───────────────┐   ┌────────┐  │
│   │  Stage 1  │───▶│   Stage 2     │───▶│   Stage 3     │──▶│Stage 4 │  │
│   │  INGEST   │    │   ENRICH      │    │   DISCOVER    │   │ SERVE  │  │
│   │           │    │  (Cascading   │    │  (MRF Probe)  │   │ (API)  │  │
│   │  CMS Data │    │   Geocoding)  │    │               │   │        │  │
│   └───────────┘    └───────────────┘    └───────────────┘   └────────┘  │
│         │                 │                    │                 │       │
│         └─────────────────┴────────────────────┴─────────────────┘       │
│                                    │                                     │
│                          ┌─────────▼─────────┐                           │
│                          │      SQLite        │                           │
│                          │  (PgSQL-compat SQL)│                           │
│                          └───────────────────┘                           │
└──────────────────────────────────────────────────────────────────────────┘
```

### Design Principles

1. **Stage independence** — Each pipeline stage can run independently and is idempotent. Re-running ingest upserts; re-running enrich skips already-geocoded records; re-running discover overwrites stale probe results.
2. **Free-first geocoding** — The cascading trait tries free providers before falling back to paid APIs, minimizing cost while maximizing coverage.
3. **Database portability** — All SQL uses PostgreSQL-compatible syntax. SQLite is the runtime engine, but the schema and queries can migrate to PostgreSQL without rewriting SQL.
4. **Polite crawling** — Hospital website probing uses token-bucket rate limiting with per-domain backoff to avoid overwhelming small hospital web servers.

---

## Pipeline Stages

### Stage 1: Ingest

**Crate:** `cms-ingest`

Fetches the "Hospital General Information" dataset from the CMS Provider Data Catalog REST API at `data.cms.gov`.

```
data.cms.gov/provider-data/
    └── datastore/query/{distribution_id}
            │
            ▼
     ┌──────────────┐
     │  JSON pages   │  (paginated, ~6,000 records)
     │  of hospital  │
     │  records      │
     └──────┬───────┘
            │  parse + normalize
            ▼
     ┌──────────────┐
     │  hospitals    │  (SQLite table)
     │  table        │
     └──────────────┘
```

- Handles API pagination (offset-based, 500 records per page).
- Normalizes data: trims whitespace, parses booleans, handles missing fields.
- Upserts into the `hospitals` table (CCN as primary key).

### Stage 2: Enrich

**Crate:** `geo-enrich`

Geocodes hospital addresses using the cascading provider chain. Also attempts to resolve each hospital's public website URL from map/place data.

```
   Un-enriched hospitals (latitude IS NULL)
            │
            ▼
   ┌────────────────────┐
   │ CascadingGeocoder   │
   │                     │
   │  Provider 1: Census │──── success ──▶ save result
   │         │ fail      │
   │  Provider 2: OSM    │──── success ──▶ save result
   │         │ fail      │
   │  Provider 3: Google │──── success ──▶ save result
   │         │ fail      │
   │    return None       │
   └────────────────────┘
```

- Bounded concurrency via `tokio::sync::Semaphore`.
- Skips hospitals that already have `enriched_at` set, unless the caller opts into a backfill pass (`?retry_incomplete=true` on `POST /api/pipeline/enrich`) for hospitals that ran once but are still missing coordinates or a website.
- Records which provider succeeded (`geo_provider` column) — `"manual"` when a human filled in the gap via `PATCH /api/hospitals/:facility_id/enrichment` rather than an automated provider (see [REST API](#rest-api)).

### Stage 3: Discover

**Crate:** `compliance-probe`

Locates a hospital's CMS-required `cms-hpt.txt` manifest and extracts its MRF file URL(s). `cms-hpt.txt`'s format was confirmed live 2026-09-15 against a real, multi-location manifest (`https://www.sentara.com/cms-hpt.txt`): a **block format**, entries separated by blank lines, each a set of `key: value` lines (`location-name`, `source-page-url`, `mrf-url`, `contact-name`, `contact-email`) — see `manifest_parser.rs`'s doc comment for the full example and IMPLEMENTATION_NOTES.md for how this was pinned down.

```
   Enriched hospitals (website_url IS NOT NULL)
            │
            ▼
   ┌──────────────────────┐
   │  1. HEAD {website}   │  ── unreachable ──▶ WebsiteUnreachable
   │  2. GET  /cms-hpt.txt│  ── 404 ──────────▶ NoManifest
   │  3. Parse manifest   │  ── extract URLs
   │  4. HEAD each MRF URL│  ── 200? ─────────▶ MrfFound
   │                      │  ── all fail ──────▶ ManifestOnly
   └──────────────────────┘
```

- Token-bucket rate limiter (configurable requests/sec).
- Per-domain backoff for 429/503 responses.
- Results stored in `mrf_discoveries` table.
- `compliance_probe::probe::Prober` implements this sequence for real (`HEAD` website → `GET`/parse `cms-hpt.txt` → `HEAD` each MRF URL), fully tested.

#### Multi-location manifests: name matching + ownership-graph cross-validation

A health-system-wide `cms-hpt.txt` (like Sentara's, 18 entries across a dozen real hospitals plus several satellite/outpatient locations with no CCN of their own) lists one block per named location, not per hospital in `hospitals`. Resolving those `location-name` entries to specific `facility_id`s — and doing it *safely*, since a manifest URL might be handed to the API directly (`POST /api/pipeline/discover`'s `network_manifest_url` mode) rather than discovered from a hospital's own already-trusted `website_url` — is a two-signal process, both required:

1. **Fuzzy name match** (`backend::mrf_match`) — Jaro-Winkler similarity on normalized (case/punctuation-collapsed) names, `location-name` against every candidate hospital's `facility_name`. Greedy highest-score-first bipartite assignment (each entry and each hospital used at most once). Default threshold `0.90`, calibrated against the real Sentara manifest cross-checked against CMS's own "Hospital General Information" names for the same hospitals — genuine matches scored ≥0.92 even with a one-word naming difference between the network's own branding and CMS's on-file name; every satellite/outpatient entry with no real CCN scored well below the bar against every candidate, including the trap case of sharing a long common prefix with its parent hospital's name.
2. **Ownership-graph cross-validation** (`db::queries::ownership_reachable_ccns`) — a name match alone isn't trusted. Every hospital that clears the name-similarity bar is used as a seed to walk CMS's own ownership-disclosure data (see below); a candidate is only tagged if it's in the **union** of every seed's ownership-reachable set. (Union, not a single anchor's set: real systems can have more than one PECOS owner-of-record entity for the same network — confirmed live against Sentara, where most hospitals list "SENTARA HEALTH" as a 5%+ direct owner but at least one lists "SENTARA HOSPITALS" under operational/managerial control instead. Anchoring on one seed alone would silently drop hospitals connected only through the other owner.)

```
cms-hpt.txt (18 entries)
      │ parse
      ▼
mrf_match::resolve_entries()  ──▶  12 name matches (≥0.90), 6 rejected (no candidate close enough)
      │
      ▼
ownership_reachable_ccns() per matched hospital, unioned
      │
      ▼
tag mrf_discoveries only for matches inside the union
```

#### The ownership graph: `hospital_enrollments` + `hospital_ownership_edges`

Fed by `POST /api/pipeline/ingest-ownership`, which fetches two more CMS datasets via `cms_ingest::ownership` (a different, newer API — `data-api/v1/dataset/{uuid}/data`, a bare JSON array, no `{results, count}` envelope, no documented total-row-count — than `client.rs`'s Provider Data Catalog endpoint):

- **Hospital Enrollments** (`f6f6505c-e8b0-4d57-b258-e2b94133aaf2`, ~9,161 rows) — the CCN ↔ PECOS `ENROLLMENT ID`/`ASSOCIATE ID` crosswalk, one row per hospital's own Medicare enrollment.
- **Hospital All Owners** (`029c119f-f79c-49be-9100-344d31d10344`, ~148,000 rows) — one row per disclosed owner/controller of a hospital's enrollment.

`ownership_reachable_ccns(seed_ccn)` walks **one hop**: seed's own enrollment → its organizational (`owner_type = 'O'`) owners with a controlling role (`5% OR GREATER DIRECT/INDIRECT OWNERSHIP INTEREST` or `OPERATIONAL/MANAGERIAL CONTROL` — an individual person's stake, or a shared corporate officer, doesn't count) → every *other* enrollment sharing one of those same owners → their CCNs. One hop already captures Sentara's whole network (a system-wide parent is listed directly as a controlling owner on each hospital's own enrollment); a structure with an unlisted intermediate holding company would need a second hop to connect — not implemented, to avoid an unbounded graph walk over real-world data that can have surprising cycles.

**Special case — two separate lists, two separate questions** (added 2026-09-15, refined same day after the first version conflated them):

- **`OWNERSHIP_GRAPH_UNIFIED_CATEGORIES`** answers *"are hospitals sharing this `hospital_ownership` category actually the same real-world network?"* — used only by `ownership_reachable_ccns` above. Some categories (CMS's own Hospital General Information field, distinct from PECOS) structurally never carry a PECOS ownership disclosure at all — there's no private ownership *stake* to disclose for a government facility, so `hospital_enrollments`/`hospital_ownership_edges` are simply empty for them, and the walk above would always yield just `{seed_ccn}` alone. For a hospital in one of these categories, `ownership_reachable_ccns` instead returns every other hospital sharing that same category, so cms-hpt.txt-driven MRF tagging can still cross-validate matches within that system. **Currently just `"Department of Defense"`** (32 real hospitals — the Military Health System/TRICARE genuinely is one coherent nationwide system) — deliberately narrow, since getting this wrong risks actually corrupting discovery data by cross-tagging unrelated hospitals' MRFs.
- **`ownership_category_has_no_pecos_stake()`** answers a different, lower-stakes question: *"is an empty owner list expected for this hospital, or does it suggest `ingest-ownership` needs to run?"* — used only by `GET /api/hospitals/:facility_id/ownership`'s `no_stake_expected` field (see below), purely to pick the right frontend message. This one is deliberately **broader**: every `"Government - *"` category (Federal, State, Local, **and** Hospital District or Authority) plus Department of Defense all genuinely have no private ownership stake to disclose, even though the ~500 independent district authorities among them are absolutely not one network with each other. Getting this one "wrong" only shows a slightly-off hint on a detail page, not a data-integrity problem, so it's safe to be inclusive here in a way the reachability list above must not be.

Both live in `backend/src/db/queries.rs`, next to each other, with doc comments cross-referencing this exact distinction — read both before touching either.

Both tables are **replaced outright** on every `ingest-ownership` run (not upserted) — unlike `hospitals`, nothing in them is ever hand-corrected, so a full refresh is simpler and avoids stale rows.

### Stage 4: Serve

**Crate:** `backend`

Axum REST API exposing all ingested, enriched, and discovered data.

---

## Crate Architecture

```
healthcare-data-rest/
├── Cargo.toml              # Workspace manifest
├── backend/                # Main binary — Axum server
│   ├── Cargo.toml
│   └── src/
│       ├── main.rs         # Entry point
│       ├── config.rs       # Environment-based configuration
│       ├── jobs.rs         # Background job tracker
│       ├── enrich_store.rs # geo_enrich::UnenrichedHospitalStore impl
│       ├── openapi.rs      # utoipa OpenApi doc aggregator (Swagger UI)
│       ├── url_check.rs    # Manual website_url verification (existence/type/size/MRF-sniff)
│       ├── mrf_match.rs    # cms-hpt.txt location-name -> hospital fuzzy matching
│       ├── db/
│       │   ├── mod.rs      # Connection pool setup
│       │   ├── models.rs   # SQLx FromRow structs
│       │   ├── queries.rs  # Typed query functions
│       │   └── migrations/ # SQL migration files
│       └── routes/
│           ├── mod.rs      # Router assembly
│           ├── hospitals.rs
│           ├── pipeline.rs
│           └── stats.rs
├── cms-ingest/             # CMS data fetching & parsing
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs          # Public API types
│       ├── client.rs       # HTTP client for data.cms.gov (Provider Data Catalog API)
│       ├── ownership.rs    # HTTP client for the data-api/v1 ownership-graph datasets
│       ├── parser.rs       # Record normalization
│       └── error.rs
├── geo-enrich/             # Cascading geocoding
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs          # GeocodingProvider trait + result types
│       ├── cascade.rs      # CascadingGeocoder orchestrator
│       ├── enricher.rs     # Batch enrichment loop
│       ├── error.rs
│       └── providers/
│           ├── mod.rs
│           ├── census.rs   # US Census Bureau Geocoder
│           ├── nominatim.rs# OpenStreetMap Nominatim
│           ├── google_maps.rs
│           └── google_places.rs # Website-discovery fallback
└── compliance-probe/       # MRF discovery
    ├── Cargo.toml
    └── src/
        ├── lib.rs          # Discovery result types
        ├── probe.rs        # Core probing logic
        ├── manifest_parser.rs  # cms-hpt.txt parser
        ├── rate_limiter.rs # Token-bucket rate limiter
        └── error.rs
```

### Dependency Graph

```
backend
  ├── cms-ingest
  ├── geo-enrich
  └── compliance-probe
```

`cms-ingest`, `geo-enrich`, and `compliance-probe` are independent of each other. Only `backend` depends on all three.

---

## Cascading Geocoding Design

The geocoding system is built around a Rust trait that allows multiple providers to be composed into a fallback chain.

### The Trait

```rust
pub trait GeocodingProvider: Send + Sync {
    /// Human-readable name (e.g., "us_census", "nominatim", "google_maps").
    fn name(&self) -> &str;

    /// Geocode an address.
    ///
    /// Returns:
    /// - Ok(Some(result)) — successfully geocoded
    /// - Ok(None)         — provider cannot resolve this address (try next)
    /// - Err(transient)   — temporary failure (try next provider)
    /// - Err(fatal)       — stop the chain
    async fn geocode(
        &self,
        address: &str,
        city: &str,
        state: &str,
        zip: &str,
    ) -> Result<Option<GeocodingResult>, GeoEnrichError>;
}
```

### The Cascade

```rust
pub struct CascadingGeocoder {
    providers: Vec<Box<dyn GeocodingProvider>>,
}

impl CascadingGeocoder {
    /// Try each provider in order. Stop on first success or fatal error.
    pub async fn geocode(&self, ...) -> Result<Option<GeocodingResult>, GeoEnrichError> {
        for provider in &self.providers {
            match provider.geocode(address, city, state, zip).await {
                Ok(Some(result)) => return Ok(Some(result)),
                Ok(None)         => continue,
                Err(e) if e.is_transient() => {
                    tracing::warn!(provider = provider.name(), "transient failure, trying next");
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(None)
    }
}
```

### Provider Priority

| Priority | Provider | Cost | Rate Limit | Notes |
|----------|----------|------|------------|-------|
| 1 | US Census Bureau | Free | Batch (10K/file) | Best for US addresses; batch mode exists (`CensusProvider::geocode_batch`) but isn't wired into the cascade yet — the cascade calls providers one hospital at a time |
| 2 | Nominatim (OSM) | Free | 1 req/sec | Good fallback; self-hostable for higher throughput |
| 3 | Google Maps | ~$5/1K | 50 req/sec | Highest accuracy; used only for Census+OSM failures |

Not part of the geocoding cascade, but part of the same "resolve everything about a hospital automatically" story: **Google Places** is a separate website-discovery fallback (`geo-enrich/src/providers/google_places.rs`), used when a geocode provider's opportunistic website read (currently only Nominatim's OSM `extratags`) comes up empty.

### Multi-Pass Strategy

Because the cascade skips already-enriched records, you can run enrichment multiple times:

1. **Pass 1** — Enable only Census. Covers ~80-90% of hospitals (free).
2. **Pass 2** — Enable Census + Nominatim. Picks up rural/unusual addresses.
3. **Pass 3** — Enable all three. Google Maps resolves remaining edge cases.

Each pass only processes hospitals where `enriched_at IS NULL`. For hospitals that already ran through every enabled provider and still came up short (a genuinely bad address, most often), see **Manual Enrichment** under [REST API](#rest-api) — some gaps (PO-box-only addresses, no street to geocode) aren't resolvable by any address-geocoding API, free or paid, and need a human to look the hospital up directly.

---

## Database Schema

All SQL is **PostgreSQL-compatible**. No SQLite-specific syntax. Timestamps are ISO 8601 `TEXT` values managed by the application layer.

### `hospitals` Table

```sql
CREATE TABLE hospitals (
    facility_id         TEXT PRIMARY KEY,
    facility_name       TEXT NOT NULL,
    address             TEXT NOT NULL,
    city                TEXT NOT NULL,
    state               TEXT NOT NULL,
    zip_code             TEXT NOT NULL,
    county_name         TEXT,
    phone_number        TEXT,
    hospital_type       TEXT NOT NULL,
    hospital_ownership  TEXT NOT NULL,
    emergency_services  BOOLEAN NOT NULL DEFAULT FALSE,
    overall_rating      INTEGER,

    -- Geocoding enrichment (NULL until enriched)
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    formatted_address   TEXT,
    website_url         TEXT,
    geo_provider        TEXT,
    geo_confidence      DOUBLE PRECISION,
    enriched_at         TEXT,

    -- Metadata
    ingested_at         TEXT NOT NULL,
    updated_at          TEXT NOT NULL
);

CREATE INDEX idx_hospitals_state ON hospitals(state);
CREATE INDEX idx_hospitals_type ON hospitals(hospital_type);
CREATE INDEX idx_hospitals_enriched ON hospitals(enriched_at);
```

`geo_provider = 'manual'` marks a hospital whose coordinates were set by a human via `PATCH /api/hospitals/:facility_id/enrichment` rather than an automated provider (`us_census` / `nominatim` / `google_maps`) — no separate schema column for this; it reuses the existing `geo_provider` field as its own distinct value, with `geo_confidence` set to `1.0`.

### `mrf_discoveries` Table

```sql
CREATE TABLE mrf_discoveries (
    id                  TEXT PRIMARY KEY,
    facility_id         TEXT NOT NULL REFERENCES hospitals(facility_id),
    website_url         TEXT,
    website_reachable   BOOLEAN,
    cms_hpt_txt_found   BOOLEAN,
    cms_hpt_txt_url     TEXT,
    mrf_urls            TEXT,           -- JSON array of MRF references
    discovery_status    TEXT NOT NULL,
    -- Added in migrations/0004_mrf_discovery_contact.sql (2026-09-15).
    -- The manifest entry's own `contact-name`/`contact-email` fields
    -- (compliance_probe::manifest_parser::MrfEntry) — part of CMS's
    -- cms-hpt.txt spec, parsed since the format was confirmed live but
    -- not persisted anywhere until this pass.
    contact_name        TEXT,
    contact_email       TEXT,
    checked_at          TEXT NOT NULL
);

CREATE INDEX idx_mrf_status ON mrf_discoveries(discovery_status);
CREATE INDEX idx_mrf_facility ON mrf_discoveries(facility_id);
CREATE INDEX idx_mrf_checked ON mrf_discoveries(checked_at);
```

### Discovery Status Values

| Status | Meaning |
|--------|---------|
| `mrf_found` | `cms-hpt.txt` found AND at least one MRF URL returned HTTP 200 |
| `manifest_only` | `cms-hpt.txt` found but all MRF URLs are inaccessible |
| `no_manifest` | No `cms-hpt.txt` at the expected location |
| `website_unreachable` | Hospital website is down, blocked, or times out |
| `no_website` | No website URL known for this hospital |

### `hospital_enrollments` and `hospital_ownership_edges` Tables

The CCN ↔ PECOS ownership graph — see [Multi-location manifests](#multi-location-manifests-name-matching--ownership-graph-cross-validation) above for how it's used. Fed by `POST /api/pipeline/ingest-ownership`; **replaced outright** on every run, not upserted.

```sql
CREATE TABLE hospital_enrollments (
    enrollment_id       TEXT PRIMARY KEY,
    ccn                  TEXT,            -- nullable: some PECOS enrollments have no CCN
    associate_id          TEXT NOT NULL,
    organization_name      TEXT NOT NULL,
    state                   TEXT,
    ingested_at              TEXT NOT NULL
);

CREATE INDEX idx_hosp_enroll_ccn ON hospital_enrollments(ccn);
CREATE INDEX idx_hosp_enroll_associate ON hospital_enrollments(associate_id);

CREATE TABLE hospital_ownership_edges (
    id                        TEXT PRIMARY KEY,
    enrollment_id              TEXT NOT NULL,
    associate_id                 TEXT NOT NULL,
    owner_associate_id            TEXT,
    owner_type                     TEXT,  -- 'I' (individual) or 'O' (organization)
    owner_role_code                  TEXT,
    owner_role_text                   TEXT,
    owner_organization_name            TEXT,
    owner_person_name                   TEXT,
    percentage_ownership                  TEXT,
    ingested_at                            TEXT NOT NULL
);

CREATE INDEX idx_owner_edges_enrollment ON hospital_ownership_edges(enrollment_id);
CREATE INDEX idx_owner_edges_owner_assoc ON hospital_ownership_edges(owner_associate_id);
CREATE INDEX idx_owner_edges_owner_org ON hospital_ownership_edges(owner_organization_name);
```

### `mrf_metadata` Table

Implemented as of 2026-09-15 (`backend/migrations/0003_mrf_metadata.sql`). Append-only, same pattern as `mrf_discoveries`: every probe of an MRF URL is its own row, never an update, so a `mrf_discovery_id`'s rows form a change-over-time history.

```sql
CREATE TABLE mrf_metadata (
    id                       TEXT PRIMARY KEY,
    mrf_discovery_id         TEXT NOT NULL REFERENCES mrf_discoveries(id),
    mrf_url                  TEXT NOT NULL,
    sha1_hash                TEXT,
    last_modified            TEXT,
    etag                     TEXT,
    cache_control             TEXT,
    content_length            BIGINT,
    content_type              TEXT,
    schema_valid               BOOLEAN,
    -- Beyond the originally sketched columns above: the actual change
    -- decision made at checked_at.
    change_detection_method    TEXT,   -- 'baseline' | 'etag' | 'last_modified' | 'sha1' | NULL (unreachable)
    changed_from_previous      BOOLEAN, -- NULL for baseline rows or when unreachable
    checked_at                 TEXT NOT NULL
);
```

**Change detection** (`backend/src/mrf_metadata.rs`, orchestrating `compliance_probe::probe::Prober::probe_mrf_headers`/`hash_mrf_content`): a `HEAD` captures `ETag`, `Last-Modified`, `Cache-Control`, `Content-Length`, `Content-Type` first. If the previously stored row has a usable `ETag` or `Last-Modified` to compare against (`ETag` preferred — a stronger, content-derived signal), that alone decides whether the file changed, with no download. Only when neither is usable — missing on the current probe, the previous one, or both — is the full file downloaded and hashed (streamed, not buffered whole, since these can be multi-GB) and compared by SHA-1 against the previous hash. The first-ever probe for a discovery (`"baseline"`) has nothing to compare against; it still downloads and hashes immediately if the headers alone won't be enough for a *future* comparison either, so a baseline never leaves the next recheck stuck with no signal at all.

`POST /api/pipeline/discover`'s network-manifest mode writes the baseline row automatically for every MRF it confirms reachable (see `routes::pipeline::trigger_discover`). `POST /api/mrf-discoveries/{id}/metadata/recheck` (below) is what appends every row after that.

---

## REST API

### Hospital Endpoints

#### `GET /api/hospitals`

List hospitals with pagination and filtering.

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `state` | `string` | Filter by two-letter state code (e.g., `CA`) |
| `hospital_type` | `string` | Filter by CMS hospital type |
| `discovery_status` | `string` | Filter by MRF discovery status |
| `enriched` | `bool` | Filter to enriched/un-enriched hospitals |
| `page` | `int` | Page number (default: 1) |
| `per_page` | `int` | Results per page (default: 50, max: 200) |

**Response:** `200 OK`
```json
{
  "data": [
    {
      "facility_id": "050454",
      "facility_name": "CEDARS-SINAI MEDICAL CENTER",
      "address": "8700 BEVERLY BLVD",
      "city": "LOS ANGELES",
      "state": "CA",
      "zip_code": "90048",
      "hospital_type": "Acute Care Hospitals",
      "latitude": 34.0762,
      "longitude": -118.3805,
      "website_url": "https://www.cedars-sinai.org",
      "discovery_status": "mrf_found"
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 50,
    "total_count": 6234,
    "total_pages": 125
  }
}
```

#### `GET /api/hospitals/:facility_id`

Full detail for a single hospital including enrichment and latest MRF discovery.

#### `GET /api/hospitals/:facility_id/ownership`

Every owner/controller CMS's PECOS ownership-disclosure data has on file for this hospital — organizations before individuals, then by name. `404` if the hospital itself doesn't exist; an empty `owners` array is a normal result otherwise. `no_stake_expected` (added 2026-09-15) tells the caller *why* it might be empty without them needing to know the ownership-category rules themselves: `true` means `ownership_category_has_no_pecos_stake()` (see [`hospital_enrollments` and `hospital_ownership_edges` Tables](#the-ownership-graph-hospital_enrollments--hospital_ownership_edges) — Department of Defense plus every `"Government - *"` category) says this hospital's `hospital_ownership` category structurally has no PECOS disclosure at all, so an empty list here is expected, not a sign `POST /api/pipeline/ingest-ownership` needs to be (re-)run. **Note this is a broader set of categories than `OWNERSHIP_GRAPH_UNIFIED_CATEGORIES`** (same section) — `no_stake_expected` says nothing about whether hospitals sharing a category are the same network, only that an empty list here isn't a data problem. This is the same `hospital_enrollments`/`hospital_ownership_edges` data `POST /api/pipeline/discover`'s ownership-graph cross-validation walks internally — this endpoint just exposes it per-hospital instead of collapsing it into a reachable-CCN set.

```json
{
  "owners": [
    {
      "enrollment_id": "O20071218000139",
      "owner_type": "O",
      "owner_role_text": "5% OR GREATER DIRECT OWNERSHIP INTEREST",
      "owner_organization_name": "SENTARA HEALTH",
      "owner_person_name": null,
      "percentage_ownership": "100"
    }
  ],
  "no_stake_expected": false
}
```

#### `GET /api/hospitals/needs-enrichment`

**Added alongside manual enrichment (see below).** Hospitals the *automated* pipeline already ran on (`enriched_at IS NOT NULL`) but couldn't fully resolve — still missing coordinates, a website, or both. This is the manual-review queue: a hospital the pipeline hasn't reached at all (`enriched_at IS NULL`) belongs in `POST /api/pipeline/enrich` instead (see below), not here.

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `missing` | `string` | `"coordinates"` or `"website"`. Omitted = either (default) |
| `state` | `string` | Filter by two-letter state code |
| `page` | `int` | Page number (default: 1) |
| `per_page` | `int` | Results per page (default: 50, max: 200) |

**Response:** `200 OK`
```json
{
  "data": [
    {
      "facility_id": "011304",
      "facility_name": "OCHSNER CHOCTAW GENERAL",
      "address": "401 VANITY FAIR LANE, PO BOX 618",
      "city": "BUTLER",
      "state": "AL",
      "zip_code": "36904",
      "missing_coordinates": true,
      "missing_website": true
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 50,
    "total_count": 270,
    "total_pages": 6
  }
}
```

#### `PATCH /api/hospitals/:facility_id/enrichment`

**Manual enrichment.** Sets coordinates and/or a website by hand for one hospital — for whatever `POST /api/pipeline/enrich` (every provider in the cascade, including the `?retry_incomplete=true` backfill pass) couldn't resolve. Typically used against the queue `GET /api/hospitals/needs-enrichment` surfaces.

**Request body** (all fields optional; provide at least one):
```json
{
  "latitude": 31.451773,
  "longitude": -85.631010,
  "website_url": "https://example-hospital.org"
}
```
- `latitude`/`longitude` must be provided together (not just one) and must be in-range (`[-90, 90]` / `[-180, 180]`).
- `website_url` must start with `http://` or `https://`, **and is independently verified before being saved** (see `backend/src/url_check.rs`): a `HEAD` request (falling back to a capped `GET` for servers that reject `HEAD`) confirms the URL is reachable — an unreachable URL (DNS failure, timeout, or a non-2xx status) is rejected with `400`, the response body naming why. This is a deliberate scope line: `website_url` is a hospital's *homepage*, not necessarily an MRF link, so only "does it exist" is enforced — its content type, size, and a best-effort guess at whether it happens to look like a CMS machine-readable file are informational only (returned in `website_check`, never blocking). Full MRF schema validation remains out of scope (see [Future Extensions](#future-extensions)) — the sniff reads at most 64 KB of the body, never the whole (often multi-GB) file.
- Whatever's provided **overwrites** the existing value outright — unlike the automated pipeline's save path, which never clobbers an existing find, a manual correction here is assumed intentional.
- A successful update stamps `geo_provider = "manual"` and `geo_confidence = 1.0` when coordinates are set, and sets `enriched_at` if it wasn't already.

**Response:** `200 OK` — the updated hospital row (full `hospitals` table row, same shape as `GET /api/hospitals/:facility_id`'s `hospital` field), plus a `website_check` field (present only when `website_url` was submitted):
```json
{
  "facility_id": "011304",
  "...": "... (rest of the hospital row) ...",
  "website_check": {
    "reachable": true,
    "status": 200,
    "content_type": "text/html",
    "content_length": 18234,
    "looks_like_mrf": false,
    "mrf_format": null,
    "error": null
  }
}
```
`404` if `facility_id` doesn't exist; `400` for a validation failure, including an unreachable `website_url` (see above).

Full interactive documentation (request/response schemas for every endpoint) is auto-generated from the route handlers via `utoipa` and served at `/swagger-ui` (raw OpenAPI 3.1 JSON at `/api-docs/openapi.json`) — see [Crate Architecture](#crate-architecture)'s `backend/src/openapi.rs`.

#### `GET /api/stats`

Aggregate statistics.

```json
{
  "total_hospitals": 6234,
  "enriched": 6102,
  "discovery": {
    "mrf_found": 4210,
    "manifest_only": 312,
    "no_manifest": 987,
    "website_unreachable": 593,
    "no_website": 132,
    "not_checked": 0
  },
  "by_state": {
    "CA": { "total": 431, "mrf_found": 312, ... },
    "TX": { "total": 502, "mrf_found": 389, ... }
  }
}
```

### Pipeline Endpoints

#### `POST /api/pipeline/ingest`

Trigger CMS data ingestion. Returns a job ID.

#### `POST /api/pipeline/enrich`

Trigger geocoding enrichment for un-enriched hospitals. Returns a job ID.

**Query Parameters:**

| Param | Type | Description |
|-------|------|-------------|
| `retry_incomplete` | `bool` | `true` also re-selects hospitals that already ran once but are still missing coordinates or a website (opt-in backfill pass — see [Multi-Pass Strategy](#multi-pass-strategy)). Default `false`: only `enriched_at IS NULL` hospitals. |

#### `POST /api/pipeline/discover`

Returns a job ID. **Two modes**, both implemented:

- **Network-manifest mode** (`network_manifest_url` set): fetches and parses exactly one `cms-hpt.txt`, resolves its entries against hospitals (fuzzy name match, cross-validated against the ownership graph — see [Multi-location manifests](#multi-location-manifests-name-matching--ownership-graph-cross-validation)), and records a `mrf_discoveries` row for each confirmed match. Run `POST /api/pipeline/ingest-ownership` first, or every match gets rejected for lack of ownership corroboration (an empty graph, not a crash).
- **Per-hospital-website mode** (`network_manifest_url` unset — implemented 2026-09-15, the pipeline diagram's originally-deferred general flow): every hospital with a non-null `website_url` (narrowed by `state`/`facility_ids`) is probed on its own site for its own `cms-hpt.txt`, up to `PROBE_CONCURRENCY` concurrently. **Every** probed hospital gets a `mrf_discoveries` row — `website_unreachable`, `no_manifest`, etc. are recorded outcomes, not skipped or treated as job failures (only a technical failure — a DB write or transport-level error — counts against `failed`). No ownership-graph step here: there's no ambiguous manifest entry to corroborate, since the hospital being probed is already known from its own `website_url`.

Either mode records an `mrf_metadata` "baseline" row (see [`mrf_metadata` Table](#mrf_metadata-table)) for every MRF URL it confirms — the starting point `POST /api/mrf-discoveries/{id}/metadata/recheck` compares later checks against.

**Body:**
```json
{
  "network_manifest_url": "https://www.sentara.com/cms-hpt.txt",
  "state": "VA",
  "facility_ids": ["490007"]
}
```
- `network_manifest_url` — optional. Set it to run network-manifest mode; omit it (or send `{}`) to run per-hospital-website mode instead.
- `state` / `facility_ids` — both optional; narrow which hospitals are considered — as fuzzy-match candidates in network-manifest mode, or as the hospitals actually probed in per-hospital-website mode. In the latter, `facility_ids: ["050454"]` is how the frontend's per-hospital "Discover" button runs discovery for just one hospital. Neither given in network-manifest mode means every hospital nationwide is a candidate — safe, since name similarity alone never writes anything.

**Response:** `202 Accepted` with the job (see `GET /api/pipeline/jobs/:id` below); `400` for an unparseable JSON body.

#### `POST /api/pipeline/ingest-ownership`

Fetches CMS's "Hospital Enrollments" and "Hospital All Owners" datasets and replaces `hospital_enrollments` / `hospital_ownership_edges` outright. Returns a job ID. No body. See [`hospital_enrollments` and `hospital_ownership_edges` Tables](#hospital_enrollments-and-hospital_ownership_edges-tables).

#### `GET /api/pipeline/jobs/:id`

```json
{
  "id": "job-abc123",
  "stage": "discover",
  "status": "running",
  "progress": {
    "total": 6234,
    "completed": 2100,
    "failed": 15
  },
  "started_at": "2026-09-14T13:00:00Z"
}
```

### MRF Metadata Endpoints

#### `GET /api/mrf-discoveries/{id}/metadata`

Change-over-time history for one `mrf_discoveries` row — every `mrf_metadata` row recorded for it, most recent first. `404` if the discovery id doesn't exist. The oldest entry is always the `"baseline"` row `POST /api/pipeline/discover` wrote when the discovery was made.

#### `POST /api/mrf-discoveries/{id}/metadata/recheck`

Re-probes the discovery's MRF URL and appends one new `mrf_metadata` row: `ETag`/`Last-Modified` decide whether the file changed when either is usable against the previous row (no download); otherwise the full file is downloaded and hashed (SHA-1) instead (see [`mrf_metadata` Table](#mrf_metadata-table) for the full decision logic). Returns a job ID (`stage = "mrf-metadata-recheck"`) — poll `GET /api/pipeline/jobs/{id}` like any other trigger, since the SHA-1 fallback can mean downloading a multi-GB file. `404` if the discovery id doesn't exist.

---

## Error Handling Strategy

Each crate defines its own error type via `thiserror`:

```rust
#[derive(Debug, thiserror::Error)]
pub enum GeoEnrichError {
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Rate limited by provider: {provider}")]
    RateLimited { provider: String },

    #[error("Invalid API response: {0}")]
    InvalidResponse(String),

    #[error("API key missing for provider: {0}")]
    MissingApiKey(String),
}

impl GeoEnrichError {
    /// Whether this error is transient (retryable / fallback-eligible).
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Http(_) | Self::RateLimited { .. })
    }
}
```

The `backend` crate maps all errors to appropriate HTTP status codes via an `IntoResponse` implementation.

---

## Concurrency & Rate Limiting

### Geocoding

- `tokio::sync::Semaphore` bounds concurrent geocoding requests (configurable, default: 10).
- Per-provider rate limiting is handled internally by each provider implementation.
- Census batch mode (`geocode_batch`) processes up to 10,000 addresses per HTTP request, but isn't wired into the cascade yet — the cascade currently geocodes one hospital at a time regardless of which provider handles it.

### MRF Probing

- Token-bucket rate limiter (configurable, default: 5 requests/sec globally).
- Per-domain backoff: if a domain returns `429` or `503`, that domain is paused with exponential backoff (1s → 2s → 4s → ... → 60s max).
- Connection timeout: 10 seconds. Read timeout: 30 seconds.
- User-Agent: `cms-hpt-checker/0.1 (compliance-audit; +https://github.com/...)`.

---

## Configuration

All configuration is environment-based (loaded via `dotenvy`):

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `sqlite:data.db` | SQLite database path |
| `SERVER_HOST` | `0.0.0.0` | Bind address |
| `SERVER_PORT` | `3000` | Bind port |
| `GEOCODING_CENSUS_ENABLED` | `true` | Enable US Census geocoder |
| `GEOCODING_NOMINATIM_ENABLED` | `true` | Enable Nominatim geocoder |
| `GEOCODING_GOOGLE_MAPS_ENABLED` | `false` | Enable Google Maps geocoder |
| `GEOCODING_GOOGLE_MAPS_API_KEY` | — | Google Maps API key |
| `GOOGLE_PLACES_ENABLED` | `false` | Enable Google Places website-discovery fallback |
| `GOOGLE_PLACES_API_KEY` | — | Google Places API key (separate from the Maps Geocoding key) |
| `ENRICH_CONCURRENCY` | `10` | Max concurrent enrichment tasks |
| `ENRICH_BATCH_LIMIT` | `200` | Hospitals processed per `POST /api/pipeline/enrich` call |
| `PROBE_CONCURRENCY` | `10` | Max concurrent probe requests |
| `PROBE_RATE_LIMIT_PER_SEC` | `5.0` | Global probe rate limit |

---

## Future Extensions

These are explicitly **out of scope** for the current phase but are anticipated in the design:

1. ~~**MRF Metadata Extraction**~~ — Implemented 2026-09-15. See [`mrf_metadata` Table](#mrf_metadata-table) and [MRF Metadata Endpoints](#mrf-metadata-endpoints).
2. **MRF Content Validation** — Validate MRF contents against the CMS JSON schema or CSV template. Requires downloading multi-GB files. (A much lighter first step exists: `url_check`'s `looks_like_mrf` sniff on manually-entered `website_url`s reads at most 64 KB and only pattern-matches a handful of field names — a heuristic hint, not schema validation. `mrf_metadata.schema_valid` is reserved for this but always `NULL` today — no check populates it yet.)
3. **Scheduled Scans** — Recurring cron-based re-probing (weekly/monthly) to track compliance over time. `POST /api/mrf-discoveries/{id}/metadata/recheck` (implemented 2026-09-15) is the one-shot building block this would call in a loop — still triggered by hand today, no scheduler wired up.
4. **Frontend Dashboard** — Web UI for browsing hospitals, viewing compliance maps, and drilling into per-hospital detail. Implemented as of this session's earlier pass (`frontend/`, React + TS) for hospital listing/detail/manual-enrichment/pipeline triggers; the MRF change-history view (`MrfMetadataPanel`) was added alongside the endpoints above. A compliance *map* specifically is still not built.
5. **PostgreSQL Migration** — All SQL is already PgSQL-compatible. Swap `sqlx` feature flag from `sqlite` to `postgres` and update the connection string.
6. **Census bulk-batch geocoding** — Wire `CensusProvider::geocode_batch` into the enrichment loop so a full run costs one HTTP call per ~1,000 hospitals instead of one per hospital, rather than only being exercised by its own unit test.
