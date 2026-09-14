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

Probes each hospital's website to locate the CMS-required `cms-hpt.txt` manifest and extract MRF file URLs.

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
│       ├── client.rs       # HTTP client for data.cms.gov
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

### Future: `mrf_metadata` Table (Deferred)

```sql
CREATE TABLE mrf_metadata (
    id                  TEXT PRIMARY KEY,
    mrf_discovery_id    TEXT NOT NULL REFERENCES mrf_discoveries(id),
    mrf_url             TEXT NOT NULL,
    sha1_hash           TEXT,
    last_modified       TEXT,
    etag                TEXT,
    cache_control       TEXT,
    content_length      BIGINT,
    content_type        TEXT,
    schema_valid        BOOLEAN,
    checked_at          TEXT NOT NULL
);
```

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
- `website_url` must start with `http://` or `https://`.
- Whatever's provided **overwrites** the existing value outright — unlike the automated pipeline's save path, which never clobbers an existing find, a manual correction here is assumed intentional.
- A successful update stamps `geo_provider = "manual"` and `geo_confidence = 1.0` when coordinates are set, and sets `enriched_at` if it wasn't already.

**Response:** `200 OK` — the updated hospital row (full `hospitals` table row, same shape as `GET /api/hospitals/:facility_id`'s `hospital` field). `404` if `facility_id` doesn't exist; `400` for a validation failure (see above).

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

Trigger MRF discovery probe. Returns a job ID.

**Optional body:**
```json
{
  "state": "CA",
  "facility_ids": ["050454", "050001"]
}
```

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

1. **MRF Metadata Extraction** — Download MRF headers and record SHA-1 hash, `Last-Modified`, `ETag`, `Cache-Control`, `Content-Length`. Tracked in the deferred `mrf_metadata` table.
2. **MRF Content Validation** — Validate MRF contents against the CMS JSON schema or CSV template. Requires downloading multi-GB files.
3. **Scheduled Scans** — Recurring cron-based re-probing (weekly/monthly) to track compliance over time.
4. **Frontend Dashboard** — Web UI for browsing hospitals, viewing compliance maps, and drilling into per-hospital detail. Would be a natural home for the manual-enrichment queue (`GET /api/hospitals/needs-enrichment` + `PATCH .../enrichment`) — a form instead of hand-written `curl` calls.
5. **PostgreSQL Migration** — All SQL is already PgSQL-compatible. Swap `sqlx` feature flag from `sqlite` to `postgres` and update the connection string.
6. **Census bulk-batch geocoding** — Wire `CensusProvider::geocode_batch` into the enrichment loop so a full run costs one HTTP call per ~1,000 hospitals instead of one per hospital, rather than only being exercised by its own unit test.
