# CMS Hospital Price Transparency — Compliance Checker

A Rust backend that ingests the CMS Hospital General Information dataset, enriches records with geolocation data via a cascading geocoding strategy, and discovers whether each US hospital publishes a Machine-Readable File (MRF) as required by the CMS Price Transparency Final Rule.

---

## Problem

The **Hospital Price Transparency Final Rule** (45 CFR Parts 180) requires every Medicare-certified hospital to publish a machine-readable pricing file and host a `cms-hpt.txt` manifest at their website root. Enforcement began April 1, 2026, yet a significant number of the ~6,000+ US hospitals remain non-compliant.

This project provides an automated pipeline to **audit MRF compliance at scale**: ingest → enrich → discover → serve.

---

## Architecture

See [Architecture.md](file:///e:/Dev/healthcare-data-rest/Architecture.md) for the full system design, crate breakdown, database schema, and API reference.

```
 CMS Data API          Geocoding APIs           Hospital Websites
     │                  (Census → OSM              │
     │                   → Google)                 │
     ▼                      ▼                      ▼
 ┌──────────┐       ┌──────────────┐       ┌──────────────┐
 │  Ingest  │──────▶│   Enrich     │──────▶│  MRF         │
 │          │       │  (Cascading) │       │  Discovery   │
 └──────────┘       └──────────────┘       └──────────────┘
       │                    │                      │
       └────────────────────┴──────────────────────┘
                            │
                    ┌───────▼───────┐
                    │    SQLite     │
                    │ (PgSQL-compat)│
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │   Axum REST   │
                    │     API       │
                    └───────────────┘
```

### Workspace Crates

| Crate | Purpose |
|-------|---------|
| `cms-ingest` | Fetch & parse the CMS Provider Data Catalog hospital dataset |
| `geo-enrich` | Cascading geocoding trait with pluggable providers (Census → Nominatim → Google Maps) |
| `compliance-probe` | Probe hospital websites for `cms-hpt.txt` and discover MRF URLs |
| `backend` | Axum HTTP server, database layer, REST API, job orchestration |

---

## Getting Started

### Prerequisites

- **Rust** 1.85+ (edition 2025)
- **SQLite** 3.x (bundled via `sqlx`)

### Build & Run

```bash
# Build all crates
cargo build --workspace

# Run the backend server (default: http://localhost:3000)
cargo run -p backend
```

### Configuration

Create a `.env` file in the project root:

```env
DATABASE_URL=sqlite:data.db
SERVER_HOST=0.0.0.0
SERVER_PORT=3000

# Geocoding (optional — providers are tried in order)
GEOCODING_CENSUS_ENABLED=true
GEOCODING_NOMINATIM_ENABLED=true
GEOCODING_GOOGLE_MAPS_ENABLED=false
GEOCODING_GOOGLE_MAPS_API_KEY=

# Probe settings
PROBE_CONCURRENCY=10
PROBE_RATE_LIMIT_PER_SEC=5.0
```

### Running the Pipeline

```bash
# 1. Ingest CMS hospital data
curl -X POST http://localhost:3000/api/pipeline/ingest

# 2. Geocode & enrich hospital records (cascading: free providers first)
curl -X POST http://localhost:3000/api/pipeline/enrich

# 3. Discover MRF files on hospital websites
curl -X POST http://localhost:3000/api/pipeline/discover

# 4. Query results
curl http://localhost:3000/api/hospitals?state=CA
curl http://localhost:3000/api/stats
```

---

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/hospitals` | List hospitals (paginated, filterable by state, type, discovery status) |
| `GET` | `/api/hospitals/:facility_id` | Single hospital with enrichment + MRF discovery data |
| `GET` | `/api/stats` | Aggregate MRF discovery statistics |
| `POST` | `/api/pipeline/ingest` | Trigger CMS data ingestion |
| `POST` | `/api/pipeline/enrich` | Trigger geocoding enrichment |
| `POST` | `/api/pipeline/discover` | Trigger MRF discovery probe |
| `GET` | `/api/pipeline/jobs/:id` | Check pipeline job status |

---

## Data Sources

1. **CMS Provider Data Catalog** — [data.cms.gov/provider-data](https://data.cms.gov/provider-data/)
   - "Hospital General Information" dataset: facility IDs (CCN), names, addresses, types, ownership, ratings.
   - Updated quarterly by CMS.

2. **CMS Price Transparency Technical Resources** — [github.com/CMSgov/hospital-price-transparency](https://github.com/CMSgov/hospital-price-transparency)
   - MRF JSON/CSV schemas, data dictionaries, validator tools, naming conventions.

3. **Geocoding Providers** (cascading, configurable):
   - US Census Bureau Geocoder (free, batch)
   - Nominatim / OpenStreetMap (free, rate-limited)
   - Google Maps Geocoding API (paid, high accuracy)

---

## Current Scope

**In scope:**
- Ingest CMS hospital directory
- Geocode hospital addresses (cascading free → paid)
- Discover `cms-hpt.txt` manifests and extract MRF URLs
- Verify MRF URL accessibility (HTTP HEAD → 200)
- REST API for querying enriched + discovery data

**Deferred:**
- MRF content validation against CMS schemas
- MRF metadata extraction (SHA-1, Last-Modified, ETag, Cache-Control)
- Recurring scheduled scans
- Frontend / dashboard

---

## Testing

```bash
# Run all tests
cargo test --workspace

# Run specific crate tests
cargo test -p cms-ingest
cargo test -p geo-enrich
cargo test -p compliance-probe
cargo test -p backend
```

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.