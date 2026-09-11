---
name: healthcare-ducklake
description: >-
  Provides runbook instructions, configuration commands, schema specifications, repository patterns,
  and query patterns for working with the hospital price transparency DuckLake (DuckDB >= 1.5 with ducklake extension).
---

# Healthcare DuckLake Operations & Reference Guide

This skill documents how to mount, query, pool, and manage the hospital price transparency DuckLake dataset.

## System Prerequisites & Dataset Provenance
- **DuckDB**: Version >= 1.5 (required for DuckLake format 1.0).
- **ducklake Extension**: Auto-installs on launch (`INSTALL ducklake; LOAD ducklake;`).
- **Data Provenance**: Aggregated by Trilliant Health from hospital Machine-Readable Files (MRF) mandated under CMS 45 CFR § 180.
- **Where to Obtain**: Download the DuckLake snapshot archive (e.g. `mrf_lake_20260721.zip`, ~74GB compressed) from **[oria-data.trillianthealth.com](https://oria-data.trillianthealth.com/)**.
- **Git Exclusions**: The 74GB archive (`mrf_lake_*.zip`), unzipped directory (`lake/`), and all DuckDB files (`*.duckdb`, `*.ducklake`, `*.parquet`) are excluded via `.gitignore`. Never commit large data files to git!
- **Data Location**: Extracted directly into `lake/` (`lake/metadata.ducklake`, `lake/catalog.duckdb`, `lake/data/`).

## Lake Directory Structure
When extracted, the `lake/` directory contains:

```text
lake/
├── open-lake.sh                     # Interactive DuckDB session script
├── open-lake.sql                    # DuckDB initialization script
├── README.md                        # Upstream lake usage notes
├── catalog.duckdb                   # Convenience views catalog (e.g. current_hospitals, current_charges)
├── metadata.ducklake                # DuckLake snapshot & data file registry
└── data/                            # Parquet storage managed directly by DuckLake
    └── main/
        ├── hospitals/
        ├── hospital_versions/
        ├── hospital_identity/
        ├── current_hospital_versions/
        ├── standard_charges/
        ├── standard_charge_details/
        ├── modifier_charges/
        └── modifier_charge_details/
```

> **Important**: Parquet files under `data/` are managed by DuckLake. Always query through `catalog.duckdb` or the attached `lake.*` catalog rather than scanning files directly.

## Mounting and Attaching the Lake

### Interactive Shell / CLI
From inside the `lake/` folder:
```bash
# Interactive SQL prompt
./open-lake.sh

# Web UI at http://localhost:4213
./open-lake.sh -ui

# Execute one-shot query
./open-lake.sh -c "SELECT COUNT(*) FROM current_hospitals"
```

### Manual Attachment Command
```bash
duckdb -readonly lake/catalog.duckdb \
  -cmd "INSTALL ducklake; LOAD ducklake; ATTACH 'ducklake:lake/metadata.ducklake' AS lake (DATA_PATH 'lake/data', OVERRIDE_DATA_PATH true, READ_ONLY);"
```

### SQL Initialization (`open-lake.sql`)
```bash
duckdb -readonly -init lake/open-lake.sql lake/catalog.duckdb
```

## Available Tables & Views

### 1. Convenience Views (Default Catalog)
Live in `catalog.duckdb` and represent the current/latest snapshot:
- `current_hospitals`: Deduplicated hospital entities with latest metadata (7,916+ facilities across 50+ states).
- `current_charges`: Aggregated service-level charges for current hospitals.

Example queries:
```sql
-- Total active hospitals
SELECT COUNT(*) FROM current_hospitals;

-- State breakdown
SELECT hospital_state, COUNT(*) 
FROM current_hospitals 
GROUP BY 1 
ORDER BY 2 DESC;

-- Sample current charges
SELECT * FROM current_charges LIMIT 100;
```

### 2. Versioned Lake Tables (`lake.*`)
Stored with full temporal history across all data runs:
- `lake.hospitals`
- `lake.hospital_versions`
- `lake.hospital_identity`
- `lake.current_hospital_versions`
- `lake.standard_charges`
- `lake.standard_charge_details` (payer-specific negotiated rates, cash prices, methodology)
- `lake.modifier_charges`
- `lake.modifier_charge_details`

Example queries:
```sql
SELECT * FROM lake.hospital_versions LIMIT 100;
SELECT * FROM lake.standard_charges LIMIT 100;

-- Payer comparison for a specific procedure code
SELECT payer_name, plan_name, standard_charge_dollar, methodology
FROM lake.standard_charge_details
WHERE cpt = '99213' AND standard_charge_dollar IS NOT NULL
LIMIT 50;
```

## Rust Backend Integration & Pooling Architecture

The Rust workspace manages DuckDB/DuckLake via `common::lake` and `common::repository`:

### Connection Pooling (`DuckLakePool`)
Defined in `common/src/lake.rs`:
- Uses `r2d2` with custom `DuckLakeConnectionManager`.
- Reads `DuckLakeConfig` with CLI arguments:
  - `--catalog-path <PATH>`: Default `lake/catalog.duckdb`
  - `--lake-metadata-path <PATH>`: Default `lake/metadata.ducklake`
  - `--data-path <PATH>`: Default `lake/data`
  - `--max-connections <N>`: Default `8` (or 16 under load)
  - `--no-ducklake-extension`: Optional flag to disable ducklake loading for fallback mode

### Repository Query Methods (`HealthcareRepository`)
Defined in `common/src/repository.rs`:
- `search_hospitals(conn, params)`: Queries `current_hospitals` with case-insensitive filtering by name, address, city, state, or license.
- `get_hospital_by_id(conn, id)`: Queries hospital details including attestation and contract provisions.
- `search_procedures(conn, params)`: Queries `current_charges` by CPT, HCPCS, MS-DRG code, or keyword.
- `compare_procedure_prices(conn, params)`: Queries `lake.standard_charge_details` joined with hospital metadata, comparing negotiated rates vs discounted cash across payers and plans.
- `get_dataset_stats(conn)`: Computes active hospital counts and state coverage.

### Tokio Blocking Task Execution Pattern
DuckDB connections are thread-affine and synchronous. In Axum handlers, execute queries inside `tokio::task::spawn_blocking`:
```rust
let pool = state.pool.clone();
let result = tokio::task::spawn_blocking(move || {
    let conn = pool.get().map_err(|e| ApiErrorResponse::internal(e.to_string()))?;
    HealthcareRepository::search_hospitals(&conn, &params)
        .map_err(|e| ApiErrorResponse::internal(e.to_string()))
}).await.map_err(...)??;
```
