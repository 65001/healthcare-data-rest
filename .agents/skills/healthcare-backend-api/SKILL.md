---
name: healthcare-backend-api
description: >-
  Provides runbook instructions, route definitions, OpenAPI specifications, and development patterns
  for the Axum + Tokio + DuckLake REST backend service.
---

# Healthcare Backend API Architecture & Developer Guide

The backend service is built with **Rust**, **Axum 0.8**, **Tokio**, **Utoipa (OpenAPI 3.0)**, and **DuckDB/DuckLake**.

## Service Execution & Listening Port

The backend HTTP server runs on **Port 3000** by default (binding to `0.0.0.0:3000` / `http://localhost:3000`):

- **Default Port**: `3000` (configurable via `--port <PORT>` or `PORT` environment variable)
- **Default Host**: `0.0.0.0` (configurable via `--host <HOST>` or `HOST` environment variable)
- **Frontend Proxy Mapping**: The Vite frontend dev server running on `http://localhost:5173` proxies all `/api` requests directly to `http://127.0.0.1:3000`.

### Starting the Backend
```bash
# Start backend on default port 3000 with real lake data
cargo run -p backend

# Start backend on a custom port
cargo run -p backend -- --port 8080
```

### Active Endpoints (Port 3000)
- **Health & Dataset Stats**: `http://localhost:3000/api/v1/stats`
- **Hospital Directory API**: `http://localhost:3000/api/v1/hospitals`
- **Procedures API**: `http://localhost:3000/api/v1/procedures`
- **Price Comparison API**: `http://localhost:3000/api/v1/prices/compare`
- **Interactive Swagger UI**: `http://localhost:3000/swagger-ui/`
- **OpenAPI 3.1 JSON Specification**: `http://localhost:3000/api/v1/openapi.json`

---

## Axum Route Registry

All routes are nested under `/api/v1` and wired via `backend::routes::create_router`:

| Method | Endpoint | Handler | Description |
|---|---|---|---|
| `GET` | `/api/v1/hospitals` | `handlers::hospitals::search_hospitals` | Paginated search of hospitals by name, city, state, or license |
| `GET` | `/api/v1/hospitals/{id}` | `handlers::hospitals::get_hospital` | Fetch complete profile and metadata for a single hospital |
| `GET` | `/api/v1/procedures` | `handlers::procedures::search_procedures` | Search standard charges by code (CPT, HCPCS, MS-DRG) or keywords |
| `GET` | `/api/v1/prices/compare` | `handlers::prices::compare_prices` | Cross-hospital and cross-payer price comparisons |
| `GET` | `/api/v1/stats` | `handlers::stats::get_stats` | High-level lake dataset metrics (hospitals, states, lake version) |

---

## Endpoint Specifications

### 1. `GET /api/v1/hospitals`
- **Query Parameters**:
  - `q` (string, optional): Text search query against hospital name, address, or city.
  - `state` (string, optional): 2-letter state code (e.g. `MA`, `CA`).
  - `city` (string, optional): City name.
  - `license` (string, optional): State license number.
  - `limit` (integer, default `20`, max `100`).
  - `offset` (integer, default `0`).
- **Response**: `PaginatedResponse<HospitalSummary>`

### 2. `GET /api/v1/hospitals/{id}`
- **Path Parameter**: `id` (integer, e.g. `/api/v1/hospitals/42`)
- **Response**: `HospitalDetail`
- **Errors**: `404 Not Found` with `ApiErrorResponse` if hospital does not exist.

### 3. `GET /api/v1/procedures`
- **Query Parameters**:
  - `q` (string, optional): Keyword query against procedure description.
  - `code` (string, optional): Specific code (e.g. `99213`).
  - `code_type` (string, optional): Code system (`cpt`, `hcpcs`, `ms_drg`).
  - `hospital_id` (integer, optional): Scope to a specific hospital.
  - `limit` (integer, default `20`, max `100`).
  - `offset` (integer, default `0`).
- **Response**: `PaginatedResponse<StandardCharge>`

### 4. `GET /api/v1/prices/compare`
- **Query Parameters**:
  - `code` (string, required): Procedure code to compare (e.g. `99213`).
  - `code_type` (string, optional): Code system (`cpt`, `hcpcs`, `ms_drg`).
  - `state` (string, optional): Filter facilities by state (e.g. `MA`).
  - `payer` (string, optional): Filter by payer name (e.g. `Blue Cross`).
  - `plan` (string, optional): Filter by insurance plan name.
  - `limit` (integer, default `50`).
- **Response**: `Vec<PriceComparisonItem>`

### 5. `GET /api/v1/stats`
- **Response**: `DatasetStats`
  ```json
  {
    "total_hospitals": 7916,
    "states_covered": 54,
    "lake_version": "1.0",
    "is_lake_attached": true
  }
  ```

---

## Concurrency & Threading Model

DuckDB connections in `r2d2::Pool` must be executed synchronously. Handlers spawn blocking tasks:
```rust
let pool = state.pool.clone();
let result = tokio::task::spawn_blocking(move || {
    let conn = pool.get().map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    HealthcareRepository::search_hospitals(&conn, &params)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))
})
.await
.map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Task join error".to_string()))??;
```

---

## OpenAPI Integration (`utoipa`)

All handlers and schemas derive `ToSchema` and `IntoParams`.
- Document configuration lives in `backend/src/openapi.rs` (`ApiDoc`).
- When adding new endpoints or models:
  1. Add `#[utoipa::path(...)]` attribute to the handler.
  2. Add the handler function to the `paths(...)` list in `ApiDoc`.
  3. Add any new response/query structs to `components(schemas(...))` in `ApiDoc`.
  4. Run `cargo test -p backend` to verify OpenAPI serialization.
