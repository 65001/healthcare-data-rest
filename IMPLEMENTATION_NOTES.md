# Implementation notes — first pass

Written alongside the initial scaffold, 2026-09-14. Read this before continuing the build.

## Scope of this pass (per the project owner's answers)

- **Full workspace layout** (4 crates, matching Architecture.md's crate breakdown) plus the **SQLite schema/migrations**.
- **`cms-ingest` is fully implemented** against the real, live-verified CMS API (see below) — pagination, parsing, error handling, unit + `wiremock` tests.
- **`geo-enrich` and `compliance-probe` are scaffolds, not working implementations.** Trait/type definitions, the cascade orchestrator, and the rate limiter are real and tested; the three geocoding providers and the website-probing/manifest-parsing logic are stubs that return `NotImplemented` errors. Each stub's doc comment says what it needs.
- **`backend` is functional end-to-end for `ingest` only.** `POST /api/pipeline/ingest` really runs `cms-ingest` and upserts into SQLite; `GET /api/hospitals`, `GET /api/hospitals/:id`, and `GET /api/stats` all work against whatever's in the DB. `POST /api/pipeline/enrich` and `/discover` return `501 Not Implemented`.

**This code has not been compiled.** This container's egress policy blocks both `crates.io` and `data.cms.gov` directly (confirmed via the agent proxy status endpoint — `data.cms.gov:443` and `index.crates.io:443` both came back `403 Host not in allowlist`), so `cargo check` couldn't run here. The CMS API details below were confirmed through the web-fetch tool instead, which goes through a different path. **Run `cargo build --workspace` as your first step** and expect to fix a handful of small things — most likely spots, in rough order of likelihood:
- `sqlx`'s TLS feature flag name (`Cargo.toml` uses `tls-rustls`; sqlx has renamed/split this a few times across 0.7→0.8 point releases).
- axum's path-param syntax (`Cargo.toml` pins axum to `"0.7"`, which uses `:facility_id`; if you bump the major version, that syntax became `{facility_id}` in axum 0.8+).
- `serde`'s handling of `Option<bool>` query params in `routes/hospitals.rs`'s `ListParams` — this is a known rough edge with `serde_urlencoded`/axum's `Query` extractor.

None of these are architectural — they're the kind of thing a `cargo build` + a couple of `cargo fix`-shaped edits resolves quickly. I'd rather hand you working-but-unverified code with this flagged clearly than quietly claim it compiles.

## CMS API — verified live, 2026-09-14

Architecture.md describes the ingest source only loosely (`data.cms.gov/provider-data/datastore/query/{distribution_id}`). Confirmed against the live API:

- **Dataset:** "Hospital General Information", id **`xubh-q36u`** (<https://data.cms.gov/provider-data/dataset/xubh-q36u>). Currently 5,419 rows (Architecture.md's "~6,000" is in the right ballpark but not exact — expect this to drift; don't hardcode a count).
- **Endpoint:** `GET https://data.cms.gov/provider-data/api/1/datastore/query/xubh-q36u/0?limit={n}&offset={m}`. The dataset has exactly one distribution, addressed by index (`/0`) rather than a separate distribution UUID — Architecture.md's `{distribution_id}` and the dataset id are the same thing here.
- **Response shape:** `{"results": [{...}, ...], "count": <total>, "schema": {...}, "query": {...}}`.
- **Max page size:** `limit=1000` works; `limit=6000` 400s. We default to 500 (Architecture.md's number) to stay well clear of the boundary — this wasn't pinned down exactly, so if throughput matters, it's worth re-testing where the real ceiling is.
- **Field names do not match Architecture.md's `hospitals` table.** CMS uses `citytown`, `countyparish`, `telephone_number` — not `city`, `county_name`, `phone_number`. `cms-ingest/src/parser.rs` is the translation layer; the internal `HospitalRecord`/DB schema keeps Architecture.md's friendlier names.
- `emergency_services` is the string `"Yes"`/`"No"` (or blank). `hospital_overall_rating` is a numeric string or `"Not Available"`/blank. Both handled in `parser.rs` with tests against real sample rows.

## Decisions carried over from the clarifying questions

1. **Website URL discovery**: Architecture.md's plan (pull `website_url` out of geocoder "map/place data") doesn't hold up — Census and Nominatim's *geocoding* endpoints don't reliably return a business website. Decision: add a **Google Places** lookup as its own enrichment step (separate API/key from Google Maps *Geocoding*) rather than trying to extract it from geocode results. Not built in this pass — flagged in `geo-enrich/src/providers/google_maps.rs` and `nominatim.rs` so the distinction isn't lost later.
2. **Google Maps geocoding**: implement fully, but leave `GEOCODING_GOOGLE_MAPS_ENABLED=false` by default (no key supplied). `GoogleMapsProvider::from_config` already returns `None` when disabled/keyless, so `backend`'s cascade-builder (not yet written) can just skip adding it.
3. **CMS dataset id**: researched live rather than guessed — see above.

## Other things worth knowing

- **`README.md` says "edition 2025"** — there is no Rust 2025 edition (editions ship 2015/2018/2021/2024; the next is expected around 2027). Pinned the workspace to **edition 2021**, which `rust-version = "1.85"` fully supports. Worth a quick fix to the README if "2025" was a placeholder for something else.
- **Jobs are in-memory only** (`backend/src/jobs.rs`) — not persisted to SQLite. Architecture.md doesn't say either way; this means job history doesn't survive a restart. Revisit if that matters.
- **`cms-hpt.txt`'s exact wire format is still unconfirmed.** Its field set is documented (CMS's FAQ PDF and the Drupal module docs both list `mrf-url`, `source-page-url`, `location-name`, `contact-name`, `contact-email`), but the line/delimiter format isn't published anywhere this session could reach — CMS's own generator tool (<https://cmsgov.github.io/hpt-tool/txt-generator/>) is a JS-rendered SPA. Fastest path once `compliance-probe` gets built: pull a handful of real `cms-hpt.txt` files from actual hospital sites and reverse-engineer the format directly, rather than continuing to chase the spec doc.
- **`geo-enrich`'s DB dependency is intentionally absent.** `enricher.rs` needs to read/write the `hospitals` table but shouldn't depend on the `backend` crate (Architecture.md's dependency graph is one-directional: `backend` depends on the other three, not the reverse). The suggested fix is a small trait (e.g. `UnenrichedHospitalStore`) that `geo-enrich` defines and `backend` implements against its `SqlitePool` — not yet written.

## Suggested next steps, in order

1. `cargo build --workspace` and fix whatever the compiler finds.
2. `cargo test --workspace` — `cms-ingest` and `compliance-probe::rate_limiter` have real test coverage today.
3. Confirm `cms-ingest` against the live API (this environment couldn't): `cargo run -p backend`, then `curl -X POST http://localhost:3000/api/pipeline/ingest`, then poll `GET /api/pipeline/jobs/:id` and check `GET /api/hospitals`.
4. Pick a provider to finish in `geo-enrich` — Census is the recommended first one (free, no key, covers most addresses per Architecture.md).
5. Pull a few real `cms-hpt.txt` files and nail down `manifest_parser`'s format before writing `probe.rs`'s HTTP sequence.
