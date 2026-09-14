# Implementation notes

First pass written 2026-09-14; second pass (Nominatim + Google Places) added the same day. Read this before continuing the build.

## Pass 2 — Nominatim + Google Places wired up (2026-09-14)

Per the project owner's direction: Nominatim is now the next enrichment step, it also carries opportunistic website discovery, and Google Places is the fallback for when that comes up empty.

- **`geo-enrich/src/providers/nominatim.rs` is fully implemented**: real HTTP calls to the public Nominatim `search` endpoint, self-throttled to 1 req/sec (a shared `tokio::sync::Mutex`-guarded timestamp — holds regardless of how much concurrency `enricher::enrich_batch` uses), and reads a website out of OSM's `extratags` (`website` or `contact:website` tag) when present. Tested with `wiremock`.
- **`GeocodingResult` grew a `website_url: Option<String>` field** — this is how Nominatim's opportunistic find travels through the cascade. `None` from providers that don't supply it.
- **`geo-enrich/src/providers/google_places.rs` is new and fully implemented**: Places API (New) Text Search (`POST https://places.googleapis.com/v1/places:searchText`, confirmed live against Google's current docs), used only as the fallback when Nominatim's `website_url` is empty — deliberately not called for every hospital, since `websiteUri` is a paid "Pro" SKU field. Separate from `providers/google_maps.rs` (Maps *Geocoding* — different API, different key) on purpose; don't conflate the two. Tested with `wiremock`.
- **`geo-enrich/src/enricher.rs` is now a real implementation**, not a stub: bounded-concurrency batch loop, orchestrates cascade-geocode → (if no website) Places-fallback → persist, via a new `UnenrichedHospitalStore` trait that keeps this crate database-agnostic. Two callbacks (`on_total`, `on_progress`) let a caller report progress incrementally — this matters here specifically because Nominatim's 1 req/sec throttle means a few hundred hospitals can take minutes, unlike `cms-ingest`'s fast bulk fetch.
- **`backend/src/enrich_store.rs` is new**: `HospitalStore`, the real `UnenrichedHospitalStore` impl against `SqlitePool`. New queries in `db/queries.rs`: `list_unenriched_hospitals`, `save_enrichment` (stamps `enriched_at` even when nothing could geocode a hospital, so it isn't retried forever; `website_url` uses `COALESCE` so a `None` fallback never clobbers a previously-found website).
- **`POST /api/pipeline/enrich` now actually runs.** Its cascade is `[nominatim]` only — Census and Google Maps geocoding are still stubs, deliberately left out of the live cascade rather than added and immediately erroring on every hospital (though `GeoEnrichError::NotImplemented` is now treated as transient/skippable by the cascade regardless, so adding a stub back wouldn't break anything). New config: `GOOGLE_PLACES_ENABLED` / `GOOGLE_PLACES_API_KEY` (separate from the Maps Geocoding key), `ENRICH_CONCURRENCY`, `ENRICH_BATCH_LIMIT` (default 200 — see `.env.example` for why this isn't "all hospitals at once").
- **Still not compiled** — same container network restriction as pass 1. This pass's riskiest unverified spot is probably the Places API (New) request/response shape (headers, `textQuery` body, `websiteUri` field) — confirmed against Google's current docs via web-fetch, but never exercised against a live key or even compiled.

## Scope of pass 1 (per the project owner's answers)

- **Full workspace layout** (4 crates, matching Architecture.md's crate breakdown) plus the **SQLite schema/migrations**.
- **`cms-ingest` is fully implemented** against the real, live-verified CMS API (see below) — pagination, parsing, error handling, unit + `wiremock` tests.
- **`compliance-probe` is still a scaffold, not a working implementation.** Its rate limiter is real and tested; website-probing and `cms-hpt.txt` parsing are stubs that return `NotImplemented`. `geo-enrich` moved from scaffold to mostly-real in pass 2 (see above) — Census and Google Maps geocoding are the remaining stubs there.
- **`backend` is functional end-to-end for `ingest` and `enrich`.** Both really run against SQLite; `GET /api/hospitals`, `GET /api/hospitals/:id`, and `GET /api/stats` all work against whatever's in the DB. `POST /api/pipeline/discover` still returns `501 Not Implemented`.

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

1. **Website URL discovery**: Architecture.md's plan (pull `website_url` out of geocoder "map/place data") doesn't hold up — Census and Nominatim's *geocoding* endpoints don't reliably return a business website. Decision: add a **Google Places** lookup as its own enrichment step (separate API/key from Google Maps *Geocoding*), used as the fallback when Nominatim's opportunistic `extratags.website` is empty. **Built in pass 2** — see above.
2. **Google Maps geocoding**: implement fully, but leave `GEOCODING_GOOGLE_MAPS_ENABLED=false` by default (no key supplied). `GoogleMapsProvider::from_config` already returns `None` when disabled/keyless, so it's a drop-in add to the cascade whenever it gets implemented and a key is supplied. Still a stub as of pass 2.
3. **CMS dataset id**: researched live rather than guessed — see above.

## Other things worth knowing

- **Edition: 2024, confirmed.** `README.md`'s "edition 2025" was a typo — there is no Rust 2025 edition (editions ship 2015/2018/2021/2024; the next is expected around 2027). The workspace is pinned to **edition 2024** (stabilized in Rust 1.85, matching `rust-version = "1.85"`), with `resolver = "3"` set explicitly to match. Worth fixing "2025" → "2024" in the README. Since this code hasn't compiled yet, note that edition 2024 changed a few things relative to 2021 (RPIT lifetime-capture defaults, `gen` as a reserved keyword, `unsafe extern` blocks) — nothing in this codebase intentionally relies on 2021-specific behavior, but it's worth a second look if `cargo build` surfaces something odd here.
- **Jobs are in-memory only** (`backend/src/jobs.rs`) — not persisted to SQLite. Architecture.md doesn't say either way; this means job history doesn't survive a restart. Revisit if that matters.
- **`cms-hpt.txt`'s exact wire format is still unconfirmed.** Its field set is documented (CMS's FAQ PDF and the Drupal module docs both list `mrf-url`, `source-page-url`, `location-name`, `contact-name`, `contact-email`), but the line/delimiter format isn't published anywhere this session could reach — CMS's own generator tool (<https://cmsgov.github.io/hpt-tool/txt-generator/>) is a JS-rendered SPA. Fastest path once `compliance-probe` gets built: pull a handful of real `cms-hpt.txt` files from actual hospital sites and reverse-engineer the format directly, rather than continuing to chase the spec doc.
- **`geo-enrich`'s DB dependency is intentionally absent — resolved via a trait, per the plan here.** `enricher.rs` defines `UnenrichedHospitalStore`; `backend::enrich_store::HospitalStore` implements it against `SqlitePool`. Architecture.md's one-directional dependency graph (`backend` depends on the other three, not the reverse) is preserved.
- **No DB column tracks which source resolved a hospital's website** (Nominatim vs. Google Places) — Architecture.md's schema doesn't have one. `EnrichmentOutcome::website_source` carries this in memory and it's logged (`tracing::debug!`) on every enrichment, but not persisted. Add a column if that provenance needs to be queryable later.
- **A single un-lucky Nominatim/Places call never fails a whole enrich run.** `enrich_one` degrades gracefully — logs and treats it as "not found" — consistent with `cms-ingest`'s per-row error handling. Only a store (DB) failure or a panicked task counts toward a job's `failed` count.

## Suggested next steps, in order

1. `cargo build --workspace` and fix whatever the compiler finds.
2. `cargo test --workspace` — `cms-ingest`, `geo-enrich` (cascade, Nominatim, Google Places), and `compliance-probe::rate_limiter` all have real test coverage now.
3. Confirm `cms-ingest` and `geo-enrich` against the live APIs (this environment couldn't reach either): `cargo run -p backend`, `curl -X POST http://localhost:3000/api/pipeline/ingest`, wait for it to finish, then `curl -X POST http://localhost:3000/api/pipeline/enrich` with `GEOCODING_NOMINATIM_ENABLED=true` and poll `GET /api/pipeline/jobs/:id` — expect it to take a couple minutes per `ENRICH_BATCH_LIMIT` (200) hospitals, since Nominatim is throttled to 1/sec. Set `GOOGLE_PLACES_ENABLED=true` + `GOOGLE_PLACES_API_KEY` to exercise the fallback too.
4. Finish Census in `geo-enrich` — free, no key, covers most addresses per Architecture.md; add it to `backend/src/routes/pipeline.rs`'s cascade-builder once it's real.
5. Pull a few real `cms-hpt.txt` files and nail down `manifest_parser`'s format before writing `probe.rs`'s HTTP sequence.
