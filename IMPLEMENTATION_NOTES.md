# Implementation notes

First pass written 2026-09-14; second pass (Nominatim + Google Places), pass 2b (governor crate), and pass 3 (Census wired in, Google Maps built, backfill support) all landed the same day. Pass 4 (2026-09-15) built out manual enrichment, Swagger/OpenAPI docs, and `compliance-probe`'s discover stage for real. Pass 5 (also 2026-09-15) implemented the previously-deferred `mrf_metadata` conditional-caching feature end-to-end, backend and frontend, plus a Dashboard button for `POST /api/pipeline/ingest-ownership`. Pass 6 (also 2026-09-15) added a per-hospital ownership view. Pass 7 (also 2026-09-15) built the general per-hospital-website discover mode, per-hospital/all-hospitals discover UI, and a DoD-specific ownership-reachability rule. Pass 8 (also 2026-09-15) added `mrf_discoveries.contact_name`/`contact_email` tracking and fixed `Prober::probe` to normalize a deep-path `website_url` to its origin before probing. Read this before continuing the build.

## Pass 8 — manifest contact tracking + probe base-URL normalization (2026-09-15)

Two small, unrelated fixes landed together:

**1. `mrf_discoveries.contact_name`/`contact_email`.** `compliance_probe::manifest_parser::MrfEntry` already parsed `cms-hpt.txt`'s `contact-name`/`contact-email` fields (confirmed live against Sentara's real manifest back in pass 4) but nothing persisted them — silently dropped at the `insert_mrf_discovery`/`insert_discovery_result` call sites. Added the two columns (`migrations/0004_mrf_discovery_contact.sql`, plain nullable `ALTER TABLE ADD COLUMN`s) and threaded the values through both discover modes: network-manifest mode takes them straight off the matched `MrfEntry`; per-hospital-website mode takes the first manifest entry's contact (a hospital's own `cms-hpt.txt` normally has exactly one entry). Frontend shows it as "Manifest contact: Casey Simpkins <payerdropbox@sentara.com>" (mailto-linked) under "Latest MRF discovery". **Verified live against real Sentara data twice** — once against a temporary copy of `data.db` while the real one was locked (see below), once for real after the lock cleared.

**2. `Prober::probe` now normalizes `website_url` to its origin *before* the reachability `HEAD`, not after.** The project owner flagged a real failure mode with a concrete example: `hospitals.website_url` is often sourced from a geocoding provider (Nominatim/Google Places) that can return a specific location-profile subpage rather than the site root — e.g. `https://www.valleyhealthlink.com/our-locations/profile/warren-memorial-hospital`. The old code HEADed that exact path first and only computed the origin *afterward* (for the `/cms-hpt.txt` URL) — so a deep path that 404s under a plain `HEAD` on a JS-routed site (common; the path exists client-side-only) incorrectly marked the whole hospital `WebsiteUnreachable` before discovery ever got to try `/cms-hpt.txt`, even though the site was completely up. Fixed by extracting the existing origin-computation logic (previously only used after the HEAD) into a `base_url()` helper and calling it *first*, so both the reachability check and the manifest URL now target the origin. New tests reproduce the exact failure mode (root mocked reachable, deep path never mocked — would 404 if probed as-is) and confirm it now succeeds.

**Non-code detour worth recording:** the real `data.db` got stuck under a persistent OS-level exclusive lock mid-pass (rename failed, no `backend.exe`/`cargo` process held it per `tasklist`) — eventually traced to something on the project owner's machine outside this session's control (closing VS Code didn't clear it; it cleared on its own between retries). Worked around by verifying pass 7's contact-tracking feature against a temporary copy of `data.db` in the scratchpad rather than blocking all progress on an external lock — a reasonable pattern if this recurs: copy the real DB, point a scratch backend instance at the copy on an isolated port, verify, then retry the real restart once free.

`cargo test -p backend` (35 tests) and `cargo test -p compliance-probe` (29 tests, 4 new) both pass; `cargo check --workspace` clean. Real `backend`/`frontend` dev servers rebuilt and restarted to pick up both fixes.

## Pass 7 — general discover mode, discover UI, and the DoD ownership special case (2026-09-15)

Three related asks landed this pass:

**1. The general per-hospital-website discover mode, finally built.** `POST /api/pipeline/discover` without `network_manifest_url` used to return `501` — Architecture.md's pipeline diagram always described this mode (probe every hospital's own `website_url` for its own `cms-hpt.txt`), but only the network-manifest mode had ever been implemented. `compliance_probe::probe::Prober::probe` already did the real work per-hospital; it just wasn't wired into a job that iterates hospitals. Now it is: `db::queries::list_discoverable_hospitals` (website_url IS NOT NULL, optionally state/facility_ids-filtered) feeds a `futures::stream::buffer_unordered(config.probe_concurrency)` — **`Config.probe_concurrency` was dead code until this pass**, flagged as a known gap in pass 4's notes; this is what it was for. `db::queries::insert_discovery_result` records *every* probed hospital's outcome (found, unreachable, no manifest — all legitimate, none of them job failures), and an `mrf_metadata` baseline is captured for every MRF URL found, same as the network-manifest mode. `trigger_discover`'s huge single function got split into `run_network_manifest_discover`/`run_per_hospital_discover` to keep each mode's logic separate and readable. `ApiError::NotImplemented` is now genuinely dead (nothing returns `501` from this endpoint anymore) and was removed rather than left as unreachable cruft.

**2. Discover UI, frontend.** Dashboard gained "Run MRF discovery (all hospitals)" (per-hospital-website mode, no filters — behind a `window.confirm` since it can touch thousands of hospitals under one shared rate limit and take a long time) and the hospital detail page gained a per-hospital "Discover" button (`facility_ids: [this one]`, disabled when the hospital has no `website_url`) next to "Latest MRF discovery" — `useDiscoverHospital` in `useHospitals.ts` polls the job and invalidates that hospital's query on completion so a newly found discovery shows up live. **Verified live** against a real hospital (SOUTHEAST HEALTH MEDICAL CENTER, 010001): clicking Discover found its real `cms-hpt.txt` and a real 45.7MB MRF file with real `ETag`/`Last-Modified` headers, and both the discovery card and the MRF-metadata history panel updated without a manual refresh.

**3. Department of Defense ownership special case — and why it ended up as *two* lists, not one.** The project owner noted that DoD-owned hospitals (`hospitals.hospital_ownership = "Department of Defense"`, 32 real rows) will always have an empty PECOS ownership disclosure — there's no private ownership stake for a federal facility to disclose — so `ownership_reachable_ccns`'s normal walk would always yield just `{seed_ccn}` alone for them, meaning a DoD network-manifest discover could never ownership-corroborate any match. First fix: a single `NO_PECOS_STAKE_OWNERSHIP_CATEGORIES` const (`["Department of Defense"]`) used both by `ownership_reachable_ccns` (blanket-connect hospitals sharing the category) and by a new `GET /api/hospitals/{id}/ownership` field (`no_stake_expected`, telling the frontend an empty owner list is expected, not missing data). When the project owner asked for "the same thing" for `"Government - Hospital District or Authority"`, that category turned out to cover ~500 real, *independent* local hospital districts (confirmed via the real DB) — correct for the "is this empty list expected" question, but blanket-connecting all 500 in the reachability graph would reintroduce exactly the false-positive cross-hospital linking that mechanism exists to prevent. The two questions the one const was answering aren't the same question. **Split into two**, both in `db/queries.rs` with doc comments cross-referencing each other: `OWNERSHIP_GRAPH_UNIFIED_CATEGORIES` stays narrow (`["Department of Defense"]` only — reachability graph, high stakes if wrong) and `ownership_category_has_no_pecos_stake()` is broader (Department of Defense *or* any `"Government - *"` prefix — `no_stake_expected` only, low stakes if wrong, just a slightly-off UI hint).

`GET /api/hospitals/{id}/ownership` returns `{owners: [...], no_stake_expected: bool}` instead of a bare array — computed backend-side specifically so the frontend never needs its own copy of the category list; the project owner explicitly asked for this after an initial pass hardcoded the category in `OwnershipPanel.tsx` and pointed out more categories will likely be added later (this is also exactly why splitting the list mattered: extending `ownership_category_has_no_pecos_stake` later must never accidentally also widen the reachability graph). `OwnershipPanel.tsx` shows a distinct, non-actionable message for `no_stake_expected` hospitals instead of the "go run ownership ingest" nudge.

`cargo test -p backend` (35 tests, all passing, 8 new this pass) and `cargo check --workspace` both clean; `tsc --noEmit` clean. Real `backend`/`frontend` dev servers rebuilt and restarted three times this pass to pick up changes, most recently to verify `010001` (a real "Government - Hospital District or Authority" hospital) shows `no_stake_expected: true` while staying reachability-isolated from every other district hospital.

## Pass 6 — per-hospital ownership view (2026-09-15)

The project owner asked whether a hospital's ownership/percentages were visible from its detail page — they weren't; `hospital_enrollments`/`hospital_ownership_edges` existed only as an internal graph `routes::pipeline::trigger_discover` walks for cross-validation (`db::queries::ownership_reachable_ccns`), with no way to read one hospital's disclosed owners directly.

- **`db::queries::list_hospital_owners(pool, facility_id)`** (new): joins `hospital_enrollments` (`ccn = facility_id`) to `hospital_ownership_edges` (`enrollment_id`), organizations sorted before individuals then by name. Empty (not an error) when ownership hasn't been ingested yet or this hospital has no PECOS enrollment/disclosed owner — same "empty is a legitimate outcome" pattern as `ownership_reachable_ccns`.
- **`GET /api/hospitals/{facility_id}/ownership`** (new, in `routes/hospitals.rs`): `404` only if the hospital itself doesn't exist; an empty list is a normal `200`. Documented via `#[utoipa::path]`, wired into `openapi.rs`.
- **Frontend**: `OwnershipPanel.tsx` (new component, `useHospitalOwnership` hook) — a table of owner/type/role/percentage, mounted on `HospitalDetailPage.tsx` below the MRF panels. The empty state links back to Dashboard → "Run ownership ingest" (added in pass 5) since that's the most likely reason the list is empty.
- **Verified against real, live data** (not scratch/seeded this time — the real DB already had ownership data from earlier live testing): `SENTARA RMH MEDICAL CENTER` (490004) correctly shows `SENTARA BLUE RIDGE, LLC` and `SENTARA HEALTH` at 100% plus its individual officers/directors; `RUSSELL COUNTY HOSPITAL` (490002) correctly shows the unrelated `BALLAD HEALTH` / `MOUNTAIN STATES HEALTH ALLIANCE` ownership chain — confirms the join is per-hospital-correct, not just structurally working. `cargo test -p backend` (28 tests, all passing, two new) and `tsc --noEmit` both clean. Real `backend`/`frontend` dev servers rebuilt and restarted to pick up the change.

## Pass 5 — MRF conditional-caching metadata, backend + frontend (2026-09-15)

The project owner asked for the `mrf_metadata` table Architecture.md had sketched but marked deferred: on an MRF discovery, capture cache-relevant response headers (`ETag`, `Last-Modified`, `Cache-Control`, `Content-Length`, `Content-Type`), and use them to tell whether the file changed over time without re-downloading it — falling back to a full download + SHA-1 hash only when the headers alone can't answer that.

- **`compliance-probe/src/probe.rs`** gained `Prober::probe_mrf_headers` (a `HEAD`, capturing the five headers above into `MrfHeaderProbe`) and `Prober::hash_mrf_content` (a streamed `GET` — `bytes_stream()` + incremental `sha1::Sha1` update, never buffers the whole body, since real MRFs can be multi-GB). The pure decision `detect_change_from_headers(previous_etag, previous_last_modified, fresh) -> Option<HeaderChangeCheck>` prefers `ETag` over `Last-Modified` when both are available and returns `None` (headers insufficient) only when neither side has a usable value to compare — that `None` is the caller's signal to fall back to hashing. Needed `reqwest`'s `stream` feature and the `futures`/`sha1` crates added to `compliance-probe/Cargo.toml`.
- **`backend/src/mrf_metadata.rs`** (new) orchestrates the decision against the database: `check_mrf_metadata(prober, mrf_url, previous: Option<&MrfMetadata>)`. Baseline (`previous = None`) downloads and hashes immediately *only if* neither `ETag` nor `Last-Modified` came back — otherwise there's nothing to gain from paying for a download today, since headers alone will do for the next comparison. A later check against a `previous` row tries headers first, hashes only as the fallback. Database-agnostic (takes `previous` as a plain argument) so it's exercised with `wiremock` in this crate's own tests, no DB needed.
- **`backend/migrations/0003_mrf_metadata.sql`**: the table as Architecture.md sketched it, plus two columns beyond the original sketch — `change_detection_method` and `changed_from_previous` — needed to actually expose the decision, not just the raw headers.
- **`routes::pipeline::trigger_discover`** now writes the `mrf_metadata` **baseline** row automatically for every MRF it confirms reachable, right after the `mrf_discoveries` insert (needed `insert_mrf_discovery` to start returning the generated id instead of `()`). **`backend/src/routes/mrf_metadata.rs`** (new) adds `GET /api/mrf-discoveries/{id}/metadata` (history, most recent first) and `POST /api/mrf-discoveries/{id}/metadata/recheck` (re-probe, decide, append — runs as a background job like every other pipeline trigger, since the SHA-1 fallback can mean a large download). Both fully documented via `#[utoipa::path]` and wired into `openapi.rs`'s `ApiDoc` under a new `mrf-metadata` tag.
- **Frontend**: `MrfMetadataPanel.tsx` (new component) renders the change history as a table (checked-at, a changed/unchanged/baseline/unreachable pill, detection method, ETag, Last-Modified, content type/size, SHA-1) plus a "Recheck now" button; `useMrfMetadata.ts` (new hook) fetches the history and drives the recheck mutation, polling the returned job via the existing `useJob` and invalidating the history query once it completes. Mounted into `HospitalDetailPage.tsx` under the existing "Latest MRF discovery" card, whenever a discovery exists.
- **Verified live**: a scratch backend (isolated SQLite DB, isolated port, run from a scratchpad `cwd` so `dotenvy` couldn't pick up the real `.env`) was seeded with a hospital, a discovery, and four hand-built `mrf_metadata` rows (baseline → unchanged-via-etag → changed-via-etag → unchanged-via-sha1) to confirm the table/pills render correctly, then "Recheck now" was clicked for real against the seeded (fake, unresolvable) MRF URL — it correctly appended an "Unreachable" row with no method/decision, round-tripping through the real job-polling UI. `cargo test -p backend -p compliance-probe` (51 tests, all passing) and `cargo check --workspace` both clean. Both real dev servers (`backend` on :3000, `frontend` on :5173) were rebuilt and restarted at the end of this pass to pick up the change.

## Pass 4 — manual enrichment, utoipa/Swagger, and a real discover stage (2026-09-15)

Three separate asks landed this pass, in order:

**1. Manual enrichment backend** — `GET /api/hospitals/needs-enrichment` and
`PATCH /api/hospitals/:facility_id/enrichment` were fully documented in
Architecture.md (an earlier pass wrote the spec ahead of the code) but not
implemented. Built per that spec exactly: `db/queries.rs` gained
`list_needs_enrichment` / `manual_enrich_hospital`; `routes/hospitals.rs`
gained the two handlers. `PATCH .../enrichment`'s `website_url` also got
real verification (`backend/src/url_check.rs`, new): a `HEAD` (falling back
to a capped `GET`) confirms the URL is reachable — required, `400` if not —
plus non-blocking `content_type`/`content_length`/`looks_like_mrf` info
returned in the response. `looks_like_mrf` is a lightweight ≤64KB body
sniff for CMS MRF field-name markers, *not* schema validation (still
correctly out of scope — see Future Extensions #2).

**2. `utoipa` + Swagger UI** — every route now carries a
`#[utoipa::path(...)]` annotation and every request/response type derives
`ToSchema`/`IntoParams`. `backend/src/openapi.rs` aggregates all of it into
one `OpenApi` doc, served at `/api-docs/openapi.json` with a browsable UI
at `/swagger-ui` (`utoipa-swagger-ui`, pinned to `8.1.0` — the `9.x` line's
`axum` feature targets axum 0.8, this workspace is still on 0.7).

**3. `compliance-probe`'s discover stage, for real** — the project owner
pointed at a live, real-world manifest
(`https://www.sentara.com/cms-hpt.txt`) and asked for `cms-hpt.txt`
location-names to be resolved and MRF URLs auto-tagged across a whole
hospital network. This pinned down the long-open `cms-hpt.txt` wire format
(see `manifest_parser.rs`'s doc comment — a block format, confirmed
against that real 18-entry manifest) and drove a full implementation:
`compliance_probe::probe::Prober` (real HTTP sequence, rate-limited/backed-
off), `backend::mrf_match` (Jaro-Winkler fuzzy location-name → hospital
matching, `strsim` crate, threshold `0.90` calibrated against that same
real manifest cross-checked against CMS's own hospital names), and —
because name matching alone isn't a safe basis for writing data — a second
independent signal: `db::queries::ownership_reachable_ccns` walks CMS's own
ownership-disclosure data (two more datasets, `cms_ingest::ownership`,
ingested via the new `POST /api/pipeline/ingest-ownership`) to confirm a
name match is actually the same corporate network before it's persisted.
`POST /api/pipeline/discover` gained a `network_manifest_url` mode wiring
all of this together (the general "probe every hospital's own website"
mode Architecture.md describes is still `501` — out of scope for this
pass).

**Verified end-to-end against live services**, not just unit tests: a
real ingest (5,419 hospitals) into a scratch SQLite DB, a real
`ingest-ownership` run (156,020 rows, ~16s), then a real `discover` call
against `sentara.com/cms-hpt.txt`. First run: 11/12 real Sentara hospitals
tagged, one (`Sentara Albemarle`) name-matched (score 0.92) but rejected by
the ownership check — investigating turned up a genuine design gap, not a
data problem: Albemarle's controlling owner-of-record in PECOS is
`"SENTARA HOSPITALS"` (operational/managerial control), while every other
matched hospital in this manifest lists `"SENTARA HEALTH"` (5%+ direct
ownership) — two different legal entities within the same real-world
system. The original design anchored the ownership walk on a single
(highest-scoring) seed hospital, so it only ever saw one of those two
owner clusters. Fixed by unioning `ownership_reachable_ccns` across *every*
name-matched hospital rather than just the top one — costs nothing in
safety (each seed still independently cleared the name-similarity bar on
its own merit) and only ever adds legitimate reachable hospitals. Re-run
after the fix: 12/12 real hospitals tagged, all 6 satellite/outpatient
manifest entries (no CCN of their own — `Sentara Independence`, `Sentara
BelleHarbour`, `Sentara Lake Ridge`, `Sentara Port Warwick`, `Hospital for
Extended Recovery`, an outpatient care center reusing Martha Jefferson's
MRF) correctly rejected by name matching alone, exactly as designed.

**Dataset ids used** (CMS's `data-api/v1/dataset/{uuid}/data`, confirmed
live 2026-09-15 — not in Architecture.md's original API notes, which only
covered the Provider Data Catalog endpoint `cms-ingest/client.rs` uses):
- Hospital Enrollments: `f6f6505c-e8b0-4d57-b258-e2b94133aaf2`
- Hospital All Owners: `029c119f-f79c-49be-9100-344d31d10344`

Both were found by fetching `https://data.cms.gov/data.json` (CMS's full
catalog) and searching titles — the dataset landing pages themselves
(`data.cms.gov/provider-characteristics/...`) are JS-rendered SPAs that
resist scraping, same issue as CMS's `cms-hpt.txt` generator tool noted in
pass 1.

**A process note, not a code one**: this pass accidentally deleted
`backend/data.db` mid-session (cleanup after a smoke test, without
checking first whether it held real data) and briefly killed a
project-owner-run dev server process before realizing it had live
connections. Neither was malicious, both were avoidable — flagging plainly
per this file's own stated policy on that. Live smoke-testing after this
pass onward runs from a scratch `cwd` outside the repo tree (so `dotenvy`
can't find the real `.env` and silently override a test `DATABASE_URL`/
`SERVER_PORT` — the actual root cause of the first mistake) and always
checks `netstat`/established-connections before touching any already-
running `backend.exe`.

## Pass 3 — diagnosed a real enrichment run, wired Census in, built Google Maps, added backfill (2026-09-14)

The project owner ran an enrichment pass against the live CMS dataset and reported it back: website URLs mostly blank, some hospitals missing lat/long. Inspecting `data.db` directly turned up two things, one a data-quality reality and one an actual gap in this codebase.

**What the data showed (5,419 hospitals total):**
- 5,149 (95%) got coordinates; 270 (5%) didn't.
- Of the 270 failures, `geo_provider` was `NULL` on all of them — every provider in the cascade struck out. Sampling them: mostly PO-box-only addresses (`"PO BOX 287"`, no street at all) or messy combined strings (`"83825 HIGHWAY 9    P O BOX 1270"`) — the kind of address no street-level geocoder, free or paid, reliably resolves. A couple are exact-duplicate CMS rows (same hospital, same address, two `facility_id`s — a CMS data-quality quirk, not a bug here).
- Only 744 (14%) got a `website_url` — 487 via Nominatim's `extratags.website`, 257 via Google Places. This is expected, not a bug: OSM's `extratags` carries a website for a minority of POIs, and the run's `.env` had `GOOGLE_PLACES_ENABLED=true` with a real key, so Places *was* running and still only closed a fraction of the gap — Places' Text Search only returns `websiteUri` when it can confidently match hospital name + address to a Places entry, and plenty of small/rural hospitals just aren't well-represented there.

**The actual gap: `geo_provider = 'us_census'` accounted for 4,660 of the 5,149 successful geocodes** — but the `pipeline.rs` cascade in this repo, at the start of this pass, only ever added `NominatimProvider`. Investigating further: `geo-enrich/src/providers/census.rs` in the project owner's checked-out copy is a full, tested implementation (single-address + bulk-batch geocoding against the free Census API) — not the stub this repo's earlier commits (and `IMPLEMENTATION_NOTES.md`/doc-comment claims) described it as. The run that produced this `data.db` used a build where Census *was* wired into the cascade; a later commit from this session (pass 2) replaced `pipeline.rs` without knowing Census was real, silently dropping it from the cascade going forward. That's a real regression introduced by this session's own earlier pass, not something the project owner did — worth flagging plainly rather than glossing over. Fixed this pass: `census.rs`'s implementation is unchanged, but it's now actually wired into `pipeline.rs`'s cascade again (first, ahead of Nominatim).

**Changes this pass:**
- `backend/src/routes/pipeline.rs`'s cascade is now `[census, nominatim, google_maps]`, each gated by its existing `GEOCODING_*_ENABLED` config flag (all three flags already existed; only `census`'s was actually being read before this pass — nothing to add in `.env`/`config.rs`).
- `geo-enrich/src/providers/google_maps.rs` is now a full implementation (was a stub): `GET https://maps.googleapis.com/maps/api/geocode/json`, confirmed live 2026-09-14 against Google's current docs. Maps `status` (`OK`/`ZERO_RESULTS`/`OVER_QUERY_LIMIT`/`OVER_DAILY_LIMIT`/`REQUEST_DENIED`/`INVALID_REQUEST`) onto the same `Ok(Some)`/`Ok(None)`/`RateLimited`/`InvalidResponse` shape every other provider uses, and derives a rough `confidence` from `geometry.location_type` (`ROOFTOP` = 1.0 down to `APPROXIMATE` = 0.5). Tested with `wiremock`. It's the cascade's last resort — paid, off by default (`GEOCODING_GOOGLE_MAPS_ENABLED=false`) — for addresses Census and Nominatim can't resolve; it won't help with the PO-box-only cases above (there's no street address to geocode), but should pick up some of the messier-but-real ones.
- **Backfill support, since the previous run set `enriched_at` on every row**: `enriched_at IS NULL` (the only thing `list_unenriched_hospitals` ever selected) now matches nothing, so a plain `POST /api/pipeline/enrich` would process 0 hospitals forever. Added `?retry_incomplete=true` as an opt-in query param: it broadens the selection to also include hospitals that already ran once but are still missing `latitude` or `website_url`. Opt-in rather than the default, deliberately — a normal call should never re-spend a throttled Nominatim call or a paid Places call on the PO-box addresses that will predictably fail the same way every time.
  - `geo_enrich::enricher::UnenrichedHospitalStore::list_unenriched` gained a `retry_incomplete: bool` parameter; `backend::db::queries::list_unenriched_hospitals` gained the same, plus now selects `latitude`/`website_url` so the caller knows what's already on the row.
  - `EnrichmentTarget` gained `has_coordinates: bool` and `existing_website_url: Option<String>`. `enrich_one` uses these to avoid redundant work on a backfill pass: if a hospital already has coordinates, it skips the geocode cascade entirely (no point re-running a throttled/paid call just to reach a provider's opportunistic website read) and goes straight to the Places fallback if it's still missing a website.
  - `save_enrichment`'s existing behavior (COALESCE on `website_url`, geocode columns left untouched when `geocode: None`) already made this safe — a backfill pass can never clobber a field that's already correct, it can only fill in what's still blank.
- `providers/mod.rs`, `lib.rs`, `census.rs`'s doc comments, and `GeoEnrichError`'s doc comments updated to stop describing Census/Google Maps as stubs.
- Still not compiled — same container network restriction as every prior pass.

**To use this**: rebuild (`cargo build --workspace`), then `curl -X POST 'http://localhost:3000/api/pipeline/enrich?retry_incomplete=true'` repeatedly (respecting `ENRICH_BATCH_LIMIT`) to work through the 270 + most of the 4,675 gaps — Census picking up some of what Nominatim-only missed, Google Maps (if enabled) picking up some of the rest. Expect the PO-box-only addresses and CMS's duplicate rows to remain unresolved regardless — that's a data ceiling, not a pipeline bug.

## Pass 2b — `governor` crate instead of a hand-rolled throttle (2026-09-14)

Per the project owner's suggestion: `NominatimProvider`'s 1 req/sec throttle no longer hand-rolls a `tokio::sync::Mutex<Instant>` guard. It now uses the [`governor`](https://docs.rs/governor) crate (GCRA algorithm), confirmed live at 0.10.4 on crates.io.

- `NominatimProvider` now holds `limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>` instead of `min_interval`/`next_allowed`. Built once in `with_base_url` via `RateLimiter::direct(Quota::per_second(1))` — burst size 1, so the first call goes through immediately and every call after waits for the next 1-second slot, same externally-observable behavior as before.
- `geocode()` calls `self.limiter.until_ready().await;` in place of the old `self.throttle().await;`.
- Added `governor = "0.10"` to `geo-enrich/Cargo.toml`.
- No behavior change from `enricher.rs`/`backend`'s point of view — same struct is `Clone`, same "share one instance across every concurrent enrich task" requirement applies (still true: `NominatimProvider` is constructed once per enrich job in `routes/pipeline.rs` and cloned into the geocoder's provider list).
- Still not compiled, same container restriction as passes 1 and 2.

## Pass 2 — Nominatim + Google Places wired up (2026-09-14)

Per the project owner's direction: Nominatim is now the next enrichment step, it also carries opportunistic website discovery, and Google Places is the fallback for when that comes up empty.

- **`geo-enrich/src/providers/nominatim.rs` is fully implemented**: real HTTP calls to the public Nominatim `search` endpoint, self-throttled to 1 req/sec (originally a hand-rolled `tokio::sync::Mutex`-guarded timestamp, swapped for the `governor` crate in pass 2b below — holds regardless of how much concurrency `enricher::enrich_batch` uses), and reads a website out of OSM's `extratags` (`website` or `contact:website` tag) when present. Tested with `wiremock`.
- **`GeocodingResult` grew a `website_url: Option<String>` field** — this is how Nominatim's opportunistic find travels through the cascade. `None` from providers that don't supply it.
- **`geo-enrich/src/providers/google_places.rs` is new and fully implemented**: Places API (New) Text Search (`POST https://places.googleapis.com/v1/places:searchText`, confirmed live against Google's current docs), used only as the fallback when Nominatim's `website_url` is empty — deliberately not called for every hospital, since `websiteUri` is a paid "Pro" SKU field. Separate from `providers/google_maps.rs` (Maps *Geocoding* — different API, different key) on purpose; don't conflate the two. Tested with `wiremock`.
- **`geo-enrich/src/enricher.rs` is now a real implementation**, not a stub: bounded-concurrency batch loop, orchestrates cascade-geocode → (if no website) Places-fallback → persist, via a new `UnenrichedHospitalStore` trait that keeps this crate database-agnostic. Two callbacks (`on_total`, `on_progress`) let a caller report progress incrementally — this matters here specifically because Nominatim's 1 req/sec throttle means a few hundred hospitals can take minutes, unlike `cms-ingest`'s fast bulk fetch.
- **`backend/src/enrich_store.rs` is new**: `HospitalStore`, the real `UnenrichedHospitalStore` impl against `SqlitePool`. New queries in `db/queries.rs`: `list_unenriched_hospitals`, `save_enrichment` (stamps `enriched_at` even when nothing could geocode a hospital, so it isn't retried forever; `website_url` uses `COALESCE` so a `None` fallback never clobbers a previously-found website).
- **`POST /api/pipeline/enrich` now actually runs.** Its cascade was `[nominatim]` only at the end of this pass — **corrected in pass 3 below**, where it turned out Census wasn't actually a stub and had been dropped from the cascade by mistake; read that section before trusting this bullet. New config: `GOOGLE_PLACES_ENABLED` / `GOOGLE_PLACES_API_KEY` (separate from the Maps Geocoding key), `ENRICH_CONCURRENCY`, `ENRICH_BATCH_LIMIT` (default 200 — see `.env.example` for why this isn't "all hospitals at once").
- **Still not compiled** — same container network restriction as pass 1. This pass's riskiest unverified spot is probably the Places API (New) request/response shape (headers, `textQuery` body, `websiteUri` field) — confirmed against Google's current docs via web-fetch, but never exercised against a live key or even compiled.

## Scope of pass 1 (per the project owner's answers)

- **Full workspace layout** (4 crates, matching Architecture.md's crate breakdown) plus the **SQLite schema/migrations**.
- **`cms-ingest` is fully implemented** against the real, live-verified CMS API (see below) — pagination, parsing, error handling, unit + `wiremock` tests.
- **`compliance-probe` is still a scaffold, not a working implementation.** Its rate limiter is real and tested; website-probing and `cms-hpt.txt` parsing are stubs that return `NotImplemented`. `geo-enrich` is fully real as of pass 3 — all three geocoding providers plus Google Places are implemented.
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
2. **Google Maps geocoding**: implement fully, but leave `GEOCODING_GOOGLE_MAPS_ENABLED=false` by default (no key supplied). `GoogleMapsProvider::from_config` already returns `None` when disabled/keyless. **Built in pass 3** — see above.
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
4. ~~Finish Census in `geo-enrich`~~ — done as of pass 3 (it was already real, just not wired in; now it is, first in the cascade).
5. Pull a few real `cms-hpt.txt` files and nail down `manifest_parser`'s format before writing `probe.rs`'s HTTP sequence.
