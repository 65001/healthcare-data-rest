-- Schema per Architecture.md's "Database Schema" section. SQL is written
-- to be PostgreSQL-compatible (per the design principle in
-- Architecture.md) even though SQLite is the runtime engine here; SQLite
-- accepts DOUBLE PRECISION / BOOLEAN via type affinity so no
-- SQLite-specific syntax was needed.

CREATE TABLE hospitals (
    facility_id         TEXT PRIMARY KEY,
    facility_name       TEXT NOT NULL,
    address             TEXT NOT NULL,
    city                TEXT NOT NULL,
    state               TEXT NOT NULL,
    zip_code            TEXT NOT NULL,
    county_name         TEXT,
    phone_number        TEXT,
    hospital_type       TEXT NOT NULL,
    hospital_ownership  TEXT NOT NULL,
    emergency_services  BOOLEAN NOT NULL DEFAULT FALSE,
    overall_rating      INTEGER,

    -- Geocoding enrichment (NULL until enriched)
    latitude             DOUBLE PRECISION,
    longitude            DOUBLE PRECISION,
    formatted_address    TEXT,
    website_url          TEXT,
    geo_provider          TEXT,
    geo_confidence        DOUBLE PRECISION,
    enriched_at           TEXT,

    -- Metadata
    ingested_at          TEXT NOT NULL,
    updated_at           TEXT NOT NULL
);

CREATE INDEX idx_hospitals_state ON hospitals(state);
CREATE INDEX idx_hospitals_type ON hospitals(hospital_type);
CREATE INDEX idx_hospitals_enriched ON hospitals(enriched_at);

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
