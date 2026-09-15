-- CCN <-> PECOS ownership graph, fed by `cms_ingest::ownership` (the CMS
-- "Hospital Enrollments" and "Hospital All Owners" datasets). Used by
-- `db::queries::ownership_reachable_ccns` to restrict automatic
-- cms-hpt.txt-driven MRF tagging to hospitals actually connected through
-- CMS's own ownership disclosures, not just ones a manifest's
-- location-name happens to resemble — see
-- `routes::pipeline::trigger_discover`'s `network_manifest_url` mode.

-- One row per hospital's own PECOS enrollment — the crosswalk between
-- `hospitals.facility_id` (CCN) and the PECOS identifiers the ownership
-- dataset is keyed by. `ccn` is nullable: some PECOS enrollments in this
-- dataset are non-hospital subtypes with no CCN, and are simply
-- unjoinable to `hospitals` (harmless — they're never selected as a
-- match target).
CREATE TABLE hospital_enrollments (
    enrollment_id       TEXT PRIMARY KEY,
    ccn                  TEXT,
    associate_id          TEXT NOT NULL,
    organization_name      TEXT NOT NULL,
    state                   TEXT,
    ingested_at              TEXT NOT NULL
);

CREATE INDEX idx_hosp_enroll_ccn ON hospital_enrollments(ccn);
CREATE INDEX idx_hosp_enroll_associate ON hospital_enrollments(associate_id);

-- One row per disclosed owner/controller of one hospital's enrollment —
-- a hospital typically has several. `owner_associate_id` is the join key
-- back to another enrollment's `associate_id` for a multi-hop ownership
-- walk, though `ownership_reachable_ccns` currently only walks one hop
-- (see that function's doc comment for why).
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
