-- Tracks conditional-caching metadata captured on each MRF-URL probe, so
-- later checks can tell whether the file changed without re-downloading
-- it. Matches Architecture.md's previously-deferred `mrf_metadata` table,
-- plus two columns (`change_detection_method`, `changed_from_previous`)
-- that record the actual change decision made at `checked_at` — see
-- backend/src/mrf_metadata.rs.
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
    -- 'baseline' (first capture, nothing to compare against yet), 'etag',
    -- 'last_modified', or 'sha1'. NULL when the URL was unreachable.
    change_detection_method    TEXT,
    -- NULL when there was nothing to compare against (baseline) or the
    -- URL was unreachable; otherwise whether this check found the file
    -- changed since the previous check for the same mrf_discovery_id.
    changed_from_previous      BOOLEAN,
    checked_at                 TEXT NOT NULL
);

CREATE INDEX idx_mrf_metadata_discovery ON mrf_metadata(mrf_discovery_id);
CREATE INDEX idx_mrf_metadata_url ON mrf_metadata(mrf_url);
CREATE INDEX idx_mrf_metadata_checked ON mrf_metadata(checked_at);
