CREATE TABLE IF NOT EXISTS loader_runs (
    loader_key TEXT PRIMARY KEY,
    version INTEGER NOT NULL,
    file_hash TEXT NOT NULL,
    last_run TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
