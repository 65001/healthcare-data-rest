use anyhow::Result;
use sqlx::PgPool;
use std::collections::HashMap;
use std::path::Path;
use tracing::{info, warn};

use crate::traits::{CmsDataLoader, FileHash};

#[derive(Debug, Clone)]
pub struct LoaderRunStatus {
    pub version: i32,
    pub file_hash: String,
}

pub struct LoaderEngine {
    pool: PgPool,
    // Cache of known loader states from DB: key -> status
    loader_states: HashMap<String, LoaderRunStatus>,
    // Registry of loaded plugins
    registry: HashMap<String, Box<dyn CmsDataLoader + Send + Sync>>,
}

impl LoaderEngine {
    pub async fn new(pool: PgPool) -> Result<Self> {
        let mut engine = Self {
            pool,
            loader_states: HashMap::new(),
            registry: HashMap::new(),
        };
        engine.scan().await?;
        Ok(engine)
    }

    /// Scans the loader_runs table and populates the local cache.
    pub async fn scan(&mut self) -> Result<()> {
        info!("Scanning loader_runs table...");
        let records = sqlx::query!("SELECT loader_key, version, file_hash FROM loader_runs")
            .fetch_all(&self.pool)
            .await?;

        for record in records {
            self.loader_states.insert(
                record.loader_key,
                LoaderRunStatus {
                    version: record.version,
                    file_hash: record.file_hash,
                },
            );
        }
        info!(
            "Loaded {} loader states from database.",
            self.loader_states.len()
        );
        Ok(())
    }

    pub fn register(&mut self, loader: Box<dyn CmsDataLoader + Send + Sync>) {
        let key = loader.key().to_string();
        if self.registry.contains_key(&key) {
            warn!("Overwriting existing loader for key: {}", key);
        }
        self.registry.insert(key, loader);
    }

    pub async fn run(&mut self, data_dir: &Path) -> Result<()> {
        info!(
            "Starting execution of {} registered loaders...",
            self.registry.len()
        );

        // Collect keys to avoid mutable/immutable borrow conflicts with `self`
        let keys: Vec<String> = self.registry.keys().cloned().collect();

        for key in keys {
            info!("Processing loader: {}", key);

            // 1. Get metadata (downloads file if needed and computes hash)
            // We need the loader to get metadata.
            let (metadata, plugin_version) = {
                let loader = self
                    .registry
                    .get(&key)
                    .expect("Loader missing from registry");
                (
                    loader.get_metadata(data_dir).await?,
                    loader.version() as i32,
                )
            };

            let file_hash_str = match &metadata.file_hash {
                FileHash::Sha256(h) => h.clone(),
                FileHash::Sha512(h) => h.clone(),
                FileHash::Md5(h) => h.clone(),
                FileHash::RustHasher(h) => h.clone(),
            };

            // 2. Check if loading is needed
            if self.should_load(&key, &file_hash_str, plugin_version) {
                info!("Data needs update/loading for '{}'...", key);

                // 3. Load data (extracts, parses, and inserts)
                {
                    let loader = self
                        .registry
                        .get(&key)
                        .expect("Loader missing from registry");

                    loader.load(&metadata.file, &self.pool).await?;
                    loader.cleanup(&metadata).await?;
                }

                // 4. Update status
                self.update_status(&key, &file_hash_str, plugin_version)
                    .await?;
                info!("Loader '{}' completed successfully.", key);
            } else {
                info!("Data up to date for key: {}", key);
            }
        }

        Ok(())
    }

    /// Checks if the loader should run based on the file hash and version.
    /// Returns true if the data should be loaded (i.e., new version or different hash).
    pub fn should_load(&self, key: &str, current_file_hash: &str, plugin_version: i32) -> bool {
        match self.loader_states.get(key) {
            Some(status) => {
                if plugin_version > status.version {
                    info!(
                        "Loader '{}' update needed: Plugin version increased (old: {}, new: {}).",
                        key, status.version, plugin_version
                    );
                    return true;
                }

                if status.file_hash == current_file_hash {
                    info!(
                        "Loader '{}' skipped: File hash matches existing record (v{}).",
                        key, status.version
                    );
                    false
                } else {
                    info!(
                        "Loader '{}' update needed: File hash changed (old: {}, new: {}).",
                        key, status.file_hash, current_file_hash
                    );
                    true
                }
            }
            None => {
                info!("Loader '{}' running for the first time.", key);
                true
            }
        }
    }

    /// Updates the loader run status after a successful load.
    pub async fn update_status(&mut self, key: &str, file_hash: &str, version: i32) -> Result<()> {
        sqlx::query!(
            "INSERT INTO loader_runs (loader_key, version, file_hash, last_run)
             VALUES ($1, $2, $3, NOW())
             ON CONFLICT (loader_key) 
             DO UPDATE SET version = $2, file_hash = $3, last_run = NOW()",
            key,
            version,
            file_hash
        )
        .execute(&self.pool)
        .await?;

        // Update local cache
        self.loader_states.insert(
            key.to_string(),
            LoaderRunStatus {
                version,
                file_hash: file_hash.to_string(),
            },
        );

        info!(
            "Updated loader status for '{}' to version {}.",
            key, version
        );
        Ok(())
    }
}
