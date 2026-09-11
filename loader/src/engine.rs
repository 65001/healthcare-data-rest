use crate::traits::Plugin;
use anyhow::Result;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::info;

pub struct LoaderEngine {
    pool: PgPool,
    registry: HashMap<String, Box<dyn Plugin + Send + Sync>>,
}

impl LoaderEngine {
    pub async fn new(pool: PgPool) -> Result<Self> {
        Ok(Self {
            pool,
            registry: HashMap::new(),
        })
    }

    pub fn register(&mut self, loader: Box<dyn Plugin + Send + Sync>) {
        let key = loader.key().to_string();
        self.registry.insert(key, loader);
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting loader engine...");

        // Collect keys
        let keys: Vec<String> = self.registry.keys().cloned().collect();

        for key in keys {
            info!("Processing loader: {}", key);
            let loader = self.registry.get(&key).unwrap();

            info!("Checking for updates...");
            if let Some(date) = loader.check_update().await? {
                info!("Found data version: {}", date);
                // Here we would check against DB, but for now we just load
                loader.load(&self.pool).await?;
            } else {
                info!("No date found or update check failed.");
            }
        }

        info!("Loader engine run complete.");
        Ok(())
    }
}
