use crate::traits::CmsDataLoader;
use anyhow::Result;
use chromiumoxide::{Browser, BrowserConfig};
use futures::StreamExt;
use sqlx::PgPool;
use std::collections::HashMap;
use tracing::info;

pub struct LoaderEngine {
    pool: PgPool,
    registry: HashMap<String, Box<dyn CmsDataLoader + Send + Sync>>,
}

impl LoaderEngine {
    pub async fn new(pool: PgPool) -> Result<Self> {
        Ok(Self {
            pool,
            registry: HashMap::new(),
        })
    }

    pub fn register(&mut self, loader: Box<dyn CmsDataLoader + Send + Sync>) {
        let key = loader.key().to_string();
        self.registry.insert(key, loader);
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Initializing Chromium browser...");
        let (mut browser, mut handler) = Browser::launch(
            BrowserConfig::builder()
                .build()
                .map_err(|e| anyhow::anyhow!(e))?,
        )
        .await?;

        // Spawn the handler thread
        let handle = tokio::task::spawn(async move {
            while let Some(h) = handler.next().await {
                if h.is_err() {
                    break;
                }
            }
        });

        // Collect keys
        let keys: Vec<String> = self.registry.keys().cloned().collect();

        for key in keys {
            info!("Processing loader: {}", key);
            let loader = self.registry.get(&key).unwrap();

            info!("Navigating to {}", loader.url());
            let page = browser.new_page(loader.url()).await?;

            info!("Checking for updates...");
            if let Some(date) = loader.check_update(&page).await? {
                info!("Found data version: {}", date);
                // Here we would check against DB, but for now we just load
                loader.load(&page, &self.pool).await?;
            } else {
                info!("No date found or update check failed.");
            }

            page.close().await?;
        }

        browser.close().await?;
        handle.await?;

        Ok(())
    }
}
