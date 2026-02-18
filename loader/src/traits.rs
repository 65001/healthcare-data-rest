use anyhow::Result;
use async_trait::async_trait;
use chromiumoxide::Page;
use chrono::NaiveDate;
use sqlx::PgPool;

/// Trait for defining a loader for a specific CMS dataset.
#[async_trait]
pub trait CmsDataLoader: Send + Sync {
    /// A unique key to identify this dataset (e.g., "cms_hospital").
    fn key(&self) -> &str;

    /// The source URL for the dataset.
    fn url(&self) -> &str;

    /// Checks if an update is available by inspecting the page.
    /// Returns `Some(date)` if a version is found, `None` otherwise.
    async fn check_update(&self, page: &Page) -> Result<Option<NaiveDate>>;

    /// Orchestrates the loading process using the browser page.
    async fn load(&self, page: &Page, pool: &PgPool) -> Result<()>;
}
