use crate::cms;
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDate;
use reqwest;
use serde_json;
use sqlx::PgPool;

/// Base trait for data loaders.
#[async_trait]
pub trait Plugin: Send + Sync {
    fn key(&self) -> &str;
    async fn check_update(&self) -> Result<Option<NaiveDate>>;
    async fn load(&self, pool: &PgPool) -> Result<()>;
}

/// Trait for CMS-specific plugins with shared logic.
#[async_trait]
pub trait CmsPlugin: Plugin {
    /// The source URL for the dataset.
    fn url(&self) -> &str;

    async fn get_slug(&self) -> Result<cms::SlugResponse> {
        let full_url = self.url();
        let path = full_url.replace("https://data.cms.gov", "");

        // 1. Get Slug Info
        let slug_url = "https://data.cms.gov/data-api/v1/slug";
        let slug_resp = reqwest::Client::new()
            .get(slug_url)
            .query(&[("path", &path)])
            .send()
            .await?
            .json::<crate::cms::SlugResponse>()
            .await?;

        println!("Slug Response: {:?}", slug_resp);

        Ok(slug_resp)
    }

    async fn get_resources(&self) -> Result<cms::CmsResourceResponse> {
        println!("Getting resources...");
        let slug_resp = self.get_slug().await?;

        let uuid = slug_resp.data.current_dataset.uuid;

        let resources_url = format!(
            "https://data.cms.gov/data-api/v1/dataset/{}/resources",
            uuid
        );
        let resp = reqwest::get(&resources_url)
            .await?
            .json::<crate::cms::CmsResourceResponse>()
            .await?;

        Ok(resp)
    }

    /// Locates the download URL and metadata from the CMS API.
    /// Returns the first primary resource found.
    async fn locate_download_url(&self) -> Result<serde_json::Value> {
        let resources = self.get_resources().await;

        println!("Response: {:?}", resources);

        todo!("Implement")
    }

    /// Default implementation for checking updates using the API.
    async fn check_update(&self) -> Result<Option<NaiveDate>> {
        todo!("")
    }
}
