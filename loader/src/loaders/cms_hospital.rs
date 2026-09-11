use crate::traits::{CmsPlugin, Plugin};
use anyhow::Result;
use async_trait::async_trait;
use chrono::NaiveDate;
use sqlx::PgPool;
use tracing::info;

pub struct CmsHospitalLoader;

#[async_trait]
impl CmsPlugin for CmsHospitalLoader {
    fn url(&self) -> &str {
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments"
    }
}

#[async_trait]
impl Plugin for CmsHospitalLoader {
    fn key(&self) -> &str {
        "cms_hospital_enrollments"
    }

    async fn check_update(&self) -> Result<Option<NaiveDate>> {
        info!("Checking for updates via CMS API...");
        CmsPlugin::check_update(self).await
    }

    async fn load(&self, _pool: &PgPool) -> Result<()> {
        info!("Starting load process for {}", self.key());

        let metadata = self.locate_download_url().await?;
        if let Some(url) = metadata.get("file_url").and_then(|u| u.as_str()) {
            info!("Found download URL: {}", url);
            // TODO: implement actual download and DB load
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_locate_download_url() {
        let loader = CmsHospitalLoader;
        let result = loader.locate_download_url().await;
        assert!(
            result.is_ok(),
            "Failed to locate download URL: {:?}",
            result.err()
        );
        let metadata = result.unwrap();
        println!("Metadata: {:?}", metadata);
        assert!(metadata.get("file_url").is_some());
    }
}
