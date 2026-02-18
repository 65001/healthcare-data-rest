use crate::traits::CmsDataLoader;
use anyhow::Result;
use async_trait::async_trait;
use chromiumoxide::Page;
use chrono::NaiveDate;
use sqlx::PgPool;
use tracing::{info, warn};

pub struct CmsHospitalLoader;

#[async_trait]
impl CmsDataLoader for CmsHospitalLoader {
    fn key(&self) -> &str {
        "cms_hospital_enrollments"
    }

    fn url(&self) -> &str {
        "https://data.cms.gov/provider-characteristics/hospitals-and-other-facilities/hospital-enrollments"
    }

    async fn check_update(&self, page: &Page) -> Result<Option<NaiveDate>> {
        info!("Checking for updates on CMS Hospital page...");
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        let selector = "#DatasetHero > div > div > div.DatasetHero__meta-container > div:nth-child(2) > span:nth-child(2) > div > div";

        // Wait for the element to appear
        let element = page.find_element(selector).await;

        if let Ok(el) = element {
            let text = el
                .inner_text()
                .await
                .map_err(|e| anyhow::anyhow!(e))?
                .unwrap_or_default();
            info!("Found date text: {}", text);
            // Text format expected: "February 2026"
            // Parse logic
            let date_str = text.trim();
            // Append " 1" to make it "February 2026 1" for parsing
            let parse_str = format!("{} 1", date_str);
            match NaiveDate::parse_from_str(&parse_str, "%B %Y %d") {
                Ok(date) => return Ok(Some(date)),
                Err(e) => {
                    warn!("Failed to parse date '{}': {}", date_str, e);
                    return Ok(None);
                }
            }
        }

        warn!("Could not find date element with selector: {}", selector);
        Ok(None)
    }

    async fn load(&self, page: &Page, _pool: &PgPool) -> Result<()> {
        info!("Starting download process...");

        let selector = "#DatasetPage > div.DatasetPage__inner > div.DataBar.container > div > div > div.DataBar__button-container.DataBar__button-container--link.download > button > span";
        let download_btn = page
            .find_element(selector)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;

        download_btn.click().await.map_err(|e| anyhow::anyhow!(e))?;
        info!("Clicked top level download button.");

        // Wait for modal
        // "We will also alter @[migrations] to drop the providers table and the address table."
        // "Select Latest Dataset Only and then click on Download Files"

        // Wait for modal options

        tokio::time::sleep(std::time::Duration::from_secs(3)).await;

        // Find "Latest Dataset Only" (likely a radio button or list item)
        // User said: "css selector for OptionsList and then select Latest Dataset Only"

        // Assuming there is a text search for "Latest Dataset Only"
        let modal_selector = "body > div.fade.DownloadModal.modal.show > div > div > div.DownloadModal__modal-body.modal-body > div.OptionsList > div:nth-child(2) > div > div:nth-child(2) > span:nth-child(1)";

        let download_options = page
            .find_element(modal_selector)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        download_options
            .click()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        info!("Clicked download options.");

        tokio::time::sleep(std::time::Duration::from_secs(7)).await;

        let final_download_selector = "body > div.fade.DownloadModal.modal.show > div > div > div.DownloadModal__modal-footer.modal-footer > div.ButtonSection.SingleButton > button";
        let final_download_btn = page
            .find_element(final_download_selector)
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        final_download_btn
            .click()
            .await
            .map_err(|e| anyhow::anyhow!(e))?;
        info!("Clicked final download button.");

        // At this point, we have declared our intent to download the file, but the we are seeing
        //  WARN chromiumoxide::browser: Browser was not closed manually, it will be killed automatically in the background

        Ok(())
    }
}
