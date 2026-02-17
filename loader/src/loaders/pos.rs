use anyhow::Result;
use common::model::{Address, Provider};
use common::traits::CmsDataLoader;
use csv::ReaderBuilder;
use std::fs::File;
use std::io::Cursor;
use std::path::Path;
use tracing::{error, info};

pub struct ProviderOfServicesLoader;

impl CmsDataLoader for ProviderOfServicesLoader {
    type Output = (Vec<Provider>, Vec<Address>);

    fn key(&self) -> &str {
        "pos_iqies"
    }

    fn url(&self) -> &str {
        "https://data.cms.gov/sites/default/files/dataset_zips/741d954b3c14b2299052175b44e895f4/Provider%20of%20Services%20File%20-%20Internet%20Quality%20Improvement%20and%20Evaluation%20System.zip"
    }

    fn load(&self, data_dir: &Path) -> Result<Self::Output> {
        let zip_path = data_dir.join(format!("{}.zip", self.key()));

        // 1. Download if not exists
        if !zip_path.exists() {
            info!("Downloading POS data to {:?}...", zip_path);
            let response = reqwest::blocking::get(self.url())?.error_for_status()?;
            let bytes = response.bytes()?;
            let mut file = File::create(&zip_path)?;
            std::io::copy(&mut Cursor::new(bytes), &mut file)?;
        } else {
            info!("Using existing POS zip at {:?}", zip_path);
        }

        // 2. Extract and Find CSV
        info!("Extracting zip...");
        let file = File::open(&zip_path)?;
        let mut archive = zip::ZipArchive::new(file)?;

        let mut csv_file_name = String::new();

        // Find the first CSV in the archive (simplified for now, logic might need robustness)
        for i in 0..archive.len() {
            let file = archive.by_index(i)?;
            if file.name().ends_with(".csv") && file.name().contains("POS_File") {
                csv_file_name = file.name().to_string();
                break;
            }
        }

        if csv_file_name.is_empty() {
            return Err(anyhow::anyhow!("No suitable CSV found in zip archive"));
        }

        info!("Found CSV: {}", csv_file_name);

        // Extract strictly the CSV file
        let mut csv_file_in_zip = archive.by_name(&csv_file_name)?;
        let extracted_csv_path = data_dir.join(&csv_file_name);
        // Create parent directories if needed (the csv might be in a subdir in the zip)
        if let Some(parent) = extracted_csv_path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut outfile = File::create(&extracted_csv_path)?;
        std::io::copy(&mut csv_file_in_zip, &mut outfile)?;

        // 3. Parse CSV
        info!("Parsing CSV from {:?}...", extracted_csv_path);
        let mut rdr = ReaderBuilder::new()
            .has_headers(true)
            .from_path(&extracted_csv_path)?;

        let mut providers = Vec::new();
        let mut addresses = Vec::new();

        for result in rdr.deserialize() {
            let row: common::model::ProviderOfServiceRow = result.map_err(|e| {
                error!("Error parsing record: {}", e);
                e
            })?;

            let provider: Provider = row.clone().into();
            let address: Address = row.into();

            providers.push(provider);
            addresses.push(address);
        }

        Ok((providers, addresses))
    }
}
