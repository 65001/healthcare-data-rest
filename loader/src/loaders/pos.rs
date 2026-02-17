use anyhow::Result;
use async_trait::async_trait;
use common::model::{Address, Provider};
use common::traits::{CmsDataLoader, CmsMetadata, FileHash};
use csv::ReaderBuilder;
use sha2::{Digest, Sha256};
use std::fs::File;
use std::io::{Cursor, Read, Seek};
use std::path::Path;
use tracing::{error, info};

pub struct ProviderOfServicesLoader;

#[async_trait]
impl CmsDataLoader for ProviderOfServicesLoader {
    fn key(&self) -> &str {
        "pos_iqies"
    }

    fn url(&self) -> &str {
        "https://data.cms.gov/sites/default/files/dataset_zips/741d954b3c14b2299052175b44e895f4/Provider%20of%20Services%20File%20-%20Internet%20Quality%20Improvement%20and%20Evaluation%20System.zip"
    }

    fn version(&self) -> usize {
        2
    }

    async fn get_metadata(&self, data_dir: &Path) -> Result<CmsMetadata> {
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

        // Calculate file hash
        let mut file = File::open(&zip_path)?;
        let mut hasher = Sha256::new();

        let mut buffer = [0; 1024];
        loop {
            let count = file.read(&mut buffer)?;
            if count == 0 {
                break;
            }
            hasher.update(&buffer[..count]);
        }
        let hash_result = hasher.finalize();
        let file_hash_string = hex::encode(hash_result);
        info!("File hash (SHA256): {}", file_hash_string);

        Ok(CmsMetadata {
            file: zip_path.into(),
            file_hash: FileHash::Sha256(file_hash_string),
        })
    }

    async fn load(&self, file: &Box<Path>, pool: &sqlx::PgPool) -> Result<()> {
        let mut file = File::open(file)?;

        info!("Extracting zip from stream...");
        // Rewind the file just in case
        file.rewind()?;

        let mut archive = zip::ZipArchive::new(file)?;

        let mut csv_file_name = String::new();

        // Find the first CSV in the archive
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

        // Scope the borrow of archive
        let (providers, unique_addresses, provider_to_addr_map) = {
            let csv_file = archive.by_name(&csv_file_name)?;

            // Stream parse
            info!("Parsing CSV stream...");
            let mut rdr = ReaderBuilder::new().has_headers(true).from_reader(csv_file);

            let mut providers = Vec::new();
            let mut unique_addresses = Vec::new();
            let mut address_to_index = std::collections::HashMap::new();
            let mut provider_to_addr_map = Vec::new();

            for result in rdr.deserialize() {
                let row: common::model::ProviderOfServiceRow = result.map_err(|e| {
                    error!("Error parsing record: {}", e);
                    e
                })?;

                let provider: Provider = row.clone().into();

                // Identity for deduplication matches the DB unique index:
                // COALESCE(street_address, ''), COALESCE(city, ''), COALESCE(state_code, ''), COALESCE(zip_code, '')
                let identity = (
                    row.street_address.as_deref().unwrap_or("").to_string(),
                    row.city.as_deref().unwrap_or("").to_string(),
                    row.state_code.as_deref().unwrap_or("").to_string(),
                    row.zip_code.as_deref().unwrap_or("").to_string(),
                );

                let addr_idx = *address_to_index.entry(identity).or_insert_with(|| {
                    let idx = unique_addresses.len();
                    unique_addresses.push(Address::from(row));
                    idx
                });

                providers.push(provider);
                provider_to_addr_map.push(addr_idx);
            }
            (providers, unique_addresses, provider_to_addr_map)
        };

        // 4. Insert Data
        if !unique_addresses.is_empty() {
            info!(
                "Inserting {} unique addresses (from {} total rows) for '{}'...",
                unique_addresses.len(),
                providers.len(),
                self.key()
            );
            let address_ids = common::db::bulk_insert_addresses(pool, &unique_addresses).await?;

            let mut providers = providers;
            info!("Linking {} providers to addresses...", providers.len());
            for (p_idx, addr_idx) in provider_to_addr_map.into_iter().enumerate() {
                if addr_idx < address_ids.len() {
                    providers[p_idx].address_id = Some(address_ids[addr_idx]);
                }
            }

            info!(
                "Inserting {} providers for '{}'...",
                providers.len(),
                self.key()
            );
            common::db::bulk_insert_providers(pool, &providers).await?;
        }

        Ok(())
    }
}
