mod loaders;

use crate::loaders::pos::ProviderOfServicesLoader;
use common::traits::CmsDataLoader;
use std::path::Path;

fn main() -> anyhow::Result<()> {
    let loader = ProviderOfServicesLoader;
    let data_dir = Path::new("data");
    
    // Ensure data directory exists
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }

    println!("Starting load for key: {}", loader.key());
    let (providers, addresses) = loader.load(data_dir)?;
    
    println!("Successfully loaded {} providers and {} addresses.", providers.len(), addresses.len());
    
    // Print a sample to verify fields are populated
    if let Some(p) = providers.first() {
        println!("Sample Provider: {:?}", p);
    }
     if let Some(a) = addresses.first() {
        println!("Sample Address: {:?}", a);
    }

    Ok(())
}

