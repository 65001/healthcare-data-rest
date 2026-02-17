mod loaders;

use crate::loaders::pos::ProviderOfServicesLoader;
use common::traits::CmsDataLoader;
use std::path::Path;

use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

fn main() -> anyhow::Result<()> {
    // Initialize logging with default level INFO if RUST_LOG is not set
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let loader = ProviderOfServicesLoader;
    let data_dir = Path::new("data");

    // Ensure data directory exists
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }

    info!("Starting load for key: {}", loader.key());
    let (providers, addresses) = loader.load(data_dir)?;

    info!(
        "Successfully loaded {} providers and {} addresses.",
        providers.len(),
        addresses.len()
    );

    // Print a sample to verify fields are populated
    if let Some(p) = providers.first() {
        info!("Sample Provider: {:?}", p);
    }
    if let Some(a) = addresses.first() {
        info!("Sample Address: {:?}", a);
    }

    Ok(())
}
