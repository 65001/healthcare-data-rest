use clap::Parser;
use common::args::PostgresSqlArguments;
use common::state::AppState;
use dotenvy::dotenv;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

mod loaders;
use crate::loaders::pos::ProviderOfServicesLoader;
use common::traits::CmsDataLoader;
use std::path::Path;

#[derive(Parser, Debug)]
struct Cli {
    #[command(flatten)]
    postgres: PostgresSqlArguments,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if it exists
    dotenv().ok();

    // Initialize logging with default level INFO if RUST_LOG is not set
    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Cli::parse();

    info!("Initializing application state...");
    let state = AppState::new(args.postgres).await?;

    info!("Running database migrations...");
    sqlx::migrate!("../migrations").run(&state.pool).await?;
    info!("Migrations completed successfully.");

    let _state = state; // Keep state alive if needed later, though used above

    let loader = ProviderOfServicesLoader;
    let data_dir = Path::new("data");

    // Ensure data directory exists
    if !data_dir.exists() {
        std::fs::create_dir_all(data_dir)?;
    }

    info!("Starting load for key: {}", loader.key());
    let result = loader.load(data_dir)?;
    let (providers, addresses) = result.data;

    if let common::traits::FileHash::Sha256(hash) = result.metadata.file_hash {
        info!("Metadata File Hash: {}", hash);
    }

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
