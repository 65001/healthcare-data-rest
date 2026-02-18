use clap::Parser;
use common::args::PostgresSqlArguments;
use common::state::AppState;
use dotenvy::dotenv;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

mod engine;
mod loaders;
mod traits;

use crate::loaders::cms_hospital::CmsHospitalLoader;

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

    // Use local LoaderEngine
    let mut engine = engine::LoaderEngine::new(state.pool.clone()).await?;

    info!("Registering loaders...");
    engine.register(Box::new(CmsHospitalLoader));

    info!("Running engine...");
    engine.run().await?; // No data_dir argument needed for now

    Ok(())
}
