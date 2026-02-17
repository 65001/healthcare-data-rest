use clap::Parser;
use common::args::PostgresSqlArguments;
use common::state::AppState;
use dotenvy::dotenv;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

#[derive(Parser, Debug)]
struct Cli {
    #[command(flatten)]
    postgres: PostgresSqlArguments,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load .env file if it exists
    dotenv().ok();

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Cli::parse();

    info!("Initializing application state...");
    let state = AppState::new(args.postgres).await?;

    info!("Backend started successfully with DB connection pool.");

    Ok(())
}
