mod config;
mod db;
mod enrich_store;
mod error;
mod jobs;
mod mrf_match;
mod mrf_metadata;
mod openapi;
mod routes;
mod url_check;

use std::sync::Arc;

use tracing_subscriber::EnvFilter;

use config::Config;
use jobs::JobTracker;
use routes::AppState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Picks up a `.env` file in the working directory if present; falls
    // back silently to real environment variables / built-in defaults
    // otherwise (see config.rs).
    dotenvy::dotenv_override().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let config = Config::from_env();
    tracing::info!(database_url = %config.database_url, "connecting to database");

    let pool = db::connect(&config.database_url).await?;
    let jobs = JobTracker::new();
    let http = url_check::http_client()?;

    let host = config.server_host.clone();
    let port = config.server_port;

    let state = AppState {
        pool,
        config: Arc::new(config),
        jobs,
        http,
    };

    let app = routes::build_router(state);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "backend listening");

    axum::serve(listener, app).await?;

    Ok(())
}
