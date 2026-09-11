use std::net::SocketAddr;
use backend::routes;
use clap::Parser;
use common::{args::DuckLakeArguments, state::AppState};
use dotenvy::dotenv;
use tower_http::{cors::CorsLayer, trace::TraceLayer};
use tracing::info;
use tracing_subscriber::{fmt, EnvFilter};

#[derive(Parser, Debug)]
#[command(
    name = "healthcare-backend",
    about = "Healthcare Price Transparency REST API & OpenAPI Engine"
)]
struct Cli {
    #[command(flatten)]
    lake: DuckLakeArguments,

    /// Host interface to bind to
    #[arg(long, env = "HOST", default_value = "0.0.0.0")]
    host: String,

    /// Port to listen on
    #[arg(short, long, env = "PORT", default_value = "3000")]
    port: u16,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv().ok();

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Cli::parse();

    info!("Initializing DuckLake database connection pool...");
    let state = AppState::new(args.lake.into())?;
    info!("DuckLake pool initialized successfully.");

    let app = routes::create_router(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = format!("{}:{}", args.host, args.port).parse()?;
    info!("Starting HTTP server listening on http://{}", addr);
    info!("Interactive Swagger UI available at: http://{}/swagger-ui/", addr);
    info!("OpenAPI JSON specification at: http://{}/api/v1/openapi.json", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
