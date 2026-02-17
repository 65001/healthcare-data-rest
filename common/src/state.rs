use crate::args::PostgresSqlArguments;
use anyhow::Result;
use sqlx::postgres::{PgPool, PgPoolOptions};

#[derive(Clone, Debug)]
pub struct AppState {
    pub pool: PgPool,
}

impl AppState {
    pub async fn new(args: PostgresSqlArguments) -> Result<Self> {
        let connection_string = args.get_connection_string();
        let pool = PgPoolOptions::new()
            .max_connections(args.max_connections)
            .connect(&connection_string)
            .await?;

        Ok(Self { pool })
    }
}
