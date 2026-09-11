use crate::lake::{DuckLakeConfig, DuckLakePool, create_lake_pool};

#[derive(Clone)]
pub struct AppState {
    pub pool: DuckLakePool,
}

impl AppState {
    pub fn new(config: DuckLakeConfig) -> Result<Self, r2d2::Error> {
        let pool = create_lake_pool(config)?;
        Ok(Self { pool })
    }
}
