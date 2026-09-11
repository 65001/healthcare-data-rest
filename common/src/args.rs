use clap::Args;
use std::path::PathBuf;
use crate::lake::DuckLakeConfig;

#[derive(Args, Debug, Clone)]
pub struct DuckLakeArguments {
    /// Path to the catalog.duckdb holding convenience views
    #[arg(long, env = "DUCKDB_CATALOG_PATH", default_value = "lake/catalog.duckdb")]
    pub catalog_path: PathBuf,

    /// Path to metadata.ducklake DuckLake catalog file
    #[arg(long, env = "DUCKLAKE_METADATA_PATH", default_value = "lake/metadata.ducklake")]
    pub lake_metadata_path: PathBuf,

    /// Path to the data directory containing managed Parquet files
    #[arg(long, env = "DUCKLAKE_DATA_PATH", default_value = "lake/data")]
    pub data_path: PathBuf,

    /// Maximum number of pooled DuckDB connections
    #[arg(long, env = "DUCKDB_MAX_CONNECTIONS", default_value = "8")]
    pub max_connections: u32,
}

impl From<DuckLakeArguments> for DuckLakeConfig {
    fn from(args: DuckLakeArguments) -> Self {
        Self {
            catalog_path: args.catalog_path,
            lake_metadata_path: Some(args.lake_metadata_path),
            data_path: Some(args.data_path),
            max_connections: args.max_connections,
            load_ducklake_extension: true,
        }
    }
}
