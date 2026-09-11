use std::path::PathBuf;
use duckdb::{AccessMode, Config, Connection, Error as DuckDbError};
use r2d2::ManageConnection;
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct DuckLakeConfig {
    /// Path to catalog.duckdb holding the convenience views
    pub catalog_path: PathBuf,
    /// Path to metadata.ducklake snapshot registry
    pub lake_metadata_path: Option<PathBuf>,
    /// Path to data/ directory holding Parquet files
    pub data_path: Option<PathBuf>,
    /// Maximum number of pooled connections
    pub max_connections: u32,
    /// Whether to attempt loading the ducklake extension
    pub load_ducklake_extension: bool,
}

impl Default for DuckLakeConfig {
    fn default() -> Self {
        Self {
            catalog_path: PathBuf::from("lake/catalog.duckdb"),
            lake_metadata_path: Some(PathBuf::from("lake/metadata.ducklake")),
            data_path: Some(PathBuf::from("lake/data")),
            max_connections: 8,
            load_ducklake_extension: true,
        }
    }
}

/// Custom r2d2 connection manager for DuckDB with DuckLake support
#[derive(Debug, Clone)]
pub struct DuckLakeConnectionManager {
    config: DuckLakeConfig,
}

impl DuckLakeConnectionManager {
    pub fn new(config: DuckLakeConfig) -> Self {
        Self { config }
    }

    fn initialize_connection(&self, conn: &Connection) -> Result<(), DuckDbError> {
        // Optimize connection for analytical read queries
        let _ = conn.execute_batch("SET threads TO 4; SET enable_progress_bar = false;");

        if self.config.load_ducklake_extension {
            // First try loading without network round-trip; install only if not found
            let load_res = conn.execute_batch("LOAD ducklake;");
            if load_res.is_err() {
                let _ = conn.execute_batch("INSTALL ducklake; LOAD ducklake;");
            }

            if let (Some(lake_meta), Some(data_path)) = (
                &self.config.lake_metadata_path,
                &self.config.data_path,
            ) {
                if lake_meta.exists() {
                    let lake_str = lake_meta.to_string_lossy().replace('\\', "/");
                    let data_str = data_path.to_string_lossy().replace('\\', "/");
                    let attach_sql = format!(
                        "ATTACH 'ducklake:{}' AS lake (DATA_PATH '{}', OVERRIDE_DATA_PATH true, READ_ONLY);",
                        lake_str, data_str
                    );
                    match conn.execute_batch(&attach_sql) {
                        Ok(_) => {
                            info!("Attached DuckLake catalog 'lake' from {}", lake_str);
                            // Pre-materialize the small 7,916 hospital table in-memory for sub-millisecond searches
                            let _ = conn.execute_batch(
                                "CREATE TEMP TABLE IF NOT EXISTS fast_hospitals AS SELECT * FROM current_hospitals;"
                            );
                        }
                        Err(e) => warn!("Failed to attach DuckLake catalog: {}", e),
                    }
                } else {
                    debug!("DuckLake metadata path does not exist yet: {:?}", lake_meta);
                }
            }
        }

        Ok(())
    }
}

impl ManageConnection for DuckLakeConnectionManager {
    type Connection = Connection;
    type Error = DuckDbError;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        let conn = if self.config.catalog_path.exists() {
            let config = Config::default()
                .access_mode(AccessMode::ReadOnly)?;
            Connection::open_with_flags(&self.config.catalog_path, config)?
        } else {
            warn!(
                "Catalog file {:?} not found, opening in-memory fallback DuckDB session",
                self.config.catalog_path
            );
            Connection::open_in_memory()?
        };

        self.initialize_connection(&conn)?;
        Ok(conn)
    }

    fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        conn.execute_batch("SELECT 1;")
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}

pub type DuckLakePool = r2d2::Pool<DuckLakeConnectionManager>;

pub fn create_lake_pool(config: DuckLakeConfig) -> Result<DuckLakePool, r2d2::Error> {
    let manager = DuckLakeConnectionManager::new(config.clone());
    r2d2::Pool::builder()
        .max_size(config.max_connections)
        .build(manager)
}
