use anyhow::Result;
use async_trait::async_trait;
use std::path::Path;

pub enum FileHash {
    Sha256(String),
    Sha512(String),
    Md5(String),
    RustHasher(String),
}

pub struct CmsMetadata {
    pub file: Box<Path>,
    pub file_hash: FileHash,
}

pub struct CmsDataResult {
    pub metadata: CmsMetadata,
}

/// Trait for defining a loader for a specific CMS dataset.
#[async_trait]
pub trait CmsDataLoader: Send + Sync {
    /// A unique key to identify this dataset (e.g., "pos_iqies").
    fn key(&self) -> &str;

    /// The source URL for the dataset.
    fn url(&self) -> &str;

    /// The version of the plugin
    /// Each time the *logic* of the plugin changes, this version should be incremented.
    /// This forces an update to the db, even if the file hash is the same.
    fn version(&self) -> usize;

    async fn get_metadata(&self, data_dir: &Path) -> Result<CmsMetadata>;

    /// Orchestrates the loading process: download, extract, and parse.
    ///
    /// # Arguments
    /// * `file` - The opened file containing the dataset.
    /// * `pool` - The database connection pool.
    async fn load(&self, file: &Box<Path>, pool: &sqlx::PgPool) -> Result<()>;

    async fn cleanup(&self, metadata: &CmsMetadata) -> Result<()> {
        std::fs::remove_file(&metadata.file)?;
        Ok(())
    }
}
