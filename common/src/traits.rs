use anyhow::Result;
use std::path::Path;

pub enum FileHash {
    Sha256(String),
    Sha512(String),
    Md5(String),
    RustHasher(String),
}

pub struct CmsMetadata {
    pub file_hash: FileHash,
}

pub struct CmsDataResult<T> {
    pub data: T,
    pub metadata: CmsMetadata,
}

/// Trait for defining a loader for a specific CMS dataset.
pub trait CmsDataLoader {
    /// The type of data this loader produces.
    /// Typically a tuple of Vectors, e.g., (Vec<Provider>, Vec<Address>)
    type Output;

    /// A unique key to identify this dataset (e.g., "pos_iqies").
    fn key(&self) -> &str;

    /// The source URL for the dataset.
    fn url(&self) -> &str;

    /// The version of the plugin
    /// Each time the *logic* of the plugin changes, this version should be incremented.
    /// This forces an update to the db, even if the file hash is the same.
    fn version(&self) -> usize;

    /// Orchestrates the loading process: download, extract, and parse.
    ///
    /// # Arguments
    /// * `data_dir` - The directory where raw files should be stored/cached.
    fn load(&self, data_dir: &Path) -> Result<CmsDataResult<Self::Output>>;
}
