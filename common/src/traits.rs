use std::path::Path;
use anyhow::Result;

/// Trait for defining a loader for a specific CMS dataset.
pub trait CmsDataLoader {
    /// The type of data this loader produces.
    /// Typically a tuple of Vectors, e.g., (Vec<Provider>, Vec<Address>)
    type Output;

    /// A unique key to identify this dataset (e.g., "pos_iqies").
    fn key(&self) -> &str;

    /// The source URL for the dataset.
    fn url(&self) -> &str;

    /// Orchestrates the loading process: download, extract, and parse.
    /// 
    /// # Arguments
    /// * `data_dir` - The directory where raw files should be stored/cached.
    fn load(&self, data_dir: &Path) -> Result<Self::Output>;
}
