use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SlugResponse {
    pub data: SlugData,
}

#[derive(Debug, Deserialize)]
pub struct SlugData {
    pub uuid: String,
    pub name: String,
    pub current_dataset: CurrentDataset,
}

#[derive(Debug, Deserialize)]
pub struct CurrentDataset {
    pub uuid: String,
    pub name: String,
    pub last_modified_date: Option<String>,
    pub version: String,
}

#[derive(Debug, Deserialize)]
pub struct CmsResource {
    pub r#type: String,
    pub title: String,
    pub file_mime: String,
    pub file_url: String,
}

#[derive(Debug, Deserialize)]
pub struct CmsResourceResponse {
    pub data: Vec<CmsResource>,
}
