//! Error type for the `cms-ingest` crate.

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("HTTP request to CMS datastore failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("CMS datastore returned HTTP {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },

    #[error("failed to parse CMS datastore response as JSON: {0}")]
    InvalidJson(#[from] serde_json::Error),

    #[error("record {facility_id} is missing required field `{field}`")]
    MissingField {
        facility_id: String,
        field: &'static str,
    },

    #[error("record {facility_id} has an unparseable value for `{field}`: {value:?}")]
    InvalidFieldValue {
        facility_id: String,
        field: &'static str,
        value: String,
    },
}

impl IngestError {
    /// Whether retrying the same request might succeed (server hiccup, rate
    /// limit, etc.) as opposed to a permanent client-side problem.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::Http(_) | Self::UnexpectedStatus { status: 429..=599, .. }
        )
    }
}
