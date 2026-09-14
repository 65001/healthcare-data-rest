//! `GET /api/stats`, per Architecture.md's REST API section.

use std::collections::HashMap;

use axum::extract::State;
use axum::Json;
use serde::Serialize;

use crate::db::queries;
use crate::error::ApiError;
use crate::routes::AppState;

#[derive(Debug, Default, Serialize)]
pub struct StatusBreakdown {
    pub total: i64,
    pub mrf_found: i64,
    pub manifest_only: i64,
    pub no_manifest: i64,
    pub website_unreachable: i64,
    pub no_website: i64,
    pub not_checked: i64,
}

impl StatusBreakdown {
    fn add(&mut self, status: &str, count: i64) {
        self.total += count;
        match status {
            "mrf_found" => self.mrf_found += count,
            "manifest_only" => self.manifest_only += count,
            "no_manifest" => self.no_manifest += count,
            "website_unreachable" => self.website_unreachable += count,
            "no_website" => self.no_website += count,
            // Anything else (including the "not_checked" sentinel from
            // the query layer) falls in here.
            _ => self.not_checked += count,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub total_hospitals: i64,
    pub enriched: i64,
    pub discovery: StatusBreakdown,
    pub by_state: HashMap<String, StatusBreakdown>,
}

pub async fn get_stats(State(state): State<AppState>) -> Result<Json<StatsResponse>, ApiError> {
    let total_hospitals = queries::total_hospitals(&state.pool).await?;
    let enriched = queries::enriched_count(&state.pool).await?;
    let rows = queries::status_counts_by_state(&state.pool).await?;

    let mut discovery = StatusBreakdown::default();
    let mut by_state: HashMap<String, StatusBreakdown> = HashMap::new();

    for row in rows {
        discovery.add(&row.status, row.count);
        by_state.entry(row.state.clone()).or_default().add(&row.status, row.count);
    }

    Ok(Json(StatsResponse {
        total_hospitals,
        enriched,
        discovery,
        by_state,
    }))
}
