//! `GET /api/hospitals` and `GET /api/hospitals/:facility_id`, per
//! Architecture.md's REST API section.

use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::db::models::{Hospital, MrfDiscovery};
use crate::db::queries::{self, HospitalFilters};
use crate::error::ApiError;
use crate::routes::AppState;

#[derive(Debug, Deserialize)]
pub struct ListParams {
    pub state: Option<String>,
    pub hospital_type: Option<String>,
    pub discovery_status: Option<String>,
    #[serde(default, deserialize_with = "deserialize_bool_opt")]
    pub enriched: Option<bool>,
    pub page: Option<u32>,
    pub per_page: Option<u32>,
}

fn deserialize_bool_opt<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::Visitor;

    struct OptBoolVisitor;

    impl<'de> Visitor<'de> for OptBoolVisitor {
        type Value = Option<bool>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a boolean or boolean-like string (true/false/1/0)")
        }

        fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
            Ok(Some(v))
        }

        fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            match v.trim().to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" | "" => Ok(Some(true)),
                "false" | "0" | "no" => Ok(Some(false)),
                _ => Err(serde::de::Error::custom(format!("invalid boolean: {v}"))),
            }
        }

        fn visit_none<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            deserializer.deserialize_any(self)
        }

        fn visit_unit<E>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
    }

    deserializer.deserialize_option(OptBoolVisitor)
}

#[derive(Debug, Serialize)]
pub struct Pagination {
    pub page: u32,
    pub per_page: u32,
    pub total_count: i64,
    pub total_pages: i64,
}

#[derive(Debug, Serialize)]
pub struct ListResponse {
    pub data: Vec<crate::db::models::HospitalListItem>,
    pub pagination: Pagination,
}

pub async fn list(
    State(state): State<AppState>,
    Query(params): Query<ListParams>,
) -> Result<Json<ListResponse>, ApiError> {
    let page = params.page.unwrap_or(1).max(1);
    let per_page = params.per_page.unwrap_or(50).clamp(1, 200);

    let filters = HospitalFilters {
        state: params.state,
        hospital_type: params.hospital_type,
        discovery_status: params.discovery_status,
        enriched: params.enriched,
    };

    let (data, total_count) = queries::list_hospitals(&state.pool, &filters, page, per_page).await?;
    let total_pages = if total_count == 0 {
        0
    } else {
        (total_count + per_page as i64 - 1) / per_page as i64
    };

    Ok(Json(ListResponse {
        data,
        pagination: Pagination {
            page,
            per_page,
            total_count,
            total_pages,
        },
    }))
}

#[derive(Debug, Serialize)]
pub struct HospitalDetail {
    #[serde(flatten)]
    pub hospital: Hospital,
    pub latest_discovery: Option<MrfDiscovery>,
}

pub async fn get_one(
    State(state): State<AppState>,
    Path(facility_id): Path<String>,
) -> Result<Json<HospitalDetail>, ApiError> {
    let (hospital, latest_discovery) = queries::get_hospital(&state.pool, &facility_id)
        .await?
        .ok_or(ApiError::NotFound)?;

    Ok(Json(HospitalDetail {
        hospital,
        latest_discovery,
    }))
}
