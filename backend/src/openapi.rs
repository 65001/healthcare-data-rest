use utoipa::OpenApi;
use crate::handlers;

#[derive(OpenApi)]
#[openapi(
    paths(
        handlers::hospitals::search_hospitals,
        handlers::hospitals::get_hospital,
        handlers::procedures::search_procedures,
        handlers::prices::compare_prices,
        handlers::stats::get_stats,
    ),
    components(
        schemas(
            common::model::HospitalSummary,
            common::model::HospitalDetail,
            common::model::StandardCharge,
            common::model::StandardChargeDetail,
            common::model::PriceComparisonItem,
            common::model::DatasetStats,
            common::model::PaginationMeta,
            common::model::PaginatedResponse<common::model::HospitalSummary>,
            common::model::PaginatedResponse<common::model::StandardCharge>,
            common::model::ApiErrorResponse,
        )
    ),
    tags(
        (name = "Hospitals", description = "Hospital lookup and directory endpoints"),
        (name = "Procedures", description = "Standard charge and procedure search"),
        (name = "Pricing", description = "Cross-plan and cross-hospital price comparisons"),
        (name = "Stats", description = "DuckLake metadata and system statistics")
    ),
    info(
        title = "Healthcare Price Transparency API",
        version = "1.0.0",
        description = "High-performance REST API querying hospital machine-readable files and price transparency data powered by DuckDB & DuckLake."
    )
)]
pub struct ApiDoc;
