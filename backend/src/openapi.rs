//! Aggregates every route's `#[utoipa::path(...)]` annotation and every
//! `ToSchema` model into one `OpenApi` document, served as JSON at
//! `/api-docs/openapi.json` and browsable via Swagger UI at `/swagger-ui`
//! (see `routes::build_router`).
//!
//! Adding a new route: add it to `paths(...)` below and make sure every
//! type it references (request body, response body, query/path params)
//! derives `utoipa::ToSchema` (or `IntoParams` for query structs) and is
//! listed in `components(schemas(...))`.

use utoipa::OpenApi;

use crate::db::models::{Hospital, HospitalListItem, MrfDiscovery, NeedsEnrichmentItem};
use crate::error::ErrorResponse;
use crate::jobs::{Job, JobProgress, JobStatus};
use crate::routes::hospitals::{HospitalDetail, ListResponse, ManualEnrichmentRequest, NeedsEnrichmentResponse, Pagination};
use crate::routes::stats::{StatsResponse, StatusBreakdown};

#[derive(OpenApi)]
#[openapi(
    info(
        title = "CMS Hospital Price Transparency Compliance Checker API",
        description = "REST API over the four-stage ingest → enrich → discover → serve \
                       pipeline described in Architecture.md.",
        version = "0.1.0",
        license(name = "MIT"),
    ),
    paths(
        crate::routes::hospitals::list,
        crate::routes::hospitals::get_one,
        crate::routes::hospitals::needs_enrichment,
        crate::routes::hospitals::patch_enrichment,
        crate::routes::stats::get_stats,
        crate::routes::pipeline::trigger_ingest,
        crate::routes::pipeline::trigger_enrich,
        crate::routes::pipeline::trigger_discover,
        crate::routes::pipeline::get_job,
    ),
    components(schemas(
        Hospital,
        HospitalListItem,
        MrfDiscovery,
        NeedsEnrichmentItem,
        HospitalDetail,
        ListResponse,
        NeedsEnrichmentResponse,
        ManualEnrichmentRequest,
        Pagination,
        StatsResponse,
        StatusBreakdown,
        Job,
        JobStatus,
        JobProgress,
        ErrorResponse,
    )),
    tags(
        (name = "hospitals", description = "Hospital listing, detail, and manual enrichment"),
        (name = "stats", description = "Aggregate statistics"),
        (name = "pipeline", description = "Pipeline stage triggers and job status"),
    ),
)]
pub struct ApiDoc;
