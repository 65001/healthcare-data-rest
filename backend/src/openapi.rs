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

use crate::db::models::{Hospital, HospitalListItem, HospitalOwner, MrfDiscovery, MrfMetadata, NeedsEnrichmentItem};
use crate::error::ErrorResponse;
use crate::jobs::{Job, JobProgress, JobStatus};
use crate::routes::hospitals::{
    HospitalDetail, HospitalOwnershipResponse, ListResponse, ManualEnrichmentRequest, ManualEnrichmentResponse,
    NeedsEnrichmentResponse, Pagination,
};
use crate::routes::pipeline::DiscoverRequest;
use crate::routes::stats::{StatsResponse, StatusBreakdown};
use crate::url_check::{MrfFormat, UrlCheckResult};

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
        crate::routes::hospitals::get_ownership,
        crate::routes::hospitals::needs_enrichment,
        crate::routes::hospitals::patch_enrichment,
        crate::routes::stats::get_stats,
        crate::routes::pipeline::trigger_ingest,
        crate::routes::pipeline::trigger_enrich,
        crate::routes::pipeline::trigger_discover,
        crate::routes::pipeline::trigger_ingest_ownership,
        crate::routes::pipeline::get_job,
        crate::routes::mrf_metadata::list_metadata,
        crate::routes::mrf_metadata::recheck_metadata,
    ),
    components(schemas(
        Hospital,
        HospitalListItem,
        HospitalOwner,
        MrfDiscovery,
        MrfMetadata,
        NeedsEnrichmentItem,
        HospitalDetail,
        HospitalOwnershipResponse,
        ListResponse,
        NeedsEnrichmentResponse,
        ManualEnrichmentRequest,
        ManualEnrichmentResponse,
        Pagination,
        StatsResponse,
        StatusBreakdown,
        Job,
        JobStatus,
        JobProgress,
        ErrorResponse,
        UrlCheckResult,
        MrfFormat,
        DiscoverRequest,
    )),
    tags(
        (name = "hospitals", description = "Hospital listing, detail, and manual enrichment"),
        (name = "stats", description = "Aggregate statistics"),
        (name = "pipeline", description = "Pipeline stage triggers and job status"),
        (name = "mrf-metadata", description = "Conditional-caching metadata and change history for discovered MRF URLs"),
    ),
)]
pub struct ApiDoc;
