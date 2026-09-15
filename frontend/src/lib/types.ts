// Mirrors backend/src/db/models.rs and the request/response structs in
// backend/src/routes/*.rs. Keep these in sync with the Rust side by hand —
// there's no shared schema generation (yet); /api-docs/openapi.json is the
// source of truth if this drifts.

export interface Hospital {
  facility_id: string
  facility_name: string
  address: string
  city: string
  state: string
  zip_code: string
  county_name: string | null
  phone_number: string | null
  hospital_type: string
  hospital_ownership: string
  emergency_services: boolean
  overall_rating: number | null

  latitude: number | null
  longitude: number | null
  formatted_address: string | null
  website_url: string | null
  geo_provider: string | null
  geo_confidence: number | null
  enriched_at: string | null

  ingested_at: string
  updated_at: string
}

export interface HospitalListItem {
  facility_id: string
  facility_name: string
  address: string
  city: string
  state: string
  zip_code: string
  hospital_type: string
  latitude: number | null
  longitude: number | null
  website_url: string | null
  discovery_status: string | null
}

export interface MrfDiscovery {
  id: string
  facility_id: string
  website_url: string | null
  website_reachable: boolean | null
  cms_hpt_txt_found: boolean | null
  cms_hpt_txt_url: string | null
  mrf_urls: string | null
  // From the manifest entry's contact-name/contact-email fields — part of
  // CMS's cms-hpt.txt spec, not persisted until this column was added
  // (2026-09-15).
  contact_name: string | null
  contact_email: string | null
  discovery_status: string
  checked_at: string
}

// GET /api/hospitals/:id flattens `Hospital`'s fields onto the response
// object (`#[serde(flatten)]`) alongside `latest_discovery`.
export type HospitalDetail = Hospital & { latest_discovery: MrfDiscovery | null }

// One row of GET /api/mrf-discoveries/:id/metadata — a single
// conditional-caching probe of one MRF URL. `change_detection_method` is
// null only when the URL was unreachable that check; `changed_from_previous`
// is additionally null for the first ("baseline") row, since there's
// nothing yet to compare against.
export interface MrfMetadata {
  id: string
  mrf_discovery_id: string
  mrf_url: string
  sha1_hash: string | null
  last_modified: string | null
  etag: string | null
  cache_control: string | null
  content_length: number | null
  content_type: string | null
  schema_valid: boolean | null
  change_detection_method: 'baseline' | 'etag' | 'last_modified' | 'sha1' | null
  changed_from_previous: boolean | null
  checked_at: string
}

// One row of GET /api/hospitals/:id/ownership's `owners` array — a
// disclosed owner or controller from CMS's PECOS ownership data.
// `owner_type` is `"O"` (organization) or `"I"` (individual); the
// corresponding name field is set accordingly.
export interface HospitalOwner {
  enrollment_id: string
  owner_type: string | null
  owner_role_text: string | null
  owner_organization_name: string | null
  owner_person_name: string | null
  percentage_ownership: string | null
}

// GET /api/hospitals/:id/ownership. `no_stake_expected` is the backend's
// call (backend/src/db/queries.rs's NO_PECOS_STAKE_OWNERSHIP_CATEGORIES)
// on whether an empty `owners` list is normal for this hospital's
// ownership category (e.g. Department of Defense) rather than a sign
// ownership data needs ingesting — the frontend has no copy of that list
// and shouldn't grow one; always defer to this field.
export interface HospitalOwnershipResponse {
  owners: HospitalOwner[]
  no_stake_expected: boolean
}

export interface NeedsEnrichmentItem {
  facility_id: string
  facility_name: string
  address: string
  city: string
  state: string
  zip_code: string
  missing_coordinates: boolean
  missing_website: boolean
}

export interface Pagination {
  page: number
  per_page: number
  total_count: number
  total_pages: number
}

export interface ListResponse {
  data: HospitalListItem[]
  pagination: Pagination
}

export interface NeedsEnrichmentResponse {
  data: NeedsEnrichmentItem[]
  pagination: Pagination
}

export interface ListParams {
  state?: string
  hospital_type?: string
  discovery_status?: string
  enriched?: boolean
  page?: number
  per_page?: number
}

export interface NeedsEnrichmentParams {
  missing?: 'coordinates' | 'website'
  state?: string
  page?: number
  per_page?: number
}

// Body for POST /api/pipeline/discover. Unset network_manifest_url runs
// the per-hospital-website mode (probe every hospital with a website on
// its own site); set it to run the network-manifest mode instead.
export interface DiscoverRequest {
  network_manifest_url?: string
  state?: string
  facility_ids?: string[]
}

export interface ManualEnrichmentRequest {
  latitude?: number | null
  longitude?: number | null
  website_url?: string | null
}

export interface StatusBreakdown {
  total: number
  mrf_found: number
  manifest_only: number
  no_manifest: number
  website_unreachable: number
  no_website: number
  not_checked: number
}

export interface StatsResponse {
  total_hospitals: number
  enriched: number
  discovery: StatusBreakdown
  by_state: Record<string, StatusBreakdown>
}

export type JobStatus = 'pending' | 'running' | 'completed' | 'failed'

export interface JobProgress {
  total: number
  completed: number
  failed: number
}

export interface Job {
  id: string
  stage: string
  status: JobStatus
  progress: JobProgress
  started_at: string
  finished_at: string | null
  error: string | null
}

export interface ApiErrorBody {
  error: string
}
