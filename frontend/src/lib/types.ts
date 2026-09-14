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
  discovery_status: string
  checked_at: string
}

// GET /api/hospitals/:id flattens `Hospital`'s fields onto the response
// object (`#[serde(flatten)]`) alongside `latest_discovery`.
export type HospitalDetail = Hospital & { latest_discovery: MrfDiscovery | null }

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
