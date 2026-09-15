import type {
  ApiErrorBody,
  DiscoverRequest,
  Hospital,
  HospitalDetail,
  HospitalOwnershipResponse,
  Job,
  ListParams,
  ListResponse,
  ManualEnrichmentRequest,
  MrfMetadata,
  NeedsEnrichmentParams,
  NeedsEnrichmentResponse,
  StatsResponse,
} from './types'

export class ApiError extends Error {
  status: number
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    ...init,
    headers: {
      ...(init?.body ? { 'Content-Type': 'application/json' } : {}),
      ...init?.headers,
    },
  })

  if (!res.ok) {
    let message = res.statusText
    try {
      const body = (await res.json()) as ApiErrorBody
      if (body?.error) message = body.error
    } catch {
      // Non-JSON error body (e.g. a proxy failure) — fall back to statusText.
    }
    throw new ApiError(res.status, message)
  }

  // 202 Accepted / 204 No Content etc. may have no body.
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

function buildQuery(params: object): string {
  const search = new URLSearchParams()
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== '') search.set(key, String(value))
  }
  const qs = search.toString()
  return qs ? `?${qs}` : ''
}

export const api = {
  listHospitals(params: ListParams): Promise<ListResponse> {
    return request(`/api/hospitals${buildQuery(params)}`)
  },

  getHospital(facilityId: string): Promise<HospitalDetail> {
    return request(`/api/hospitals/${encodeURIComponent(facilityId)}`)
  },

  getHospitalOwnership(facilityId: string): Promise<HospitalOwnershipResponse> {
    return request(`/api/hospitals/${encodeURIComponent(facilityId)}/ownership`)
  },

  needsEnrichment(params: NeedsEnrichmentParams): Promise<NeedsEnrichmentResponse> {
    return request(`/api/hospitals/needs-enrichment${buildQuery(params)}`)
  },

  patchEnrichment(facilityId: string, body: ManualEnrichmentRequest): Promise<Hospital> {
    return request(`/api/hospitals/${encodeURIComponent(facilityId)}/enrichment`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    })
  },

  getStats(): Promise<StatsResponse> {
    return request('/api/stats')
  },

  triggerIngest(): Promise<Job> {
    return request('/api/pipeline/ingest', { method: 'POST' })
  },

  triggerEnrich(retryIncomplete: boolean): Promise<Job> {
    return request(`/api/pipeline/enrich${buildQuery({ retry_incomplete: retryIncomplete })}`, {
      method: 'POST',
    })
  },

  triggerIngestOwnership(): Promise<Job> {
    return request('/api/pipeline/ingest-ownership', { method: 'POST' })
  },

  triggerDiscover(body: DiscoverRequest): Promise<Job> {
    return request('/api/pipeline/discover', {
      method: 'POST',
      body: JSON.stringify(body),
    })
  },

  getJob(id: string): Promise<Job> {
    return request(`/api/pipeline/jobs/${encodeURIComponent(id)}`)
  },

  getMrfMetadata(discoveryId: string): Promise<MrfMetadata[]> {
    return request(`/api/mrf-discoveries/${encodeURIComponent(discoveryId)}/metadata`)
  },

  recheckMrfMetadata(discoveryId: string): Promise<Job> {
    return request(`/api/mrf-discoveries/${encodeURIComponent(discoveryId)}/metadata/recheck`, {
      method: 'POST',
    })
  },
}
