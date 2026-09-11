import type {
  DatasetStats,
  HospitalDetail,
  HospitalSearchParams,
  HospitalSummary,
  PaginatedResponse,
  PriceComparisonItem,
  PriceComparisonParams,
  ProcedureSearchParams,
  StandardCharge,
} from './types';

const BASE_URL = '/api/v1';

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
  }
}

async function request<T>(endpoint: string, params?: Record<string, any>): Promise<T> {
  const url = new URL(BASE_URL + endpoint, window.location.origin);
  if (params) {
    Object.entries(params).forEach(([key, val]) => {
      if (val !== undefined && val !== null && val !== '') {
        url.searchParams.append(key, String(val));
      }
    });
  }

  const response = await fetch(url.toString(), {
    headers: {
      Accept: 'application/json',
    },
  });

  if (!response.ok) {
    let errorMsg = `Request failed with status ${response.status}`;
    try {
      const errJson = await response.json();
      if (errJson && errJson.message) {
        errorMsg = errJson.message;
      }
    } catch {
      // ignore json parse errors for status responses
    }
    throw new ApiError(response.status, errorMsg);
  }

  return response.json() as Promise<T>;
}

export const api = {
  getHospitals: (params?: HospitalSearchParams) =>
    request<PaginatedResponse<HospitalSummary>>('/hospitals', params),

  getHospitalById: (id: number) =>
    request<HospitalDetail>(`/hospitals/${id}`),

  getProcedures: (params?: ProcedureSearchParams) =>
    request<PaginatedResponse<StandardCharge>>('/procedures', params),

  comparePrices: (params: PriceComparisonParams) =>
    request<PriceComparisonItem[]>('/prices/compare', params),

  getStats: () =>
    request<DatasetStats>('/stats'),
};
