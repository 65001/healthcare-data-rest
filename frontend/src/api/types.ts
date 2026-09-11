/**
 * Healthcare Price Transparency API Data Models
 * Synchronized with Rust `common::model` backend definitions.
 */

export interface HospitalSummary {
  hospital_id: number;
  hospital_name: string;
  hospital_address?: string | null;
  hospital_city?: string | null;
  hospital_state?: string | null;
  license_number?: string | null;
  last_updated_on?: string | null;
  version?: string | null;
}

export interface HospitalDetail {
  hospital_id: number;
  hospital_name: string;
  hospital_address?: string | null;
  hospital_city?: string | null;
  hospital_state?: string | null;
  hospital_location?: string | null;
  license_number?: string | null;
  last_updated_on?: string | null;
  version?: string | null;
  affirmation?: string | null;
  confirm_affirmation?: boolean | null;
  financial_aid_policy?: string | null;
  general_contract_provisions?: string | null;
  all_addresses: string[];
  all_locations: string[];
  mrf_hospital_name?: string | null;
  hpt_hospital_name?: string | null;
}

export interface StandardCharge {
  charge_id: number;
  charge_seq?: number | null;
  hospital_id: number;
  hospital_name?: string | null;
  description: string;
  gross_charge?: number | null;
  discounted_cash?: number | null;
  minimum?: number | null;
  maximum?: number | null;
  setting?: string | null;
  billing_class?: string | null;
  cpt?: string | null;
  hcpcs?: string | null;
  ms_drg?: string | null;
  rc?: string | null;
  cdm?: string | null;
  ndc?: string | null;
  payer_count?: number | null;
  distinct_payer_count?: number | null;
  avg_negotiated_rate?: number | null;
  min_negotiated_rate?: number | null;
  max_negotiated_rate?: number | null;
}

export interface StandardChargeDetail {
  detail_id: number;
  charge_id: number;
  hospital_id: number;
  hospital_name?: string | null;
  description: string;
  payer_name: string;
  plan_name?: string | null;
  standard_charge_dollar?: number | null;
  standard_charge_percentage?: number | null;
  estimated_amount?: number | null;
  methodology?: string | null;
  payer_group?: string | null;
  billing_class?: string | null;
  setting?: string | null;
  cpt?: string | null;
  hcpcs?: string | null;
  ms_drg?: string | null;
}

export interface PriceComparisonItem {
  hospital_id: number;
  hospital_name: string;
  hospital_city?: string | null;
  hospital_state?: string | null;
  description: string;
  payer_name?: string | null;
  plan_name?: string | null;
  negotiated_dollar?: number | null;
  discounted_cash?: number | null;
  gross_charge?: number | null;
  estimated_amount?: number | null;
  methodology?: string | null;
  setting?: string | null;
}

export interface DatasetStats {
  total_hospitals: number;
  states_covered: number;
  lake_version: string;
  is_lake_attached: boolean;
}

export type PaginationMeta =
  | { mode: 'exact'; total: number }
  | { mode: 'cursor'; has_more: boolean };

export type PaginatedResponse<T> = {
  items: T[];
  limit: number;
  offset: number;
} & PaginationMeta;

export interface ApiErrorResponse {
  error: string;
  message: string;
}

export interface HospitalSearchParams {
  q?: string;
  state?: string;
  city?: string;
  license?: string;
  limit?: number;
  offset?: number;
}

export interface ProcedureSearchParams {
  q?: string;
  code?: string;
  code_type?: 'cpt' | 'hcpcs' | 'ms_drg';
  hospital_id?: number;
  limit?: number;
  offset?: number;
}

export interface PriceComparisonParams {
  code: string;
  code_type?: 'cpt' | 'hcpcs' | 'ms_drg';
  state?: string;
  payer?: string;
  plan?: string;
  limit?: number;
}
