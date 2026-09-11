use duckdb::Connection;
use thiserror::Error;

use crate::model::{
    DatasetStats, HospitalDetail, HospitalSearchParams, HospitalSummary,
    PaginatedResponse, PriceComparisonItem, PriceComparisonParams,
    ProcedureSearchParams, StandardCharge,
};

#[derive(Error, Debug)]
pub enum RepositoryError {
    #[error("DuckDB error: {0}")]
    DuckDb(#[from] duckdb::Error),
    #[error("Resource not found: {0}")]
    NotFound(String),
}

pub struct HealthcareRepository;

impl HealthcareRepository {
    /// Detects whether in-memory fast_hospitals table is available, otherwise falls back to view
    fn hospital_source_table(conn: &Connection) -> &'static str {
        if conn.query_row("SELECT 1 FROM fast_hospitals LIMIT 1;", [], |_| Ok(())).is_ok() {
            "fast_hospitals"
        } else {
            "current_hospitals"
        }
    }

    /// Search hospitals using in-memory cached table or convenience view
    pub fn search_hospitals(
        conn: &Connection,
        params: &HospitalSearchParams,
    ) -> Result<PaginatedResponse<HospitalSummary>, RepositoryError> {
        let table = Self::hospital_source_table(conn);
        let limit = params.limit.unwrap_or(20).clamp(1, 100);
        let offset = params.offset.unwrap_or(0);

        let mut where_clauses = Vec::new();
        let mut query_params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();

        if let Some(q) = params.q.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            let pattern = format!("%{}%", q);
            where_clauses.push("(hospital_name ILIKE ? OR enriched_hospital_address ILIKE ? OR enriched_hospital_city ILIKE ?)");
            query_params.push(Box::new(pattern.clone()));
            query_params.push(Box::new(pattern.clone()));
            query_params.push(Box::new(pattern));
        }

        if let Some(state) = params.state.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("hospital_state = ?");
            query_params.push(Box::new(state.to_uppercase()));
        }

        if let Some(city) = params.city.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("enriched_hospital_city ILIKE ?");
            query_params.push(Box::new(format!("%{}%", city)));
        }

        if let Some(license) = params.license.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("license_number ILIKE ?");
            query_params.push(Box::new(format!("%{}%", license)));
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_clauses.join(" AND "))
        };

        // 1. Total count
        let count_sql = format!("SELECT COUNT(*) FROM {} {}", table, where_sql);
        let total: i64 = {
            let mut stmt = conn.prepare(&count_sql)?;
            let slice: Vec<&dyn duckdb::ToSql> = query_params.iter().map(|b| b.as_ref()).collect();
            stmt.query_row(slice.as_slice(), |row| row.get(0)).unwrap_or(0)
        };

        // 2. Query page items
        let select_sql = format!(
            "SELECT hospital_id, hospital_name, enriched_hospital_address, enriched_hospital_city, \
                    hospital_state, license_number, last_updated_on, version \
             FROM {} {} \
             ORDER BY hospital_name ASC \
             LIMIT {} OFFSET {}",
            table, where_sql, limit, offset
        );

        let mut stmt = conn.prepare(&select_sql)?;
        let slice: Vec<&dyn duckdb::ToSql> = query_params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(slice.as_slice(), |row| {
            Ok(HospitalSummary {
                hospital_id: row.get(0)?,
                hospital_name: row.get(1)?,
                hospital_address: row.get(2)?,
                hospital_city: row.get(3)?,
                hospital_state: row.get(4)?,
                license_number: row.get(5)?,
                last_updated_on: row.get(6)?,
                version: row.get(7)?,
            })
        })?;

        let mut items = Vec::new();
        for item in rows {
            items.push(item?);
        }

        Ok(PaginatedResponse::exact(
            items,
            total as u64,
            limit,
            offset,
        ))
    }

    /// Retrieve full hospital detail by ID
    pub fn get_hospital_by_id(
        conn: &Connection,
        hospital_id: i64,
    ) -> Result<Option<HospitalDetail>, RepositoryError> {
        let table = Self::hospital_source_table(conn);
        let sql = format!(
            "SELECT hospital_id, hospital_name, enriched_hospital_address, enriched_hospital_city, \
                    hospital_state, license_number, last_updated_on, version, \
                    attestation, confirm_attestation, attester_name, \
                    financial_aid_policy, general_contract_provisions \
             FROM {} \
             WHERE hospital_id = ? \
             LIMIT 1;",
            table
        );

        let mut stmt = conn.prepare(&sql)?;
        let mut rows = stmt.query_map([hospital_id], |row| {
            Ok(HospitalDetail {
                hospital_id: row.get(0)?,
                hospital_name: row.get(1)?,
                hospital_address: row.get(2)?,
                hospital_city: row.get(3)?,
                hospital_state: row.get(4)?,
                license_number: row.get(5)?,
                last_updated_on: row.get(6)?,
                version: row.get(7)?,
                attestation: row.get(8)?,
                confirm_attestation: row.get(9)?,
                attester_name: row.get(10)?,
                financial_aid_policy: row.get(11)?,
                general_contract_provisions: row.get(12)?,
            })
        })?;

        if let Some(res) = rows.next() {
            Ok(Some(res?))
        } else {
            Ok(None)
        }
    }

    /// Search procedures and standard charges from `current_charges`
    pub fn search_procedures(
        conn: &Connection,
        params: &ProcedureSearchParams,
    ) -> Result<PaginatedResponse<StandardCharge>, RepositoryError> {
        let limit = params.limit.unwrap_or(20).clamp(1, 100);
        let offset = params.offset.unwrap_or(0);

        let mut where_clauses = Vec::new();
        let mut query_params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();

        if let Some(q) = params.q.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("description ILIKE ?");
            query_params.push(Box::new(format!("%{}%", q)));
        }

        if let Some(code) = params.code.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            match params.code_type.as_deref() {
                Some("cpt") => {
                    where_clauses.push("cpt = ?");
                    query_params.push(Box::new(code.to_string()));
                }
                Some("hcpcs") => {
                    where_clauses.push("hcpcs = ?");
                    query_params.push(Box::new(code.to_string()));
                }
                Some("ms_drg") => {
                    where_clauses.push("ms_drg = ?");
                    query_params.push(Box::new(code.to_string()));
                }
                _ => {
                    where_clauses.push("(cpt = ? OR hcpcs = ? OR ms_drg = ?)");
                    query_params.push(Box::new(code.to_string()));
                    query_params.push(Box::new(code.to_string()));
                    query_params.push(Box::new(code.to_string()));
                }
            }
        }

        if let Some(hospital_id) = params.hospital_id {
            where_clauses.push("hospital_id = ?");
            query_params.push(Box::new(hospital_id));
        }

        let where_sql = if where_clauses.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", where_clauses.join(" AND "))
        };

        // Fetch limit + 1 to determine cursor continuation without expensive full-table COUNT(*)
        let fetch_limit = limit + 1;
        let select_sql = format!(
            "SELECT charge_id, charge_seq, hospital_id, description, gross_charge, \
                    discounted_cash, minimum, maximum, setting, billing_class, \
                    cpt, hcpcs, ms_drg, rc, cdm, ndc, payer_count, distinct_payer_count, \
                    avg_negotiated_rate, min_negotiated_rate, max_negotiated_rate \
             FROM current_charges {} \
             ORDER BY description ASC \
             LIMIT {} OFFSET {}",
            where_sql, fetch_limit, offset
        );

        let mut stmt = conn.prepare(&select_sql)?;
        let slice: Vec<&dyn duckdb::ToSql> = query_params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(slice.as_slice(), |row| {
            Ok(StandardCharge {
                charge_id: row.get(0)?,
                charge_seq: row.get(1)?,
                hospital_id: row.get(2)?,
                hospital_name: None,
                description: row.get(3)?,
                gross_charge: row.get(4)?,
                discounted_cash: row.get(5)?,
                minimum: row.get(6)?,
                maximum: row.get(7)?,
                setting: row.get(8)?,
                billing_class: row.get(9)?,
                cpt: row.get(10)?,
                hcpcs: row.get(11)?,
                ms_drg: row.get(12)?,
                rc: row.get(13)?,
                cdm: row.get(14)?,
                ndc: row.get(15)?,
                payer_count: row.get(16)?,
                distinct_payer_count: row.get(17)?,
                avg_negotiated_rate: row.get(18)?,
                min_negotiated_rate: row.get(19)?,
                max_negotiated_rate: row.get(20)?,
            })
        })?;

        let mut items = Vec::new();
        for item in rows {
            items.push(item?);
        }

        let has_more = items.len() > limit as usize;
        if has_more {
            items.truncate(limit as usize);
        }

        Ok(PaginatedResponse::cursor(
            items,
            has_more,
            limit,
            offset,
        ))
    }

    /// Compare prices across hospitals and payers using `current_charge_details`
    pub fn compare_procedure_prices(
        conn: &Connection,
        params: &PriceComparisonParams,
    ) -> Result<Vec<PriceComparisonItem>, RepositoryError> {
        let table = Self::hospital_source_table(conn);
        let limit = params.limit.unwrap_or(50).clamp(1, 200);

        let mut where_clauses = Vec::new();
        let mut query_params: Vec<Box<dyn duckdb::ToSql>> = Vec::new();

        let code_clean = params.code.trim();
        match params.code_type.as_deref() {
            Some("cpt") => {
                where_clauses.push("d.cpt = ?");
                query_params.push(Box::new(code_clean.to_string()));
            }
            Some("hcpcs") => {
                where_clauses.push("d.hcpcs = ?");
                query_params.push(Box::new(code_clean.to_string()));
            }
            Some("ms_drg") => {
                where_clauses.push("d.ms_drg = ?");
                query_params.push(Box::new(code_clean.to_string()));
            }
            _ => {
                where_clauses.push("(d.cpt = ? OR d.hcpcs = ? OR d.ms_drg = ?)");
                query_params.push(Box::new(code_clean.to_string()));
                query_params.push(Box::new(code_clean.to_string()));
                query_params.push(Box::new(code_clean.to_string()));
            }
        }

        where_clauses.push("d.standard_charge_dollar > 0");

        if let Some(state) = params.state.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("d.hospital_state = ?");
            query_params.push(Box::new(state.to_uppercase()));
        }

        if let Some(payer) = params.payer.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("d.payer_name ILIKE ?");
            query_params.push(Box::new(format!("%{}%", payer)));
        }

        if let Some(plan) = params.plan.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
            where_clauses.push("d.plan_name ILIKE ?");
            query_params.push(Box::new(format!("%{}%", plan)));
        }

        let where_sql = format!("WHERE {}", where_clauses.join(" AND "));

        let sql = format!(
            "SELECT d.hospital_id, \
                    COALESCE(h.hospital_name, 'Facility #' || CAST(d.hospital_id AS VARCHAR)) AS hospital_name, \
                    h.enriched_hospital_city AS hospital_city, \
                    d.hospital_state, \
                    d.description, \
                    d.payer_name, \
                    d.plan_name, \
                    d.standard_charge_dollar, \
                    d.discounted_cash, \
                    d.gross_charge, \
                    d.estimated_amount, \
                    d.methodology, \
                    d.setting \
             FROM current_charge_details d \
             LEFT JOIN {} h ON d.hospital_id = h.hospital_id \
             {} \
             ORDER BY d.standard_charge_dollar ASC NULLS LAST \
             LIMIT {}",
            table, where_sql, limit
        );

        let mut stmt = conn.prepare(&sql)?;
        let slice: Vec<&dyn duckdb::ToSql> = query_params.iter().map(|b| b.as_ref()).collect();
        let rows = stmt.query_map(slice.as_slice(), |row| {
            Ok(PriceComparisonItem {
                hospital_id: row.get(0)?,
                hospital_name: row.get(1)?,
                hospital_city: row.get(2)?,
                hospital_state: row.get(3)?,
                description: row.get(4)?,
                payer_name: row.get(5)?,
                plan_name: row.get(6)?,
                negotiated_dollar: row.get(7)?,
                discounted_cash: row.get(8)?,
                gross_charge: row.get(9)?,
                estimated_amount: row.get(10)?,
                methodology: row.get(11)?,
                setting: row.get(12)?,
            })
        })?;

        let mut items = Vec::new();
        for item in rows {
            items.push(item?);
        }

        Ok(items)
    }

    /// Retrieve summary counts for dataset health and stats
    pub fn get_dataset_stats(conn: &Connection) -> Result<DatasetStats, RepositoryError> {
        let table = Self::hospital_source_table(conn);
        let sql = format!("SELECT COUNT(*), COUNT(DISTINCT hospital_state) FROM {};", table);
        let (total_hospitals, states_covered) = conn
            .query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?)))
            .unwrap_or((0, 0));

        // Ultra-fast reachability check without full table scan
        let is_lake_attached = conn
            .query_row("SELECT 1 FROM lake.hospitals LIMIT 1;", [], |_| Ok(()))
            .is_ok();

        Ok(DatasetStats {
            total_hospitals,
            states_covered,
            lake_version: "1.0".to_string(),
            is_lake_attached,
        })
    }
}
