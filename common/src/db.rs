use crate::model::{Address, Provider};
use anyhow::Result;
use sqlx::{Postgres, QueryBuilder, Row, postgres::PgPool};

pub async fn bulk_insert_addresses(pool: &PgPool, addresses: &[Address]) -> Result<Vec<i32>> {
    let mut tx = pool.begin().await?;
    let mut all_ids = Vec::with_capacity(addresses.len());

    // PostgreSQL has a limit of 65535 parameters per query.
    // We have 12 columns, batch size of 1000 is safe.
    const BATCH_SIZE: usize = 1000;

    for chunk in addresses.chunks(BATCH_SIZE) {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO addresses (
                street_address, city, state_code, zip_code,
                ssa_county_code, ssa_state_code, state_region_code, region_code,
                fips_state_code, fips_county_code, cbsa_code, cbsa_urban_rural_indicator
            ) ",
        );

        query_builder.push_values(chunk, |mut b, addr| {
            b.push_bind(&addr.street_address)
                .push_bind(&addr.city)
                .push_bind(&addr.state_code)
                .push_bind(&addr.zip_code)
                .push_bind(&addr.ssa_county_code)
                .push_bind(&addr.ssa_state_code)
                .push_bind(&addr.state_region_code)
                .push_bind(&addr.region_code)
                .push_bind(&addr.fips_state_code)
                .push_bind(&addr.fips_county_code)
                .push_bind(&addr.cbsa_code)
                .push_bind(&addr.cbsa_urban_rural_indicator);
        });

        // Use the functional index expressions for ON CONFLICT
        // We do a dummy update to ensure RETURNING id includes existing rows
        query_builder.push(
            " ON CONFLICT (
                COALESCE(street_address, ''), 
                COALESCE(city, ''), 
                COALESCE(state_code, ''), 
                COALESCE(zip_code, '')
            ) DO UPDATE SET street_address = EXCLUDED.street_address",
        );

        query_builder.push(" RETURNING id");

        let rows = query_builder.build().fetch_all(&mut *tx).await?;
        for row in rows {
            all_ids.push(row.get(0));
        }
    }

    tx.commit().await?;
    Ok(all_ids)
}

pub async fn bulk_insert_providers(pool: &PgPool, providers: &[Provider]) -> Result<()> {
    let mut tx = pool.begin().await?;

    const BATCH_SIZE: usize = 1000;

    for chunk in providers.chunks(BATCH_SIZE) {
        let mut query_builder: QueryBuilder<Postgres> = QueryBuilder::new(
            "INSERT INTO providers (
                cms_certification_number, name, provider_subtype_id, medicaid_vendor_number, provider_type_id,
                address_id, original_participation_date, certification_date, termination_expiration_date,
                change_of_ownership_date, asc_begin_service_date, processing_date, phone_number, fax_number,
                accreditation_type_code, intermediary_carrier_code, acceptable_poc_switch, fiscal_year_end_date,
                compliance_status_code, certification_action_type_code, bed_count, certified_bed_count,
                hospice_bed_count, aids_bed_count, alzheimer_bed_count, dialysis_bed_count,
                disabled_children_bed_count, head_trauma_bed_count, huntington_disease_bed_count,
                medicare_medicaid_snf_bed_count, medicare_snf_bed_count, rehab_bed_count, ventilator_bed_count,
                lpn_lvn_count, rn_count, employee_count, change_of_ownership_switch, hospital_based_switch,
                multi_owned_facility_switch, clia_lab_number, facility_category_code, ownership_type_code
            ) ",
        );

        query_builder.push_values(chunk, |mut b, p| {
            b.push_bind(&p.cms_certification_number)
                .push_bind(&p.name)
                .push_bind(p.provider_subtype_id)
                .push_bind(&p.medicaid_vendor_number)
                .push_bind(p.provider_type_id)
                .push_bind(p.address_id)
                .push_bind(p.original_participation_date)
                .push_bind(p.certification_date)
                .push_bind(p.termination_expiration_date)
                .push_bind(p.change_of_ownership_date)
                .push_bind(p.asc_begin_service_date)
                .push_bind(p.processing_date)
                .push_bind(&p.phone_number)
                .push_bind(&p.fax_number)
                .push_bind(&p.accreditation_type_code)
                .push_bind(&p.intermediary_carrier_code)
                .push_bind(p.acceptable_poc_switch)
                .push_bind(&p.fiscal_year_end_date)
                .push_bind(&p.compliance_status_code)
                .push_bind(&p.certification_action_type_code)
                .push_bind(p.bed_count)
                .push_bind(p.certified_bed_count)
                .push_bind(p.hospice_bed_count)
                .push_bind(p.aids_bed_count)
                .push_bind(p.alzheimer_bed_count)
                .push_bind(p.dialysis_bed_count)
                .push_bind(p.disabled_children_bed_count)
                .push_bind(p.head_trauma_bed_count)
                .push_bind(p.huntington_disease_bed_count)
                .push_bind(p.medicare_medicaid_snf_bed_count)
                .push_bind(p.medicare_snf_bed_count)
                .push_bind(p.rehab_bed_count)
                .push_bind(p.ventilator_bed_count)
                .push_bind(p.lpn_lvn_count)
                .push_bind(p.rn_count)
                .push_bind(p.employee_count)
                .push_bind(p.change_of_ownership_switch)
                .push_bind(p.hospital_based_switch)
                .push_bind(p.multi_owned_facility_switch)
                .push_bind(&p.clia_lab_number)
                .push_bind(&p.facility_category_code)
                .push_bind(&p.ownership_type_code);
        });

        query_builder.push(
            " ON CONFLICT (cms_certification_number) DO UPDATE SET
            name = EXCLUDED.name,
            provider_subtype_id = EXCLUDED.provider_subtype_id,
            medicaid_vendor_number = EXCLUDED.medicaid_vendor_number,
            provider_type_id = EXCLUDED.provider_type_id,
            address_id = EXCLUDED.address_id,
            original_participation_date = EXCLUDED.original_participation_date,
            certification_date = EXCLUDED.certification_date,
            termination_expiration_date = EXCLUDED.termination_expiration_date,
            change_of_ownership_date = EXCLUDED.change_of_ownership_date,
            asc_begin_service_date = EXCLUDED.asc_begin_service_date,
            processing_date = EXCLUDED.processing_date,
            phone_number = EXCLUDED.phone_number,
            fax_number = EXCLUDED.fax_number,
            accreditation_type_code = EXCLUDED.accreditation_type_code,
            intermediary_carrier_code = EXCLUDED.intermediary_carrier_code,
            acceptable_poc_switch = EXCLUDED.acceptable_poc_switch,
            fiscal_year_end_date = EXCLUDED.fiscal_year_end_date,
            compliance_status_code = EXCLUDED.compliance_status_code,
            certification_action_type_code = EXCLUDED.certification_action_type_code,
            bed_count = EXCLUDED.bed_count,
            certified_bed_count = EXCLUDED.certified_bed_count,
            hospice_bed_count = EXCLUDED.hospice_bed_count,
            aids_bed_count = EXCLUDED.aids_bed_count,
            alzheimer_bed_count = EXCLUDED.alzheimer_bed_count,
            dialysis_bed_count = EXCLUDED.dialysis_bed_count,
            disabled_children_bed_count = EXCLUDED.disabled_children_bed_count,
            head_trauma_bed_count = EXCLUDED.head_trauma_bed_count,
            huntington_disease_bed_count = EXCLUDED.huntington_disease_bed_count,
            medicare_medicaid_snf_bed_count = EXCLUDED.medicare_medicaid_snf_bed_count,
            medicare_snf_bed_count = EXCLUDED.medicare_snf_bed_count,
            rehab_bed_count = EXCLUDED.rehab_bed_count,
            ventilator_bed_count = EXCLUDED.ventilator_bed_count,
            lpn_lvn_count = EXCLUDED.lpn_lvn_count,
            rn_count = EXCLUDED.rn_count,
            employee_count = EXCLUDED.employee_count,
            change_of_ownership_switch = EXCLUDED.change_of_ownership_switch,
            hospital_based_switch = EXCLUDED.hospital_based_switch,
            multi_owned_facility_switch = EXCLUDED.multi_owned_facility_switch,
            clia_lab_number = EXCLUDED.clia_lab_number,
            facility_category_code = EXCLUDED.facility_category_code,
            ownership_type_code = EXCLUDED.ownership_type_code",
        );

        query_builder.build().execute(&mut *tx).await?;
    }

    tx.commit().await?;
    Ok(())
}
