use chrono::NaiveDate;
use serde::{Deserialize, Deserializer};

#[derive(Debug, Deserialize, Clone)]
pub struct Address {
    #[serde(skip)]
    pub id: Option<i32>, // Database ID, populated later

    #[serde(rename = "st_adr", deserialize_with = "deserialize_na_string")]
    pub street_address: Option<String>,
    #[serde(rename = "city_name", deserialize_with = "deserialize_na_string")]
    pub city: Option<String>,
    #[serde(rename = "state_cd", deserialize_with = "deserialize_na_string")]
    pub state_code: Option<String>,
    #[serde(rename = "zip_cd", deserialize_with = "deserialize_na_string")]
    pub zip_code: Option<String>,

    // Geographic Codes
    #[serde(rename = "ssa_cnty_cd", deserialize_with = "deserialize_na_string")]
    pub ssa_county_code: Option<String>,
    #[serde(rename = "ssa_state_cd", deserialize_with = "deserialize_na_string")]
    pub ssa_state_code: Option<String>,
    #[serde(rename = "state_rgn_cd", deserialize_with = "deserialize_na_string")]
    pub state_region_code: Option<String>,
    #[serde(rename = "rgn_cd", deserialize_with = "deserialize_na_string")]
    pub region_code: Option<String>,
    #[serde(rename = "fips_state_cd", deserialize_with = "deserialize_na_string")]
    pub fips_state_code: Option<String>,
    #[serde(rename = "fips_cnty_cd", deserialize_with = "deserialize_na_string")]
    pub fips_county_code: Option<String>,
    #[serde(rename = "cbsa_cd", deserialize_with = "deserialize_na_string")]
    pub cbsa_code: Option<String>,
    #[serde(
        rename = "cbsa_urbn_rrl_ind",
        deserialize_with = "deserialize_na_string"
    )]
    pub cbsa_urban_rural_indicator: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Provider {
    // --- Identification ---
    #[serde(rename = "prvdr_num")]
    pub cms_certification_number: String, // PK
    #[serde(rename = "fac_name", deserialize_with = "deserialize_na_string")]
    pub name: Option<String>,
    #[serde(rename = "prvdr_sbtyp_id", deserialize_with = "deserialize_na_option")]
    pub provider_subtype_id: Option<i32>,
    #[serde(rename = "mdcd_vndr_num", deserialize_with = "deserialize_na_string")]
    pub medicaid_vendor_number: Option<String>,
    #[serde(rename = "prvdr_type_id", deserialize_with = "deserialize_na_option")]
    pub provider_type_id: Option<i32>,

    // Location FK (Populated after normalization)
    #[serde(skip)]
    pub address_id: Option<i32>,

    // --- Dates ---
    #[serde(
        rename = "orgnl_prtcptn_dt",
        deserialize_with = "deserialize_optional_date"
    )]
    pub original_participation_date: Option<NaiveDate>,
    #[serde(rename = "crtfctn_dt", deserialize_with = "deserialize_optional_date")]
    pub certification_date: Option<NaiveDate>,
    #[serde(
        rename = "trmntn_exprtn_dt",
        deserialize_with = "deserialize_optional_date"
    )]
    pub termination_expiration_date: Option<NaiveDate>,
    #[serde(rename = "chow_dt", deserialize_with = "deserialize_optional_date")]
    pub change_of_ownership_date: Option<NaiveDate>,
    #[serde(
        rename = "asc_bgn_srvc_dt",
        deserialize_with = "deserialize_optional_date"
    )]
    pub asc_begin_service_date: Option<NaiveDate>,
    #[serde(
        rename = "processing_date",
        deserialize_with = "deserialize_optional_date"
    )]
    pub processing_date: Option<NaiveDate>,

    // --- Contact ---
    #[serde(rename = "phne_num", deserialize_with = "deserialize_na_string")]
    pub phone_number: Option<String>,
    #[serde(rename = "fax_phne_num", deserialize_with = "deserialize_na_string")]
    pub fax_number: Option<String>,

    // --- Characteristics & flags ---
    #[serde(rename = "acrdtn_type_cd", deserialize_with = "deserialize_na_string")]
    pub accreditation_type_code: Option<String>,
    #[serde(
        rename = "intrmdry_carr_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub intermediary_carrier_code: Option<String>,
    #[serde(rename = "acptbl_poc_sw", deserialize_with = "deserialize_yes_no")]
    pub acceptable_poc_switch: Option<bool>,

    #[serde(
        rename = "fy_end_mo_day_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub fiscal_year_end_date: Option<String>,
    #[serde(rename = "cmplnc_stus_cd", deserialize_with = "deserialize_na_string")]
    pub compliance_status_code: Option<String>,
    #[serde(
        rename = "crtfctn_actn_type_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub certification_action_type_code: Option<String>,

    // --- Bed Counts (Integers) ---
    #[serde(rename = "bed_cnt", deserialize_with = "deserialize_na_option")]
    pub bed_count: Option<i32>,
    #[serde(rename = "crtfd_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub certified_bed_count: Option<i32>,
    #[serde(rename = "hospc_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub hospice_bed_count: Option<i32>,
    #[serde(rename = "aids_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub aids_bed_count: Option<i32>,
    #[serde(rename = "alzhmr_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub alzheimer_bed_count: Option<i32>,
    #[serde(rename = "dlys_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub dialysis_bed_count: Option<i32>,
    #[serde(
        rename = "dsbl_chldrn_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub disabled_children_bed_count: Option<i32>,
    #[serde(
        rename = "head_trma_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub head_trauma_bed_count: Option<i32>,
    #[serde(
        rename = "hntgtn_dease_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub huntington_disease_bed_count: Option<i32>,
    #[serde(
        rename = "mdcr_mdcd_snf_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub medicare_medicaid_snf_bed_count: Option<i32>,
    #[serde(
        rename = "mdcr_snf_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub medicare_snf_bed_count: Option<i32>,
    #[serde(rename = "rehab_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub rehab_bed_count: Option<i32>,
    #[serde(rename = "vntltr_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub ventilator_bed_count: Option<i32>,

    // --- Staffing Counts (Floats/Doubles based on dict usually, but here listed as counts) ---
    #[serde(rename = "lpn_lvn_cnt", deserialize_with = "deserialize_na_option")]
    pub lpn_lvn_count: Option<f64>,
    #[serde(rename = "rn_cnt", deserialize_with = "deserialize_na_option")]
    pub rn_count: Option<f64>,
    #[serde(rename = "emplee_cnt", deserialize_with = "deserialize_na_option")]
    pub employee_count: Option<f64>,

    // --- Services (Boolean Switches or Codes) ---
    #[serde(rename = "chow_sw", deserialize_with = "deserialize_yes_no")]
    pub change_of_ownership_switch: Option<bool>,
    #[serde(rename = "hosp_bsd_sw", deserialize_with = "deserialize_yes_no")]
    pub hospital_based_switch: Option<bool>,
    #[serde(
        rename = "mlt_ownd_fac_org_sw",
        deserialize_with = "deserialize_yes_no"
    )]
    pub multi_owned_facility_switch: Option<bool>,

    #[serde(rename = "clia_lb_nb", deserialize_with = "deserialize_na_string")]
    pub clia_lab_number: Option<String>,

    // --- Category / Types ---
    #[serde(
        rename = "gnrl_fac_type_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub facility_category_code: Option<String>,
    #[serde(rename = "control_type", deserialize_with = "deserialize_na_string")]
    pub ownership_type_code: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProviderOfServiceRow {
    // --- Address Fields ---
    #[serde(rename = "st_adr", deserialize_with = "deserialize_na_string")]
    pub street_address: Option<String>,
    #[serde(rename = "city_name", deserialize_with = "deserialize_na_string")]
    pub city: Option<String>,
    #[serde(rename = "state_cd", deserialize_with = "deserialize_na_string")]
    pub state_code: Option<String>,
    #[serde(rename = "zip_cd", deserialize_with = "deserialize_na_string")]
    pub zip_code: Option<String>,

    // Geographic Codes
    #[serde(rename = "ssa_cnty_cd", deserialize_with = "deserialize_na_string")]
    pub ssa_county_code: Option<String>,
    #[serde(rename = "ssa_state_cd", deserialize_with = "deserialize_na_string")]
    pub ssa_state_code: Option<String>,
    #[serde(rename = "state_rgn_cd", deserialize_with = "deserialize_na_string")]
    pub state_region_code: Option<String>,
    #[serde(rename = "rgn_cd", deserialize_with = "deserialize_na_string")]
    pub region_code: Option<String>,
    #[serde(rename = "fips_state_cd", deserialize_with = "deserialize_na_string")]
    pub fips_state_code: Option<String>,
    #[serde(rename = "fips_cnty_cd", deserialize_with = "deserialize_na_string")]
    pub fips_county_code: Option<String>,
    #[serde(rename = "cbsa_cd", deserialize_with = "deserialize_na_string")]
    pub cbsa_code: Option<String>,
    #[serde(
        rename = "cbsa_urbn_rrl_ind",
        deserialize_with = "deserialize_na_string"
    )]
    pub cbsa_urban_rural_indicator: Option<String>,

    // --- Provider Fields ---
    #[serde(rename = "prvdr_num")]
    pub cms_certification_number: String, // PK
    #[serde(rename = "fac_name", deserialize_with = "deserialize_na_string")]
    pub name: Option<String>,
    #[serde(rename = "prvdr_sbtyp_id", deserialize_with = "deserialize_na_option")]
    pub provider_subtype_id: Option<i32>,
    #[serde(rename = "mdcd_vndr_num", deserialize_with = "deserialize_na_string")]
    pub medicaid_vendor_number: Option<String>,
    #[serde(rename = "prvdr_type_id", deserialize_with = "deserialize_na_option")]
    pub provider_type_id: Option<i32>,

    // --- Dates ---
    #[serde(
        rename = "orgnl_prtcptn_dt",
        deserialize_with = "deserialize_optional_date"
    )]
    pub original_participation_date: Option<NaiveDate>,
    #[serde(rename = "crtfctn_dt", deserialize_with = "deserialize_optional_date")]
    pub certification_date: Option<NaiveDate>,
    #[serde(
        rename = "trmntn_exprtn_dt",
        deserialize_with = "deserialize_optional_date"
    )]
    pub termination_expiration_date: Option<NaiveDate>,
    #[serde(rename = "chow_dt", deserialize_with = "deserialize_optional_date")]
    pub change_of_ownership_date: Option<NaiveDate>,
    #[serde(
        rename = "asc_bgn_srvc_dt",
        deserialize_with = "deserialize_optional_date"
    )]
    pub asc_begin_service_date: Option<NaiveDate>,
    #[serde(
        rename = "processing_date",
        deserialize_with = "deserialize_optional_date"
    )]
    pub processing_date: Option<NaiveDate>,

    // --- Contact ---
    #[serde(rename = "phne_num", deserialize_with = "deserialize_na_string")]
    pub phone_number: Option<String>,
    #[serde(rename = "fax_phne_num", deserialize_with = "deserialize_na_string")]
    pub fax_number: Option<String>,

    // --- Characteristics & flags ---
    #[serde(rename = "acrdtn_type_cd", deserialize_with = "deserialize_na_string")]
    pub accreditation_type_code: Option<String>,
    #[serde(
        rename = "intrmdry_carr_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub intermediary_carrier_code: Option<String>,
    #[serde(rename = "acptbl_poc_sw", deserialize_with = "deserialize_yes_no")]
    pub acceptable_poc_switch: Option<bool>,

    #[serde(
        rename = "fy_end_mo_day_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub fiscal_year_end_date: Option<String>,
    #[serde(rename = "cmplnc_stus_cd", deserialize_with = "deserialize_na_string")]
    pub compliance_status_code: Option<String>,
    #[serde(
        rename = "crtfctn_actn_type_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub certification_action_type_code: Option<String>,

    // --- Bed Counts (Integers) ---
    #[serde(rename = "bed_cnt", deserialize_with = "deserialize_na_option")]
    pub bed_count: Option<i32>,
    #[serde(rename = "crtfd_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub certified_bed_count: Option<i32>,
    #[serde(rename = "hospc_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub hospice_bed_count: Option<i32>,
    #[serde(rename = "aids_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub aids_bed_count: Option<i32>,
    #[serde(rename = "alzhmr_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub alzheimer_bed_count: Option<i32>,
    #[serde(rename = "dlys_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub dialysis_bed_count: Option<i32>,
    #[serde(
        rename = "dsbl_chldrn_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub disabled_children_bed_count: Option<i32>,
    #[serde(
        rename = "head_trma_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub head_trauma_bed_count: Option<i32>,
    #[serde(
        rename = "hntgtn_dease_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub huntington_disease_bed_count: Option<i32>,
    #[serde(
        rename = "mdcr_mdcd_snf_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub medicare_medicaid_snf_bed_count: Option<i32>,
    #[serde(
        rename = "mdcr_snf_bed_cnt",
        deserialize_with = "deserialize_na_option"
    )]
    pub medicare_snf_bed_count: Option<i32>,
    #[serde(rename = "rehab_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub rehab_bed_count: Option<i32>,
    #[serde(rename = "vntltr_bed_cnt", deserialize_with = "deserialize_na_option")]
    pub ventilator_bed_count: Option<i32>,

    // --- Staffing Counts (Floats/Doubles based on dict usually, but here listed as counts) ---
    #[serde(rename = "lpn_lvn_cnt", deserialize_with = "deserialize_na_option")]
    pub lpn_lvn_count: Option<f64>,
    #[serde(rename = "rn_cnt", deserialize_with = "deserialize_na_option")]
    pub rn_count: Option<f64>,
    #[serde(rename = "emplee_cnt", deserialize_with = "deserialize_na_option")]
    pub employee_count: Option<f64>,

    // --- Services (Boolean Switches or Codes) ---
    #[serde(rename = "chow_sw", deserialize_with = "deserialize_yes_no")]
    pub change_of_ownership_switch: Option<bool>,
    #[serde(rename = "hosp_bsd_sw", deserialize_with = "deserialize_yes_no")]
    pub hospital_based_switch: Option<bool>,
    #[serde(
        rename = "mlt_ownd_fac_org_sw",
        deserialize_with = "deserialize_yes_no"
    )]
    pub multi_owned_facility_switch: Option<bool>,

    #[serde(
        rename = "clia_lb_nb",
        default,
        deserialize_with = "deserialize_na_string"
    )]
    pub clia_lab_number: Option<String>,

    // --- Category / Types ---
    #[serde(
        rename = "gnrl_fac_type_cd",
        deserialize_with = "deserialize_na_string"
    )]
    pub facility_category_code: Option<String>,
    #[serde(rename = "control_type", deserialize_with = "deserialize_na_string")]
    pub ownership_type_code: Option<String>,
}

impl From<ProviderOfServiceRow> for Address {
    fn from(row: ProviderOfServiceRow) -> Self {
        Address {
            id: None,
            street_address: row.street_address,
            city: row.city,
            state_code: row.state_code,
            zip_code: row.zip_code,
            ssa_county_code: row.ssa_county_code,
            ssa_state_code: row.ssa_state_code,
            state_region_code: row.state_region_code,
            region_code: row.region_code,
            fips_state_code: row.fips_state_code,
            fips_county_code: row.fips_county_code,
            cbsa_code: row.cbsa_code,
            cbsa_urban_rural_indicator: row.cbsa_urban_rural_indicator,
        }
    }
}

impl From<ProviderOfServiceRow> for Provider {
    fn from(row: ProviderOfServiceRow) -> Self {
        Provider {
            cms_certification_number: row.cms_certification_number,
            name: row.name,
            provider_subtype_id: row.provider_subtype_id,
            medicaid_vendor_number: row.medicaid_vendor_number,
            provider_type_id: row.provider_type_id,
            address_id: None, // This will need to be set after persisting address
            original_participation_date: row.original_participation_date,
            certification_date: row.certification_date,
            termination_expiration_date: row.termination_expiration_date,
            change_of_ownership_date: row.change_of_ownership_date,
            asc_begin_service_date: row.asc_begin_service_date,
            processing_date: row.processing_date,
            phone_number: row.phone_number,
            fax_number: row.fax_number,
            accreditation_type_code: row.accreditation_type_code,
            intermediary_carrier_code: row.intermediary_carrier_code,
            acceptable_poc_switch: row.acceptable_poc_switch,
            fiscal_year_end_date: row.fiscal_year_end_date,
            compliance_status_code: row.compliance_status_code,
            certification_action_type_code: row.certification_action_type_code,
            bed_count: row.bed_count,
            certified_bed_count: row.certified_bed_count,
            hospice_bed_count: row.hospice_bed_count,
            aids_bed_count: row.aids_bed_count,
            alzheimer_bed_count: row.alzheimer_bed_count,
            dialysis_bed_count: row.dialysis_bed_count,
            disabled_children_bed_count: row.disabled_children_bed_count,
            head_trauma_bed_count: row.head_trauma_bed_count,
            huntington_disease_bed_count: row.huntington_disease_bed_count,
            medicare_medicaid_snf_bed_count: row.medicare_medicaid_snf_bed_count,
            medicare_snf_bed_count: row.medicare_snf_bed_count,
            rehab_bed_count: row.rehab_bed_count,
            ventilator_bed_count: row.ventilator_bed_count,
            lpn_lvn_count: row.lpn_lvn_count,
            rn_count: row.rn_count,
            employee_count: row.employee_count,
            change_of_ownership_switch: row.change_of_ownership_switch,
            hospital_based_switch: row.hospital_based_switch,
            multi_owned_facility_switch: row.multi_owned_facility_switch,
            clia_lab_number: row.clia_lab_number,
            facility_category_code: row.facility_category_code,
            ownership_type_code: row.ownership_type_code,
        }
    }
}

// Helper for 'Not Applicable' / 'Not Available' -> None
fn deserialize_na_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de> + std::str::FromStr,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(ref v)
            if v.eq_ignore_ascii_case("Not Applicable")
                || v.eq_ignore_ascii_case("Not Available")
                || v.trim().is_empty() =>
        {
            Ok(None)
        }
        Some(v) => {
            // Try to parse the value into target type T
            match v.parse::<T>() {
                Ok(val) => Ok(Some(val)),
                Err(_) => Ok(None), // Treat parsing errors as None for now
            }
        }
        None => Ok(None),
    }
}

// Special case for Strings
fn deserialize_na_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(ref v)
            if v.eq_ignore_ascii_case("Not Applicable")
                || v.eq_ignore_ascii_case("Not Available")
                || v.trim().is_empty() =>
        {
            Ok(None)
        }
        Some(v) => Ok(Some(v)),
        None => Ok(None),
    }
}

// Helper for "Yes"/"No" -> bool
fn deserialize_yes_no<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(ref v) if v.eq_ignore_ascii_case("Yes") => Ok(Some(true)),
        Some(ref v) if v.eq_ignore_ascii_case("No") => Ok(Some(false)),
        _ => Ok(None),
    }
}

// Helper for Date YYYY-MM-DD/YYYYMMDD
fn deserialize_optional_date<'de, D>(deserializer: D) -> Result<Option<NaiveDate>, D::Error>
where
    D: Deserializer<'de>,
{
    let s: Option<String> = Option::deserialize(deserializer)?;
    match s {
        Some(ref v)
            if v.eq_ignore_ascii_case("Not Applicable")
                || v.eq_ignore_ascii_case("Not Available")
                || v.trim().is_empty() =>
        {
            Ok(None)
        }
        Some(date_str) => {
            if let Ok(d) = NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                return Ok(Some(d));
            }
            if let Ok(d) = NaiveDate::parse_from_str(&date_str, "%Y%m%d") {
                return Ok(Some(d));
            }
            Ok(None)
        }
        None => Ok(None),
    }
}
