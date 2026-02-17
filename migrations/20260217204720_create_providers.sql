-- Create Providers table
CREATE TABLE IF NOT EXISTS providers (
    cms_certification_number TEXT PRIMARY KEY,
    name TEXT,
    provider_subtype_id INTEGER,
    medicaid_vendor_number TEXT,
    provider_type_id INTEGER,
    
    -- Foreign Key to Address
    address_id INTEGER REFERENCES addresses(id),
    
    -- Dates
    original_participation_date DATE,
    certification_date DATE,
    termination_expiration_date DATE,
    change_of_ownership_date DATE,
    asc_begin_service_date DATE,
    processing_date DATE,
    
    -- Contact
    phone_number TEXT,
    fax_number TEXT,
    
    -- Characteristics & flags
    accreditation_type_code TEXT,
    intermediary_carrier_code TEXT,
    acceptable_poc_switch BOOLEAN,
    fiscal_year_end_date TEXT,
    compliance_status_code TEXT,
    certification_action_type_code TEXT,
    
    -- Bed Counts
    bed_count INTEGER,
    certified_bed_count INTEGER,
    hospice_bed_count INTEGER,
    aids_bed_count INTEGER,
    alzheimer_bed_count INTEGER,
    dialysis_bed_count INTEGER,
    disabled_children_bed_count INTEGER,
    head_trauma_bed_count INTEGER,
    huntington_disease_bed_count INTEGER,
    medicare_medicaid_snf_bed_count INTEGER,
    medicare_snf_bed_count INTEGER,
    rehab_bed_count INTEGER,
    ventilator_bed_count INTEGER,
    
    -- Staffing Counts
    lpn_lvn_count DOUBLE PRECISION,
    rn_count DOUBLE PRECISION,
    employee_count DOUBLE PRECISION,
    
    -- Services & switches
    change_of_ownership_switch BOOLEAN,
    hospital_based_switch BOOLEAN,
    multi_owned_facility_switch BOOLEAN,
    clia_lab_number TEXT,
    
    -- Category / Types
    facility_category_code TEXT,
    ownership_type_code TEXT
);

-- Index for FK
CREATE INDEX IF NOT EXISTS idx_providers_address_id ON providers(address_id);
