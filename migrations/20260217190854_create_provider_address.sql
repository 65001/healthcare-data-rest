-- Create Address table
CREATE TABLE IF NOT EXISTS addresses (
    id SERIAL PRIMARY KEY,
    street_address TEXT,
    city TEXT,
    state_code TEXT,
    zip_code TEXT,
    
    -- Geographic Codes
    ssa_county_code TEXT,
    ssa_state_code TEXT,
    state_region_code TEXT,
    region_code TEXT,
    fips_state_code TEXT,
    fips_county_code TEXT,
    cbsa_code TEXT,
    cbsa_urban_rural_indicator TEXT
);

-- Index for searching addresses (optional, but good practice)
CREATE INDEX IF NOT EXISTS idx_addresses_state_code ON addresses(state_code);
CREATE INDEX IF NOT EXISTS idx_addresses_zip_code ON addresses(zip_code);
