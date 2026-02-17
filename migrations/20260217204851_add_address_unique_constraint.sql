-- Remove duplicates before creating the index
DELETE FROM addresses a
USING addresses b
WHERE a.id > b.id
  AND COALESCE(a.street_address, '') = COALESCE(b.street_address, '')
  AND COALESCE(a.city, '') = COALESCE(b.city, '')
  AND COALESCE(a.state_code, '') = COALESCE(b.state_code, '')
  AND COALESCE(a.zip_code, '') = COALESCE(b.zip_code, '');

-- Add a unique constraint to addresses to allow for deduplication during bulk inserts.
-- We use COALESCE to handle NULL values since standard UNIQUE constraints treat NULL as distinct.
CREATE UNIQUE INDEX IF NOT EXISTS idx_addresses_identity 
ON addresses (
    COALESCE(street_address, ''), 
    COALESCE(city, ''), 
    COALESCE(state_code, ''), 
    COALESCE(zip_code, '')
);
