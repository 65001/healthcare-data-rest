-- Persists the `contact-name`/`contact-email` fields cms-hpt.txt already
-- carries per entry (compliance_probe::manifest_parser::MrfEntry) — parsed
-- since the manifest-format work but previously dropped rather than
-- stored anywhere. One discovery row corresponds to one manifest entry in
-- network-manifest mode, or the hospital's own (usually singular) entry
-- in per-hospital-website mode, so a flat nullable column each is enough
-- — no need for a separate per-entry contacts table.
ALTER TABLE mrf_discoveries ADD COLUMN contact_name TEXT;
ALTER TABLE mrf_discoveries ADD COLUMN contact_email TEXT;
