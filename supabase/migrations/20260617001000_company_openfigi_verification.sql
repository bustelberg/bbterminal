-- Per-company OpenFIGI verification status, surfaced as a column on /companies.
--
-- We verify each company against OpenFIGI by its ISIN: does the stored ISIN
-- resolve to the security we think it is (our listing or matching name), or to
-- a DIFFERENT company (a wrong-ISIN trap, e.g. "Hindustan Aeronautics" whose
-- stored ISIN BMG455841020 actually maps to "HAL TRUST")? Populated on demand
-- by the /companies "Verify OpenFIGI" bulk action + the per-row re-check.
--
--   openfigi_status:
--     'verified'   ISIN resolves to our listing or a matching company name
--     'mismatch'   ISIN resolves to a DIFFERENT company (review the ISIN)
--     'not_found'  OpenFIGI has no security for this ISIN
--     'no_isin'    company has no ISIN to check
--     'error'      the verification call failed (transient; re-run)
--     NULL         never checked
--   openfigi_name:       the name OpenFIGI returned for the ISIN (mismatch tooltip)
--   openfigi_checked_at: when the row was last verified

ALTER TABLE company
    ADD COLUMN IF NOT EXISTS openfigi_status     text,
    ADD COLUMN IF NOT EXISTS openfigi_name       text,
    ADD COLUMN IF NOT EXISTS openfigi_checked_at timestamptz;
