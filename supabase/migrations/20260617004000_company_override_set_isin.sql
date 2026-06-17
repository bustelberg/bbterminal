-- Third `company_override` kind: 'set_isin' — pin a specific company's stored
-- ISIN, re-applied on every ingest (prune phase + after the ISIN backfill).
--
-- The ISIN backfill is NULL-only, so a hand-corrected ISIN normally sticks — but
-- the SOURCE that seeded the wrong value can re-seed it whenever the row is
-- re-created (e.g. Leonteq carries BOTH the Class A `US98954M1018` and Class C
-- `US98954M2008` ISINs against the one Zillow Group row, and the backfill's
-- Leonteq pass picks one arbitrarily). A `set_isin` override matched by
-- (`ticker`, `exchange`) overwrites `company.isin` with `canonical_isin` every
-- ingest, so the correction is durable across re-creations and reconstructions.

ALTER TABLE company_override DROP CONSTRAINT IF EXISTS company_override_kind_check;
ALTER TABLE company_override
    ADD CONSTRAINT company_override_kind_check
    CHECK (kind IN ('alias', 'exclude', 'set_isin'));
