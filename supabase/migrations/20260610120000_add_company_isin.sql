-- Add ISIN to the company table.
--
-- ISIN (International Securities Identification Number) identifies the
-- issuer/security and is sourced from GuruFocus (`summary.company_data.isin`)
-- and from the Leonteq scrape (`leonteq_equity.isin`, already stored).
--
-- NOT unique: an ISIN identifies an issuer, which can have several listings,
-- and we store companies per-listing (e.g. ADR vs local line). Two company
-- rows for the same issuer's different listings legitimately share an ISIN.
-- Indexed for lookup / future dedupe-by-ISIN.
ALTER TABLE public.company ADD COLUMN IF NOT EXISTS isin character varying;

CREATE INDEX IF NOT EXISTS idx_company_isin ON public.company USING btree (isin);
