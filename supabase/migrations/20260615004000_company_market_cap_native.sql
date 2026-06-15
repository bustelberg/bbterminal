-- Store the NATIVE market cap + the FX details behind `market_cap_eur`, so the
-- /companies page can show "€6.81B  ($7.35B USD × 1/1.0823, as of …)" and the
-- conversion is auditable instead of a black box.
--
-- `market_cap_eur` stays the absolute EUR figure. We add:
--   market_cap_native    — absolute market cap in the listing's own currency
--   market_cap_currency  — that currency code (e.g. USD, JPY; EUR for euro lines)
--   market_cap_fx_rate   — ECB units-per-EUR used (EUR = native / rate; 1.0 for EUR)
-- The capture date is the existing `market_cap_date`.
ALTER TABLE public.company
  ADD COLUMN IF NOT EXISTS market_cap_native double precision,
  ADD COLUMN IF NOT EXISTS market_cap_currency varchar,
  ADD COLUMN IF NOT EXISTS market_cap_fx_rate double precision;

COMMENT ON COLUMN public.company.market_cap_native IS
  'Absolute market cap in the listing''s native currency (GuruFocus summary.company_data.mktcap × 1e6).';
COMMENT ON COLUMN public.company.market_cap_currency IS 'Native currency of market_cap_native.';
COMMENT ON COLUMN public.company.market_cap_fx_rate IS
  'ECB units-per-EUR used to convert native → EUR (EUR = native / rate; 1.0 for EUR-quoted lines).';
