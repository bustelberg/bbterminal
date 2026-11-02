-- Preserve AIRS's distinction between a security and its accrued-interest line.
ALTER TABLE public.airs_holding ADD COLUMN IF NOT EXISTS valuation_component text;
COMMENT ON COLUMN public.airs_holding.valuation_component IS
  'AIRS source component: security or accrued_interest; NULL when the export did not label it.';
NOTIFY pgrst, 'reload schema';
