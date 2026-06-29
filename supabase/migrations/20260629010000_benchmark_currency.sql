-- Add a trading currency to benchmarks (ETFs/bonds). Auto-detected from
-- GuruFocus on add (the stock summary's `general.currency`) and editable on the
-- diversifier page. Surfaced on /schedule's current-portfolio table so an
-- ETF/bond sleeve shows its native currency next to the local price (and, when
-- EUR, a 1:1 FX rate; foreign-currency EUR conversion is left blank until a
-- proper FX pass is added).
ALTER TABLE public.benchmark
    ADD COLUMN IF NOT EXISTS currency text;

NOTIFY pgrst, 'reload schema';
