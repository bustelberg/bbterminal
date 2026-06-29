-- Add an ISIN to benchmarks (ETFs/bonds). ETF/bond holdings appear in
-- scheduled-strategy snapshots with a NEGATIVE company_id (= -benchmark_id),
-- so the /schedule holdings tables can't resolve their ISIN from the `company`
-- table. Storing it on the benchmark itself lets the UI show the ISIN column
-- for ETF/bond sleeves too. Set/edited from the diversifier page.
ALTER TABLE public.benchmark
    ADD COLUMN IF NOT EXISTS isin text;

NOTIFY pgrst, 'reload schema';
