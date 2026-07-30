-- "Leonteq Verified" membership: the set of identifiers pulled from the Leonteq
-- (lynqs) endpoint. An asset is Leonteq-verified iff its ISIN (equity/ETF) OR its
-- analysis symbol (crypto, e.g. BTC-USD) is in this set. Non-destructive — it's a
-- badge, not a prune; re-uploading the Leonteq list REPLACES the set so the badge
-- always reflects the current file. Anything not in the set gets no badge.

CREATE TABLE IF NOT EXISTS public.leonteq_universe (
    identifier text PRIMARY KEY,
    added_at   timestamptz NOT NULL DEFAULT now()
);
ALTER TABLE public.leonteq_universe ENABLE ROW LEVEL SECURITY;  -- deny-all; service_role bypasses
GRANT SELECT, INSERT, UPDATE, DELETE ON public.leonteq_universe TO service_role;

-- Rebuild the grid view with the leonteq_verified flag. TWO separate EXISTS
-- (OR'd) so each is a single index seek on the PK — index-friendly, unlike an
-- `l.identifier = e.isin OR l.identifier = a.symbol` correlated scan.
DROP VIEW IF EXISTS public.asset_grid;
CREATE VIEW public.asset_grid AS
SELECT
    e.execution_id,
    e.isin,
    e.analysis_id,
    e.yahoo_symbol,
    COALESCE(e.name, a.label)              AS name,
    e.exchange,
    e.currency,
    COALESCE(a.asset_class, e.asset_class) AS asset_class,
    a.sector,
    a.symbol                               AS analysis_symbol,
    e.med_adv_eur,
    e.first_date,
    e.years,
    e.wrapper,
    e.is_leveraged,
    e.is_default,
    e.status,
    e.reason,
    e.openfigi_figi,
    e.openfigi_name,
    e.openfigi_ticker,
    e.openfigi_exch,
    e.openfigi_type,
    a.parquet_path,
    a.parquet_rows,
    e.updated_at,
    a.price_from,
    a.price_to,
    a.bars,
    a.volume_from,
    a.volume_to,
    a.zero_vol_frac,
    (
        EXISTS (SELECT 1 FROM public.leonteq_universe l WHERE l.identifier = e.isin)
        OR EXISTS (SELECT 1 FROM public.leonteq_universe l WHERE l.identifier = a.symbol)
    ) AS leonteq_verified
FROM public.asset_execution e
LEFT JOIN public.asset_analysis a ON a.analysis_id = e.analysis_id;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';
