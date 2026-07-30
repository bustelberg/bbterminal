-- Asset pipeline v2: a full-OHLCV parquet archive per asset (alongside the
-- close+volume asset_price table) + OpenFIGI identity columns per ISIN, both
-- surfaced in the flat grid.
--
--   * asset-parquet bucket — one parquet blob per analysis asset holding the
--     etoro column set (date, open, high, low, close, adj_close, volume,
--     dividends, splits). asset_price stays the chart/coverage source; parquet
--     is the fuller archive (dual store).
--   * asset_analysis.parquet_{path,rows} — pointer + row count for that blob.
--   * asset_execution.openfigi_* — OpenFIGI identity for the ISIN (figi, name,
--     ticker, exchange code, security type), shown between the ISIN and the
--     yfinance columns.

-- Private bucket — only the backend (service_role) reads/writes via the Storage
-- REST API. Mirrors gurufocus-raw / backtest-results.
INSERT INTO storage.buckets (id, name, public)
VALUES ('asset-parquet', 'asset-parquet', false)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE public.asset_analysis  ADD COLUMN IF NOT EXISTS parquet_path text;
ALTER TABLE public.asset_analysis  ADD COLUMN IF NOT EXISTS parquet_rows integer;

ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS openfigi_figi   text;
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS openfigi_name   text;
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS openfigi_ticker text;
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS openfigi_exch   text;
ALTER TABLE public.asset_execution ADD COLUMN IF NOT EXISTS openfigi_type   text;

-- Rebuild the grid view with the OpenFIGI + parquet columns. DROP first (not
-- CREATE OR REPLACE) because the new columns sit BEFORE existing ones — REPLACE
-- can only append columns at the end.
DROP VIEW IF EXISTS public.asset_grid;
CREATE VIEW public.asset_grid AS
SELECT
    e.execution_id,
    e.isin,
    e.analysis_id,
    e.yahoo_symbol,
    COALESCE(e.name, a.label)                 AS name,
    e.exchange,
    e.currency,
    COALESCE(a.asset_class, e.asset_class)    AS asset_class,
    a.sector,
    a.symbol                                  AS analysis_symbol,
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
    (SELECT min(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS price_from,
    (SELECT max(p.target_date) FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS price_to,
    (SELECT count(*)           FROM public.asset_price p WHERE p.analysis_id = e.analysis_id) AS bars
FROM public.asset_execution e
LEFT JOIN public.asset_analysis a ON a.analysis_id = e.analysis_id;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';
