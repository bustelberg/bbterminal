-- The Leonteq (lynqs) CSV — id, ticker, name, productType, ric, isin, currency —
-- carries per-instrument metadata alongside the identifier. Store name/currency/
-- product_type on leonteq_universe so the grid can surface the Leonteq-provided
-- fields next to the OpenFIGI + yfinance columns, and badge any row whose ISIN
-- (or analysis symbol) is in the set as "Leonteq Verified". Re-uploading REPLACES
-- the whole set (see 20260706000000).

ALTER TABLE public.leonteq_universe
    ADD COLUMN IF NOT EXISTS name         text,
    ADD COLUMN IF NOT EXISTS currency     text,
    ADD COLUMN IF NOT EXISTS product_type text;

-- Rebuild the grid view: two separate LEFT JOINs onto the leonteq set (one on the
-- ISIN, one on the analysis symbol) so each stays a single PK index seek — then
-- COALESCE so the Leonteq metadata comes from whichever identifier matched, and
-- the badge is true when either side hit.
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
    COALESCE(li.name, la.name)                 AS leonteq_name,
    COALESCE(li.currency, la.currency)         AS leonteq_currency,
    COALESCE(li.product_type, la.product_type) AS leonteq_product_type,
    (li.identifier IS NOT NULL OR la.identifier IS NOT NULL) AS leonteq_verified
FROM public.asset_execution e
LEFT JOIN public.asset_analysis  a  ON a.analysis_id = e.analysis_id
LEFT JOIN public.leonteq_universe li ON li.identifier = e.isin
LEFT JOIN public.leonteq_universe la ON la.identifier = a.symbol;

REVOKE ALL ON public.asset_grid FROM anon, authenticated;
GRANT SELECT ON public.asset_grid TO service_role;

NOTIFY pgrst, 'reload schema';
