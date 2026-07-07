-- OpenFIGI's role in the asset pipeline is now purely CONFIRMATION: does an
-- independent identity agree with the yfinance instrument we resolved+priced?
-- `identity_status` stores that verdict per execution row (computed at resolve
-- time via resolve.same_company, the same fuzzy name-agreement check the
-- requeue-suspects job uses):
--   verified — OpenFIGI name confirms the resolved instrument
--   mismatch — OpenFIGI names a DIFFERENT company (likely wrong resolution)
--   unknown  — no OpenFIGI name to compare (unresolved / bond / no FIGI)
-- The grid then shows one OpenFIGI Name column + a Match badge instead of the
-- four raw openfigi_* columns.

ALTER TABLE public.asset_execution
    ADD COLUMN IF NOT EXISTS identity_status text;

-- Rebuild the grid view to surface identity_status alongside the (retained)
-- openfigi_* columns — openfigi_name still feeds the requeue-suspects check and
-- the grid's OpenFIGI Name column; figi/ticker/exch/type stay for the fallback
-- resolver + audit but are no longer shown.
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
    e.identity_status,
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
