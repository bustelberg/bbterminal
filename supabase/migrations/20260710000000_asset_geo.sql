-- Geography per asset: country, continent, MSCI region.
--
-- Two independent signals, kept apart because they disagree (see
-- backend/asset_pipeline/geo.py):
--   * listing_country  — the venue the security trades on. Derived from data we
--                        already hold (execution.exchange / the Yahoo symbol
--                        suffix), so it costs ZERO Yahoo calls and resolves for
--                        ETFs too.
--   * domicile_country — where the issuer is headquartered. Yahoo v10
--                        assetProfile.country, one request per symbol, and NULL
--                        for every ETF / crypto / future.
--
-- Linde lists in the US and domiciles in the UK; Alibaba's ADR lists in the US
-- and domiciles in China. `country` in the grid view coalesces domicile over
-- listing. continent (geographic) + msci_region (financial) are derived from
-- that coalesce and stored, so filters don't re-derive per query.
--
-- NOTE: an ETF's geography is a property of its HOLDINGS, not its listing —
-- these columns describe the LISTING for an ETF row, nothing more.

ALTER TABLE public.asset_analysis
    ADD COLUMN IF NOT EXISTS domicile_country text,
    ADD COLUMN IF NOT EXISTS listing_country  text,
    ADD COLUMN IF NOT EXISTS continent        text,
    ADD COLUMN IF NOT EXISTS msci_region      text,
    ADD COLUMN IF NOT EXISTS geo_checked_at   timestamptz;

-- Per-listing country: an execution row is a specific venue, so it carries its
-- OWN listing country (an analysis asset's many executions can differ).
ALTER TABLE public.asset_execution
    ADD COLUMN IF NOT EXISTS listing_country text;

CREATE INDEX IF NOT EXISTS asset_analysis_msci_region_idx ON public.asset_analysis(msci_region);
CREATE INDEX IF NOT EXISTS asset_analysis_continent_idx   ON public.asset_analysis(continent);

-- Re-create asset_grid to surface the new columns (views don't auto-pick up
-- table columns). Definition copied from 20260709000000 + geo.
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
    a.short_multiplier,
    a.symbol                               AS analysis_symbol,
    -- Geography: this ROW's listing venue, the issuer's domicile, and the
    -- resolved country (domicile wins) + its continent / MSCI region.
    e.listing_country,
    a.domicile_country,
    COALESCE(a.domicile_country, a.listing_country) AS country,
    a.continent,
    a.msci_region,
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
    a.market_cap_eur,
    a.market_cap_currency,
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
