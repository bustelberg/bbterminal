-- Asset pipeline (Yahoo-sourced, separate from the GuruFocus ingest tables).
--
-- Model: MANY execution instruments -> ONE analysis asset.
--   * asset_analysis  — the thing you BACKTEST/SIGNAL on. Dedup key = `symbol`
--                       (the resolved Yahoo analysis symbol, e.g. BTC-USD, AAPL).
--                       Many input ISINs collapse here (all BTC ETFs -> BTC-USD).
--   * asset_execution — the thing you TRADE. One row per input ISIN, linked
--                       many->one to an analysis asset. A crypto/commodity ETF
--                       wraps an underlying (wrapper='etf'); a normal equity's
--                       execution == its analysis listing.
--   * asset_price     — daily CLOSE + VOLUME, stored ONCE per analysis asset
--                       (never per execution), so signals are computed once and
--                       shared by every execution mapped to it.
--
-- RLS deny-all on all three (public-table RLS CI check); the backend uses the
-- service_role key (BYPASSRLS) for reads/writes.

CREATE TABLE IF NOT EXISTS public.asset_analysis (
    analysis_id  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol       text NOT NULL UNIQUE,   -- Yahoo analysis symbol = dedup key
    asset_class  text,                   -- crypto | equity | etf | commodity | fx | index
    label        text,
    sector       text,
    currency     text,
    first_date   date,
    years        numeric,
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.asset_execution (
    execution_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    isin         text NOT NULL UNIQUE,   -- the input identifier
    analysis_id  bigint NOT NULL REFERENCES public.asset_analysis(analysis_id) ON DELETE CASCADE,
    yahoo_symbol text,
    name         text,
    exchange     text,
    currency     text,
    med_adv_eur  numeric,                -- liquidity (median daily traded value, EUR)
    first_date   date,
    years        numeric,
    wrapper      text,                   -- 'etf' when it wraps an underlying, else NULL
    is_leveraged boolean NOT NULL DEFAULT false,  -- leveraged/inverse: NOT mapped to an underlying
    is_default   boolean NOT NULL DEFAULT false,  -- best execution for its analysis asset
    ibkr_conid   bigint,                 -- filled later by the IBKR OAuth Web API step
    tradeable_eu boolean,                -- filled later
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS asset_execution_analysis_idx ON public.asset_execution(analysis_id);

CREATE TABLE IF NOT EXISTS public.asset_price (
    analysis_id  bigint NOT NULL REFERENCES public.asset_analysis(analysis_id) ON DELETE CASCADE,
    target_date  date NOT NULL,
    close        numeric,
    volume       numeric,
    PRIMARY KEY (analysis_id, target_date)
);

ALTER TABLE public.asset_analysis  ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.asset_execution ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.asset_price     ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS asset_analysis_deny_all  ON public.asset_analysis;
DROP POLICY IF EXISTS asset_execution_deny_all ON public.asset_execution;
DROP POLICY IF EXISTS asset_price_deny_all     ON public.asset_price;
CREATE POLICY asset_analysis_deny_all  ON public.asset_analysis  FOR ALL USING (false);
CREATE POLICY asset_execution_deny_all ON public.asset_execution FOR ALL USING (false);
CREATE POLICY asset_price_deny_all     ON public.asset_price     FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_analysis  TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_execution TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_price     TO service_role;

NOTIFY pgrst, 'reload schema';
