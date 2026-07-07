-- Named liquid universes built from the asset-pipeline grid: a saved set of
-- UNIQUE yfinance tickers (analysis instruments) meeting an identity + liquidity
-- filter. `params` records the filter that produced it (for display / rebuild);
-- membership is materialised as analysis_symbols so the grid can filter by it
-- with a cheap lookup.

CREATE TABLE IF NOT EXISTS public.asset_universe (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name         text NOT NULL UNIQUE,
    params       jsonb NOT NULL DEFAULT '{}'::jsonb,
    ticker_count int NOT NULL DEFAULT 0,
    created_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.asset_universe_member (
    universe_id     bigint NOT NULL REFERENCES public.asset_universe(id) ON DELETE CASCADE,
    analysis_symbol text NOT NULL,
    PRIMARY KEY (universe_id, analysis_symbol)
);
CREATE INDEX IF NOT EXISTS asset_universe_member_symbol_idx
    ON public.asset_universe_member (analysis_symbol);

ALTER TABLE public.asset_universe        ENABLE ROW LEVEL SECURITY;  -- deny-all; service_role bypasses
ALTER TABLE public.asset_universe_member ENABLE ROW LEVEL SECURITY;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_universe        TO service_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.asset_universe_member TO service_role;

NOTIFY pgrst, 'reload schema';
