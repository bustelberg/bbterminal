-- Global fee configuration for the backtest fee waterfall.
--
-- A single-row table (id is pinned to 1) holding the four configurable
-- fee parameters backing /fees and the per-backtest fee-waterfall panel:
--   * Leonteq costs:    leonteq_annual_bps (yearly, deducted at year-end)
--                       + transaction_bps (per buy/sell, flat across all
--                         exchanges — replaces the old per-exchange
--                         exchange_fee.fee_bps cost model, which is now
--                         dormant; exchange_fee.is_broker_supported stays
--                         in use for the universe filter).
--   * Bustelberg fees:  bustelberg_mgmt_bps (yearly management fee)
--                       + bustelberg_perf_pct (high-water-mark performance
--                         fee %, crystallized yearly above the running peak).
--
-- The fee model is applied entirely client-side at render (like the
-- previous net-stats), so this table is read on every backtest view and
-- written only from /fees. Mirrors the panel_cache RLS/GRANT convention:
-- deny-all policy + an explicit service_role grant (the backend uses the
-- service key, which both bypasses RLS and needs the table GRANT — see
-- the prod GRANT gotcha).

CREATE TABLE IF NOT EXISTS public.fee_config (
    id integer PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    leonteq_annual_bps numeric(10,4) NOT NULL DEFAULT 35,
    transaction_bps numeric(10,4) NOT NULL DEFAULT 10,
    bustelberg_mgmt_bps numeric(10,4) NOT NULL DEFAULT 100,
    bustelberg_perf_pct numeric(10,4) NOT NULL DEFAULT 10,
    updated_at timestamp with time zone NOT NULL DEFAULT now()
);

ALTER TABLE public.fee_config ENABLE ROW LEVEL SECURITY;
CREATE POLICY fee_config_deny_all ON public.fee_config FOR ALL USING (false);
GRANT SELECT, INSERT, UPDATE, DELETE ON public.fee_config TO service_role;

-- Seed the single config row with the agreed defaults.
INSERT INTO public.fee_config (id) VALUES (1) ON CONFLICT (id) DO NOTHING;
