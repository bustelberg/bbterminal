-- Saved, named "diversified portfolio" — an allocation overlay built ON TOP OF
-- a saved momentum backtest: hold the strategy at a target weight, plus
-- diversifier funds (ETFs/bonds from `benchmark`) at their own weights, each
-- with a rebalance band. A SEPARATE entity from `scheduled_strategy` (which is a
-- pure momentum strategy) so it composes with — rather than mutates — the
-- momentum scheduling, risk metrics, and IBKR path. Evaluated on-demand for now
-- (no pipeline/cron): the /portfolios/{id}/state endpoint re-runs the band
-- simulation to report current drifted weights + whether a rebalance is due.
CREATE TABLE IF NOT EXISTS diversified_portfolio (
    id                  bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name                text NOT NULL,
    backtest_run_id     integer NOT NULL REFERENCES public.backtest_run(run_id) ON DELETE CASCADE,
    variant_key         text,                          -- for variant-bundle backtests
    risk_free_rate_pct  double precision NOT NULL DEFAULT 0,
    -- [{"benchmark_id": int | null (null = the strategy), "weight_pct", "band_pct"}]
    holdings            jsonb NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_diversified_portfolio_run
    ON diversified_portfolio (backtest_run_id);

-- RLS: deny-all (the backend uses the service key, which bypasses) — matches the
-- project default so the table isn't readable via the anon key.
ALTER TABLE diversified_portfolio ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS diversified_portfolio_deny_all ON diversified_portfolio;
CREATE POLICY diversified_portfolio_deny_all ON diversified_portfolio FOR ALL USING (false);

GRANT SELECT, INSERT, UPDATE, DELETE ON diversified_portfolio TO service_role;

NOTIFY pgrst, 'reload schema';
