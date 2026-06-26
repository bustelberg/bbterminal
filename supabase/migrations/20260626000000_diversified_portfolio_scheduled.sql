-- Let a diversified_portfolio overlay a LIVE scheduled strategy (not just a
-- frozen backtest), so it can be tracked live and surfaced on /schedule. When
-- `scheduled_strategy_id` is set the portfolio is "scheduled" — its strategy
-- sleeve uses the scheduled strategy's live extended curve; otherwise it stays
-- the on-demand backtest mode (`backtest_run_id`). Exactly one of the two is
-- set. The strategy stays a separate entity — this only ADDS a reference.
ALTER TABLE diversified_portfolio
    ADD COLUMN IF NOT EXISTS scheduled_strategy_id integer
        REFERENCES public.scheduled_strategy(id) ON DELETE CASCADE;

-- backtest_run_id is now optional (a scheduled-mode portfolio references the
-- strategy instead).
ALTER TABLE diversified_portfolio ALTER COLUMN backtest_run_id DROP NOT NULL;

CREATE INDEX IF NOT EXISTS idx_diversified_portfolio_sched
    ON diversified_portfolio (scheduled_strategy_id);

NOTIFY pgrst, 'reload schema';
