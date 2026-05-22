-- Index every FK column that doesn't have a leading-column index yet.
-- Supabase linter "Unindexed foreign keys" findings:
--   public.company.exchange_id              → gurufocus_exchange
--   public.gurufocus_exchange.country_code  → country
--   public.gurufocus_exchange.currency_code → currency
--   public.portfolio_weight.portfolio_id    → portfolio
--   public.scheduled_strategy.backtest_run_id → backtest_run
--
-- Why this matters: when the referenced row is deleted or updated, Postgres
-- has to verify no orphan rows exist on the referring side. Without an index
-- on the FK column, that's a seqscan of the whole referring table per delete.
-- Also speeds up application JOINs on these columns.
--
-- IF NOT EXISTS makes it safe to re-run if Supabase's auto-grant trigger or
-- a prior sync already added some of them.

CREATE INDEX IF NOT EXISTS company_exchange_id_idx
    ON public.company (exchange_id);

CREATE INDEX IF NOT EXISTS gurufocus_exchange_country_code_idx
    ON public.gurufocus_exchange (country_code);

CREATE INDEX IF NOT EXISTS gurufocus_exchange_currency_code_idx
    ON public.gurufocus_exchange (currency_code);

CREATE INDEX IF NOT EXISTS portfolio_weight_portfolio_id_idx
    ON public.portfolio_weight (portfolio_id);

CREATE INDEX IF NOT EXISTS scheduled_strategy_backtest_run_id_idx
    ON public.scheduled_strategy (backtest_run_id);

NOTIFY pgrst, 'reload schema';
