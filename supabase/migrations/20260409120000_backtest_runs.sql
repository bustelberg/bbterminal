-- ============================================================
-- Backtest runs — save/load momentum backtest results
-- ============================================================

CREATE SEQUENCE IF NOT EXISTS backtest_run_id_seq;

CREATE TABLE IF NOT EXISTS backtest_run (
  run_id          INTEGER PRIMARY KEY DEFAULT nextval('backtest_run_id_seq'),
  name            VARCHAR NOT NULL,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  config          JSONB NOT NULL,       -- {start_date, end_date, signal_weights, top_n_sectors, top_n_per_sector}
  summary         JSONB NOT NULL,       -- {total_return_pct, annualized_return_pct, max_drawdown_pct, ...}
  monthly_records JSONB NOT NULL,       -- [{date, holdings, portfolio_return_pct, cumulative_return_pct}, ...]
  universe        JSONB NOT NULL        -- [{company_id, ticker, exchange, company_name, sector}, ...]
);
