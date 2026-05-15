-- exchange_fee: per-exchange one-way transaction fee (in basis points).
--
-- Backs the /fees page. One row per exchange the user has configured —
-- absence means "no fee" (the GET endpoint left-joins from
-- gurufocus_exchange so every exchange shows up regardless).
--
-- Used by the backtest UI's net-stats math: each holding pays
-- `fee_bps / 10000` on entry (if it's a new entrant vs the previous
-- period) and again on exit (if it doesn't roll into the next period).
-- Open periods never charge sell fee since the position hasn't actually
-- been sold yet.
--
-- `fee_bps NUMERIC(10,4)` allows fractional bps (e.g. 2.5 bps) and
-- generously sized so 100% fees (10,000 bps) and beyond fit if anyone
-- ever wants to model exotic costs. Default 0 so an "unset" row reads
-- as "no fee".

CREATE TABLE IF NOT EXISTS exchange_fee (
  exchange_code VARCHAR PRIMARY KEY
    REFERENCES gurufocus_exchange(exchange_code) ON DELETE CASCADE,
  fee_bps       NUMERIC(10, 4) NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mirror the lockdown applied by 20260501000000_enable_rls_all_public_tables.
-- Service-role bypasses RLS; the anon key is denied by default (zero policies).
ALTER TABLE exchange_fee ENABLE ROW LEVEL SECURITY;

NOTIFY pgrst, 'reload schema';
