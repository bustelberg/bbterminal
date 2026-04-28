-- Cached snapshots of the momentum strategy's "current picks". A snapshot is
-- created either by a manual click in the UI (triggered_by='manual') or by
-- the weekly Railway cron (triggered_by='auto'). The UI loads the most
-- recent snapshot instantly and offers an MTD-only refresh on top of it.

CREATE SEQUENCE IF NOT EXISTS current_picks_snapshot_id_seq;

CREATE TABLE IF NOT EXISTS current_picks_snapshot (
  snapshot_id        INTEGER PRIMARY KEY DEFAULT nextval('current_picks_snapshot_id_seq'),
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  triggered_by       TEXT NOT NULL CHECK (triggered_by IN ('auto', 'manual')),
  as_of_date         DATE NOT NULL,            -- first of the rebalance month
  latest_price_date  DATE,                     -- most recent price observed when computed
  config             JSONB NOT NULL,           -- the BacktestRequest payload that produced this snapshot
  holdings           JSONB NOT NULL            -- array of MonthlyHolding-shaped objects
);

CREATE INDEX IF NOT EXISTS idx_current_picks_created_at_desc
  ON current_picks_snapshot (created_at DESC);

NOTIFY pgrst, 'reload schema';
