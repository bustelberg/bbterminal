-- Per-snapshot period return — the % gain of the holdings during the
-- period that ENDS at this snapshot. For:
--   rebalance snapshots from the backfill: the gain of the picks
--     chosen at this rebalance, measured from this rebalance to the
--     next one (i.e. the forward-looking period return the backtest
--     engine already computes for each PeriodRecord).
--   price_update snapshots (live pipeline): the running gain of the
--     last rebalance's holdings, measured from rebalance to this
--     tick's close.
--
-- Nullable: legacy snapshots and any future shape we haven't computed
-- a return for stay NULL — the UI just renders "—" then.

ALTER TABLE current_picks_snapshot
  ADD COLUMN IF NOT EXISTS period_return_pct DOUBLE PRECISION;

NOTIFY pgrst, 'reload schema';
