-- Add `is_broker_supported` to `exchange_fee` — the toggle on /fees that
-- tells the backtest stream whether the broker can actually trade this
-- exchange's listings. Default true so existing rows (and any exchange
-- with no `exchange_fee` row at all) stay supported by default; the user
-- explicitly unchecks the ones their broker can't reach.
--
-- The backtest filter (in `backtest_stream/stream.py`) drops any company
-- whose `gurufocus_exchange.exchange_code` matches a row here with
-- `is_broker_supported = false`. Companies on exchanges that simply have
-- no row in this table are treated as supported (the default).

ALTER TABLE exchange_fee
  ADD COLUMN IF NOT EXISTS is_broker_supported BOOLEAN NOT NULL DEFAULT TRUE;

NOTIFY pgrst, 'reload schema';
