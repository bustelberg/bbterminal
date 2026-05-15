-- Live-progress columns for ingest_run so the /schedule page can show
-- meaningful per-phase messages between the discrete counter writes.
--
--   current_message   the latest free-text status the active phase is
--                     emitting. ACWI passes its on_progress messages
--                     here; prices uses it to show "X of Y processed";
--                     momentum routes the inner backtest stream's
--                     `progress` event text through so the user sees
--                     "Computing signals for 2025-04…" or similar.
--   companies_total   how many companies the prices phase will walk —
--                     written once at phase start. Lets the UI render
--                     "X of Y processed" without guessing N.

ALTER TABLE ingest_run
  ADD COLUMN IF NOT EXISTS current_message TEXT,
  ADD COLUMN IF NOT EXISTS companies_total INTEGER;

NOTIFY pgrst, 'reload schema';
