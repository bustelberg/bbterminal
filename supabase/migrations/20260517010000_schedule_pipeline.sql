-- Expand `ingest_run` to back the full scheduled-refresh pipeline
-- (ACWI → prices/volumes → momentum), and add `schedule_config` as a
-- single-row table holding the user's "scheduled strategy" selection.
--
-- The pipeline writes per-phase state back to the same `ingest_run` row
-- so the /schedule page can render one timeline per run with each phase
-- expanding into its own detail panel. `current_phase` tracks where the
-- run is (or where it failed); the per-phase result columns are written
-- as each phase completes:
--
--   acwi_universe_id        FK into `universe` for the ACWI universe row
--                           that was refreshed (the same row /acwi reads)
--   acwi_target_month       YYYY-MM the ACWI snapshot was written for
--   acwi_summary            JSONB {additions, removals, renames} so the
--                           UI can render counts + click-to-expand lists
--   momentum_snapshot_id    FK into `current_picks_snapshot` for the
--                           snapshot the momentum phase produced
--   momentum_summary        JSONB {holdings_count, latest_price_date}
--                           so the UI can show a one-liner without
--                           fetching the snapshot detail
--
-- schedule_config is a singleton (CHECK id=1) so there's only ever one
-- "scheduled strategy" — the user picks which saved backtest run's
-- config the pipeline should use for the momentum compute. NULL means
-- "skip the momentum phase".

ALTER TABLE ingest_run
  ADD COLUMN IF NOT EXISTS current_phase        TEXT,
  ADD COLUMN IF NOT EXISTS acwi_universe_id     INTEGER REFERENCES universe(universe_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS acwi_target_month    VARCHAR,
  ADD COLUMN IF NOT EXISTS acwi_summary         JSONB,
  ADD COLUMN IF NOT EXISTS momentum_snapshot_id INTEGER REFERENCES current_picks_snapshot(snapshot_id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS momentum_summary     JSONB;

CREATE TABLE IF NOT EXISTS schedule_config (
  id                INTEGER PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  selected_run_id   INTEGER REFERENCES backtest_run(run_id) ON DELETE SET NULL,
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Seed the single row so PUT never has to think about insert vs update.
INSERT INTO schedule_config (id, selected_run_id) VALUES (1, NULL)
ON CONFLICT (id) DO NOTHING;

ALTER TABLE schedule_config ENABLE ROW LEVEL SECURITY;

NOTIFY pgrst, 'reload schema';
