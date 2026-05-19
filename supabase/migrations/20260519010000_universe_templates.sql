-- Universe templates: rebuild the universe model around per-type
-- "template" classes (one canonical row per template, continuously
-- updated by the pipeline) instead of user-created, frozen-in-time
-- snapshots. ACWI is the first template; SP500 stays on the old model
-- for now and will be migrated separately.
--
-- Schema changes:
--   1. `universe.template_key` — non-NULL on template-managed universes
--      (one canonical row per template_key, e.g. 'ACWI'). NULL for user-
--      created criteria-screened universes (those still exist for the
--      /universe page).
--   2. `ingest_run.templates_summary` (JSONB array) — replaces
--      `acwi_summary` (singular object). Each entry holds one template's
--      per-run diff: {template_key, universe_id, this_month, prev_month,
--      additions_count, removals_count, renames_count, additions[],
--      removals[], renames[]}. With only one template today the array
--      has one entry; when SP500 joins the abstraction it'll have two.
--   3. `ingest_run.acwi_universe_id`, `acwi_target_month`, `acwi_summary`
--      dropped — superseded by per-entry fields in `templates_summary`.
--
-- Data deletion (per design decision: "delete old, start fresh"):
--   * All existing ACWI universes (`label ILIKE '%ACWI%'`) and their
--     `universe_membership` (FK cascade).
--   * All `backtest_run` rows whose config references one of those
--     labels in `index_universe`. Cascades to `scheduled_strategy`;
--     `current_picks_snapshot.backtest_run_id` is `ON DELETE SET NULL`
--     so old snapshots survive but lose attribution.
--   * `templates_summary` is left empty for existing pipeline runs —
--     a one-time data loss of the per-run ACWI diff JSON for older runs.
--     Acceptable: the pipeline runs themselves remain in the audit log,
--     just without the inline diff. New runs after this migration will
--     populate `templates_summary` properly.

-- ── 1. universe.template_key ───────────────────────────────────────
ALTER TABLE universe
  ADD COLUMN IF NOT EXISTS template_key TEXT UNIQUE;

COMMENT ON COLUMN universe.template_key IS
  'Non-NULL on template-managed universes (one canonical row per template_key, '
  'self-updating via the pipeline). NULL on user-created criteria universes.';

-- ── 2. ingest_run.templates_summary ────────────────────────────────
ALTER TABLE ingest_run
  ADD COLUMN IF NOT EXISTS templates_summary JSONB;

COMMENT ON COLUMN ingest_run.templates_summary IS
  'Array of per-template refresh results: '
  '[{template_key, universe_id, this_month, prev_month, '
  'additions_count, removals_count, renames_count, '
  'additions[], removals[], renames[]}]. '
  'Empty array on runs with no enabled templates.';

-- ── 3. Delete old ACWI data ────────────────────────────────────────
-- Order matters: delete backtest_run rows BEFORE the universe rows so
-- the cascade through scheduled_strategy fires cleanly.
DELETE FROM backtest_run
  WHERE config->>'index_universe' ILIKE '%ACWI%';

DELETE FROM universe
  WHERE label ILIKE '%ACWI%';
  -- universe_membership cascades via FK ON DELETE CASCADE (assumed
  -- from existing schema — if a FK is RESTRICT instead, this DELETE
  -- will surface a clear error that's easy to fix.

-- ── 4. Drop legacy ACWI-specific columns on ingest_run ─────────────
-- Their content is superseded by entries in `templates_summary` (which
-- carries `universe_id`, `this_month`, and the diff fields per template).
ALTER TABLE ingest_run DROP COLUMN IF EXISTS acwi_universe_id;
ALTER TABLE ingest_run DROP COLUMN IF EXISTS acwi_target_month;
ALTER TABLE ingest_run DROP COLUMN IF EXISTS acwi_summary;

NOTIFY pgrst, 'reload schema';
