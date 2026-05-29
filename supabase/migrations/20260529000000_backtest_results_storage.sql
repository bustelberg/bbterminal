-- Move backtest result blobs out of the JSONB column and into a Supabase
-- Storage bucket. Multi-MB variant bundles were hitting Postgres's
-- statement_timeout on insert (postgrest.exceptions.APIError 57014 —
-- "canceling statement due to statement timeout") because the JSONB
-- column was the wrong home for what's effectively a serialized file.
--
-- Path forward (handled in backend/routers/momentum/backtest_crud.py):
--   * save: upload {result_blob}.json to `backtest-results/{uuid}.json`,
--     insert the row with `result_path` set and `result` left NULL.
--   * load: when `result_path` is populated, download from Storage;
--     otherwise fall back to the in-row `result` JSONB (legacy rows
--     stay readable without a one-off migration).
--   * delete: best-effort remove the Storage object alongside the row.
--
-- Both columns coexist intentionally — pre-migration rows keep their
-- in-row blob; new saves use Storage. No data movement required.

ALTER TABLE backtest_run
  ADD COLUMN IF NOT EXISTS result_path TEXT;

COMMENT ON COLUMN backtest_run.result_path IS
  'Bucket-relative path (e.g. "a1b2c3.json") inside the backtest-results '
  'Storage bucket. When set, supersedes the in-row `result` JSONB. NULL on '
  'legacy rows whose result still lives in `result`.';

-- Private bucket — only the backend (service_role) reads/writes via the
-- Storage REST API. Mirrors the `gurufocus-raw` bucket pattern.
INSERT INTO storage.buckets (id, name, public)
VALUES ('backtest-results', 'backtest-results', false)
ON CONFLICT (id) DO NOTHING;

NOTIFY pgrst, 'reload schema';
