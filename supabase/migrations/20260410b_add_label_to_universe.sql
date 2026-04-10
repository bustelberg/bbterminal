-- Add label column to universe_snapshot and update PK
-- Run this if you already have the table without the label column.

-- Step 1: Add column with default
ALTER TABLE universe_snapshot ADD COLUMN IF NOT EXISTS label TEXT NOT NULL DEFAULT 'default';

-- Step 2: Drop old PK and create new one
ALTER TABLE universe_snapshot DROP CONSTRAINT IF EXISTS universe_snapshot_pkey;
ALTER TABLE universe_snapshot ADD PRIMARY KEY (label, target_month, company_id);

-- Step 3: Update index
DROP INDEX IF EXISTS idx_universe_snapshot_month;
CREATE INDEX IF NOT EXISTS idx_universe_snapshot_label_month
  ON universe_snapshot (label, target_month);

-- Step 4: Updated RPC functions
CREATE OR REPLACE FUNCTION universe_month_counts(p_label TEXT)
RETURNS TABLE(month TEXT, total BIGINT, passing BIGINT) AS $$
  SELECT
    target_month AS month,
    COUNT(*) AS total,
    COUNT(*) FILTER (WHERE passes) AS passing
  FROM universe_snapshot
  WHERE label = p_label
  GROUP BY target_month
  ORDER BY target_month;
$$ LANGUAGE sql STABLE;

-- Drop the old no-arg version
DROP FUNCTION IF EXISTS universe_month_counts();

CREATE OR REPLACE FUNCTION universe_labels()
RETURNS TABLE(label TEXT, start_month TEXT, end_month TEXT, month_count BIGINT, avg_passing BIGINT) AS $$
  SELECT
    label,
    MIN(target_month) AS start_month,
    MAX(target_month) AS end_month,
    COUNT(DISTINCT target_month) AS month_count,
    (COUNT(*) FILTER (WHERE passes) / NULLIF(COUNT(DISTINCT target_month), 0)) AS avg_passing
  FROM universe_snapshot
  GROUP BY label
  ORDER BY label;
$$ LANGUAGE sql STABLE;
