-- Universe snapshot: stores per-company per-month screening results
CREATE TABLE IF NOT EXISTS universe_snapshot (
  label         TEXT    NOT NULL DEFAULT 'default',
  target_month  TEXT    NOT NULL,  -- "YYYY-MM"
  company_id    INTEGER NOT NULL REFERENCES company(company_id) ON DELETE CASCADE,
  total_score   INTEGER NOT NULL DEFAULT 0,
  scores        JSONB   NOT NULL DEFAULT '{}',
  details       JSONB   NOT NULL DEFAULT '{}',
  passes        BOOLEAN NOT NULL DEFAULT FALSE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (label, target_month, company_id)
);

CREATE INDEX IF NOT EXISTS idx_universe_snapshot_label_month
  ON universe_snapshot (label, target_month);

-- RPC to get per-month totals efficiently (scoped by label)
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

-- List all labels with their date ranges and counts
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
