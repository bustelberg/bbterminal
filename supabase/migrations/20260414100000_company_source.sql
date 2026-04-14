-- Add source column to track where companies originate from (longequity, sp500, etc.)
ALTER TABLE company ADD COLUMN IF NOT EXISTS source TEXT[] NOT NULL DEFAULT '{}';

-- Backfill: mark all existing companies as longequity (they were all created via LongEquity ingest)
UPDATE company SET source = ARRAY['longequity'] WHERE source = '{}';
