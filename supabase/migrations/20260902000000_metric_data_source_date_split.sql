-- `idx_metric_data_source_date (source_code, target_date)` was 555 MB of index serving a leading
-- column with TWO VALUES, and the one query it did serve, it served by walking 188,286 rows.
--
-- ⚠⚠ A LEADING COLUMN WITH TWO VALUES CANNOT NARROW ANYTHING, AND 99.74% OF THIS ONE IS A SINGLE
--   VALUE. Measured on the 70,047,753-row table:
--
--       source_code = 'gurufocus'    69,868,662 rows    99.74%
--       source_code = 'longequity'      179,091 rows     0.26%
--
--   So for every read that matters the index's first column is a no-op, and what the scan is
--   really doing is walking `target_date` order across the whole table.
--
-- ⚠⚠ THE COMMENT ON THE ONE QUERY THAT USED IT WAS WRONG, AND IT WAS WRONG IN THE DIRECTION THAT
--   GETS WORSE EVERY QUARTER. `routers/system.py::latest_price_date` (the /backtest page's default
--   end date) and `routers/momentum/_helpers.py::latest_db_price_date` (the backtest pre-flight
--   gate) both said this index let Postgres "stop at the first close_price row". It does not:
--
--     SELECT target_date FROM metric_data
--      WHERE source_code='gurufocus' AND metric_code='close_price'
--      ORDER BY target_date DESC LIMIT 1;
--
--     Index Scan Backward using idx_metric_data_source_date   1,869.8 ms
--       Filter: metric_code = 'close_price'
--       Rows Removed by Filter: 188,288
--       Buffers: shared hit=26,558 read=8,745
--
--   `metric_code` is not in the index, so it is a FILTER, not a bound. The scan enters at the
--   newest `target_date` in the table and walks backwards until it happens to hit a close.
--
-- ⚠⚠ AND WHAT IT WALKS PAST IS THE FORECASTS, WHICH IS WHY THIS DEGRADES ON ITS OWN. All 188,286
--   of those rows are `is_prediction = true` — analyst estimates carry a target_date in the
--   FUTURE, so they sort ahead of every close that has ever printed. The cost of asking "how
--   current is our price data" is therefore proportional to HOW MANY ESTIMATES WE HOLD, and
--   `benchmark_fundamentals_fill` adds more every quarter. Nothing about the price data changes
--   it. It was ~1.9s when this was written and there is no ceiling on it; the `authenticator`
--   role's statement_timeout is 8s.
--
-- THE REPLACEMENT IS TWO PARTIAL INDEXES, ONE PER REAL QUESTION.
--
--   (1) The price question, answered as an index-only scan of one tuple. `target_date DESC` with
--       both constants baked into the predicate, so there is nothing left to filter:
--
--         Index Only Scan using idx_metric_data_close_price_date    0.07 ms   (5 buffers)
--
--       1,869.8 ms -> 0.07 ms, and it no longer moves when a quarter of estimates lands.
--
--   (2) The prune question. `ingest/prune_companies.py::_load_longequity_metric_company_ids` is
--       the ONLY consumer of a `source_code`-alone filter in the codebase, and it is the 0.26%
--       side — genuinely selective, so it does need an index. Indexing `company_id` (all it
--       reads) makes it index-ONLY:
--
--         SELECT DISTINCT company_id ... WHERE source_code='longequity'
--           was:  23.8 ms   Parallel Index Scan + HashAggregate, 2,446 buffers, 2,446 heap fetches
--           now:  16.6 ms   Index Only Scan,                                        0 heap fetches
--
--         the paged form the code actually issues (LIMIT 1000 OFFSET 178000)
--           was:  69.4 ms   ->  now: 14.2 ms
--
-- ⚠ SPACE: 555 MB -> 89 MB (88 MB + 1,272 kB). A net 466 MB off a database whose indexes already
--   outweigh its heap on this table (7,483 MB heap / 14 GB indexes) — and 466 MB of index that
--   every `metric_data` INSERT was maintaining, on the hottest write path in the app.
--
-- ⚠ NO APPLICATION CODE CHANGES. Both queries keep their existing SQL; only the plan moves. The
--   two `latest_*price_date` comments are corrected in place to describe what actually happens.
--
-- ⚠ VERIFIED THERE IS NO THIRD CONSUMER. Every other `source_code` filter in the backend is
--   paired with `company_id` or `metric_code` (checked across routers/, ingest/, momentum/,
--   index_universe/, asset_pipeline/), and both of those are served as prefixes by
--   `metric_data_pkey` and `idx_metric_data_metric_source_company_date`. The plans above were
--   taken with the old index dropped inside a rolled-back transaction, so they are what this
--   migration actually produces, not a prediction.
--
-- Builds take ~35s each locally on 70M rows. A non-CONCURRENT build briefly locks the table
-- against WRITES (reads are unaffected) — fine for a one-off, but prefer to run it while the
-- ingest pipeline is idle. Zero-write-lock alternative (skip this migration, do it by hand):
-- connect with psql, `SET statement_timeout=0;`, run the three statements below with
-- `CREATE INDEX CONCURRENTLY` / `DROP INDEX CONCURRENTLY` (neither can run inside a migration's
-- transaction), then `supabase migration repair --status applied 20260902000000`.
--
-- IF NOT EXISTS / IF EXISTS make this idempotent.
SET statement_timeout = 0;

-- (1) "What is the newest close we hold?" — one tuple, no filter.
CREATE INDEX IF NOT EXISTS idx_metric_data_close_price_date
    ON public.metric_data (target_date DESC)
    WHERE metric_code = 'close_price' AND source_code = 'gurufocus';

-- (2) "Which companies have any LongEquity row?" — the 0.26% side, index-only.
CREATE INDEX IF NOT EXISTS idx_metric_data_longequity_company
    ON public.metric_data (company_id)
    WHERE source_code = 'longequity';

-- ⚠ DROPPED LAST, so a failure part-way through leaves the old index still doing its job rather
--   than a table with no answer to either question.
DROP INDEX IF EXISTS public.idx_metric_data_source_date;
