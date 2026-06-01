-- CI-only seed fixture for the `backend-stack-smoke` job.
--
-- NOT loaded by `supabase db reset` (config.toml seeds only ./seed.sql) —
-- it's applied explicitly by .github/workflows/ci.yml AFTER migrations, so
-- the smoke probe can assert *real* behaviour instead of just 2xx against
-- empty tables. Keep it tiny and self-consistent.
--
-- IDs live in a 900000+ block so they never collide with the sequences.
-- Exchange ids are looked up by code (already seeded by initial_schema)
-- so this stays valid even if the seed's exchange_id numbering changes.

INSERT INTO public.company (company_id, gurufocus_ticker, company_name, exchange_id) VALUES
  (900001, 'AAPL',  'Apple Inc',      (SELECT exchange_id FROM public.gurufocus_exchange WHERE exchange_code = 'NASDAQ')),
  (900002, 'MSFT',  'Microsoft Corp', (SELECT exchange_id FROM public.gurufocus_exchange WHERE exchange_code = 'NASDAQ')),
  (900003, 'NESTE', 'Neste Oyj',      (SELECT exchange_id FROM public.gurufocus_exchange WHERE exchange_code = 'OHEL'));

-- Close prices + volume (source 'gurufocus') across two months. The latest
-- close is 2026-05-01 → drives /api/data/latest-price-date.
INSERT INTO public.metric_data (company_id, metric_code, source_code, target_date, numeric_value) VALUES
  (900001, 'close_price', 'gurufocus', '2026-04-01', 170),
  (900001, 'close_price', 'gurufocus', '2026-05-01', 180),
  (900002, 'close_price', 'gurufocus', '2026-04-01', 410),
  (900002, 'close_price', 'gurufocus', '2026-05-01', 420),
  (900003, 'close_price', 'gurufocus', '2026-04-01', 28),
  (900003, 'close_price', 'gurufocus', '2026-05-01', 29),
  (900001, 'volume', 'gurufocus', '2026-05-01', 1000000),
  (900002, 'volume', 'gurufocus', '2026-05-01', 900000),
  (900003, 'volume', 'gurufocus', '2026-05-01', 500000);

-- LongEquity snapshots: source 'longequity' across two months →
-- /api/longequity/snapshots (get_distinct_dates).
INSERT INTO public.metric_data (company_id, metric_code, source_code, target_date, numeric_value) VALUES
  (900001, 'longequity_score', 'longequity', '2026-04-01', 90),
  (900002, 'longequity_score', 'longequity', '2026-04-01', 85),
  (900001, 'longequity_score', 'longequity', '2026-05-01', 92),
  (900003, 'longequity_score', 'longequity', '2026-05-01', 70);

-- One universe + memberships → /api/companies/memberships,
-- company_universe_labels, universe months/labels.
INSERT INTO public.universe (universe_id, label, template_key) VALUES
  (900001, 'longequity', NULL);
INSERT INTO public.universe_membership (universe_id, company_id, target_month, sector) VALUES
  (900001, 900001, '2026-05', 'Technology'),
  (900001, 900002, '2026-05', 'Technology'),
  (900001, 900003, '2026-05', 'Energy');

-- ── Backtest-engine fixture ────────────────────────────────────────────
-- 6 companies in an ACWI template universe with ~16 months of daily
-- (weekday) close prices + volumes. Distinct per-company linear trends so
-- momentum ranking is deterministic and a real backtest produces a
-- non-degenerate result. All on OHEL (EUR) so no FX conversion is needed.
INSERT INTO public.company (company_id, gurufocus_ticker, company_name, exchange_id)
SELECT 900010 + g, 'BT' || (g + 1), 'Backtest Co ' || (g + 1),
       (SELECT exchange_id FROM public.gurufocus_exchange WHERE exchange_code = 'OHEL')
FROM generate_series(0, 5) AS g;

INSERT INTO public.universe (universe_id, label, template_key) VALUES (900002, 'ACWI', 'ACWI');

-- Membership (2 sectors × 3 companies) for every month the backtest range
-- + lookback could touch.
INSERT INTO public.universe_membership (universe_id, company_id, target_month, sector)
SELECT 900002, 900010 + g, to_char(m, 'YYYY-MM'),
       CASE WHEN g < 3 THEN 'Technology' ELSE 'Energy' END
FROM generate_series(0, 5) AS g,
     generate_series(DATE '2025-06-01', DATE '2026-05-01', INTERVAL '1 month') AS m;

-- Daily close prices: weekdays only, base 100 with a per-company slope
-- (company 900015 climbs fastest → strongest momentum).
INSERT INTO public.metric_data (company_id, metric_code, source_code, target_date, numeric_value)
SELECT 900010 + g, 'close_price', 'gurufocus', d::date,
       round((100.0 * (1 + (0.0005 + g * 0.0002) * (d::date - DATE '2025-01-01')))::numeric, 4)
FROM generate_series(0, 5) AS g,
     generate_series(DATE '2025-01-02', DATE '2026-05-29', INTERVAL '1 day') AS d
WHERE EXTRACT(isodow FROM d) < 6;

INSERT INTO public.metric_data (company_id, metric_code, source_code, target_date, numeric_value)
SELECT 900010 + g, 'volume', 'gurufocus', d::date, 1000000 + g * 100000
FROM generate_series(0, 5) AS g,
     generate_series(DATE '2025-01-02', DATE '2026-05-29', INTERVAL '1 day') AS d
WHERE EXTRACT(isodow FROM d) < 6;
