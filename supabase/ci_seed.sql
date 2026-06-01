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
