# Database Schema

Current as of migration `20260603000000_fee_config.sql`.

The baseline `20260101000000_initial_schema.sql` is a squashed `pg_dump` of the
full schema; the later-timestamped migrations layer additional columns, tables,
and RPCs on top (their statements are `IF NOT EXISTS`/`CREATE OR REPLACE`, so the
final state is the union). To regenerate this doc, walk `supabase/migrations/*.sql`
in filename order — later migrations may add, alter, or drop objects created
earlier.

All `public` tables have Row-Level Security enabled with a deny-all policy; the
backend reaches them via the Supabase **service key** (bypasses RLS + holds the
table GRANTs). Users never query Postgres directly.

## Entity-Relationship Diagram

```mermaid
erDiagram
    country {
        varchar country_code PK
        varchar country_name
    }

    currency {
        varchar currency_code PK
        varchar currency_name
        varchar source
        double peg_to_usd
    }

    gurufocus_exchange {
        integer exchange_id PK
        varchar exchange_code UK
        varchar exchange_name
        boolean is_us
        varchar country_code FK
        varchar currency_code FK
    }

    fx_rate {
        varchar currency_code PK_FK
        date rate_date PK
        double rate
    }

    company {
        integer company_id PK
        varchar gurufocus_ticker
        varchar company_name
        integer exchange_id FK
        timestamptz delisted_at
        timestamptz gurufocus_lookup_failed_at
        timestamptz out_of_scope_at
        text out_of_scope_reason
    }

    company_source {
        integer company_id PK_FK
        varchar source_code PK
        date first_seen
        timestamptz created_at
    }

    metric_data {
        integer company_id PK_FK
        varchar metric_code PK
        varchar source_code PK
        date target_date PK
        double numeric_value
        varchar text_value
        boolean is_prediction
        timestamp recorded_at
    }

    universe {
        integer universe_id PK
        varchar label UK
        varchar description
        timestamptz created_at
        integer parent_universe_id FK
        jsonb filter_config
        text template_key UK
        timestamptz last_refreshed_at
    }

    universe_membership {
        integer universe_id PK_FK
        integer company_id PK_FK
        varchar target_month PK
        varchar universe_ticker
        varchar sector
        varchar industry
        timestamptz created_at
    }

    portfolio {
        integer portfolio_id PK
        varchar portfolio_name
        date target_date
        date published_at
    }

    portfolio_weight {
        integer portfolio_id PK_FK
        integer company_id PK_FK
        double weight_value
    }

    benchmark {
        integer benchmark_id PK
        varchar ticker UK
        varchar name
        text sector
        timestamptz created_at
    }

    benchmark_price {
        integer benchmark_id PK_FK
        date target_date PK
        double price
    }

    backtest_run {
        integer run_id PK
        varchar name
        timestamptz created_at
        jsonb config
        jsonb result
        text result_path
    }

    backtest_cache {
        integer cache_id PK
        text strategy_hash
        date data_date
        jsonb config
        jsonb payload
        timestamptz created_at
    }

    current_picks_snapshot {
        integer snapshot_id PK
        timestamptz created_at
        text triggered_by
        date as_of_date
        date latest_price_date
        jsonb config
        jsonb holdings
        jsonb daily_picks
        text strategy_hash
        text name
        integer ingest_run_id FK
        integer backtest_run_id FK
        integer scheduled_strategy_id FK
        text kind
        boolean is_backfill
        double period_return_pct
    }

    current_picks_day {
        text strategy_hash PK
        date target_date PK
        date as_of_date
        jsonb holdings
        numeric portfolio_return_pct
        numeric next_day_return_pct
        integer turnover_abs
        numeric turnover_pct
        jsonb config
        timestamptz created_at
    }

    ingest_run {
        integer run_id PK
        text job_name
        text triggered_by
        text status
        timestamptz started_at
        timestamptz finished_at
        text current_phase
        text current_message
        integer companies_processed
        integer companies_total
        integer prices_refreshed
        integer volumes_refreshed
        integer forbidden_count
        integer delisted_count
        integer error_count
        text error_summary
        jsonb templates_summary
        jsonb momentum_summary
    }

    scheduled_strategy {
        integer id PK
        integer backtest_run_id FK
        boolean enabled
        text name
        text frequency
        jsonb config
        date start_date
        timestamptz last_run_at
        timestamptz next_due_at
        text backfill_status
        integer backfill_progress_pct
        text backfill_message
        text backfill_error
        timestamptz backfill_started_at
        timestamptz backfill_finished_at
        timestamptz created_at
        timestamptz updated_at
    }

    exchange_fee {
        varchar exchange_code PK_FK
        numeric fee_bps
        boolean is_broker_supported
        timestamptz updated_at
    }

    fee_config {
        integer id PK
        numeric leonteq_annual_bps
        numeric transaction_bps
        numeric bustelberg_mgmt_bps
        numeric bustelberg_perf_pct
        timestamptz updated_at
    }

    leonteq_equity {
        integer id PK
        text name
        text ticker
        text isin
        text sector
        text industry
        text gurufocus_url
        integer company_id FK
        timestamptz scraped_at
    }

    panel_cache {
        bigserial cache_id PK
        text universe_label
        text index_universe
        date cutoff_date
        jsonb panel_jsonb
        integer n_companies
        timestamptz created_at
        timestamptz updated_at
    }

    ticker_override {
        varchar ticker PK
        varchar gurufocus_ticker
        varchar gurufocus_exchange
        varchar source
        timestamp created_at
    }

    airs_performance {
        text portefeuille PK
        date periode PK
        numeric beginvermogen
        numeric koersresultaat
        numeric opbrengsten
        numeric beleggingsresultaat
        numeric eindvermogen
        numeric rendement
        numeric cumulatief_rendement
        timestamptz fetched_at
    }

    api_usage {
        integer id PK
        text month
        text region
        integer request_count
    }

    country ||--o{ gurufocus_exchange : "has"
    currency ||--o{ gurufocus_exchange : "trades-in"
    currency ||--o{ fx_rate : "daily-rate"
    gurufocus_exchange ||--o{ company : "listed-on"
    gurufocus_exchange ||--o{ exchange_fee : "fee-for"
    company ||--o{ company_source : "sourced-from"
    company ||--o{ metric_data : "prices-volumes-metrics"
    company ||--o{ portfolio_weight : "in-portfolio"
    company ||--o{ universe_membership : "member-of"
    company ||--o{ leonteq_equity : "resolved-to"
    portfolio ||--o{ portfolio_weight : "contains"
    universe ||--o{ universe_membership : "defines"
    universe ||--o{ universe : "derived-from"
    benchmark ||--o{ benchmark_price : "monthly-price"
    backtest_run ||--o{ scheduled_strategy : "scheduled-as"
    backtest_run ||--o{ current_picks_snapshot : "produced-by"
    ingest_run ||--o{ current_picks_snapshot : "produced-during"
    scheduled_strategy ||--o{ current_picks_snapshot : "snapshot-of"
```

`current_picks_day` has no FK — it's keyed by `(strategy_hash, target_date)` and
joined to `current_picks_snapshot` logically via `strategy_hash`, not a constraint.

## Notes on key tables

### `metric_data`
Single time-series store for everything that varies over time per company.
Disambiguated by `(company_id, metric_code, source_code, target_date)`.
- **Prices**: `metric_code='close_price'`, `source_code='gurufocus'`
- **Volumes**: `metric_code='volume'`, `source_code='gurufocus'`
- **Derived screening metrics**: `source_code='derived'` (used by derived universes — see `universe.filter_config`)
- Cutoff: no rows stored with `target_date < 1998-01-01` (`DATA_CUTOFF` in `backend/ingest/constants.py`).
- The `company_id` FK is **not** `ON DELETE CASCADE` — deletion is cascaded manually by the app/prune path.

### `company`
- Unique on `(gurufocus_ticker, exchange_id)`. `exchange_id` may be NULL for unresolved companies.
- Three mutually-exclusive "why this listing has no usable price data" flags, each surfaced as a badge in `/companies`:
  - `delisted_at` — GuruFocus reports the listing as delisted (paywalled on the plan); the listing was real.
  - `gurufocus_lookup_failed_at` — `(ticker, exchange)` doesn't resolve on GuruFocus at all (primary + every fallback returned "Stock not found"); usually a wrong exchange mapping.
  - `out_of_scope_at` (+ `out_of_scope_reason`) — listing exists on a real exchange we deliberately don't cover; tagged via `gf_ticker_overrides.json`. Stays in `company` for visibility but excluded from `universe_membership` and the price phase.

### `universe` and `universe_membership`
- `universe` rows are named groupings. A row with `parent_universe_id IS NULL` is a base universe; a non-NULL parent makes it a **derived universe** (per-month subset of the parent filtered against `metric_data` rows where `source_code='derived'`, using thresholds in `filter_config`).
- `template_key` (UNIQUE) is non-NULL on template-managed canonical universes (ACWI, LEONTEQ) — one self-updating row per template; NULL on user-created criteria universes. `last_refreshed_at` is the cache-invalidation key / HTTP ETag input, set on every `UniverseTemplate.refresh()`.
- `universe_membership` carries the per-month `(universe, company, target_month, sector, industry)` rows.

### `backtest_run`
- `config` JSONB stores the full request payload (signal weights, date range, top_n_sectors, selection_mode, etc.).
- `result` JSONB holds `{summary, monthly_records, universe}` for legacy rows. New saves leave `result` NULL and set `result_path` instead — the blob lives in the `backtest-results` Storage bucket (multi-MB variant bundles were hitting Postgres's `statement_timeout` on insert). Loaders prefer `result_path` when set, else fall back to `result`.

### `backtest_cache`
Cached standard-backtest results keyed by `(strategy_hash, data_date)` — lets repeat backtests against the same data short-circuit the compute.

### `current_picks_snapshot` / `current_picks_day`
- `current_picks_snapshot` — one row per current-picks compute: locked-at-start `holdings` + current-month `daily_picks` blob, tagged with `strategy_hash`. `triggered_by ∈ {auto, manual}`. Pipeline-produced rows also carry `ingest_run_id` + `backtest_run_id` + `scheduled_strategy_id` FKs (so `/schedule`'s per-strategy history is a clean JOIN). `kind ∈ {rebalance, price_update}` (nullable).
- `current_picks_day` — per-trading-day row keyed `(strategy_hash, target_date)`; backs the cross-month "Daily picks history" view and the cache lookup. Only ever gains rows for the current month at compute time — never backfills closed months.

### `ingest_run`
Per-pipeline-run audit row backing `/schedule`. Tracks phase (`current_phase`/`current_message`), per-class counters, and two JSONB arrays: `templates_summary` (one entry per template refresh — additions/removals/renames) and `momentum_summary` (one entry per scheduled strategy run). `status ∈ {running, ok, error}`.

### `scheduled_strategy`
One row per saved backtest pinned to the pipeline's momentum phase. `frequency ∈ {daily, weekly, monthly, bimonthly, quarterly}`; `config` is a full BacktestRequest. `start_date` is the configurable go-live date (red dashed marker on the equity curve + live cutoff for run history; NULL → falls back to `created_at`). The `backfill_*` columns track the one-shot historical backfill job.

### `exchange_fee` / `fee_config`
- `exchange_fee` — per-exchange row keyed by `exchange_code` (FK → `gurufocus_exchange`). `is_broker_supported` drives the universe filter (unsupported exchanges are dropped from the backtest universe). `fee_bps` is the old per-exchange cost model, now **dormant** (superseded by `fee_config.transaction_bps`).
- `fee_config` — single-row table (`id` pinned to 1) holding the global fee-waterfall parameters: `leonteq_annual_bps`, `transaction_bps`, `bustelberg_mgmt_bps`, `bustelberg_perf_pct`. Read on every backtest view, written only from `/fees`; the waterfall is applied client-side.

### `leonteq_equity`
Scraped Leonteq investable-universe rows; `company_id` resolves to `company` (`ON DELETE SET NULL`).

### `panel_cache`
DB-persisted signal-breakdown panels (survives Railway redeploys; shareable across replicas). Keyed by `(universe_label, index_universe, cutoff_date)` via a COALESCE unique index (so NULL+NULL counts as a duplicate). Panels are pure functions of pre-cutoff data, so no TTL — invalidated manually only when a backfill adds pre-cutoff prices.

### `airs_performance`
Bustelberg AirSPMS broker performance scrape — Dutch column names match the source UI.

### `ticker_override`
OpenFIGI ticker resolutions: `ticker` is the input we tried to look up, the `gurufocus_*` columns are the resolved values.

## Views

| View | Type | Purpose |
|------|------|---------|
| `universe_monthly_counts` | view | Per-`(universe, month)` member count. |
| `universe_sector_counts` | view | Per-`(universe, sector)` member count, with `(unknown)` sentinel for null/blank sectors. |
| `universe_summary` | view | Per-universe aggregate: `total_rows`, `unique_companies`, `unique_tickers`, `month_count`, month range. |
| `universe_stats` | materialized view | Per-universe summary (label, description, month range, unique-ticker count). Refreshed explicitly; unique index on `universe_id`. |

## RPC functions

| Function | Returns |
|----------|---------|
| `get_distinct_dates(p_source_code)` | All distinct `target_date` values in `metric_data` for a given source. |
| `get_company_ids_for_date(p_source_code, p_target_date)` | Company IDs with metric data on a specific date. |
| `company_latest_close_price_dates()` | Latest `close_price` `target_date` per company. |
| `company_universe_labels()` | Per-company array of the universe labels it belongs to. |
| `company_flat_price_run(window_days)` | Companies whose latest `window_days` closes are all the identical value (stale/dead-listing or wrong dual-listing signal) — backs `/api/admin/companies/flagged`. |
| `increment_api_usage(p_month, p_region, p_count)` | Upserts the `api_usage` counter. |
| `merge_company_data(p_from_id, p_to_id)` | Moves non-conflicting `metric_data` rows from one company to another, deletes the rest — used during deduplication. |
| `universe_all_companies_ever(p_universe_id)` | Every company ever in a universe + first/last month, months count, `still_current` flag. Backs the template `all-companies.csv` export. |
| `universe_available_months(p_universe_id)` | Distinct `target_month` values for a universe. |
| `universe_full_stats()` | One-shot aggregate per universe (total/unique counts, monthly counts, sector counts) used by `/api/universe/labels`. |

A `set_admin_role_on_signup()` trigger on `auth.users` stamps `app_metadata.role = 'admin'` when a new user's SHA-256(lower(email)) matches a hardcoded admin-hash allowlist (keeps admin emails out of source).
