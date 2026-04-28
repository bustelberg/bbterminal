# Database Schema

Current as of the latest migration (`20260423000000_universe_aggregate_views.sql`).

To regenerate this from migrations, walk `supabase/migrations/*.sql` in filename order — later migrations may drop or alter tables created in earlier ones.

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
        integer exchange_id FK
        varchar company_name
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
        serial universe_id PK
        varchar label UK
        varchar description
        integer parent_universe_id FK
        jsonb filter_config
        timestamptz created_at
    }

    universe_membership {
        integer universe_id PK_FK
        integer company_id PK_FK
        varchar target_month PK
        varchar universe_ticker
        varchar sector
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
        serial id PK
        text month
        text region
        integer request_count
    }

    country ||--o{ gurufocus_exchange : "has"
    currency ||--o{ gurufocus_exchange : "trades-in"
    currency ||--o{ fx_rate : "daily-rate"
    gurufocus_exchange ||--o{ company : "listed-on"
    company ||--o{ company_source : "sourced-from"
    company ||--o{ metric_data : "prices-volumes-metrics"
    company ||--o{ portfolio_weight : "in-portfolio"
    portfolio ||--o{ portfolio_weight : "contains"
    universe ||--o{ universe_membership : "defines"
    universe ||--o{ universe : "derived-from"
    company ||--o{ universe_membership : "member-of"
    benchmark ||--o{ benchmark_price : "monthly-price"
```

## Notes on key tables

### `metric_data`
Single time-series store for everything that varies over time per company. Disambiguated by `(company_id, metric_code, source_code, target_date)`.
- **Prices**: `metric_code='close_price'`, `source_code='gurufocus'`
- **Volumes**: `metric_code='volume'`, `source_code='gurufocus'`
- **Derived screening metrics**: `source_code='derived'` (used by derived universes — see `universe.filter_config`)
- Cutoff: no rows stored with `target_date < 1998-01-01`.

### `company`
- Unique on `(gurufocus_ticker, exchange_id)`.
- `exchange_id` may be NULL for unresolved companies.
- The legacy `primary_ticker`, `primary_exchange`, `country`, `sector`, `longequity_ticker`, `source` columns were dropped in `20260418000000_normalized_schema.sql` — all moved to dedicated tables (`gurufocus_exchange`, `company_source`) or the universe membership row's `sector` field.

### `universe` and `universe_membership`
- `universe` rows are named groupings (e.g. `ACWI`, `SP500`, `LongEquity_2025-04`).
- A `universe` row with `parent_universe_id IS NULL` is a base universe.
- A row with `parent_universe_id IS NOT NULL` is a **derived universe**: a per-month subset of the parent filtered against `metric_data` rows where `source_code='derived'`, using thresholds in `filter_config` JSONB.
- `universe_membership` carries the per-month `(universe, company, target_month, sector)` rows.

### `backtest_run`
- `config` JSONB stores the full request payload (signal weights, date range, top_n_sectors, selection_mode, etc.).
- `result` JSONB stores `{summary, monthly_records, universe}` — used to be three separate columns, consolidated in the normalized migration.

### `airs_performance`
Bustelberg AirSPMS broker performance scrape — Dutch column names match the source UI.

### `ticker_override`
OpenFIGI ticker resolutions: `ticker` is the input we tried to look up, the `gurufocus_*` columns are the resolved values.

## Views

| View | Purpose |
|------|---------|
| `universe_stats` | Per-universe summary: month range, total unique tickers. |
| `universe_monthly_counts` | Per-(universe, month) member count. |
| `universe_sector_counts` | Per-(universe, sector) member count, with `(unknown)` sentinel for null/blank sectors. |
| `universe_summary` | Per-universe aggregate: `total_rows`, `unique_companies`, `unique_tickers`, `month_count`, month range. |

## RPC functions

| Function | Returns |
|----------|---------|
| `get_distinct_dates(p_source_code)` | All distinct `target_date` values in `metric_data` for a given source. |
| `get_company_ids_for_date(p_source_code, p_target_date)` | Company IDs with metric data on a specific date. |
| `increment_api_usage(p_month, p_region, p_count)` | Upserts the `api_usage` counter. |
| `merge_company_data(p_from_id, p_to_id)` | Moves non-conflicting `metric_data` rows from one company to another, deletes the rest — used during deduplication. |
| `universe_full_stats()` | One-shot aggregate per universe (total/unique counts, monthly counts, sector counts) used by `/api/universe/labels`. |
