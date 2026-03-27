# src/quick_insight/pipeline/load_to_duckdb.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

from quick_insight.config.config import settings
from quick_insight.db import ensure_schema
from quick_insight.ingest.long_equity.transformation import PreparedForSchema


@dataclass(frozen=True)
class LoadResult:
    company_inserted: int
    metric_inserted: int
    snapshot_inserted: int
    source_inserted: int
    facts_number_inserted: int
    facts_text_inserted: int


def _insert_with_count(con: duckdb.DuckDBPyConnection, table: str, sql: str) -> int:
    before = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.execute(sql)
    after = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return int(after - before)


def load_prepared_into_duckdb(
    prepared: PreparedForSchema,
    *,
    db_path: str | Path | None = None,
    schema_path: str | Path | None = None,
) -> LoadResult:
    """
    Load PreparedForSchema into DuckDB and return the number of
    newly inserted rows per table.

    Assumes PreparedForSchema is fully enriched:
      - company has primary_ticker + primary_exchange populated
      - facts staging includes primary_ticker + primary_exchange
    """
    db_path = Path(db_path or settings.db_path).expanduser().resolve()
    schema_path = Path(schema_path or settings.schema_path).expanduser().resolve()

    ensure_schema(str(db_path), str(schema_path))

    con = duckdb.connect(str(db_path))

    try:
        con.execute("BEGIN TRANSACTION;")

        # Register staging views
        con.register("company_staging", prepared.company)
        con.register("metric_staging", prepared.metric)
        con.register("snapshot_staging", prepared.snapshot)
        con.register("source_staging", prepared.source)
        con.register("facts_number_staging", prepared.facts_number)
        con.register("facts_text_staging", prepared.facts_text)

        # COMPANY
        # Schema unique key is (primary_ticker, primary_exchange)
        company_inserted = _insert_with_count(
            con,
            "company",
            """
            INSERT INTO company (
              longequity_ticker,
              primary_ticker,
              primary_exchange,
              country,
              company_name,
              sector
            )
            SELECT
              longequity_ticker,
              primary_ticker,
              primary_exchange,
              country,
              company_name,
              sector
            FROM company_staging
            ON CONFLICT (primary_ticker, primary_exchange) DO NOTHING;
            """,
        )

        # METRIC
        metric_inserted = _insert_with_count(
            con,
            "metric",
            """
            INSERT INTO metric (metric_code, value_type)
            SELECT metric_code, value_type
            FROM metric_staging
            ON CONFLICT (metric_code) DO NOTHING;
            """,
        )

        # SNAPSHOT
        snapshot_inserted = _insert_with_count(
            con,
            "snapshot",
            """
            INSERT INTO snapshot (target_date, published_at)
            SELECT
              CAST(as_of_date AS DATE) AS target_date,
              CAST(as_of_date AS DATE) AS published_at
            FROM snapshot_staging
            ON CONFLICT (target_date, published_at) DO NOTHING;
            """,
        )

        # SOURCE
        source_inserted = _insert_with_count(
            con,
            "source",
            """
            INSERT INTO source (source_code)
            SELECT source_code
            FROM source_staging
            ON CONFLICT (source_code) DO NOTHING;
            """,
        )

        # FACTS_NUMBER
        # Canonical join: (primary_ticker, primary_exchange)
        facts_number_inserted = _insert_with_count(
            con,
            "facts_number",
            """
            INSERT INTO facts_number (
              company_id, metric_id, snapshot_id, source_id, metric_value, is_prediction
            )
            SELECT
              c.company_id,
              m.metric_id,
              s.snapshot_id,
              so.source_id,
              f.metric_value,
              COALESCE(f.is_prediction, FALSE) AS is_prediction
            FROM facts_number_staging f
            JOIN company  c
              ON c.primary_ticker   = f.primary_ticker
             AND c.primary_exchange = f.primary_exchange
            JOIN metric   m  ON m.metric_code = f.metric_code
            JOIN snapshot s
              ON s.target_date  = CAST(f.as_of_date AS DATE)
             AND s.published_at = CAST(f.as_of_date AS DATE)
            JOIN source   so ON so.source_code = f.source_code
            WHERE f.metric_value IS NOT NULL
            ON CONFLICT DO NOTHING;
            """,
        )

        # FACTS_TEXT
        facts_text_inserted = _insert_with_count(
            con,
            "facts_text",
            """
            INSERT INTO facts_text (
              company_id, metric_id, snapshot_id, source_id, metric_value
            )
            SELECT
              c.company_id,
              m.metric_id,
              s.snapshot_id,
              so.source_id,
              f.metric_value
            FROM facts_text_staging f
            JOIN company  c
              ON c.primary_ticker   = f.primary_ticker
             AND c.primary_exchange = f.primary_exchange
            JOIN metric   m  ON m.metric_code = f.metric_code
            JOIN snapshot s
              ON s.target_date  = CAST(f.as_of_date AS DATE)
             AND s.published_at = CAST(f.as_of_date AS DATE)
            JOIN source   so ON so.source_code = f.source_code
            ON CONFLICT DO NOTHING;
            """,
        )

        con.execute("COMMIT;")

        return LoadResult(
            company_inserted=company_inserted,
            metric_inserted=metric_inserted,
            snapshot_inserted=snapshot_inserted,
            source_inserted=source_inserted,
            facts_number_inserted=facts_number_inserted,
            facts_text_inserted=facts_text_inserted,
        )

    except Exception:
        con.execute("ROLLBACK;")
        raise

    finally:
        for name in (
            "company_staging",
            "metric_staging",
            "snapshot_staging",
            "source_staging",
            "facts_number_staging",
            "facts_text_staging",
        ):
            try:
                con.unregister(name)
            except Exception:
                pass
        con.close()
