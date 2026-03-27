# src/quick_insight/ingest/gurufocus/load_into_db.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb
import pandas as pd

from quick_insight.config.config import settings
from quick_insight.db import ensure_schema, connect


# ---------------------------------------------------------------------------
# Shared result type
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LoadFactsResult:
    metric_inserted: int
    snapshot_inserted: int
    source_inserted: int
    facts_number_inserted: int
    facts_number_updated: int


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _table_cols(con: duckdb.DuckDBPyConnection, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}


def _require_cols(df: pd.DataFrame, required: set[str]) -> None:
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input df is missing required columns: {sorted(missing)}")


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["primary_ticker"] = df["primary_ticker"].astype("string").str.strip()
    df["primary_exchange"] = df["primary_exchange"].astype("string").str.strip()
    df["metric_code"] = df["metric_code"].astype("string").str.strip()
    df["source_code"] = df["source_code"].astype("string").str.strip()

    df["target_date"] = pd.to_datetime(df["target_date"], errors="coerce").dt.date
    df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce").dt.date
    df["imported_at"] = pd.to_datetime(df["imported_at"], errors="coerce")

    df["value"] = pd.to_numeric(df["value"], errors="coerce")

    if not pd.api.types.is_bool_dtype(df["is_prediction"]):
        truthy = {"true", "1", "yes", "y", "t"}
        falsy = {"false", "0", "no", "n", "f"}
        lowered = df["is_prediction"].astype("string").str.strip().str.lower()
        df["is_prediction"] = lowered.map(
            lambda x: True if x in truthy else False if x in falsy else pd.NA
        )

    df["is_prediction"] = df["is_prediction"].fillna(False).astype("bool")

    return df


def _dedupe_input(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate within the incoming batch so that each business key appears once.

    We keep the latest imported_at row per:
      (primary_ticker, primary_exchange, metric_code,
       target_date, published_at, source_code, is_prediction)
    """
    key_cols = [
        "primary_ticker",
        "primary_exchange",
        "metric_code",
        "target_date",
        "published_at",
        "source_code",
        "is_prediction",
    ]

    df = df.sort_values(
        by=key_cols + ["imported_at"],
        kind="mergesort",
        na_position="last",
    )

    return df.drop_duplicates(subset=key_cols, keep="last").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def load_facts_df_into_duckdb(
    df: pd.DataFrame,
    *,
    db_path: str | Path | None = None,
    schema_path: str | Path | None = None,
    require_company_exists: bool = True,
) -> LoadFactsResult:
    """
    Load a long-format facts DataFrame into DuckDB.

    Expected columns
    ----------------
    primary_ticker, primary_exchange,
    metric_code, target_date, published_at, imported_at,
    source_code, value, is_prediction

    Behaviour
    ---------
    - Upserts METRIC, SOURCE, SNAPSHOT
    - Inserts new FACTS_NUMBER rows
    - Updates existing FACTS_NUMBER rows when values changed
    - No binary-search smart loading; all provided rows are considered
    - Uses in-memory registered DataFrames as staging relations
    """
    _require_cols(df, {
        "primary_ticker",
        "primary_exchange",
        "metric_code",
        "target_date",
        "published_at",
        "imported_at",
        "source_code",
        "value",
        "is_prediction",
    })

    df = _normalize_df(df)
    df = _dedupe_input(df)

    required_non_null = [
        "primary_ticker",
        "primary_exchange",
        "metric_code",
        "target_date",
        "published_at",
        "source_code",
    ]
    bad_mask = df[required_non_null].isna().any(axis=1)
    if bad_mask.any():
        bad_count = int(bad_mask.sum())
        raise ValueError(
            f"Input df contains {bad_count} rows with nulls in required business-key columns: "
            f"{required_non_null}"
        )

    db_path = Path(db_path or settings.db_path).expanduser().resolve()
    schema_path = Path(schema_path or settings.schema_path).expanduser().resolve()

    ensure_schema(db_path, schema_path)
    con = connect(db_path, read_only=False)

    staging_names = [
        "metric_staging",
        "source_staging",
        "snapshot_staging",
        "facts_number_staging",
        "facts_number_resolved",
        "facts_number_to_update",
        "facts_number_to_insert",
    ]

    try:
        facts_number_cols = _table_cols(con, "facts_number")
        snapshot_cols = _table_cols(con, "snapshot")
        company_cols = _table_cols(con, "company")

        facts_value_col = (
            "metric_value" if "metric_value" in facts_number_cols else
            "value" if "value" in facts_number_cols else
            None
        )
        if facts_value_col is None:
            raise RuntimeError(
                "facts_number table has neither 'metric_value' nor 'value' column."
            )

        facts_has_is_prediction = "is_prediction" in facts_number_cols
        snapshot_has_imported_at = "imported_at" in snapshot_cols

        for col in ("primary_ticker", "primary_exchange"):
            if col not in company_cols:
                raise RuntimeError(
                    f"company table is missing column '{col}'. "
                    "Please migrate company table to use primary_ticker/primary_exchange."
                )

        in_txn = False
        try:
            con.execute("BEGIN TRANSACTION;")
            in_txn = True

            # ------------------------------------------------------------------
            # Build staging relations
            # ------------------------------------------------------------------
            metric_staging = (
                df[["metric_code"]]
                .drop_duplicates()
                .assign(value_type="number")
                .reset_index(drop=True)
            )

            source_staging = (
                df[["source_code"]]
                .drop_duplicates()
                .reset_index(drop=True)
            )

            snapshot_cols_needed = ["target_date", "published_at"] + (
                ["imported_at"] if snapshot_has_imported_at else []
            )
            snapshot_staging = (
                df[snapshot_cols_needed]
                .drop_duplicates()
                .reset_index(drop=True)
            )

            facts_number_staging = df[[
                "primary_ticker",
                "primary_exchange",
                "metric_code",
                "target_date",
                "published_at",
                "source_code",
                "value",
                "is_prediction",
            ]].copy()

            facts_number_staging = facts_number_staging[
                facts_number_staging["value"].notna()
            ].reset_index(drop=True)

            con.register("metric_staging", metric_staging)
            con.register("source_staging", source_staging)
            con.register("snapshot_staging", snapshot_staging)
            con.register("facts_number_staging", facts_number_staging)

            # ------------------------------------------------------------------
            # Dimension inserts with explicit counts
            # ------------------------------------------------------------------
            metric_inserted = int(con.execute("""
                SELECT COUNT(*)
                FROM metric_staging ms
                LEFT JOIN metric m
                  ON m.metric_code = ms.metric_code
                WHERE m.metric_id IS NULL;
            """).fetchone()[0])

            if metric_inserted > 0:
                con.execute("""
                    INSERT INTO metric (metric_code, value_type)
                    SELECT ms.metric_code, ms.value_type
                    FROM metric_staging ms
                    LEFT JOIN metric m
                      ON m.metric_code = ms.metric_code
                    WHERE m.metric_id IS NULL;
                """)

            source_inserted = int(con.execute("""
                SELECT COUNT(*)
                FROM source_staging ss
                LEFT JOIN source s
                  ON s.source_code = ss.source_code
                WHERE s.source_id IS NULL;
            """).fetchone()[0])

            if source_inserted > 0:
                con.execute("""
                    INSERT INTO source (source_code)
                    SELECT ss.source_code
                    FROM source_staging ss
                    LEFT JOIN source s
                      ON s.source_code = ss.source_code
                    WHERE s.source_id IS NULL;
                """)

            snapshot_inserted = int(con.execute("""
                SELECT COUNT(*)
                FROM snapshot_staging st
                LEFT JOIN snapshot s
                  ON s.target_date = CAST(st.target_date AS DATE)
                 AND s.published_at = CAST(st.published_at AS DATE)
                WHERE s.snapshot_id IS NULL;
            """).fetchone()[0])

            if snapshot_inserted > 0:
                if snapshot_has_imported_at:
                    con.execute("""
                        INSERT INTO snapshot (target_date, published_at, imported_at)
                        SELECT st.target_date, st.published_at, st.imported_at
                        FROM snapshot_staging st
                        LEFT JOIN snapshot s
                          ON s.target_date = CAST(st.target_date AS DATE)
                         AND s.published_at = CAST(st.published_at AS DATE)
                        WHERE s.snapshot_id IS NULL;
                    """)
                else:
                    con.execute("""
                        INSERT INTO snapshot (target_date, published_at)
                        SELECT st.target_date, st.published_at
                        FROM snapshot_staging st
                        LEFT JOIN snapshot s
                          ON s.target_date = CAST(st.target_date AS DATE)
                         AND s.published_at = CAST(st.published_at AS DATE)
                        WHERE s.snapshot_id IS NULL;
                    """)

            # ------------------------------------------------------------------
            # Optional company existence guard
            # ------------------------------------------------------------------
            if require_company_exists:
                missing = con.execute("""
                    SELECT COUNT(*)
                    FROM (
                        SELECT DISTINCT primary_ticker, primary_exchange
                        FROM facts_number_staging
                    ) f
                    LEFT JOIN company c
                      ON c.primary_ticker = f.primary_ticker
                     AND c.primary_exchange = f.primary_exchange
                    WHERE c.company_id IS NULL;
                """).fetchone()[0]

                if int(missing) > 0:
                    raise ValueError(
                        f"{missing} (primary_ticker, primary_exchange) combos not found in COMPANY. "
                        "Insert companies first or pass require_company_exists=False."
                    )

            # ------------------------------------------------------------------
            # Resolve business keys -> surrogate keys once
            # ------------------------------------------------------------------
            is_pred_select = (
                "CAST(f.is_prediction AS BOOLEAN) AS is_prediction"
                if facts_has_is_prediction
                else "FALSE AS is_prediction"
            )

            con.execute(f"""
                CREATE TEMP TABLE facts_number_resolved AS
                SELECT
                    c.company_id,
                    m.metric_id,
                    s.snapshot_id,
                    so.source_id,
                    CAST(f.value AS DOUBLE) AS incoming_value,
                    {is_pred_select}
                FROM facts_number_staging f
                JOIN company c
                  ON c.primary_ticker = f.primary_ticker
                 AND c.primary_exchange = f.primary_exchange
                JOIN metric m
                  ON m.metric_code = f.metric_code
                JOIN snapshot s
                  ON s.target_date = CAST(f.target_date AS DATE)
                 AND s.published_at = CAST(f.published_at AS DATE)
                JOIN source so
                  ON so.source_code = f.source_code;
            """)

            # ------------------------------------------------------------------
            # Identify rows to update
            # ------------------------------------------------------------------
            if facts_has_is_prediction:
                con.execute(f"""
                    CREATE TEMP TABLE facts_number_to_update AS
                    SELECT
                        r.company_id,
                        r.metric_id,
                        r.snapshot_id,
                        r.source_id,
                        r.incoming_value,
                        r.is_prediction
                    FROM facts_number_resolved r
                    JOIN facts_number fn
                      ON fn.company_id = r.company_id
                     AND fn.metric_id = r.metric_id
                     AND fn.snapshot_id = r.snapshot_id
                     AND fn.source_id = r.source_id
                    WHERE
                        fn.{facts_value_col} IS DISTINCT FROM r.incoming_value
                        OR fn.is_prediction IS DISTINCT FROM r.is_prediction;
                """)
            else:
                con.execute(f"""
                    CREATE TEMP TABLE facts_number_to_update AS
                    SELECT
                        r.company_id,
                        r.metric_id,
                        r.snapshot_id,
                        r.source_id,
                        r.incoming_value
                    FROM facts_number_resolved r
                    JOIN facts_number fn
                      ON fn.company_id = r.company_id
                     AND fn.metric_id = r.metric_id
                     AND fn.snapshot_id = r.snapshot_id
                     AND fn.source_id = r.source_id
                    WHERE fn.{facts_value_col} IS DISTINCT FROM r.incoming_value;
                """)

            facts_number_updated = int(con.execute("""
                SELECT COUNT(*) FROM facts_number_to_update;
            """).fetchone()[0])

            if facts_number_updated > 0:
                if facts_has_is_prediction:
                    con.execute(f"""
                        UPDATE facts_number AS fn
                        SET
                            {facts_value_col} = u.incoming_value,
                            is_prediction = u.is_prediction
                        FROM facts_number_to_update u
                        WHERE fn.company_id = u.company_id
                          AND fn.metric_id = u.metric_id
                          AND fn.snapshot_id = u.snapshot_id
                          AND fn.source_id = u.source_id;
                    """)
                else:
                    con.execute(f"""
                        UPDATE facts_number AS fn
                        SET {facts_value_col} = u.incoming_value
                        FROM facts_number_to_update u
                        WHERE fn.company_id = u.company_id
                          AND fn.metric_id = u.metric_id
                          AND fn.snapshot_id = u.snapshot_id
                          AND fn.source_id = u.source_id;
                    """)

            # ------------------------------------------------------------------
            # Identify rows to insert
            # ------------------------------------------------------------------
            if facts_has_is_prediction:
                con.execute("""
                    CREATE TEMP TABLE facts_number_to_insert AS
                    SELECT
                        r.company_id,
                        r.metric_id,
                        r.snapshot_id,
                        r.source_id,
                        r.incoming_value,
                        r.is_prediction
                    FROM facts_number_resolved r
                    LEFT JOIN facts_number fn
                      ON fn.company_id = r.company_id
                     AND fn.metric_id = r.metric_id
                     AND fn.snapshot_id = r.snapshot_id
                     AND fn.source_id = r.source_id
                    WHERE fn.company_id IS NULL;
                """)
            else:
                con.execute("""
                    CREATE TEMP TABLE facts_number_to_insert AS
                    SELECT
                        r.company_id,
                        r.metric_id,
                        r.snapshot_id,
                        r.source_id,
                        r.incoming_value
                    FROM facts_number_resolved r
                    LEFT JOIN facts_number fn
                      ON fn.company_id = r.company_id
                     AND fn.metric_id = r.metric_id
                     AND fn.snapshot_id = r.snapshot_id
                     AND fn.source_id = r.source_id
                    WHERE fn.company_id IS NULL;
                """)

            facts_number_inserted = int(con.execute("""
                SELECT COUNT(*) FROM facts_number_to_insert;
            """).fetchone()[0])

            if facts_number_inserted > 0:
                if facts_has_is_prediction:
                    con.execute(f"""
                        INSERT INTO facts_number (
                            company_id,
                            metric_id,
                            snapshot_id,
                            source_id,
                            {facts_value_col},
                            is_prediction
                        )
                        SELECT
                            company_id,
                            metric_id,
                            snapshot_id,
                            source_id,
                            incoming_value,
                            is_prediction
                        FROM facts_number_to_insert;
                    """)
                else:
                    con.execute(f"""
                        INSERT INTO facts_number (
                            company_id,
                            metric_id,
                            snapshot_id,
                            source_id,
                            {facts_value_col}
                        )
                        SELECT
                            company_id,
                            metric_id,
                            snapshot_id,
                            source_id,
                            incoming_value
                        FROM facts_number_to_insert;
                    """)

            con.execute("COMMIT;")
            in_txn = False

            return LoadFactsResult(
                metric_inserted=metric_inserted,
                snapshot_inserted=snapshot_inserted,
                source_inserted=source_inserted,
                facts_number_inserted=facts_number_inserted,
                facts_number_updated=facts_number_updated,
            )

        except Exception:
            if in_txn:
                try:
                    con.execute("ROLLBACK;")
                except Exception:
                    pass
            raise

    finally:
        for name in staging_names:
            try:
                con.unregister(name)
            except Exception:
                pass
        con.close()