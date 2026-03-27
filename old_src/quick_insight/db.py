# src\quick_insight\db.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional
import time

import duckdb


@dataclass(frozen=True)
class DBConfig:
    """Configuration for DuckDB connection and schema initialization."""
    db_path: Path = Path("data/quick_insight.duckdb")
    schema_path: Path = Path("schema/schema.sql")


def connect(
    db_path: Path | str,
    *,
    read_only: bool = False,
    retries: int = 60,
    base_sleep_s: float = 0.5,
) -> duckdb.DuckDBPyConnection:
    """
    Open a DuckDB connection with retry/backoff to handle Windows file locks.

    - Retries for up to `retries` attempts
    - Uses exponential backoff
    - Safe for both ingest (write) and analytics (read-only)
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    last_err: Exception | None = None

    for attempt in range(1, retries + 1):
        try:
            return duckdb.connect(str(db_path), read_only=read_only)

        except Exception as e:
            last_err = e
            msg = str(e).lower()

            lock_error = (
                "being used by another process" in msg
                or "cannot open file" in msg
                or "io error" in msg
            )

            if not lock_error or attempt == retries:
                raise

            # Exponential backoff (capped at 8 seconds)
            sleep_time = min(8.0, base_sleep_s * (1.5 ** (attempt - 1)))

            print(
                f"[duckdb] DB locked (attempt {attempt}/{retries}). "
                f"Retrying in {sleep_time:.2f}s..."
            )

            time.sleep(sleep_time)

    raise RuntimeError(f"Failed to connect to DuckDB after {retries} retries: {last_err}")


def ensure_schema(db_path: Path | str, schema_path: Path | str) -> None:
    """
    Apply schema.sql to the DuckDB database.
    Safe to run multiple times if schema.sql uses IF NOT EXISTS.
    """
    db_path = Path(db_path)
    schema_path = Path(schema_path)

    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found: {schema_path}")

    schema_sql = schema_path.read_text(encoding="utf-8")

    con = connect(db_path)
    try:
        con.execute("PRAGMA enable_progress_bar=false;")
        con.execute("PRAGMA threads=4;")
        con.execute(schema_sql)
    finally:
        con.close()


def execute(
    db_path: Path | str,
    sql: str,
    params: Optional[Iterable[Any]] = None,
) -> None:
    """Convenience helper to execute a statement and close the connection."""
    con = connect(db_path)
    try:
        if params is None:
            con.execute(sql)
        else:
            con.execute(sql, params)
    finally:
        con.close()


def fetch_df(
    db_path: Path | str,
    sql: str,
    params: Optional[Iterable[Any]] = None,
):
    """Convenience helper: return a pandas DataFrame from a query."""
    con = connect(db_path, read_only=True)
    try:
        if params is None:
            return con.execute(sql).df()
        return con.execute(sql, params).df()
    finally:
        con.close()