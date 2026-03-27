from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import duckdb
import pandas as pd

from quick_insight.config.config import settings


def _db_path() -> str:
    p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
    if not p:
        raise RuntimeError("No DuckDB path configured. Expected settings.db_path or settings.duckdb_path.")
    return str(p)


@dataclass(frozen=True)
class PortfolioRepo:
    """
    DuckDB repository for portfolio manager functionality.

    Connection policy
    -----------------
    We always open with read_only=False.  DuckDB only allows one unique
    configuration per file within a process — mixing read_only=True and
    read_only=False on the same file raises a ConnectionException.
    Opening as read_only=False is safe for read queries too, and avoids
    the conflict entirely.
    """

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(_db_path(), read_only=False)

    # ----------------------------
    # Reads
    # ----------------------------
    def load_company_lookup(self) -> pd.DataFrame:
        sql = """
        SELECT
          company_id,
          company_name,
          primary_ticker,
          primary_exchange,
          country,
          sector
        FROM company
        ORDER BY company_name
        """
        con = self.connect()
        try:
            df = con.execute(sql).df()
        finally:
            con.close()

        df = df.fillna("")
        df["label"] = (
            df["company_name"].astype(str)
            + " — "
            + df["primary_ticker"].astype(str)
            + " ("
            + df["primary_exchange"].astype(str)
            + ")"
        )
        return df

    def list_portfolios(self) -> pd.DataFrame:
        sql = """
        SELECT
          p.portfolio_id,
          p.portfolio_name,
          p.snapshot_id,
          s.target_date,
          s.published_at
        FROM portfolio p
        JOIN snapshot s ON s.snapshot_id = p.snapshot_id
        ORDER BY s.target_date DESC, s.published_at DESC, p.portfolio_name ASC
        """
        con = self.connect()
        try:
            return con.execute(sql).df()
        finally:
            con.close()

    def load_portfolio_weights(self, portfolio_id: int) -> pd.DataFrame:
        sql = """
        SELECT
          pw.portfolio_id,
          pw.company_id,
          pw.weight_value,
          c.company_name,
          c.primary_ticker,
          c.primary_exchange
        FROM portfolio_weight pw
        JOIN company c ON c.company_id = pw.company_id
        WHERE pw.portfolio_id = ?
        ORDER BY pw.weight_value DESC
        """
        con = self.connect()
        try:
            return con.execute(sql, [int(portfolio_id)]).df()
        finally:
            con.close()

    # ----------------------------
    # Mutations
    # ----------------------------
    def delete_portfolio(self, portfolio_id: int) -> None:
        con = self.connect()
        try:
            con.begin()
            con.execute("DELETE FROM portfolio_weight WHERE portfolio_id = ?;", [int(portfolio_id)])
            con.commit()

            con.begin()
            try:
                con.execute("DELETE FROM portfolio WHERE portfolio_id = ?;", [int(portfolio_id)])
                con.commit()
            except duckdb.ConstraintException as e:
                try:
                    con.rollback()
                except Exception:
                    pass

                tables = con.execute(
                    """
                    SELECT table_schema, table_name
                    FROM information_schema.columns
                    WHERE column_name = 'portfolio_id'
                    ORDER BY table_schema, table_name
                    """
                ).fetchall()

                blockers: list[str] = []
                for schema, table in tables:
                    if schema == "main" and table == "portfolio":
                        continue
                    cnt = con.execute(
                        f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE portfolio_id = ?;',
                        [int(portfolio_id)],
                    ).fetchone()[0]
                    if int(cnt) > 0:
                        blockers.append(f"{schema}.{table} (rows={int(cnt)})")

                msg = (
                    "Cannot delete portfolio because it is still referenced by:\n"
                    + ("\n".join(f"- {b}" for b in blockers) if blockers else "- (unknown table)")
                    + "\n\nFix: delete from these tables first, or use ON DELETE CASCADE."
                )
                raise RuntimeError(msg) from e
        finally:
            con.close()

    def resolve_snapshot_id(self, *, target_date: date, published_at: date | None = None) -> int:
        con = self.connect()
        try:
            if published_at is None:
                row = con.execute(
                    """
                    SELECT snapshot_id FROM snapshot
                    WHERE target_date = ?
                    ORDER BY published_at DESC
                    LIMIT 1
                    """,
                    [target_date],
                ).fetchone()
                if row is not None:
                    return int(row[0])
                pub = target_date
                con.execute(
                    "INSERT INTO snapshot (target_date, published_at) VALUES (?, ?) ON CONFLICT DO NOTHING",
                    [target_date, pub],
                )
                return int(
                    con.execute(
                        "SELECT snapshot_id FROM snapshot WHERE target_date = ? AND published_at = ?",
                        [target_date, pub],
                    ).fetchone()[0]
                )

            con.execute(
                "INSERT INTO snapshot (target_date, published_at) VALUES (?, ?) ON CONFLICT DO NOTHING",
                [target_date, published_at],
            )
            return int(
                con.execute(
                    "SELECT snapshot_id FROM snapshot WHERE target_date = ? AND published_at = ?",
                    [target_date, published_at],
                ).fetchone()[0]
            )
        finally:
            con.close()

    def upsert_portfolio(self, *, portfolio_name: str, snapshot_id: int) -> int:
        con = self.connect()
        try:
            con.execute(
                """
                INSERT INTO portfolio (portfolio_name, snapshot_id)
                VALUES (?, ?)
                ON CONFLICT (portfolio_name, snapshot_id) DO NOTHING
                """,
                [portfolio_name, int(snapshot_id)],
            )
            pid = con.execute(
                "SELECT portfolio_id FROM portfolio WHERE portfolio_name = ? AND snapshot_id = ?",
                [portfolio_name, int(snapshot_id)],
            ).fetchone()[0]
            return int(pid)
        finally:
            con.close()

    def replace_portfolio_weights(self, *, portfolio_id: int, weights: list[tuple[int, float]]) -> None:
        con = self.connect()
        try:
            con.execute("BEGIN;")
            con.execute("DELETE FROM portfolio_weight WHERE portfolio_id = ?;", [int(portfolio_id)])
            if weights:
                con.executemany(
                    "INSERT INTO portfolio_weight (portfolio_id, company_id, weight_value) VALUES (?, ?, ?);",
                    [(int(portfolio_id), int(cid), float(w)) for cid, w in weights],
                )
            con.execute("COMMIT;")
        except Exception:
            con.execute("ROLLBACK;")
            raise
        finally:
            con.close()