from __future__ import annotations

from dataclasses import dataclass

import duckdb
import pandas as pd

from quick_insight.config.config import settings


def _db_path() -> str:
    p = getattr(settings, "db_path", None) or getattr(settings, "duckdb_path", None)
    if not p:
        raise RuntimeError("No DuckDB path configured.")
    return str(p)


@dataclass(frozen=True)
class CompanyRepo:
    """
    DuckDB repository for company CRUD.
    Always opens read_only=False to avoid connection configuration conflicts.
    """

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(_db_path(), read_only=False)

    # ------------------------------------------------------------------ reads

    def list_companies(self) -> pd.DataFrame:
        sql = """
        SELECT
            company_id,
            company_name,
            primary_ticker,
            primary_exchange,
            longequity_ticker,
            country,
            sector
        FROM company
        ORDER BY company_name
        """
        con = self.connect()
        try:
            return con.execute(sql).df().fillna("")
        finally:
            con.close()

    def company_exists(self, *, primary_ticker: str, primary_exchange: str) -> bool:
        con = self.connect()
        try:
            row = con.execute(
                "SELECT 1 FROM company WHERE primary_ticker = ? AND primary_exchange = ? LIMIT 1",
                [primary_ticker.strip().upper(), primary_exchange.strip().upper()],
            ).fetchone()
            return row is not None
        finally:
            con.close()

    def usage_counts(self, company_id: int) -> dict[str, int]:
        """How many rows in child tables reference this company."""
        con = self.connect()
        try:
            counts = {}
            for table in ("portfolio_weight", "facts_number", "facts_text"):
                n = con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE company_id = ?", [int(company_id)]
                ).fetchone()[0]
                counts[table] = int(n)
            return counts
        finally:
            con.close()


    def list_distinct_exchanges(self) -> list[str]:
        """All distinct non-empty primary_exchange values, sorted."""
        con = self.connect()
        try:
            rows = con.execute(
                """
                SELECT DISTINCT primary_exchange
                FROM company
                WHERE primary_exchange IS NOT NULL AND primary_exchange != ''
                ORDER BY primary_exchange
                """
            ).fetchall()
            return [r[0] for r in rows]
        finally:
            con.close()

    def list_distinct_sectors(self) -> list[str]:
        """All distinct non-empty sector values, sorted."""
        con = self.connect()
        try:
            rows = con.execute(
                """
                SELECT DISTINCT sector
                FROM company
                WHERE sector IS NOT NULL AND sector != ''
                ORDER BY sector
                """
            ).fetchall()
            return [r[0] for r in rows]
        finally:
            con.close()

    def list_distinct_countries(self) -> list[str]:
        """All distinct non-empty country values, sorted."""
        con = self.connect()
        try:
            rows = con.execute(
                """
                SELECT DISTINCT country
                FROM company
                WHERE country IS NOT NULL AND country != ''
                ORDER BY country
                """
            ).fetchall()
            return [r[0] for r in rows]
        finally:
            con.close()

    # ----------------------------------------------------------------- writes

    def add_company(
        self,
        *,
        company_name: str,
        primary_ticker: str,
        primary_exchange: str,
        longequity_ticker: str = "",
        country: str = "",
        sector: str = "",
    ) -> int:
        """Insert and return new company_id."""
        con = self.connect()
        try:
            con.execute(
                """
                INSERT INTO company
                    (company_name, primary_ticker, primary_exchange,
                     longequity_ticker, country, sector)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    company_name.strip() or None,
                    primary_ticker.strip().upper(),
                    primary_exchange.strip().upper(),
                    longequity_ticker.strip() or None,
                    country.strip() or None,
                    sector.strip() or None,
                ],
            )
            row = con.execute(
                "SELECT company_id FROM company WHERE primary_ticker = ? AND primary_exchange = ?",
                [primary_ticker.strip().upper(), primary_exchange.strip().upper()],
            ).fetchone()
            return int(row[0])
        finally:
            con.close()

    def update_company(
        self,
        *,
        company_id: int,
        company_name: str,
        primary_ticker: str,
        primary_exchange: str,
        longequity_ticker: str = "",
        country: str = "",
        sector: str = "",
    ) -> None:
        con = self.connect()
        try:
            con.execute(
                """
                UPDATE company SET
                    company_name      = ?,
                    primary_ticker    = ?,
                    primary_exchange  = ?,
                    longequity_ticker = ?,
                    country           = ?,
                    sector            = ?
                WHERE company_id = ?
                """,
                [
                    company_name.strip() or None,
                    primary_ticker.strip().upper(),
                    primary_exchange.strip().upper(),
                    longequity_ticker.strip() or None,
                    country.strip() or None,
                    sector.strip() or None,
                    int(company_id),
                ],
            )
        finally:
            con.close()

    def delete_company(self, company_id: int) -> None:
        """Raises RuntimeError if child rows still reference this company."""
        con = self.connect()
        try:
            con.begin()
            try:
                con.execute("DELETE FROM company WHERE company_id = ?", [int(company_id)])
                con.commit()
            except duckdb.ConstraintException as e:
                try:
                    con.rollback()
                except Exception:
                    pass
                blockers = []
                for table in ("portfolio_weight", "facts_number", "facts_text"):
                    n = con.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE company_id = ?", [int(company_id)]
                    ).fetchone()[0]
                    if int(n) > 0:
                        blockers.append(f"{table} ({int(n)} rows)")
                msg = (
                    "Cannot delete — company is still referenced by:\n"
                    + ("\n".join(f"  • {b}" for b in blockers) if blockers else "  • (unknown table)")
                    + "\n\nRemove those references first."
                )
                raise RuntimeError(msg) from e
        finally:
            con.close()