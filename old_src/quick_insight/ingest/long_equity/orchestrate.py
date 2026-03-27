# orchestrate.py
from __future__ import annotations

import re
from datetime import date
from pathlib import Path
import pandas as pd
from quick_insight.ingest.long_equity.acquire import acquire_raw_longequity_backfill
from quick_insight.ingest.long_equity.flatten import flatten_excel
from quick_insight.ingest.long_equity.extend_primary import enrich_flattened_df_with_primary_listing
from quick_insight.ingest.long_equity.transformation import prepare_flattened_for_duckdb_schema
from quick_insight.ingest.long_equity.load_into_db import load_prepared_into_duckdb
from quick_insight.config.config import settings
from quick_insight.db import ensure_schema

def _debug_sector(df: pd.DataFrame, step: str, n: int = 5) -> None:
    print(f"\n[DEBUG] {step}")

    if "sector" not in df.columns:
        print("  ❌ column 'sector' NOT present")
        print(f"  columns: {list(df.columns)}")
        return

    non_null = df["sector"].notna().sum()
    total = len(df)

    print("  ✅ column 'sector' present")
    print(f"  non-null: {non_null}/{total}")

    if non_null > 0:
        print("  sample values:")
        print(df["sector"].dropna().unique()[:n])


_MONTHS = {
    "january": 1,
    "february": 2,
    "march": 3,
    "april": 4,
    "may": 5,
    "june": 6,
    "july": 7,
    "august": 8,
    "september": 9,
    "october": 10,
    "november": 11,
    "december": 12,
}


def _as_of_date_from_filename(path: Path) -> date:
    """
    Extracts Month + Year from filenames like:
      Long-Equity---December-2025---Global-Compounders-Database.xlsx

    Business rule:
      "December 2025" -> as_of_date = 2026-01-01 (first day of next month)
    """
    name = path.name.lower()

    # Find "<month>-<year>" anywhere in the filename
    m = re.search(r"(january|february|march|april|may|june|july|august|september|october|november|december)[-_](\d{4})", name)
    if not m:
        raise ValueError(f"Could not parse month-year from filename: {path.name}")

    month_name = m.group(1)
    year = int(m.group(2))
    month = _MONTHS[month_name]

    # first day of next month
    if month == 12:
        return date(year + 1, 1, 1)
    return date(year, month + 1, 1)



def main() -> None:
    # Ensure schema exists once
    ensure_schema(settings.db_path, settings.schema_path)

    paths = acquire_raw_longequity_backfill()

    # Optional: process oldest -> newest (so history loads in order)
    # If acquire_raw_longequity_backfill already returns sorted, you can remove this.
    paths = sorted(paths, key=lambda p: _as_of_date_from_filename(Path(p)))
    print(f"Paths: {paths}")

    for p in paths:
        print(f'Working on path {p.name}')
        p = Path(p)
        as_of = _as_of_date_from_filename(p)


        df = flatten_excel(excel_path=p, print_preview=False)
        # _debug_sector(df, "after flatten_excel")
        df = enrich_flattened_df_with_primary_listing(df)
        # _debug_sector(df, "after enrich_flattened_df_with_primary_listing")

        # row = df[df["ticker"] == "SE"]

        # print("\n--- ROW FOR SE ---")
        # print(row)


        prepared = prepare_flattened_for_duckdb_schema(
            df,
            as_of_date=as_of,               # <-- derived from filename
            source_code="longequity",
            print_preview=False,
        )

        result = load_prepared_into_duckdb(prepared)
        # print(f"Finished work on {p.name} -> as_of_date={as_of.isoformat()} -> {result}")


if __name__ == "__main__":
    main()
