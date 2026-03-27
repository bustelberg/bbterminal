from __future__ import annotations

import json
from pathlib import Path
import pandas as pd


def _norm_str(x: object | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def load_fill_df(fill_path: Path) -> pd.DataFrame:
    data = json.loads(fill_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("fill_ticker.json must be a JSON list")

    rows: list[dict] = []
    for row in data:
        if not isinstance(row, dict):
            continue
        t = _norm_str(row.get("ticker"))
        if not t:
            continue

        cleaned = dict(row)
        cleaned["ticker"] = t
        cleaned["_ticker_upper"] = t.upper()
        rows.append(cleaned)

    df_fill = pd.DataFrame(rows)

    if df_fill.empty:
        return pd.DataFrame(columns=["ticker", "_ticker_upper", "exchange", "primary_ticker", "primary_exchange"])

    dupes = df_fill["_ticker_upper"][df_fill["_ticker_upper"].duplicated()].unique().tolist()
    if dupes:
        raise ValueError(
            "Duplicate ticker(s) in fill_ticker.json (case-insensitive): "
            + ", ".join(dupes)
        )

    return df_fill


def enrich_flattened_df_with_primary_listing(
    df_flat: pd.DataFrame,
    *,
    fill_path: Path | None = None,
    default_exchange: str = "UNKNOWN",
) -> pd.DataFrame:
    if "ticker" not in df_flat.columns:
        raise ValueError("df_flat must contain a 'ticker' column")

    df = df_flat.copy()
    df["ticker"] = df["ticker"].astype("string")

    # ✅ Ensure df has exchange column
    if "exchange" not in df.columns:
        df["exchange"] = pd.Series([None] * len(df), dtype="string")
    else:
        df["exchange"] = df["exchange"].astype("string")

    # ✅ NEW: ensure optional primary_* columns exist (so we can fallback to them)
    if "primary_ticker" not in df.columns:
        df["primary_ticker"] = pd.Series([None] * len(df), dtype="string")
    else:
        df["primary_ticker"] = df["primary_ticker"].astype("string")

    if "primary_exchange" not in df.columns:
        df["primary_exchange"] = pd.Series([None] * len(df), dtype="string")
    else:
        df["primary_exchange"] = df["primary_exchange"].astype("string")

    if fill_path is None:
        HERE = Path(__file__).resolve().parent
        fill_path = HERE / "fill_ticker.json"

    df_fill = load_fill_df(fill_path)

    # Build merge key
    df["_ticker_upper"] = df["ticker"].map(
        lambda x: _norm_str(x).upper() if _norm_str(x) else None
    ).astype("string")

    if not df_fill.empty:
        df_fill2 = df_fill.rename(
            columns={
                "exchange": "exchange_fill",
                "primary_ticker": "primary_ticker_fill",
                "primary_exchange": "primary_exchange_fill",
            }
        )

        df = df.merge(
            df_fill2[["_ticker_upper", "exchange_fill", "primary_ticker_fill", "primary_exchange_fill"]],
            on="_ticker_upper",
            how="left",
        )
    else:
        df["exchange_fill"] = pd.Series([None] * len(df), dtype="string")
        df["primary_ticker_fill"] = pd.Series([None] * len(df), dtype="string")
        df["primary_exchange_fill"] = pd.Series([None] * len(df), dtype="string")

    # 1) exchange(df) overwritten from json.exchange when present
    df["exchange"] = (
        df["exchange_fill"]
        .combine_first(df["exchange"])
        .fillna(default_exchange)
        .astype("string")
    )

    # 2) primary_ticker: json.primary_ticker else df.primary_ticker else df.ticker
    df["primary_ticker"] = (
        df["primary_ticker_fill"]
        .combine_first(df["primary_ticker"])   # ✅ NEW fallback
        .combine_first(df["ticker"])
        .astype("string")
    )

    # 3) primary_exchange:
    #    json.primary_exchange else json.exchange else df.primary_exchange else df.exchange
    df["primary_exchange"] = (
        df["primary_exchange_fill"]
        .combine_first(df["exchange_fill"])
        .combine_first(df["primary_exchange"])  # ✅ NEW fallback
        .combine_first(df["exchange"])
        .fillna(default_exchange)
        .astype("string")
    )

    # Cleanup helper columns
    df = df.drop(columns=["_ticker_upper", "exchange_fill", "primary_ticker_fill", "primary_exchange_fill"])

    return df
