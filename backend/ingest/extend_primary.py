from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


def _norm_str(x: object | None) -> str | None:
    if x is None:
        return None
    s = str(x).strip()
    return s or None


def _norm_ticker(t: str) -> str:
    """Normalize a ticker for case-insensitive, dash-tolerant matching.
    Treats NOVO-B and NOVO.B as the same ticker.
    """
    return t.upper().replace("-", ".")


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
        cleaned["_ticker_upper"] = _norm_ticker(t)
        rows.append(cleaned)

    df_fill = pd.DataFrame(rows)

    if df_fill.empty:
        return pd.DataFrame(
            columns=["ticker", "_ticker_upper", "exchange", "primary_ticker", "primary_exchange"]
        )

    dupes = df_fill["_ticker_upper"][df_fill["_ticker_upper"].duplicated()].unique().tolist()
    if dupes:
        raise ValueError(
            "Duplicate ticker(s) in fill_ticker.json (case-insensitive): " + ", ".join(dupes)
        )

    return df_fill


def enrich_flattened_df_with_primary_listing(
    df_flat: pd.DataFrame,
    *,
    fill_path: Path | None = None,
    extra_overrides: list[dict] | None = None,
    default_exchange: str = "UNKNOWN",
) -> pd.DataFrame:
    """
    Enrich df_flat with primary_ticker and primary_exchange.

    Resolution priority (highest → lowest):
      1. extra_overrides  — DB-persisted resolutions (ticker_override table) + OpenFIGI results
      2. fill_ticker.json — manually curated static mappings
      3. existing columns in df_flat
      4. ticker itself as fallback for primary_ticker
      5. "UNKNOWN" as fallback for primary_exchange

    extra_overrides: list of {ticker, primary_ticker, primary_exchange} dicts.
    """
    if "ticker" not in df_flat.columns:
        raise ValueError("df_flat must contain a 'ticker' column")

    df = df_flat.copy()
    df["ticker"] = df["ticker"].astype("string")

    if "exchange" not in df.columns:
        df["exchange"] = pd.Series([None] * len(df), dtype="string")
    else:
        df["exchange"] = df["exchange"].astype("string")

    if "primary_ticker" not in df.columns:
        df["primary_ticker"] = pd.Series([None] * len(df), dtype="string")
    else:
        df["primary_ticker"] = df["primary_ticker"].astype("string")

    if "primary_exchange" not in df.columns:
        df["primary_exchange"] = pd.Series([None] * len(df), dtype="string")
    else:
        df["primary_exchange"] = df["primary_exchange"].astype("string")

    if fill_path is None:
        fill_path = Path(__file__).resolve().parent / "fill_ticker.json"

    df_fill = load_fill_df(fill_path)

    # Merge extra_overrides on top of fill_ticker.json (they take precedence)
    if extra_overrides:
        extra_rows = []
        for row in extra_overrides:
            t = _norm_str(row.get("ticker"))
            if not t:
                continue
            extra_rows.append({
                "ticker": t,
                "_ticker_upper": t.upper(),
                "primary_ticker": _norm_str(row.get("primary_ticker")),
                "primary_exchange": _norm_str(row.get("primary_exchange")),
            })
        if extra_rows:
            df_extra = pd.DataFrame(extra_rows)
            extra_uppers = set(df_extra["_ticker_upper"].tolist())
            df_fill = df_fill[~df_fill["_ticker_upper"].isin(extra_uppers)]
            df_fill = pd.concat([df_fill, df_extra], ignore_index=True)

    df["_ticker_upper"] = df["ticker"].map(
        lambda x: _norm_ticker(_norm_str(x)) if _norm_str(x) else None
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

    df["exchange"] = (
        df["exchange_fill"].combine_first(df["exchange"]).fillna(default_exchange).astype("string")
    )
    df["primary_ticker"] = (
        df["primary_ticker_fill"]
        .combine_first(df["primary_ticker"])
        .combine_first(df["ticker"])
        .astype("string")
    )
    df["primary_exchange"] = (
        df["primary_exchange_fill"]
        .combine_first(df["exchange_fill"])
        .combine_first(df["primary_exchange"])
        .combine_first(df["exchange"])
        .fillna(default_exchange)
        .astype("string")
    )

    df = df.drop(
        columns=["_ticker_upper", "exchange_fill", "primary_ticker_fill", "primary_exchange_fill"]
    )
    return df
