# upload_portfolio_helpers/excel_parser.py
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import pandas as pd


def _detect_weight_scale(w: pd.Series) -> str:
    """
    Heuristic:
      - if values look like 0..1 and sum ~ 1 -> "fraction"
      - if values look like 0..100 and sum ~ 100 -> "percent"
      - else: decide by max
    """
    x = pd.to_numeric(w, errors="coerce").dropna()
    if x.empty:
        return "fraction"
    s = float(x.sum())
    mx = float(x.max())

    if mx <= 1.5 and 0.5 <= s <= 1.5:
        return "fraction"
    if mx <= 150 and 50 <= s <= 150:
        return "percent"
    return "percent" if mx > 1.5 else "fraction"


def _to_fraction_weights(w: pd.Series) -> pd.Series:
    scale = _detect_weight_scale(w)
    x = pd.to_numeric(w, errors="coerce")
    if scale == "percent":
        return x / 100.0
    return x


@dataclass(frozen=True)
class ParsedHoldings:
    df: pd.DataFrame  # holding_name, mv_eur, weight_raw, weight(0..1), currency, include, weight_source


def parse_holdings_excel(file_bytes: bytes) -> ParsedHoldings:
    """
    Expected columns (Dutch):
      - Fondsomschrijving
      - Huidige waarde  EUR
      - Weging (optional)
      - Valuta (optional)

    Rules:
      - "Effectenrekening" ignored by default
      - If Weging missing/empty -> compute from mv_eur / sum(mv_eur), excluding Effectenrekening from the sum.
    """
    df = pd.read_excel(BytesIO(file_bytes))
    df = df.copy()

    col_name = "Fondsomschrijving"
    col_mv_eur = "Huidige waarde  EUR"
    col_weight = "Weging"
    col_ccy = "Valuta"

    missing = [c for c in [col_name, col_mv_eur] if c not in df.columns]
    if missing:
        raise ValueError(
            f"Excel mist kolommen: {missing}. Verwacht minimaal: '{col_name}' en '{col_mv_eur}'. "
            "Controleer of je het juiste export-format gebruikt."
        )

    out = pd.DataFrame()
    out["holding_name"] = df[col_name].astype(str).fillna("").str.strip()
    out["currency"] = df[col_ccy].astype(str).fillna("").str.strip() if col_ccy in df.columns else ""

    out["mv_eur"] = pd.to_numeric(df[col_mv_eur], errors="coerce")

    lower = out["holding_name"].str.lower()
    is_effectenrekening = lower.str.contains("effectenrekening", na=False)

    # If Weging exists and has values -> use it (percent or fraction)
    if col_weight in df.columns and pd.to_numeric(df[col_weight], errors="coerce").notna().any():
        out["weight_raw"] = df[col_weight]
        out["weight"] = _to_fraction_weights(out["weight_raw"])
        weight_source = "excel"
    else:
        # Compute from mv_eur, excluding Effectenrekening from denominator and numerator
        mv = out["mv_eur"].copy()
        mv.loc[is_effectenrekening] = 0.0
        total = float(mv.fillna(0).sum())
        out["weight_raw"] = pd.NA
        out["weight"] = mv / total if total > 0 else pd.NA
        weight_source = "computed"

    # Default include logic:
    # - ignore empty names
    # - ignore Effectenrekening by default
    out["include"] = (out["holding_name"].str.len() > 0) & (~is_effectenrekening)

    # Optional heuristic: pre-exclude "fund-like" rows
    fund_terms = [" index", "etf", "tracker", "fonds", "fund", "selection index"]
    out.loc[out["holding_name"].str.lower().str.contains("|".join(fund_terms), na=False), "include"] = False

    out = out[out["holding_name"].str.len() > 0].reset_index(drop=True)
    out["weight_source"] = weight_source
    return ParsedHoldings(df=out)
