"""
portfolio.py
Parse AIRS Excel export and compute YTD returns in EUR.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import pandas as pd


@dataclass
class ParsedHolding:
    holding_name: str
    quantity: Optional[int]
    currency: str
    weight: Optional[float]
    start_value_eur: Optional[float]
    current_value_eur: Optional[float]
    ytd_return_eur: Optional[float]
    ytd_return_pct: Optional[float]
    ytd_return_local_pct: Optional[float]


def parse_airs_excel(file_bytes: bytes) -> list[ParsedHolding]:
    """
    Parse AIRS Excel export and compute YTD return in EUR per holding.
    Weight is computed from Huidige waarde EUR as share of total.
    """
    df = pd.read_excel(BytesIO(file_bytes))

    col_name = "Fondsomschrijving"
    col_qty = "Aantal"
    col_start_eur = "Beginwaarde lopend jaar EUR"
    col_current_eur = "Huidige waarde  EUR"
    col_start_local = "Beginwaarde lopend jaar"
    col_current_local = "Huidige waarde"
    col_ccy = "Valuta"

    required = [col_name, col_start_eur, col_current_eur]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"Excel missing columns: {missing}. "
            f"Found: {list(df.columns)}"
        )

    # Compute weights from current EUR values
    current_eur_series = pd.to_numeric(df[col_current_eur], errors="coerce").fillna(0)
    total_current_eur = float(current_eur_series.sum())

    results: list[ParsedHolding] = []
    for _, row in df.iterrows():
        name = str(row.get(col_name, "")).strip()
        if not name:
            continue

        start_eur = pd.to_numeric(row.get(col_start_eur), errors="coerce")
        current_eur = pd.to_numeric(row.get(col_current_eur), errors="coerce")
        qty = pd.to_numeric(row.get(col_qty), errors="coerce") if col_qty in df.columns else None
        ccy = str(row.get(col_ccy, "")).strip() if col_ccy in df.columns else ""

        start_val = float(start_eur) if pd.notna(start_eur) else None
        current_val = float(current_eur) if pd.notna(current_eur) else None

        # Weight = current EUR value / total current EUR
        weight: Optional[float] = None
        if current_val is not None and total_current_eur > 0:
            weight = round(current_val / total_current_eur, 6)

        # YTD return in EUR
        ytd_eur: Optional[float] = None
        ytd_pct: Optional[float] = None
        if start_val is not None and current_val is not None:
            ytd_eur = round(current_val - start_val, 2)
            if start_val != 0:
                ytd_pct = round((current_val - start_val) / abs(start_val), 6)

        # Currency-neutral return (local currency)
        ytd_local_pct: Optional[float] = None
        if col_start_local in df.columns and col_current_local in df.columns:
            start_local = pd.to_numeric(row.get(col_start_local), errors="coerce")
            current_local = pd.to_numeric(row.get(col_current_local), errors="coerce")
            if pd.notna(start_local) and pd.notna(current_local) and float(start_local) != 0:
                ytd_local_pct = round((float(current_local) - float(start_local)) / abs(float(start_local)), 6)

        results.append(ParsedHolding(
            holding_name=name,
            quantity=int(qty) if pd.notna(qty) else None,
            currency=ccy,
            weight=weight,
            start_value_eur=start_val,
            current_value_eur=current_val,
            ytd_return_eur=ytd_eur,
            ytd_return_pct=ytd_pct,
            ytd_return_local_pct=ytd_local_pct,
        ))

    return results
