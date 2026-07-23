"""
portfolio.py
Parse the AIRS Vermogensoverzicht (VOLK) Excel export.

WHAT AIRS GIVES US PER HOLDING, AND WHAT WE MAKE OF IT
    The sheet carries fourteen columns:

        Fondsomschrijving · Aantal · Kostprijs lopend jaar · Beginwaarde lopend jaar ·
        Beginwaarde lopend jaar EUR · Huidige koers · Huidige waarde · Huidige waarde EUR ·
        Weging · Fondsresultaat · Valutaresultaat · Resultaat in % · Valuta · ISIN-code

⚠ `ISIN-code` IS OPTIONAL AND IS THE MOST VALUABLE COLUMN ON THE SHEET.
    It was switched on in AirSPMS on 2026-07-23; every snapshot taken before that has only
    `Fondsomschrijving`, a NAME. That is why `_airs_holding_isin` exists at all — it recovers
    the identity by fuzzy-matching the name against the Fixed portfolio's positions and then
    price-checking the result, and when the stored model predates a swap that machinery has to
    place a holding it has no position for (measured: four books reported `Invesco Wld EW ETF
    Acc` as DE000A0F5UH1). An ISIN on the book's own row ends all of that: the join is exact.

    It is parsed as OPTIONAL, permanently. An older snapshot must keep parsing, and a portfolio
    whose export does not carry the column must not fail — it falls back to the name route.

    We derive `weight` / `ytd_return_*` from the values ourselves AND carry AIRS's own
    `Weging` / `Resultaat in %` beside them, deliberately unreconciled: two independent
    statements of the same quantity are a cross-check, and collapsing them into one would
    throw away the only evidence that either is right.

⚠ OURS AND AIRS'S ARE 100× APART, AND BOTH ARE NAMED `pct`.
    `ytd_return_pct` is a FRACTION we compute; `airs_result_pct` is AIRS's `Resultaat in %`
    as reported, a PERCENT — and they are the same quantity (the EUR return). Measured on a
    real download (BUS_MTS_OFF_AFS_DYN, row `Visa`): AIRS 11.41 against our
    (38211.21-34298.74)/34298.74 = 0.1141. `Weging` (5.46) vs our `weight` (0.0546) is the
    same trap. NOTHING here rescales either into the other: they are carried side by side
    precisely so the two can be compared, and a reader who sees only one is told which.

⚠ `fund_result_eur` / `fx_result_eur` ARE IN EUR — MEASURED, NOT ASSUMED.
    `Fondsresultaat` + `Valutaresultaat` = the EUR value delta: Visa 3099 + 813.18 =
    3912.18 against 38211.21 - 34298.74 = 3912.47 (to rounding). They are NOT local — the
    local delta is 3553.96, which matches neither leg, so a holding's `Fondsresultaat` is
    its performance measured in euros, not in its own currency.

    They are the prize: the split of a holding's result into PERFORMANCE and FX. Nothing we
    compute can produce it — our `ytd_return_pct` and `ytd_return_local_pct` bracket the FX
    leg but never isolate it.

⚠ A HOLDING'S FIGURE IS NOT THE PORTFOLIO'S. Do not aggregate anything here into a
    portfolio return: these are price returns over a book with deposits and withdrawals,
    and `AITopSelectie OFF DYN` measures -5.85% that way against AIRS's own +46.12%. The
    portfolio return is `airs_performance.cumulatief_rendement`; see `_airs_accounts.py`.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from io import BytesIO
from typing import Optional

import pandas as pd


@dataclass
class ParsedHolding:
    holding_name: str
    # AIRS's own ISIN for the position (`ISIN-code`). None on the cash line, on a pre-2026-07-23
    # snapshot, and on anything that is not a well-formed ISIN — see `_isin`.
    isin: Optional[str]
    quantity: Optional[int]
    currency: str
    weight: Optional[float]
    start_value_eur: Optional[float]
    current_value_eur: Optional[float]
    ytd_return_eur: Optional[float]
    ytd_return_pct: Optional[float]
    ytd_return_local_pct: Optional[float]
    # --- AIRS's own columns, as reported (see the module docstring on units) ---
    cost_basis_local: Optional[float] = None      # Kostprijs lopend jaar
    current_price_local: Optional[float] = None   # Huidige koers
    airs_weight: Optional[float] = None           # Weging
    fund_result_eur: Optional[float] = None       # Fondsresultaat  (performance leg, EUR)
    fx_result_eur: Optional[float] = None         # Valutaresultaat (FX leg, EUR)
    airs_result_pct: Optional[float] = None       # Resultaat in %


def _norm(name: object) -> str:
    """A column header reduced to its comparable form: case- and whitespace-insensitive.

    AIRS ships `Huidige waarde  EUR` with TWO spaces, which the literal header strings this
    parser used to carry matched only by luck. Normalising is what makes that a non-event.
    """
    return re.sub(r"\s+", " ", str(name)).strip().lower()


def _resolve_columns(df: pd.DataFrame) -> dict[str, str]:
    """{normalised header -> the actual column}. First wins, so a duplicate header (pandas
    renames those `X.1`) cannot silently displace the real one."""
    out: dict[str, str] = {}
    for c in df.columns:
        out.setdefault(_norm(c), c)
    return out


def _col(cols: dict[str, str], header: str) -> Optional[str]:
    """The real column for `header`, or None.

    ⚠ EXACT match on the normalised name — never a prefix. `Huidige waarde` and
    `Huidige waarde  EUR` are DIFFERENT columns (local vs EUR), and a `startswith` would
    hand back the EUR one for the local lookup: a silent 1.16× on every USD holding.
    """
    return cols.get(_norm(header))


# AIRS is not consistent about this header across its exports: the model-portfolio sheet says
# `ISINCode`, the Vermogensoverzicht says `ISIN-code`. Both are accepted; `_norm` handles case and
# whitespace, so only the hyphen actually differs.
_ISIN_HEADERS = ("ISIN-code", "ISINCode", "ISIN")

# ISO 6166: two-letter country, nine alphanumerics, one check digit.
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


def _isin(row: pd.Series, col: Optional[str]) -> Optional[str]:
    """AIRS's ISIN for this row, or None.

    ⚠ THE CASH LINE'S EMPTY CELL ARRIVES AS THE STRING `"nan"`, WHICH IS TRUTHY. pandas reads a
    blank as float NaN, `str()` renders it `"nan"`, and every downstream test of "does this row
    have an ISIN" then says yes — the same trap that once counted a cash line as a holding.

    Anything that is not a well-formed ISIN is treated as ABSENT rather than stored: a malformed
    identity is worse than none, because it matches no instrument while looking like an answer,
    and the row would stop falling back to the name route that would have resolved it.
    """
    if not col:
        return None
    v = row.get(col)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip().upper()
    if s in ("", "NAN", "NONE", "NAT"):
        return None
    return s if _ISIN_RE.match(s) else None


def _num(row: pd.Series, col: Optional[str]) -> Optional[float]:
    """`col` off `row` as a float, or None when absent/blank. A 0 is a value, not a gap."""
    if not col:
        return None
    v = pd.to_numeric(row.get(col), errors="coerce")
    return float(v) if pd.notna(v) else None


def parse_airs_excel(file_bytes: bytes) -> list[ParsedHolding]:
    """
    Parse AIRS Excel export and compute YTD return in EUR per holding.
    Weight is computed from Huidige waarde EUR as share of total.
    """
    df = pd.read_excel(BytesIO(file_bytes))
    cols = _resolve_columns(df)

    col_name = _col(cols, "Fondsomschrijving")
    col_qty = _col(cols, "Aantal")
    col_start_eur = _col(cols, "Beginwaarde lopend jaar EUR")
    col_current_eur = _col(cols, "Huidige waarde  EUR")
    col_start_local = _col(cols, "Beginwaarde lopend jaar")
    col_current_local = _col(cols, "Huidige waarde")
    col_ccy = _col(cols, "Valuta")
    # AIRS's own figures — all optional: an older export that lacks them must still parse.
    col_cost_local = _col(cols, "Kostprijs lopend jaar")
    col_price_local = _col(cols, "Huidige koers")
    col_weging = _col(cols, "Weging")
    col_fund_result = _col(cols, "Fondsresultaat")
    col_fx_result = _col(cols, "Valutaresultaat")
    col_result_pct = _col(cols, "Resultaat in %")
    col_isin = next((c for c in (_col(cols, h) for h in _ISIN_HEADERS) if c), None)

    required = {
        "Fondsomschrijving": col_name,
        "Beginwaarde lopend jaar EUR": col_start_eur,
        "Huidige waarde  EUR": col_current_eur,
    }
    missing = [want for want, got in required.items() if not got]
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

        qty = pd.to_numeric(row.get(col_qty), errors="coerce") if col_qty else None
        ccy = str(row.get(col_ccy, "")).strip() if col_ccy else ""

        start_val = _num(row, col_start_eur)
        current_val = _num(row, col_current_eur)

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
        start_local = _num(row, col_start_local)
        current_local = _num(row, col_current_local)
        if start_local is not None and current_local is not None and start_local != 0:
            ytd_local_pct = round((current_local - start_local) / abs(start_local), 6)

        results.append(ParsedHolding(
            holding_name=name,
            isin=_isin(row, col_isin),
            quantity=int(qty) if qty is not None and pd.notna(qty) else None,
            currency=ccy,
            weight=weight,
            start_value_eur=start_val,
            current_value_eur=current_val,
            ytd_return_eur=ytd_eur,
            ytd_return_pct=ytd_pct,
            ytd_return_local_pct=ytd_local_pct,
            # AIRS's own, as reported — never rescaled, never recomputed.
            cost_basis_local=_num(row, col_cost_local),
            current_price_local=_num(row, col_price_local),
            airs_weight=_num(row, col_weging),
            fund_result_eur=_num(row, col_fund_result),
            fx_result_eur=_num(row, col_fx_result),
            airs_result_pct=_num(row, col_result_pct),
        ))

    return results
