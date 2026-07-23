"""The AIRS `MODEL` report — a dynamic portfolio's OWN model weights.

WHY THIS REPLACES THE FIXED↔DYNAMIC PAIRING
    A book (`*_Dyn`) carries quantities and values; its strategy's weights used to live in a
    SEPARATE AirSPMS portfolio (`*_FX`/`*_AFS`), and everything hung on pairing the two — a
    pairing that was a name guess on 27 of 28 accounts, with a mis-pairing filing a book's money
    under another strategy's name and nothing else on the row looking wrong.

    `rapport_types=MODEL` (the same dropdown as VOLK/ATT/MUT) is that pairing made unnecessary:
    it is scoped to ONE dynamic portfolio and returns its model weights directly. Discovered by
    probe 2026-07-23 — like MUT, a wrong code returns ZERO bytes rather than an error.

THE SHEET, MEASURED ON BUS_Neutraal_Dyn (42 rows)
    Fondsomschrijving · Model percentage · Werkelijk percentage · Afwijking percentage ·
    Afwijking in euro · Kopen · Verkopen · Waarde volgens model · Koers in locale valuta ·
    Geschat orderbedrag

    `Model percentage` sums to EXACTLY 100.000 on every book tried — so it is the full model, not
    a subset, and a sum that is not ~100 means the download was partial.

⚠ THERE IS NO ISIN ON THIS SHEET EITHER, AND IT NO LONGER MATTERS. The identity now comes from
    the Vermogensoverzicht's own `ISIN-code` column (live since 2026-07-23), so this report only
    has to supply a WEIGHT. The join is `Fondsomschrijving` -> `airs_holding.holding_name`, and
    unlike the old cross-portfolio match these two strings come from the SAME portfolio in the
    SAME system: measured, 40 of 42 match byte-for-byte.

⚠ THE CASH LINE IS RENAMED, AND IT IS THE ONLY SYSTEMATIC MISMATCH. This sheet calls it
    `Effectenrekening Liquiditeiten`; the Vermogensoverzicht calls it `Effectenrekening`. One row,
    both books, every time — so it is aliased explicitly rather than fuzzy-matched. Everything
    else that fails to match is drift worth seeing, not a matching problem to paper over.

⚠ A NAME IN THE MODEL AND NOT IN THE BOOK IS A FINDING, NOT A MISS. Measured on BUS_Neutraal_Dyn:
    `iShares Global Select Dividend 100` is in the model and NOT held. That is the strategy saying
    buy something the book has not bought — exactly the drift this view exists to show — and it is
    returned in `unheld` rather than dropped.
"""
from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO

import pandas as pd

# AirSPMS `rapport_types` code, alongside 'ATT', 'VOLK' and 'MUT'.
MODEL_RAPPORT_TYPE = "MODEL"

# The one systematic rename between this sheet and the Vermogensoverzicht. Keyed on the MODEL
# spelling; the value is what `airs_holding.holding_name` calls it.
NAME_ALIASES = {"Effectenrekening Liquiditeiten": "Effectenrekening"}


@dataclass
class ModelWeight:
    """One line of the model, as AIRS reports it. Percentages are PERCENTS (3.25), not fractions."""

    fonds: str
    model_pct: float | None = None
    actual_pct: float | None = None          # Werkelijk percentage
    drift_pct: float | None = None           # Afwijking percentage
    drift_eur: float | None = None           # Afwijking in euro
    buy: float | None = None                 # Kopen
    sell: float | None = None                # Verkopen
    model_value_eur: float | None = None     # Waarde volgens model


def _num(v: object) -> float | None:
    n = pd.to_numeric(v, errors="coerce")
    return float(n) if pd.notna(n) else None


def _text(v: object) -> str:
    """⚠ A blank cell is float NaN, `str()` renders it "nan", and "nan" is TRUTHY."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat") else s


def parse_model(file_bytes: bytes) -> list[ModelWeight]:
    """Every line of the MODEL sheet, with `fonds` already aliased to the holdings' spelling."""
    df = pd.read_excel(BytesIO(file_bytes))
    cols = {str(c).strip().lower(): c for c in df.columns}

    def col(name: str):
        return cols.get(name.lower())

    c_naam, c_model = col("Fondsomschrijving"), col("Model percentage")
    if c_naam is None or c_model is None:
        raise ValueError(
            f"MODEL export missing columns: {[n for n, c in (('Fondsomschrijving', c_naam), ('Model percentage', c_model)) if c is None]}. "
            f"Found: {list(df.columns)}")

    out: list[ModelWeight] = []
    for _, r in df.iterrows():
        naam = _text(r.get(c_naam))
        if not naam:
            continue
        out.append(ModelWeight(
            fonds=NAME_ALIASES.get(naam, naam),
            model_pct=_num(r.get(c_model)),
            actual_pct=_num(r.get(col("Werkelijk percentage"))) if col("Werkelijk percentage") else None,
            drift_pct=_num(r.get(col("Afwijking percentage"))) if col("Afwijking percentage") else None,
            drift_eur=_num(r.get(col("Afwijking in euro"))) if col("Afwijking in euro") else None,
            buy=_num(r.get(col("Kopen"))) if col("Kopen") else None,
            sell=_num(r.get(col("Verkopen"))) if col("Verkopen") else None,
            model_value_eur=_num(r.get(col("Waarde volgens model"))) if col("Waarde volgens model") else None,
        ))
    return out


def model_total_pct(rows: list[ModelWeight]) -> float:
    """Σ `Model percentage`. ⚠ Measured at EXACTLY 100.000 on every book — anything far from it
    means the sheet is partial, and a partial model silently understates every weight."""
    return round(sum(r.model_pct or 0 for r in rows), 3)


def attach_model(rows: list[ModelWeight], holding_names: set[str]) -> tuple[dict[str, ModelWeight],
                                                                           list[ModelWeight]]:
    """`(by_holding_name, in_the_model_but_not_held)`.

    ⚠ The second half is DRIFT, not leftovers — the strategy says hold something the book does
    not. It is the whole reason to look at a model beside a book, so it is returned, never dropped.
    """
    by_name = {r.fonds: r for r in rows if r.fonds in holding_names}
    unheld = [r for r in rows if r.fonds not in holding_names]
    unheld.sort(key=lambda r: -(r.model_pct or 0))
    return by_name, unheld
