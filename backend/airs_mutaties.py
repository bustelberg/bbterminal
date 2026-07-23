"""Parse the AIRS Mutaties (MUT) journal — the income a book actually received.

WHY THIS EXISTS
    Every return on /portfolios is a PRICE return: `Huidige waarde / Beginwaarde - 1`. It cannot
    see a dividend, because a dividend leaves the position's value and turns up as cash. So a
    high-yield holding reads as a laggard against one that pays nothing, and the book's own
    flow-aware `cumulatief_rendement` sits above the sum of its rows with no visible reason.

    `rapport_types=MUT` (same dropdown as VOLK/ATT) is the journal that closes the gap. Discovered
    by probe 2026-07-23; the wrong codes return ZERO bytes rather than an error, which is why
    `_download_report_sync` checks the length.

THE SHEET, MEASURED ON BUS_Neutraal_Dyn (2026-01-01..2026-07-23, 91 rows)
    Grootboek · Boekdatum · Omschrijving · Fonds · Rekening · Debet · Credit · Valuta ·
    Rekvaluta · Valutakoers · Bedrag eur · Bedrag vv · Bedrag

⚠ `Bedrag eur` IS ALREADY SIGNED AND ALREADY IN EUR. Do not re-derive it from Debet/Credit and do
    not apply `Valutakoers` yourself — both are how you double-count or flip a sign. Measured:
        Dividend          ASML 2026-02-18   Credit 62.40   Bedrag eur  +62.400000
        Dividendbelasting ASML 2026-02-18   Debet   9.36   Bedrag eur   -9.360000
        Dividend          MSFT 2026-03-12   Credit 49.14 USD @ 0.866026 -> +42.556508
    The tax rows are NEGATIVE, so the net a holding earned is a plain SUM over both ledgers.

⚠ WITHHOLDING TAX IS ITS OWN ROW, SO GROSS AND NET ARE DIFFERENT NUMBERS. 54 `Dividend` rows
    against 37 `Dividendbelasting` on one book. `net` is what reached the account and is what a
    return should use; `gross` and `tax` ride along because the difference is a real fact about a
    holding (a US name loses 15%, a Dutch one nothing).

⚠ ONLY THE DIVIDEND LEDGERS COUNT, AND THE FILTER IS EXPLICIT. Measured, this report returned
    exactly `Dividend` and `Dividendbelasting` on every book tried — but a journal is a journal,
    and the day AIRS adds `Aankoop` or `Storting` rows an unfiltered sum turns a deposit into
    investment income. Unknown ledgers are counted in `ignored` rather than silently dropped.

⚠ THE JOIN IS BY `Fonds`, A NAME — THERE IS NO ISIN ON THIS SHEET. Measured: 24 of 27 match a
    holding_name EXACTLY, and both fields are truncated by AIRS at the same 50 characters, so an
    exact match is safe and no fuzzy matching is wanted here (see `_airs_holding_isin` for what
    fuzzy matching costs). The other 3 are not truncations — no holding starts with them:

⚠ A SOLD POSITION EARNED INCOME AND HAS NO ROW TO PUT IT ON. `Automatic Data Proc.`,
    `Marsh&Mclennan` and `iShares Markit iBoxx High Yld Cpd Bd` paid dividends into this book and
    were then sold, so they are absent from the holdings snapshot. Their income is REAL and
    attaching it to nothing would understate the book. `unattached` keeps it, named, rather than
    dropping it — a portfolio-level income figure that quietly omits sold positions is wrong in a
    way nothing on screen would reveal.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from io import BytesIO

import pandas as pd

# AirSPMS `rapport_types` code for the Mutaties journal, alongside 'ATT' (Rendementen) and
# 'VOLK' (Vermogensoverzicht). Overridable via AIRS_MUTATIES_RAPPORT_TYPE.
MUTATIES_RAPPORT_TYPE = "MUT"

# The two ledgers that make up investment income. Anything else is NOT income (see the docstring).
LEDGER_DIVIDEND = "Dividend"
LEDGER_DIVIDEND_TAX = "Dividendbelasting"
INCOME_LEDGERS = frozenset({LEDGER_DIVIDEND, LEDGER_DIVIDEND_TAX})


@dataclass
class Mutatie:
    """One journal line. `amount_eur` is AIRS's own signed EUR figure, never re-derived."""

    grootboek: str
    boekdatum: date | None
    fonds: str
    omschrijving: str
    amount_eur: float
    amount_local: float | None = None
    currency: str | None = None
    fx_rate: float | None = None


@dataclass
class DirectResult:
    """What one instrument paid the book over the window, in EUR."""

    fonds: str
    gross_eur: float = 0.0
    tax_eur: float = 0.0          # negative, as AIRS books it
    payments: int = 0             # count of `Dividend` rows, not of journal lines
    first: date | None = None
    last: date | None = None

    @property
    def net_eur(self) -> float:
        """What actually reached the account. The figure a return should use."""
        return round(self.gross_eur + self.tax_eur, 2)


@dataclass
class MutatiesSummary:
    by_fonds: dict[str, DirectResult] = field(default_factory=dict)
    # Ledger codes we did not recognise, with their row counts. Never silently dropped: an
    # unknown ledger is either new income we are missing or a movement we must not count.
    ignored: dict[str, int] = field(default_factory=dict)
    rows: int = 0


def _num(v: object) -> float | None:
    n = pd.to_numeric(v, errors="coerce")
    return float(n) if pd.notna(n) else None


def _text(v: object) -> str:
    """⚠ A blank cell arrives as float NaN and `str()` renders it `"nan"`, which is TRUTHY — the
    same trap that once counted a cash line as a holding."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat") else s


def _day(v: object) -> date | None:
    d = pd.to_datetime(v, errors="coerce")
    return None if pd.isna(d) else d.date()


def parse_mutaties(file_bytes: bytes) -> list[Mutatie]:
    """Every journal line on the sheet, unfiltered and unsummed.

    Filtering belongs to `direct_result`, so a caller that wants to look at what was ignored can.
    """
    df = pd.read_excel(BytesIO(file_bytes))
    cols = {str(c).strip().lower(): c for c in df.columns}

    def col(name: str):
        return cols.get(name.lower())

    c_led, c_date, c_fonds = col("Grootboek"), col("Boekdatum"), col("Fonds")
    c_oms, c_eur = col("Omschrijving"), col("Bedrag eur")
    c_vv, c_ccy, c_fx = col("Bedrag vv"), col("Valuta"), col("Valutakoers")
    missing = [n for n, c in (("Grootboek", c_led), ("Fonds", c_fonds), ("Bedrag eur", c_eur))
               if c is None]
    if missing:
        raise ValueError(f"Mutaties export missing columns: {missing}. Found: {list(df.columns)}")

    out: list[Mutatie] = []
    for _, r in df.iterrows():
        eur = _num(r.get(c_eur))
        if eur is None:
            continue                      # not a booked amount; nothing to add up
        out.append(Mutatie(
            grootboek=_text(r.get(c_led)),
            boekdatum=_day(r.get(c_date)) if c_date else None,
            fonds=_text(r.get(c_fonds)),
            omschrijving=_text(r.get(c_oms)) if c_oms else "",
            amount_eur=eur,
            amount_local=_num(r.get(c_vv)) if c_vv else None,
            currency=_text(r.get(c_ccy)) or None if c_ccy else None,
            fx_rate=_num(r.get(c_fx)) if c_fx else None,
        ))
    return out


def direct_result(rows: list[Mutatie]) -> MutatiesSummary:
    """The income each instrument paid, keyed by AIRS's own `Fonds` string.

    ⚠ Keyed on the RAW `Fonds`, matched to `airs_holding.holding_name` EXACTLY by the caller.
    Both are AIRS strings truncated at the same 50 characters; anything looser re-imports the
    fuzzy-matching failure mode this codebase spent a long time removing.
    """
    s = MutatiesSummary(rows=len(rows))
    for m in rows:
        if m.grootboek not in INCOME_LEDGERS:
            s.ignored[m.grootboek or "(blank)"] = s.ignored.get(m.grootboek or "(blank)", 0) + 1
            continue
        if not m.fonds:
            # Income the sheet does not attribute to an instrument. Counted as ignored rather
            # than folded into some arbitrary holding.
            s.ignored["(no Fonds)"] = s.ignored.get("(no Fonds)", 0) + 1
            continue
        d = s.by_fonds.setdefault(m.fonds, DirectResult(fonds=m.fonds))
        if m.grootboek == LEDGER_DIVIDEND:
            d.gross_eur = round(d.gross_eur + m.amount_eur, 6)
            d.payments += 1
        else:
            d.tax_eur = round(d.tax_eur + m.amount_eur, 6)
        if m.boekdatum:
            d.first = m.boekdatum if d.first is None else min(d.first, m.boekdatum)
            d.last = m.boekdatum if d.last is None else max(d.last, m.boekdatum)
    return s


def attach(summary: MutatiesSummary, holding_names: set[str]) -> tuple[dict[str, DirectResult],
                                                                      list[DirectResult]]:
    """Split the income into what a current holding can carry and what it cannot.

    ⚠ THE SECOND HALF IS NOT LEFTOVER, IT IS THE BOOK'S MONEY. A position sold during the year
    paid real dividends and is absent from the holdings snapshot, so it has no row. Measured on
    BUS_Neutraal_Dyn: 3 of 27 (`Automatic Data Proc.`, `Marsh&Mclennan`, an iShares HY fund).
    A portfolio income figure that silently drops them is understated with nothing on screen to
    say so.
    """
    attached = {f: d for f, d in summary.by_fonds.items() if f in holding_names}
    unattached = [d for f, d in summary.by_fonds.items() if f not in holding_names]
    unattached.sort(key=lambda d: -abs(d.net_eur))
    return attached, unattached
