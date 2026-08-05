"""Parse the AIRS Transacties (TRANS) report — what the book actually BOUGHT and SOLD.

WHY THIS EXISTS
    /portfolios can say what a book holds (VOLK), what it earned (MUT) and what its strategy asks
    for (MODEL). It cannot say what it DID: every buy and sell is invisible, so a position that
    appeared mid-year, a name that was sold out entirely, and a weight that drifted because the
    market moved all look identical from the outside.

THE SHEET, MEASURED ON AITopSelectie OFF DYN (2026-01-01..2026-08-05, 40 rows)
    Tt · Datum · Fonds · Koers · Aantal · Waarde · Waarde.1 · Kostprijs · Res. YtD · Fondskoers ·
    Res. in % · Waarde EUR · Waarde EUR.1 · Res. voorg. jr.

⚠ THE HEADERS ARE GROUPED, WHICH IS WHY `Waarde` APPEARS TWICE. The report is laid out as a BUY
    block beside a SELL block, so pandas suffixes the repeats (`Waarde.1`, `Waarde  EUR.1`). Which
    physical column carries a figure depends on `Tt`, and the unused side is a hard **0.0**, not a
    blank — so summing `Waarde` across the sheet silently adds buys to nothing and calls it
    turnover. Measured, per transaction type:

        Tt='A' (Aankoop, 31 rows)   Aantal · Fondskoers (buy price, local) ·
                                    Waarde (local) · Waarde EUR
        Tt='V' (Verkoop,  8 rows)   Aantal · Koers (sell price, local) ·
                                    Waarde.1 (local) · Waarde EUR.1 (proceeds, EUR) ·
                                    Kostprijs (EUR) · Res. YtD · Res. in % · Res. voorg. jr.
        Tt='D' (1 row)              Aantal ONLY — every money column 0.0. NOT interpreted; see
                                    `UNKNOWN_TYPES` below.

    ⚠ `Koers` AND `Fondskoers` ARE THE SAME QUANTITY ON OPPOSITE SIDES — the price — and each is
    0.0 on the other side. Reading "the price column" without asking `Tt` gets zero half the time.

⚠ THE IDENTITY, VERIFIED ON ALL 8 SELL ROWS TO THE CENT:
        Res. YtD  ==  Waarde EUR.1  -  Kostprijs
    Synopsys 2026-01-22: 1,778.516063896 - 1,647.5308636176 = 130.9852002784, and `Res. in %`
    7.9503943246797 == that over Kostprijs. `Waarde.1` is the same trade in its LOCAL currency
    (Koers 521.95 x Aantal 4 = 2,087.80 USD), so the EUR pair is the one that reconciles.

⚠⚠ THE REALISED YTD IS `Res. YtD`, **NEVER** `Waarde EUR.1 - Kostprijs`, AND THIS BOOK CANNOT TELL
    YOU THAT. `Res. voorg. jr.` is the part of a realised gain that belongs to PREVIOUS years — a
    position bought in 2024 and sold in 2026 realises a result of which only some is this year's.
    On AITopSelectie every position was bought in 2026, so `Res. voorg. jr.` is 0.00 on all eight
    rows and the two formulas AGREE EXACTLY. Validating on this account alone would therefore
    bless either choice, and the wrong one overstates the year by the whole prior-year gain on any
    book that carries positions across a year end. Same shape as the EBIT-vs-Operating-Income and
    Net-Income-vs-NCI traps: two figures that coincide on the company you happened to check.

⚠ A SELL IS A REALISATION, NOT A CLOSURE. Synopsys was sold 4 shares on 2026-01-22 and is STILL
    HELD. So a name can legitimately appear in both the held list and the realised list, and
    calling this block "closed positions" would be wrong for most of it. Whether a name is
    genuinely closed out is decided by its ABSENCE from the holdings snapshot, not by its presence
    here.

⚠ WHAT IS STILL NOT KNOWN, AND IS THEREFORE NOT USED. `Tt='D'` appeared once (KLA-Tencor,
    2026-06-12, 369 shares, every money column 0.0). KLA split 9:1 in 2026, so a corporate action
    is the obvious reading — and "obvious" is not "measured". It carries no money, so excluding it
    from every sum costs nothing and assuming it would risk everything; it is COUNTED and named
    instead (`unknown_types`), so a `D` that one day carries a value cannot slip in unnoticed.

⚠ AN EMPTY REPORT IS AN ANSWER. A book that has not traded this year has no transactions, which
    is a fact about the book — not a download that failed. The caller must be able to tell the two
    apart, so a legitimately empty sheet parses to zero rows rather than raising.

⚠ A COLUMN IS TYPED BY ITS DTYPE, NEVER BY ITS NAME. A column called "Bedrag" that AIRS exported
    as text stays TEXT here and shows as text on screen, which is a visible fact about the export.
    Sniffing the name and coercing would hide it — and a number that only sometimes parses is how
    a total silently omits the rows that did not.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from io import BytesIO

import pandas as pd

# AirSPMS `rapport_types` code for the Transacties report, alongside 'ATT' (Rendementen),
# 'VOLK' (Vermogensoverzicht), 'MUT' (Mutaties) and 'MODEL'. Overridable via
# AIRS_TRANSACTIES_RAPPORT_TYPE if AirSPMS ever renames it.
#
# ⚠ AN UNKNOWN CODE RETURNS ZERO BYTES, NOT AN ERROR — see `_download_report_sync`, whose length
# check is the only thing standing between a typo here and a silent "this book never traded".
TRANSACTIES_RAPPORT_TYPE = "TRANS"

# What a column holds, decided from the dtype pandas inferred. Display-level only: it decides
# alignment and formatting, never meaning.
KIND_NUMBER = "number"
KIND_DATE = "date"
KIND_TEXT = "text"


@dataclass
class ParsedSheet:
    """One AIRS report, as the sheet itself — no schema imposed.

    `columns` keeps the sheet's OWN order: a report is laid out the way its author reads it, and
    re-ordering it into ours would make the screen and the export disagree for no gain.
    """

    columns: list[str] = field(default_factory=list)
    # Per column: KIND_NUMBER | KIND_DATE | KIND_TEXT, keyed by the column name.
    kinds: dict[str, str] = field(default_factory=dict)
    # One dict per row, keyed by column name. Values are float | str | None — never NaN, which is
    # not JSON and which `str()` renders as the TRUTHY string "nan".
    rows: list[dict[str, object]] = field(default_factory=list)


def _text(v: object) -> str | None:
    """⚠ A BLANK CELL ARRIVES AS FLOAT NaN AND `str()` RENDERS IT `"nan"`, WHICH IS TRUTHY. That is
    the same trap that once counted a cash line as a holding, and the same one `airs_mutaties`
    guards. `None`, so an empty cell reads as empty everywhere downstream."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    return None if s.lower() in ("nan", "none", "nat", "") else s


def _num(v: object) -> float | None:
    n = pd.to_numeric(v, errors="coerce")
    return float(n) if pd.notna(n) else None


def _day(v: object) -> str | None:
    """ISO date. ⚠ The DATE, not the timestamp — AIRS books a transaction to a day, and rendering
    `2026-03-12T00:00:00` would imply a precision the report does not carry."""
    d = pd.to_datetime(v, errors="coerce")
    return None if pd.isna(d) else d.date().isoformat()


def _kind_of(s: pd.Series) -> str:
    """⚠ FROM THE DTYPE ALONE. See the module docstring: a name sniff is how a text column becomes
    a silently-truncated number column."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return KIND_DATE
    if pd.api.types.is_bool_dtype(s):
        return KIND_TEXT
    if pd.api.types.is_numeric_dtype(s):
        return KIND_NUMBER
    return KIND_TEXT


def parse_transacties(file_bytes: bytes) -> ParsedSheet:
    """Every row on the sheet, unfiltered and unsummed, with each column's own name and type.

    ⚠ NOTHING IS DROPPED AND NOTHING IS RENAMED. A filter here would need to know what the rows
    MEAN, and that is precisely what has not been measured yet. `airs_mutaties` splits parse from
    `direct_result` for the same reason — a caller that wants to see what was ignored can only do
    so if the parser did not do the ignoring.
    """
    df = pd.read_excel(BytesIO(file_bytes))
    # ⚠ HEADERS ARRIVE WITH TRAILING SPACES ON SOME AIRS EXPORTS, and " Fonds" is a different key
    # from "Fonds" to every consumer downstream. Stripped once, here.
    df.columns = [str(c).strip() for c in df.columns]
    # A column with no header at all comes back as "Unnamed: 3". Keep it — an unnamed column with
    # data in it is a column of the report, and dropping it is how you lose the one field that
    # turns out to matter — but say so, rather than printing pandas' internal placeholder.
    df.columns = [c if not c.startswith("Unnamed:") else f"(column {i + 1})"
                  for i, c in enumerate(df.columns)]

    sheet = ParsedSheet(columns=list(df.columns))
    sheet.kinds = {c: _kind_of(df[c]) for c in df.columns}

    for _, r in df.iterrows():
        row: dict[str, object] = {}
        for c in df.columns:
            kind = sheet.kinds[c]
            row[c] = (_day(r[c]) if kind == KIND_DATE
                      else _num(r[c]) if kind == KIND_NUMBER
                      else _text(r[c]))
        # ⚠ A WHOLLY EMPTY ROW IS A SPACER, NOT A TRANSACTION. AIRS pads some exports with blank
        # lines between sections; counting them would report trades that never happened.
        if any(v is not None for v in row.values()):
            sheet.rows.append(row)
    return sheet


# ─────────────────────────────────────────────────────────────────────────────────────────────
# The MEASURED layer. Everything above reads the sheet as a sheet; everything below knows what
# the columns mean, and REFUSES rather than guessing when they are not the ones measured.
# ─────────────────────────────────────────────────────────────────────────────────────────────

TT = "Tt"
TT_BUY = "A"       # Aankoop
TT_SELL = "V"      # Verkoop

# The sell block. ⚠ `Res.  YtD` and `Res.  voorg. jr.` carry TWO spaces after "Res." — that is
# AIRS's own header, not a typo here, and a single-space lookup finds nothing.
COL_FONDS = "Fonds"
COL_DATE = "Datum"
COL_QTY = "Aantal"
COL_SELL_PROCEEDS_EUR = "Waarde  EUR.1"
COL_SELL_COST_EUR = "Kostprijs"
COL_REALISED_YTD_EUR = "Res.  YtD"
COL_REALISED_PRIOR_EUR = "Res.  voorg. jr."
COL_BUY_VALUE_EUR = "Waarde  EUR"

# ⚠ EVERY COLUMN THE REALISED FIGURE DEPENDS ON. If one is missing the sheet is not the sheet that
# was measured, and the honest output is "we cannot read this", never a total of the columns that
# happened to be present — which would be a confident EUR 0.00 realised.
_REQUIRED = (TT, COL_FONDS, COL_SELL_PROCEEDS_EUR, COL_SELL_COST_EUR, COL_REALISED_YTD_EUR)


@dataclass
class RealisedLeg:
    """One instrument's realised result this year, summed over its sales."""

    fonds: str
    sales: int = 0
    quantity: float = 0.0
    proceeds_eur: float = 0.0
    cost_eur: float = 0.0
    # ⚠ AIRS's OWN `Res. YtD`, summed — never `proceeds - cost`. See the module docstring: the two
    # differ by `Res. voorg. jr.` on any position carried across a year end.
    realised_ytd_eur: float = 0.0
    prior_year_eur: float = 0.0
    first: str | None = None
    last: str | None = None


@dataclass
class RealisedSummary:
    legs: dict[str, RealisedLeg] = field(default_factory=dict)
    buys_eur: float = 0.0
    buy_count: int = 0
    # Transaction types this module does not interpret, with their row counts. Never silently
    # dropped — see the `Tt='D'` note in the module docstring.
    unknown_types: dict[str, int] = field(default_factory=dict)
    # Why nothing could be read, when nothing could. None on success.
    unreadable: str | None = None

    @property
    def realised_ytd_eur(self) -> float:
        return round(sum(leg.realised_ytd_eur for leg in self.legs.values()), 2)


def realised_results(sheet: ParsedSheet) -> RealisedSummary:
    """What this book REALISED this year, per instrument, from its sales.

    ⚠ AGGREGATED BY `Fonds`, BECAUSE ONE POSITION IS SOLD IN PIECES. AITopSelectie sold Synopsys on
    two dates; two rows for one instrument would read as two positions on any list built from this.
    The name is AIRS's own string, matched to `airs_holding.holding_name` EXACTLY wherever it is
    joined — both are AIRS strings truncated at the same width, and nothing fuzzy belongs here
    (see `_airs_holding_isin` for what fuzzy matching costs).

    ⚠ REFUSES ON AN UNRECOGNISED SHEET. A missing column means this is not the report that was
    measured, and a sum over the columns that survive is a plausible number rather than an error.
    """
    missing = [c for c in _REQUIRED if c not in sheet.columns]
    if missing:
        return RealisedSummary(unreadable=(
            f"This Transacties sheet does not carry the columns that were measured "
            f"(missing: {', '.join(missing)}), so no realised result can be read from it."))

    s = RealisedSummary()
    for r in sheet.rows:
        tt = (r.get(TT) or "").strip() if isinstance(r.get(TT), str) else None
        if tt == TT_BUY:
            s.buy_count += 1
            s.buys_eur = round(s.buys_eur + _f(r.get(COL_BUY_VALUE_EUR)), 2)
            continue
        if tt != TT_SELL:
            # ⚠ COUNTED, NOT DROPPED. A type we do not interpret is either a corporate action with
            # no money (harmless) or something new that belongs in the total (not harmless), and
            # only a visible count can ever tell the two apart.
            s.unknown_types[tt or "(blank)"] = s.unknown_types.get(tt or "(blank)", 0) + 1
            continue
        fonds = r.get(COL_FONDS)
        if not isinstance(fonds, str) or not fonds:
            # A sale attributed to no instrument. Counted, never folded into an arbitrary name.
            s.unknown_types["(sale with no Fonds)"] = (
                s.unknown_types.get("(sale with no Fonds)", 0) + 1)
            continue
        leg = s.legs.setdefault(fonds, RealisedLeg(fonds=fonds))
        leg.sales += 1
        leg.quantity = round(leg.quantity + _f(r.get(COL_QTY)), 6)
        leg.proceeds_eur = round(leg.proceeds_eur + _f(r.get(COL_SELL_PROCEEDS_EUR)), 2)
        leg.cost_eur = round(leg.cost_eur + _f(r.get(COL_SELL_COST_EUR)), 2)
        leg.realised_ytd_eur = round(leg.realised_ytd_eur + _f(r.get(COL_REALISED_YTD_EUR)), 2)
        leg.prior_year_eur = round(leg.prior_year_eur + _f(r.get(COL_REALISED_PRIOR_EUR)), 2)
        d = r.get(COL_DATE)
        if isinstance(d, str):
            leg.first = d if leg.first is None else min(leg.first, d)
            leg.last = d if leg.last is None else max(leg.last, d)
    return s


def _f(v: object) -> float:
    """A money cell as a number. ⚠ 0.0 for a blank, and here that IS right: the sheet writes a
    hard 0.0 in the block that does not apply, so an absent value and a zero mean the same thing
    on this report. (It is NOT right on a return — see `_mark_at` elsewhere — which is why this
    helper is local to the money sums and not exported.)"""
    return float(v) if isinstance(v, (int, float)) else 0.0
