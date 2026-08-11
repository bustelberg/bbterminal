"""ONE loader for `fx_rate` -> `{currency: {date: units per EUR}}`.

WHY THIS MODULE EXISTS

    `_benchmark_index._fx_to_eur` and `_airs_portfolio_perf._fx` were twins -- the docstring of
    each said so, in those words. They resolved minor units the same way, paged the same way, and
    dropped falsy rates the same way. They differed in exactly one respect: **only one of them had
    the COPY fast path**, and the other went on paying 17 sequential PostgREST requests for a load
    the first did in one.

    Measured on the Analyse modal (2026-08-11): `fx_rate` was **17 HTTP round trips and 13,617 of
    the request's 22,416 rows** -- and ⚠ **14 of those 17 were the SAME query differing only by
    `offset`**. The benchmark side, on the identical table in the identical request, used 4 COPYs.
    Two implementations of one idea, one of them a generation behind, and nothing to make that
    visible.

⚠ THE DUPLICATION WAS THE REAL BUG, NOT THE MISSING FAST PATH. Every rule below is a correctness
    rule with an incident behind it, and each one had to be right in two places at once. That is
    the arrangement that produced the silent-truncation bug in the first place. One definition.

THE RULES, ALL OF THEM LOAD-BEARING

⚠ ASK FOR THE **MAJOR** CURRENCY. `fx_rate` has `GBP`; it has never had `GBp`, because pence is a
    quoting convention and not a currency -- and 343 of our rows are quoted in it. Requesting the
    literal code returns zero rows and the holding reads as unpriceable with every bar present.
    Resolution is `asset_pipeline.fx.SUBUNIT`, shared, never re-derived; the DIVISOR is applied by
    each caller's `_rate`, not here, so there is one place for the pence rule and one for the map.

⚠ THE READ MUST PAGE, AND TRUNCATION HERE IS INVISIBLE TWICE OVER. PostgREST silently caps a
    response at 1,000 rows (cloud) / 10,000 (local). A currency whose early rows are cut has no EUR
    series before the cut, so `_eur_series` drops every close without a rate on or before it, the
    holding has no mark at the anchor, it is classed unpriceable, it **silently leaves the basket**,
    and the return renormalises over what survived. No error, no gap -- a confident number computed
    over a different portfolio. Measured: TWD came back as 20 rows starting 2026-05-27 (real
    history 2014), so Taiwan Semiconductor -- 5% of AITopSelectie OFF FX, 6,606 bars, correctly
    resolved -- vanished from its own book. ⚠ **The two caps differ tenfold, so each environment cut
    different currencies and reported a different number off identical code**: 36.64% local vs
    44.14% production, and 49 of 56 models changed once paged.

⚠ SORT ON A UNIQUE KEY -- `(rate_date, currency_code)`. Postgres promises nothing about tied rows
    across separate LIMIT/OFFSET queries, so a page boundary inside a tie serves a row twice or
    never.

⚠ ADVANCE BY WHAT CAME BACK (`off += len(rows)`), and stop on an EMPTY page. "A short page is the
    last page" is only true while the server's cap is at least the page size -- which is precisely
    the assumption that failed. Correct under any cap, at the cost of one empty request per chunk.

⚠ A FALSY RATE IS DROPPED, on both paths. Not because zero is implausible but because the rate is
    the DENOMINATOR of every conversion (`eur = native / rate`), and dividing by it raises.
"""
from __future__ import annotations

import csv
import io
import logging

from asset_pipeline.fx import SUBUNIT
from common.pg import _db_url, _run_copy
# ⚠ `deps.supabase` IS RESOLVED AT CALL TIME, NOT BOUND AT IMPORT. `from deps import
# supabase` captures the object once, so a test (or anything else) that swaps
# `deps.supabase` afterwards cannot reach this module — and because this module is a
# SHARED loader, the reads it performs used to live in the routers where the tests patch.
# Moving them here silently took them out of reach of every one of those patches and CI
# went red with `KeyError: 'SUPABASE_URL'` (the real proxy trying to build a client).
# Going through the module keeps one patch point for the whole app.
import deps
from deps import IN_CHUNK_SIZE

log = logging.getLogger(__name__)

_PAGE = 1000

FxRates = dict[str, dict[str, float]]


def major_currencies(currencies: set[str]) -> list[str]:
    """The sorted MAJOR-currency codes to ask `fx_rate` for (EUR dropped, `GBp` -> `GBP`)."""
    return sorted({SUBUNIT.get(c, (c, 1.0))[0] for c in currencies if c and c != "EUR"})


def _via_copy(major: list[str], start: str, end: str) -> FxRates | None:
    """The whole window in ONE COPY, or `None` to fall back to the pager.

    `major` is already resolved, so this takes no view on minor units -- that would be a second
    place for the pence rule to live.

    ⚠ `None` ON ANY FAILURE, NEVER A PARTIAL DICT. A currency missing from this map has no EUR
    series, so its holdings silently leave the basket and the weights renormalise -- the exact
    failure described above. Falling back to the pager is slow; returning half the currencies
    would be wrong AND would look like a number.
    """
    if not _db_url() or not major:
        return None
    sql = ("COPY (SELECT currency_code, rate_date, rate FROM fx_rate "
           "WHERE currency_code = ANY(%s) AND rate_date BETWEEN %s AND %s) "
           "TO STDOUT WITH (FORMAT csv)")
    try:
        buf = _run_copy(sql, (major, start, end))
    except Exception as e:  # noqa: BLE001 — a fast path must never be why a page 500s
        log.warning("[fx] COPY failed, falling back to PostgREST paging: %s: %s",
                    type(e).__name__, e)
        return None
    if buf is None:
        return None
    out: FxRates = {}
    if buf.getbuffer().nbytes == 0:
        return out
    for row in csv.reader(io.TextIOWrapper(buf, encoding="utf-8", newline="")):
        if len(row) != 3:
            continue
        code, rate_date, rate = row
        # COPY writes NULL as an empty field, and `float("")` raises. A missing rate is a real
        # state in this table (a currency whose feed had a gap that day).
        if not rate:
            continue
        val = float(rate)
        if val:
            out.setdefault(code, {})[rate_date] = val
    return out


def _paged(major: list[str], start: str, end: str) -> FxRates:
    """The PostgREST fallback. Correct but expensive: one request per 1,000 rows."""
    out: FxRates = {}
    for i in range(0, len(major), IN_CHUNK_SIZE):
        chunk = major[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            rows = (deps.supabase.table("fx_rate")
                    .select("currency_code,rate_date,rate")
                    .in_("currency_code", chunk)
                    .gte("rate_date", start).lte("rate_date", end)
                    .order("rate_date").order("currency_code")
                    .range(off, off + _PAGE - 1).execute().data or [])
            if not rows:
                break
            for r in rows:
                if r["rate"]:
                    out.setdefault(r["currency_code"], {})[r["rate_date"]] = float(r["rate"])
            off += len(rows)
    return out


def load_fx_to_eur(currencies: set[str], start: str, end: str) -> FxRates:
    """`{major_currency: {date: units per EUR}}` over `[start, end]`.

    Direction matters: `rate` is units of the currency PER EUR, so `EUR = native / rate` (mirrors
    `momentum/data/fx.py`, which divides). Upside down would invert every FX move.

    COPY first, pager as the fallback -- identical shape either way, so nothing downstream can
    tell which ran. Inside a `read_cache()` block an identical COPY is served from the first one.
    """
    major = major_currencies(currencies)
    if not major:
        return {}
    fast = _via_copy(major, start, end)
    if fast is not None:
        return fast
    return _paged(major, start, end)
