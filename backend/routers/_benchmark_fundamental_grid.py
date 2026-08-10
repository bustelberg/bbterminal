"""Every constituent's fundamentals for ONE period, side by side, with the cap that weights them.

WHY THIS EXISTS, AND WHY IT IS NOT THE COVERAGE TABLE
    `_benchmark_fundamentals` answers "which periods do we HOLD for each company" — spans, counts,
    a `from` and a `to`. This answers "what were the NUMBERS in period P, across the whole index,
    and what was each company worth at the time". Different question, and the second one cannot be
    read off the first.

    The shape follows from what the reader is doing: weighting is a CROSS-SECTIONAL operation. To
    weight FY2021 you need every constituent's cap in FY2021 at once — so rows are companies,
    columns are the lines, and the period is a slider, not a column.

⚠ ONE 12-MONTH AXIS, WHICH IS THE WHOLE REASON `cadence="quarterly"` MEANS **TTM** HERE.
    `_metric_by_year`'s quarterly basis is trailing twelve months, not the raw quarter (see
    `earnings._TTM_RULE`), and this module keeps that deliberately rather than reaching past it for
    raw quarters. The slider then moves the AS-OF DATE and nothing else: FY2024 and TTM-through-Q3
    are both twelve-month figures, so a cell means the same thing wherever the slider sits. Raw
    quarters would change the UNIT under half the columns — revenue falls ~4x while market cap, a
    snapshot, does not — and a table whose units move when you drag a control is how a plausible
    wrong number gets believed. The period label says `TTM` for exactly this reason.

⚠ EVERY VALUE IS CONVERTED TO EUR, AND THAT IS A CORRECTNESS REQUIREMENT, NOT A PREFERENCE.
    GuruFocus reports financials in the LISTING's trading currency, per fiscal period (see the
    `_asset_financials` traps). Summing a JPY market cap beside a USD one to get an index total
    over-weights Japan by ~150x — and the result looks entirely reasonable, because every input was
    right. The native figure and the rate used ride along per cell so the conversion is auditable
    rather than asserted.

⚠ THE FX DATE IS THE PERIOD'S OWN END, NEVER THE CALENDAR YEAR'S. Apple's FY2025 ends in
    September; converting it at 31 December's rate applies a rate struck three months after the
    figure. `_latest_per_year_dated` and `_ttm_by_period(key="date")` both keep the real
    period-end, which is the only reason this can be done properly.

⚠ COVERAGE IS PER PERIOD AND IT GATES THE AGGREGATE. Measured 2026-08-04, 92 of SP500's 503
    constituents had ingested fundamentals at all — and coverage falls off further the further back
    you scrub, because a company's history starts when its ingest did. A cap-weighted index row
    renormalised over 18% of the index is not an approximation of the index, it is a number about
    our ingest wearing the index's name. `covered_pct` is reported per period and the aggregate is
    WITHHELD below `MIN_COVERAGE_PCT` — the same refusal `_airs_portfolio_perf` makes, for the same
    reason.

⚠⚠ AN INDEX THAT CAPS GETS NO WEIGHTS HERE AND NO INDEX ROW — `INDEX_CAP_PCT`.
    `cap_i / Σcap` is the index's weighting only for an index that does not cap. The AEX holds 25
    names and Euronext caps a constituent at 15% at each review, precisely because ASML would
    otherwise swallow it: uncapped it is 37.53% of the AEX against the real index's 15.00%. A
    cap-weighted total built on that is not an approximation of the AEX, it is an ASML tracker
    wearing the AEX's name — `_benchmark_index`'s own words, in the comment above `INDEX_CAP_PCT`.

    That comment also records why this is REFUSED rather than patched: the weight formula had
    already leaked into four inline copies, `index_weights` was made the one place a weight is
    formed, and the note warns that **a cap applied to three of four would be worse than no cap at
    all**. This grid was the fifth copy. Rather than become a second capping implementation, it
    declines: a capped index shows its per-company figures (each individually true) with no weight
    column and no index row, and says so.

⚠ THE WEIGHT IS THE PERIOD'S OWN CAP, NOT TODAY'S. That is the entire point of the exercise:
    weighting a 2018 cross-section by 2026 caps hands the winners a share of the index they did not
    have, which is the look-ahead bias `_benchmark_index._window_rows` exists to undo for returns.
    Here it is simpler than there — GuruFocus publishes `Market Cap` per fiscal period, so the
    period's cap is READ, not reconstructed from a price ratio.

⚠ KNOWN AND NOT FIXED HERE: membership is TODAY's. `_members` is the current constituent list, so
    scrubbing to 2018 shows 2018's figures for the companies in the index NOW — survivorship bias,
    the same one the backtester carries when no `index_universe` is picked. It is surfaced
    (`membership_as_of`) rather than silently absorbed. Fixing it means resolving membership per
    period against the reconstruction, which is a separate piece of work.
"""
from __future__ import annotations

import logging
from time import perf_counter as _perf

from deps import supabase
from routers._benchmark_fundamentals import COLUMNS, normalise_cadence

_log = logging.getLogger(__name__)

# Below this share of the index's constituents, the cap-weighted aggregate row is withheld rather
# than computed over what happens to be present. Matches `_airs_portfolio_analysis`'s floor — the
# judgement is the same one ("a renormalised minority is a fabrication"), so the number is too.
MIN_COVERAGE_PCT = 60.0

# The first period offered. Matches the benchmark overlay's own floor in `earnings.py`, so the
# grid and the charts start their history in the same place.
_FIRST_PERIOD = "2015"

# ⚠ WHAT THE INDEX ROW MAY DO WITH A COLUMN — DERIVED FROM THE **UNIT**, NOT FROM THE TTM RULE.
#
# The first version of this took `_TTM_RULE` as the authority, on the reasoning that it had already
# had to decide flow-vs-rate. It has not: the TTM rule aggregates ONE company OVER TIME, and this
# aggregates MANY companies AT ONE TIME. They agree on most lines and part company exactly where it
# matters — `shares` is `mean` over time (a share count IS an average across four quarters) and was
# therefore handed a cap-weighted mean across companies, which is a number with no referent. "The
# S&P 500's share count" is not a quantity.
#
#   sum            a currency amount — revenue, market cap, equity. The index total is the sum.
#   weighted_mean  a rate (ROIC %). Summed across 500 companies it reads ~5,000%, which the cell
#                  would print with a % sign after it. Cap-weighting is the only honest average.
#   none           ⚠ REFUSED, AND THAT IS AN ANSWER. A share COUNT and a PER-SHARE amount have no
#                  index-level total: summing dividends-per-share over 500 companies, or their
#                  share counts, produces a well-formed number that means nothing. The per-company
#                  cells are true and stay; the index row shows a dash and says why. Inventing an
#                  aggregate here would be the "renormalise quietly" failure in a new place.
_AGG_BY_UNIT = {"millions": "sum", "percent": "weighted_mean",
                "shares": "none", "per_share": "none"}

# ⚠⚠ THE UNIT IS DECLARED, NEVER INFERRED, AND IT DECIDES WHETHER FX IS APPLIED AT ALL.
#
# Every other line here is a currency amount in millions, so the obvious loop divides the whole row
# by the FX rate. Two of the nineteen are not currency, and both come back silently wrong when it
# does — measured, before this map existed: NVIDIA's ~24,000M diluted shares rendered as **20,902M**
# (divided by 1.19, a share count "converted to euros"), and `ROIC %` would have reported 13.3% as
# 11.2% "in EUR". Neither throws, neither looks odd, and a share count is exactly the number a
# reader would use to sanity-check a market cap.
#
# This is the same rule `_asset_financials._ITEMS` states for the same reason, and it is why the
# unit cannot be sniffed from the field name: `EPS (Diluted)` does not contain "per share", and
# `shares` is not a currency despite being a plain number in millions like the fifteen around it.
#
#   millions   a currency amount in millions      -> convert
#   per_share  a currency amount per share        -> convert (same rate, different scale)
#   shares     a COUNT, in millions of shares     -> NEVER convert
#   percent    already a rate                     -> NEVER convert
_UNIT: dict[str, str] = {
    "div_ps": "per_share", "price_ps": "per_share",
    "shares": "shares", "roic": "percent",
}
_CURRENCY_UNITS = frozenset({"millions", "per_share"})


def _unit(metric: str) -> str:
    return _UNIT.get(metric, "millions")


def _period_label(date: str) -> str:
    """A quarter-end date → the `YYYY-Qn` label. The same expression `_ttm_by_period` uses.

    ⚠ DERIVED FROM THE REAL MONTH, never synthesised from the calendar. A fiscal quarter need not
    end on 03-31/06-30/09-30/12-31, and forcing it there moves every point of an off-calendar
    filer into a quarter it does not belong to.
    """
    return f"{date[:4]}-Q{(int(date[5:7]) - 1) // 3 + 1}"


def _values_with_dates(cids: list[int], metrics: list[str], cadence: str,
                       ) -> dict[str, dict[int, dict[str, tuple[str, float]]]]:
    """{metric: {company_id: {period label: (period-END date, native value)}}} for EVERY line.

    ⚠ THE DATE COMES BACK BECAUSE THE VALUE CANNOT BE CONVERTED WITHOUT IT — see the module header.
    Both branches reuse the bucketing that `_metric_by_year` reads through, so this cannot come to
    disagree with the charts about which observation a period is: the annual path IS
    `_latest_per_year`'s rule (it is the same function with the date retained), and the quarterly
    path is `_ttm_by_period` asked for dates instead of labels.

    ⚠⚠ ALL NINETEEN LINES IN **ONE** READ, WHICH IS WHY THIS TAKES A LIST. It used to be called
    once per column, i.e. nineteen bulk reads — and on the COPY transport each one opens its own
    Postgres connection, so ACWI paid nineteen handshakes and nineteen scans of the same index over
    the same ~1,900 constituents to fetch rows that sit side by side in the same table.
    `rows_by_metric` unions the codes; the bucketing below is unchanged and still per metric,
    because the roll-up RULE is per metric (a balance is `last`, a flow is `sum`).

    ⚠ A METRIC `rows_by_metric` REFUSED IS ABSENT, NOT EMPTY. On the quarterly basis a line with no
    declared TTM roll-up is omitted rather than guessed at (`_codes_and_rule` has logged which);
    the caller renders that as a column of dashes, exactly as it did before.
    """
    from routers.earnings import (  # noqa: PLC0415
        _codes_and_rule, _latest_per_year_dated, _ttm_by_period, rows_by_metric,
    )

    raw_by_metric = rows_by_metric(cids, metrics, cadence)
    out: dict[str, dict[int, dict[str, tuple[str, float]]]] = {}
    for metric, raw in raw_by_metric.items():
        # ⚠ ASKED OF `_codes_and_rule`, NOT OF `_TTM_RULE` DIRECTLY — the same call `rows_by_metric`
        # made to choose the codes, so the roll-up applied here cannot drift from the codes read.
        # It is a pair of dict lookups; re-deriving the rule is free, restating it is not.
        _codes, rule = _codes_and_rule(metric, cadence)
        per_company: dict[int, dict[str, tuple[str, float]]] = {}
        for cid, rows in raw.items():
            if rule is None:
                per_company[cid] = dict(_latest_per_year_dated(rows))
            else:
                per_company[cid] = {_period_label(d): (d, v)
                                    for d, v in _ttm_by_period(rows, rule, key="date").items()}
        out[metric] = per_company
    return out


def _unavailable_label(reason: str | None) -> str | None:
    """The two-word badge for `eligible()`'s refusal, or None when the row is fetchable.

    ⚠ CLASSIFIED HERE, NOT IN THE BROWSER. The alternative is the frontend regex-matching
    `eligible()`'s prose to pick a label, which silently changes the badge the day someone rewords
    that sentence — and rewording a message is exactly the kind of edit nobody re-tests.

    Two kinds, because they are two different situations for whoever is looking:
      * UNSUB   — the VENUE is outside the GuruFocus subscription. Nothing to do; it applies to
                  every constituent on that exchange at once and no call will ever succeed.
      * NO GF   — this one company has no GuruFocus ticker or no exchange on file. A data gap on
                  our side, fixable by mapping the row.
    """
    if not reason:
        return None
    return "UNSUB" if reason.endswith("outside the GuruFocus subscription") else "NO GF"


def _tickers(cids: list[int]) -> dict[int, dict]:
    """The identity columns that are always on screen — ticker and the currency the figures are in.

    ⚠ THE CURRENCY IS THE EXCHANGE'S, and it is pinned beside the ticker for a reason: it is what
    makes two native cells incomparable. Once every value column is EUR the column stops being a
    warning and becomes an explanation of the tooltip.

    ⚠ THE EXCHANGE IS NOT DECORATION — IT IS HALF THE IDENTIFIER. GuruFocus addresses a stock as
    `EXCHANGE:TICKER`, and a bare ticker is ambiguous across venues, so the exchange is what makes
    the ticker resolvable at all. It is also what makes the link below constructible.

    ⚠ THE URL IS BUILT HERE, NOT IN THE BROWSER. `_build_symbol` drops the prefix for US venues
    (`AAPL`, not `NASDAQ:AAPL`) and runs `normalize_gurufocus_ticker`, which zero-pads HKSE codes
    to five digits, strips IST/BKK suffixes and turns the `BRK/B` class-share separator into
    `BRK.B`. Re-implementing that in TypeScript would be a second copy of a mapping that already
    has one home — and every one of those rules is a case where the naive `EXCHANGE:TICKER` link
    would 404.
    """
    from deps import IN_CHUNK_SIZE  # noqa: PLC0415

    from ingest.prices import _build_symbol  # noqa: PLC0415

    exch = {e["exchange_id"]: e for e in
            (supabase.table("gurufocus_exchange")
             .select("exchange_id,exchange_code,currency_code").execute().data or [])}
    out: dict[int, dict] = {}
    for i in range(0, len(cids), IN_CHUNK_SIZE):
        for c in (supabase.table("company")
                  .select("company_id,gurufocus_ticker,exchange_id")
                  .in_("company_id", cids[i:i + IN_CHUNK_SIZE]).execute().data or []):
            e = exch.get(c.get("exchange_id")) or {}
            ticker, code = c.get("gurufocus_ticker"), e.get("exchange_code")
            url = None
            if ticker and code:
                # Best-effort: a normalization that raises must not take the whole grid down over
                # one row's link. A missing URL renders as plain text, which is the honest state.
                try:
                    url = f"https://www.gurufocus.com/stock/{_build_symbol(ticker, code)}/summary"
                except Exception:  # noqa: BLE001
                    url = None
            out[c["company_id"]] = {
                "ticker": ticker,
                "exchange": code,
                "currency": e.get("currency_code"),
                "gf_url": url,
            }
    return out


def fundamental_grid(label: str, cadence: str = "annual") -> dict:
    """Every constituent x every line, for every period, in EUR — the payload behind the grid.

    Returned whole rather than one period per request: both cadences come out of ONE GuruFocus
    blob (`fetch_financials` writes `annuals` and `quarterly` together), the read is bulk, and the
    reader's whole interaction is dragging a slider. Paying a round trip per slider position would
    make the one cheap thing about this feature expensive.

    ⚠ SPARSE ON PURPOSE. A company-period with no observation is ABSENT, never present as null —
    the table renders the difference as a dash, and padding would make "not ingested" and "the
    fiscal year has not landed" indistinguishable.
    """
    from routers._benchmark_index import (  # noqa: PLC0415
        INDEX_CAP_PCT, _fx_to_eur, _members, _rate,
    )
    from routers._fundamental_backfill import (  # noqa: PLC0415
        company_rows, eligible, needs,
    )

    # ⚠ TIMED PER STEP, AND AT `warning` SO IT IS ACTUALLY VISIBLE. uvicorn leaves the ROOT logger
    # at WARNING in production, so an `info` line here would be invisible in the one environment
    # whose latency is the reason any of this matters (a Railway->Supabase round trip is 50-200ms
    # against ~2ms locally, so a step's share of the total is not the same in the two places).
    # One line per load of a page a reader opens deliberately is not noise.
    t0 = _perf()
    marks: list[str] = []

    def _step(name: str) -> None:
        marks.append(f"{name} {_perf() - t0:.2f}s")

    cad = normalise_cadence(cadence)
    # ⚠ READ FROM `_benchmark_index`, NEVER RESTATED. A second list of which indices cap is a
    # second thing to forget to update — and the failure is silent, because an uncapped weight is a
    # perfectly well-formed percentage.
    cap_pct = INDEX_CAP_PCT.get(label)
    # ⚠⚠ EVERY CONSTITUENT, INCLUDING THE ONES WE CANNOT PRICE AT ALL (2026-08-06, on request).
    # `_members`' default drops any company with no stored market cap — correct for the cap-weighted
    # index, wrong for a grid that is meant to LIST the index: on the AEX it removed Shell,
    # Unilever and RELX, so the table showed 22 rows and called them the AEX. They are constituents
    # whether or not we can price them, and a row of dashes says that where an absent row says
    # nothing at all.
    members = _members(label, require_market_cap=False)
    if not members:
        return {"label": label, "cadence": cad, "periods": [], "columns": [], "rows": [],
                "members": 0, "enrolled_members": 0, "fillable": 0, "covered": 0, "by_period": {},
                "membership_as_of": "today", "min_coverage_pct": MIN_COVERAGE_PCT,
                "weight_cap_pct": cap_pct}

    by_cid = {m["company_id"]: m for m in members if m.get("company_id")}
    cids = sorted(by_cid)
    _step("members")
    ident = _tickers(cids)
    _step("tickers")

    # ── Why a row can never be filled — computed ONCE and used for both the per-row badge and the
    #    `fillable` count below, so the badge and the button cannot contradict each other.
    #
    # ⚠⚠ AN EMPTY ROW HAS TWO MEANINGS AND THE GRID COULD NOT TELL THEM APART. "Nobody has fetched
    #   this yet" and "this can never be fetched" both rendered as dashes. The second is the more
    #   common one on a global index — 236 of ACWI's 1,998 constituents — and it is not a gap to
    #   close, it is an answer: GuruFocus sells no data for that venue, so no amount of pressing
    #   Fetch will ever fill the row. Leaving them identical invites exactly the wasted effort the
    #   subscription check exists to prevent.
    #
    # ⚠ THE REASON IS PER EXCHANGE, NOT PER COMPANY. `eligible` refuses on
    #   `is_gf_subscribed_exchange`, so every constituent on LSE/NSE/ASX is refused as a block
    #   BEFORE any call is spent — one fact about a venue, not 236 separate discoveries. That is
    #   why the badge can be shown without asking GuruFocus anything.
    #
    # ⚠ SAME `eligible` THE FILL CALLS, deliberately. A second implementation here would be a
    #   second definition of "can this be fetched", and the badge would drift from the button.
    unavailable: dict[int, str] = {}
    # ⚠ `company_rows` RETURNS dict[company_id, row], NOT A LIST — and `needs()` below requires
    #   that shape too (it calls `.items()`). Iterating it directly yields the integer KEYS, which
    #   `eligible` then fails on; an empty-LIST fallback breaks `needs` the same way.
    fill_comps: dict[int, dict] = {}
    try:
        fill_comps = company_rows(cids)
        unavailable = {cid: why for cid, c in fill_comps.items()
                       if (why := eligible(c)) is not None}
    except Exception as e:  # noqa: BLE001
        # A missing badge is a smaller wrong than a failed grid: the row falls back to plain
        # dashes, which is what it showed before this existed.
        _log.warning("[grid] could not resolve per-row availability: %s", e)
    _step("availability")
    # ⚠⚠ THE DENOMINATOR IS NOT THE INDEX, AND THE TOTAL ROW WOULD OTHERWISE SAY IT WAS.
    #
    # `_members` drops any constituent with no stored `market_cap_eur` — see its own module note:
    # on the AEX that is **Shell, Unilever and RELX**, whose LSE listings GuruFocus does not cover.
    # So the grid's member count is 22 where the index holds 25, and a row reading "22/22 caps"
    # claims complete coverage while three of the largest names are not merely unpriced but ABSENT
    # from the count entirely — the most reassuring possible way to be wrong.
    #
    # This is the raw membership, reported alongside so the gap can be stated rather than hidden.
    # ⚠ IT IS NOT DEDUPED and must not be read as the index's true size either: `_members` folds
    # share classes into one row (GOOGL+GOOG are one company), so for the S&P this counts ~10 more
    # than the index has constituents. It is an upper bound on both sides of the comparison, which
    # is why the UI uses it as CONTEXT and never as the denominator.
    enrolled = len({r["company_id"] for r in
                    (supabase.table("universe_membership").select("company_id")
                     .eq("universe_id", (supabase.table("universe").select("universe_id")
                                         .eq("label", label).limit(1)
                                         .execute().data or [{}])[0].get("universe_id"))
                     .execute().data or [])})

    # ── ONE bulk read for every line, and `_values_with_dates` is where the cadence is honoured.
    per_metric = _values_with_dates(cids, [c["key"] for c in COLUMNS], cad)
    _step("metric rows")

    # ── The FX window has to reach the OLDEST period end we are about to convert. A rate lookup
    #    falls back to the most recent EARLIER rate, so a window that starts after a period leaves
    #    that period with no rate at all — and `_rate` returning None reads downstream as
    #    "unpriceable", which would drop the company from its own index silently.
    dates = [d for m in per_metric.values() for spans in m.values() for d, _v in spans.values()]
    currencies = {ident.get(c, {}).get("currency") for c in cids}
    fx = (_fx_to_eur({c for c in currencies if c}, min(dates), max(dates))
          if dates else {})
    _step("fx")

    # ⚠⚠ THE RATE LOOKUP IS MEMOISED, AND THE REASON IS ITS SHAPE, NOT ITS COST PER CALL.
    #
    # `_rate` falls back to "the most recent rate on or before this date" with a list comprehension
    # over every date it holds plus a `max()` — O(n) in the FX history, which here is ~2,800 daily
    # rows per currency. That is fine for the handful of lookups its other callers make and
    # quadratic-ish for this one: a fiscal PERIOD END is very often a market holiday (31 December
    # above all), so most cells take the slow branch, and there is one cell per
    # company x line x period — on ACWI, on the order of 10^5 scans of 10^3 dates.
    #
    # The saving is possible because the ARGUMENTS repeat almost perfectly. Every constituent in a
    # currency shares the same handful of period-end dates, so the distinct `(currency, date)`
    # pairs number in the low thousands against ~10^5 calls.
    #
    # ⚠ IT DELEGATES TO `_rate`, NEVER REIMPLEMENTS IT. That function owns the minor-unit rule
    # (`GBp` is pence: resolve to GBP, scale the RATE by 100) and the on-or-before fallback. A
    # second copy here is how the ÷100 comes to be applied in one place and not the other, which is
    # a hundredfold error that still looks like a number.
    #
    # ⚠ `None` IS CACHED TOO, and must be — "no rate on or before this period end" is the answer
    # that drops a cell, and re-deriving it is the same O(n) scan. Hence the sentinel rather than
    # `if key not in memo`-by-truthiness.
    _rate_memo: dict[tuple[str | None, str], float | None] = {}

    def _rate_for(ccy: str | None, when: str) -> float | None:
        key = (ccy, when)
        if key not in _rate_memo:
            _rate_memo[key] = _rate(fx, ccy, when)
        return _rate_memo[key]

    # ── Assemble per company. `v` is EUR (what the table shows and what any total is built from),
    #    `n` the figure as reported, `fx` the rate applied — so a reader can check the conversion
    #    instead of trusting it.
    rows: list[dict] = []
    periods: set[str] = set()
    for cid in cids:
        m = by_cid[cid]
        ccy = ident.get(cid, {}).get("currency")
        v: dict[str, dict[str, float]] = {}
        n: dict[str, dict[str, float]] = {}
        rates: dict[str, float] = {}
        for key, by_company in per_metric.items():
            is_currency = _unit(key) in _CURRENCY_UNITS
            for period, (date, native) in (by_company.get(cid) or {}).items():
                if period < _FIRST_PERIOD:
                    continue
                if not is_currency:
                    # ⚠ NO RATE IS LOOKED UP AND NONE IS NEEDED. A share count and a percent are
                    # unit-less across currencies — and gating them on a rate would DROP them for
                    # any company whose FX is missing, i.e. hide the two columns that were still
                    # perfectly good. `v == n` here by construction; the frontend shows no native
                    # tooltip for these because there is no second reading to show.
                    periods.add(period)
                    v.setdefault(period, {})[key] = native
                    n.setdefault(period, {})[key] = native
                    continue
                rate = _rate_for(ccy, date)
                if rate is None:
                    # No rate on or before this period end. Recorded as a MISS, never as the native
                    # figure passed through — an unconverted JPY revenue sitting in a EUR column is
                    # the exact error this module exists to prevent, and it would look fine.
                    continue
                periods.add(period)
                v.setdefault(period, {})[key] = native / rate
                n.setdefault(period, {})[key] = native
                rates[period] = rate
        # ⚠ NO `if not v: continue`. A company we hold nothing for still gets a row — it is in the
        # index, and that is the fact the reader is looking for. Dropping it made "not a
        # constituent" and "nothing ingested" render identically (as absence), which is the one
        # distinction this grid exists to draw; the Fill button's whole purpose is closing gaps you
        # can see. `v`/`n`/`fx` stay empty and every cell renders a dash.
        rows.append({
            "company_id": cid, "isin": (m.get("isin") or "").strip().upper() or None,
            "name": m.get("company_name"),
            "ticker": ident.get(cid, {}).get("ticker"), "currency": ccy,
            "exchange": ident.get(cid, {}).get("exchange"),
            "gf_url": ident.get(cid, {}).get("gf_url"),
            # None when the row is fetchable — absent from the payload for ~88% of constituents.
            "unavailable": unavailable.get(cid),
            "unavailable_label": _unavailable_label(unavailable.get(cid)),
            "v": v, "n": n, "fx": rates,
        })

    # ── How many constituents "Fetch all" would actually fetch.
    #
    # ⚠⚠ ASKED OF THE FILL ITSELF, NOT DERIVED FROM `covered`. The obvious label is
    # `members - covered`, and on SP500 that is 234 while the fill fetches 206. Three different
    # denominators are in play — this grid counts every constituent (`require_market_cap=False`),
    # `_members` default drops those with no stored cap, and `covered` means "has any of the
    # nineteen lines" whereas the fill's `needs()` keys on ONE sentinel (Free Cash Flow), so a
    # company with revenue but no FCF is covered here and still needs fetching there. A button
    # promising 234 and fetching 206 is not a rounding difference, it is the button describing
    # something other than what it does.
    #
    # So it calls the same `needs`/`eligible` the job calls. Not duplicated logic — the same
    # functions, which is what makes the number guaranteed to match rather than merely close.
    #
    # ⚠⚠ `feeds=("fin",)` — ONE SENTINEL, AND THE OTHER TWO WERE PURE WASTE HERE. Each sentinel is
    # its own read of `metric_data`, and the only flag this line reads is `need_fin`; the estimates
    # and indicator probes were computed and discarded on every grid load. They are also the
    # expensive two — `indicator_q_forward_pe_ratio` is ~526 rows per company against Free Cash
    # Flow's ~28 — so on ACWI this was the largest single cost of opening the pane. Narrowing it
    # cannot change the answer: `needs` drops a company only when EVERY probed feed is present, and
    # a company with `need_fin` false is dropped under either scope, so the sum is identical.
    #
    # ⚠ THE BULK FILL NARROWS THE SAME WAY under its default `feeds="statements"`, which is what
    # keeps this count and that button's work list the same set. See the ⚠⚠ on `fill_index_
    # fundamentals`: selection and action have to narrow together or the run iterates rows it has
    # nothing to do for.
    try:
        # `fill_comps` is the read done above for the per-row badges — reused, not repeated, so
        # the count and the badges are computed from one snapshot of the company table.
        fillable = sum(1 for c in needs(fill_comps, feeds=("fin",))
                       if c.get("need_fin") and eligible(c) is None)
    except Exception as e:  # noqa: BLE001
        # A count is not worth failing the grid over; the button falls back to no number.
        _log.warning("[grid] could not count the fillable constituents: %s", e)
        fillable = 0
    _step("fillable")

    ordered = sorted(periods)
    by_period = {p: _period_summary(rows, p, len(by_cid), capped=cap_pct is not None)
                 for p in ordered}
    # Heaviest first in the LATEST period we hold — the order a reader expects an index in, and
    # stable across slider positions so a row does not move under the cursor while scrubbing.
    last = ordered[-1] if ordered else None
    rows.sort(key=lambda r: -((r["v"].get(last) or {}).get("market_cap") or 0.0) if last else 0.0)
    _step("assemble")
    # ⚠ CUMULATIVE MARKS, NOT PER-STEP DURATIONS — so the line reads as a timeline and the total is
    # simply the last number. Which STEP dominates is the whole question when this feels slow, and
    # the answer differs by environment: locally the round trips are ~2ms and the assembly loop
    # shows; against Supabase the reads do.
    _log.warning("[grid] %s %s: %d constituents, %d periods in %.2fs — %s",
                 label, cad, len(by_cid), len(ordered), _perf() - t0, " | ".join(marks))
    return {
        "label": label,
        "cadence": cad,
        "periods": ordered,
        "columns": [{"key": c["key"], "label": c["label"], "note": c.get("note"),
                     "unit": _unit(c["key"]),
                     "agg": _AGG_BY_UNIT.get(_unit(c["key"]), "sum")}
                    for c in COLUMNS],
        "members": len(by_cid),
        # The raw membership — see the ⚠⚠ above. Now that nothing is filtered out for want of a
        # cap, `enrolled - members` is only the SHARE CLASSES the dedupe folded (GOOGL+GOOG are one
        # company), not constituents gone missing. Kept because it documents that fold; it is no
        # longer a gap and the UI no longer flags it as one.
        "enrolled_members": enrolled,
        # What "Fetch all" would fetch, counted by the fill's own rules — see the ⚠⚠ above.
        "fillable": fillable,
        # ⚠ ROWS WE HOLD SOMETHING FOR, NOT `len(rows)`. Every constituent has a row now, so
        # `len(rows)` is the membership and would report 100% coverage on an empty grid.
        "covered": sum(1 for r in rows if r["v"]),
        "rows": rows,
        "by_period": by_period,
        # ⚠ STATED, NOT ASSUMED. See the module header: these are TODAY's constituents shown at an
        # older period's figures, and a reader scrubbing to 2016 is entitled to know that.
        "membership_as_of": "today",
        "min_coverage_pct": MIN_COVERAGE_PCT,
        # None for an index whose raw cap weights ARE its weights (SP500, ACWI). A number means
        # this index caps and every weight here would be wrong — see the ⚠⚠ in the module header.
        "weight_cap_pct": cap_pct,
    }


def _period_summary(rows: list[dict], period: str, members: int, *, capped: bool = False) -> dict:
    """What the index looked like in one period: who we could price, and the cap that weights them.

    ⚠ THE DENOMINATOR IS THE INDEX, NOT THE COVERED SET. `covered_pct` divides by every
    constituent, so it falls as you scrub back — which is the finding. Dividing by the rows that
    happen to have data would pin it at 100% in every period and describe nothing.

    ⚠ `weights_usable` IS THE GATE, AND IT IS SEPARATE FROM `covered_pct` ON PURPOSE. A weight is
    only meaningful if the caps behind it cover enough of the index; below the floor the frontend
    shows the per-company figures (which are individually true) and withholds every total and
    weight (which would not be).

    ⚠ `capped` FAILS IT OUTRIGHT, WHATEVER THE COVERAGE. For an index that caps a constituent
    (the AEX at 15%), `cap_i / Σcap` is not that index's weighting at any coverage level — full
    data makes an uncapped weight more precisely wrong, not less. The two reasons stay separate
    fields so the UI can say WHICH one applies: "fill more constituents" is actionable, "this index
    caps" is not.
    """
    # ⚠ NOT NAMED `capped` — THAT IS THE PARAMETER, AND IT WAS SHADOWED HERE. A list of the rows
    # that HAVE a market cap and a flag saying the INDEX caps are unrelated facts that both want
    # the word "cap"; when the local won, `not capped` read "no row has a market cap", so a fully
    # covered uncapped index withheld its weights and the caller had no way to see why. Caught by
    # `TestACappedIndexGetsNoWeightsAtAnyCoverage`, which asserts the uncapped side too — a test
    # that had only checked the refusal would have passed on a function that refused everything.
    with_cap = [r for r in rows if (r["v"].get(period) or {}).get("market_cap")]
    with_any = [r for r in rows if r["v"].get(period)]
    total_cap = sum((r["v"][period]["market_cap"]) for r in with_cap)
    covered_pct = round(100.0 * len(with_any) / members, 2) if members else 0.0
    cap_cov_pct = round(100.0 * len(with_cap) / members, 2) if members else 0.0
    return {
        "covered": len(with_any),
        "members": members,
        "covered_pct": covered_pct,
        "with_market_cap": len(with_cap),
        "cap_covered_pct": cap_cov_pct,
        "total_market_cap_eur": total_cap or None,
        "weights_usable": (not capped) and cap_cov_pct >= MIN_COVERAGE_PCT and total_cap > 0,
    }
