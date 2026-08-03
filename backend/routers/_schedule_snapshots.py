"""Current-picks snapshot writers for the schedule.

Extracted from `routers.scheduled_strategies` (the HTTP layer). These build
`current_picks_snapshot` rows — the price-update re-pricer the pipeline's
momentum phase calls every non-rebalance tick, plus the backtest-seed used
when a strategy is first added. No FastAPI here; pure DB + the backtest
loader.
"""
from __future__ import annotations

import logging
from datetime import date

import pandas as pd

from deps import fetch_in_chunks, supabase

_log = logging.getLogger(__name__)

# Currency symbols → ISO codes. ETF/benchmark currency is sometimes stored as a
# raw symbol ('$') instead of the ISO code the fx_rate table keys on ('USD'), so
# `fx_rates.get('$')` misses → the holding's EUR marks come back blank. Normalize
# before the FX lookup so '$' converts like 'USD'.
_CCY_SYMBOLS = {
    "$": "USD", "US$": "USD", "USD$": "USD",
    "€": "EUR", "£": "GBP", "¥": "JPY",
    "C$": "CAD", "CA$": "CAD", "A$": "AUD", "HK$": "HKD", "CHF": "CHF",
}


def _normalize_currency(raw: str | None) -> str | None:
    """Map a currency symbol/code to its ISO code ('$' → 'USD'); pass real ISO
    codes through; None/empty → None. Unknown symbols are returned uppercased so
    the FX lookup fails gracefully (→ None) rather than mis-converting."""
    if not raw:
        return None
    s = str(raw).strip().upper()
    return _CCY_SYMBOLS.get(s, s) or None


def _fx_asof(series: "pd.Series | None", day_iso: str) -> float | None:
    """Last FX rate on or before `day_iso` (units of the currency per 1 EUR);
    falls back to the earliest rate when the date predates the series. None
    when there's no series."""
    if series is None or len(series) == 0 or not day_iso:
        return None
    ts = pd.Timestamp(day_iso)
    sub = series.loc[series.index <= ts]
    if len(sub) == 0:
        return float(series.iloc[0])
    return float(sub.iloc[-1])


def _to_eur(local: float | None, currency: str | None, day_iso: str,
            fx_rates: dict) -> float | None:
    """Convert a local-currency price to EUR at `day_iso`'s rate. EUR / no
    currency passes through; returns None when the FX rate is unavailable so
    the caller can fall back to a local computation for that holding."""
    if local is None:
        return None
    ccy = (currency or "").upper()
    if not ccy or ccy == "EUR":
        return float(local)
    rate = _fx_asof(fx_rates.get(ccy), day_iso)
    if not rate or rate <= 0:
        return None
    return float(local) / rate


# Two prices this close are the same number to the cent. Below it we are looking at float noise,
# not at the seven-year gap the entry-price correction exists to catch.
_PRICE_EPS = 0.005


def _benchmark_asof(benchmark_id: int, day: str) -> float | None:
    """A benchmark's close ON OR BEFORE `day` — ONE indexed row, never a series scan.

    ⚠ DELIBERATELY NOT A FULL-SERIES READ. Loading the whole series to pick one price is what
    caused the bug this repairs: unpaged and ascending, PostgREST cut it to the oldest thousand
    bars and the lookup answered every recent date with a 2019 close. `order desc + limit 1`
    cannot be truncated into a wrong answer — there is only ever one row to return, and the
    `(benchmark_id, target_date)` index serves it directly.
    """
    rows = (
        supabase.table("benchmark_price")
        .select("price")
        .eq("benchmark_id", benchmark_id)
        .lte("target_date", day)
        .order("target_date", desc=True)
        .limit(1)
        .execute()
    ).data or []
    p = rows[0].get("price") if rows else None
    return float(p) if p is not None else None


def compute_and_save_price_update(
    strategy_id: int,
    ingest_run_id: int | None,
    is_backfill: bool = False,
    as_of_iso: str | None = None,
    cash_pct: float | None = None,
) -> int | None:
    """Build a price_update snapshot for `strategy_id` by re-pricing the
    most recent rebalance's holdings against the latest available close
    prices. Returns the new snapshot_id, or None when no prior rebalance
    exists (nothing to update from).

    Output snapshot fields:
      * `holdings`: same set as the rebalance, but each holding's
        `exit_price_local` + `exit_date` + `forward_return_pct` are
        updated to reflect the latest close.
      * `as_of_date`: unchanged from the rebalance (the entry point
        the returns are measured against).
      * `latest_price_date`: the most recent close-price date seen
        across holdings.
      * `kind`: 'price_update'.

    Used by:
      - the weekly pipeline tick, for every enabled strategy that
        isn't due to rebalance on this tick;
      - the backfill flow, for past Tuesdays where the strategy
        wouldn't have rebalanced (`is_backfill=True`).
    """
    # Order by `as_of_date` (the rebalance date itself), NOT `created_at`.
    # Backfill inserts every historical period's rebalance row in one
    # batch, so all 5 of them share a created_at within milliseconds of
    # each other — `created_at desc` then picks an essentially random
    # row, often the OLDEST as_of_date (the period inserted last by the
    # backfill loop). Ordering by `as_of_date desc` deterministically
    # picks the most-recent rebalance, which is what "the strategy's
    # current open period" actually means.
    rebal_resp = (
        supabase.table("current_picks_snapshot")
        .select("*")
        .eq("scheduled_strategy_id", strategy_id)
        .eq("kind", "rebalance")
        .order("as_of_date", desc=True)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    if not rebal_resp.data:
        return None
    rebal = rebal_resp.data[0]
    holdings = rebal.get("holdings") or []
    if not holdings:
        return None
    # Strip any existing cash sleeve — we re-derive it from the CURRENT cash_pct
    # below, so changing the allocation takes effect on the next re-price. The
    # cash % defaults to the strategy's stored config when not passed explicitly.
    holdings = [h for h in holdings if not h.get("is_cash")]
    if cash_pct is None:
        cash_pct = float((rebal.get("config") or {}).get("cash_pct") or 0.0)

    # Fetch the latest close-price observation for every holding's
    # company_id in one batched query. We `order desc` and pick the
    # first hit per cid in-process — Postgres has no efficient
    # DISTINCT ON via PostgREST. ETF/benchmark holdings carry a NEGATIVE
    # company_id (= -benchmark_id; the engine-wide convention) and are
    # priced from `benchmark_price` instead of `metric_data`.
    cids = [
        h.get("company_id") for h in holdings
        if h.get("company_id") is not None and h.get("company_id") > 0
    ]
    bids = [
        -h["company_id"] for h in holdings
        if h.get("company_id") is not None and h["company_id"] < 0
    ]
    latest_by_cid: dict[int, dict] = {}
    # Chunk to stay under the PostgREST URL-length window (see fetch_in_chunks).
    for r in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("metric_data")
        .select("company_id, target_date, numeric_value")
        .eq("metric_code", "close_price")
        .in_("company_id", chunk)
        .order("target_date", desc=True)
        .execute(),
    ):
        cid = r["company_id"]
        if cid not in latest_by_cid:
            latest_by_cid[cid] = r

    # Latest benchmark close per benchmark_id (for ETF overlay holdings).
    latest_by_bid: dict[int, dict] = {}
    for r in fetch_in_chunks(
        bids,
        lambda chunk: supabase.table("benchmark_price")
        .select("benchmark_id, target_date, price")
        .in_("benchmark_id", chunk)
        .order("target_date", desc=True)
        .execute(),
    ):
        bid = r["benchmark_id"]
        if bid not in latest_by_bid:
            latest_by_bid[bid] = r

    # ETF currency from the `benchmark` table (the AUTHORITATIVE ISO code). The
    # `currency` stored on the holding is unreliable — often None or a raw symbol
    # like '$' — which left ETF EUR marks blank (the '$' case) or silently
    # unconverted as if EUR (the None case). Prefer the benchmark row's code.
    ccy_by_bid: dict[int, str] = {}
    for r in fetch_in_chunks(
        bids,
        lambda chunk: supabase.table("benchmark")
        .select("benchmark_id, currency")
        .in_("benchmark_id", chunk)
        .execute(),
    ):
        if r.get("currency"):
            ccy_by_bid[r["benchmark_id"]] = r["currency"]

    # FX setup so the re-price is in EUR (matching the rebalance path + the
    # rest of the EUR-reported UI). The OLD code computed `forward_return_pct`
    # straight off LOCAL prices ((exit_local-entry_local)/entry_local), so after
    # any price-update the per-holding return + the weighted `period_return_pct`
    # silently dropped the FX leg — a USD/JPY/CHF holding's EUR return diverged
    # from its local one and disagreed with the EUR daily curve + the (€) UI.
    from momentum.data import load_company_currency, load_fx_rates  # noqa: PLC0415

    company_cids = [
        int(h["company_id"]) for h in holdings
        if h.get("company_id") is not None and h["company_id"] > 0
    ]
    ccy_by_cid = load_company_currency(supabase, company_cids) if company_cids else {}

    def _hold_ccy(h: dict) -> str | None:
        cid = h.get("company_id")
        if cid is not None and cid < 0:
            # ETF: the benchmark table's ISO code first, else normalize whatever
            # the holding stored ('$' → USD). Both pass through _normalize_currency
            # so a symbol never silently breaks the FX lookup.
            return _normalize_currency(ccy_by_bid.get(-cid) or h.get("currency"))
        return ccy_by_cid.get(int(cid)) if cid is not None else None

    entry_dates = [str(h["entry_date"])[:10] for h in holdings if h.get("entry_date")]
    start_iso = min(entry_dates) if entry_dates else str(rebal.get("as_of_date") or "")[:10]
    try:
        fx_start = date.fromisoformat(start_iso)
    except ValueError:
        fx_start = date.today()
    currencies = sorted({(_hold_ccy(h) or "EUR") for h in holdings})
    fx_rates = load_fx_rates(supabase, currencies, fx_start, date.today())

    updated_holdings: list[dict] = []
    latest_price_date: str | None = None
    for h in holdings:
        new_h = dict(h)
        cid = h.get("company_id")
        is_etf = cid is not None and cid < 0
        entry_local = h.get("entry_price_local")
        ccy = _hold_ccy(h)
        entry_date_iso = str(h.get("entry_date") or rebal.get("as_of_date") or "")[:10]

        # ⚠ AN ETF'S ENTRY PRICE IS RE-DERIVED EVERY RUN, LIKE ITS ENTRY EUR ALREADY IS.
        #
        # For an overlay sleeve there is exactly one right answer — `benchmark_price` as of the
        # holding's own `entry_date` — and it is a pure function of data we hold. A STORED value
        # can only ever be equal to it or wrong, so re-deriving is idempotent and, when the stored
        # one is wrong, self-healing.
        #
        # It was wrong. `_apply_etf_overlay_to_snapshot` built its as-of lookup from an UNPAGED
        # ascending read, and PostgREST's 1,000-row cloud cap silently cut the series to its
        # OLDEST thousand bars. Measured on SPMO (2,716 bars from 2015-10-12): the cut ends at
        # 2019-10-01, price 40.18 — which /schedule showed as the entry beside a correct 143.83
        # exit on the same date, a +258% return on a days-old position that drifted the weight to
        # 74.5% against a 45% target. The read is paged now, but every snapshot already written
        # kept the bad number, and nothing in the daily pass could repair it.
        #
        # This is the SAME argument the entry-EUR line below already makes, one level up — and
        # making it here means a corrupted entry heals on the next tick instead of needing a
        # script or a button.
        #
        # ⚠ ETFs ONLY, AND THAT LIMIT IS THE POINT. A company's entry comes from `metric_data`,
        # which is NOT append-only in `target_date` — GuruFocus publishes late closes with their
        # true earlier date, so re-deriving a company's entry could legitimately CHANGE a price
        # the strategy actually traded at, rewriting history to match today's data. That is the
        # thing the golden-master test exists to prevent. A benchmark series has no such
        # behaviour, and `benchmark_price` is its single authority.
        if is_etf and entry_date_iso:
            fixed = _benchmark_asof(-cid, entry_date_iso)
            if fixed is not None and (
                not entry_local or abs(float(entry_local) - fixed) > _PRICE_EPS
            ):
                _log.warning(
                    "[schedule] %s entry price corrected %s -> %s (benchmark %s as of %s) — a "
                    "stored entry that disagrees with its own source; see the truncated-read note",
                    h.get("ticker") or f"benchmark {-cid}", entry_local, fixed, -cid,
                    entry_date_iso)
                entry_local = fixed
                new_h["entry_price_local"] = fixed
                # The stored EUR mark was derived from the wrong local price, so it must not be
                # reused — the branch below re-derives it for ETFs unconditionally anyway.
                new_h.pop("entry_price_eur", None)
        # Entry EUR basis. Companies keep their rebalance EUR mark (set with a
        # proper conversion). ETFs ALWAYS re-derive from the benchmark local +
        # entry-date FX: a stored ETF entry_price_eur can be a stale pre-FX
        # pass-through (the local price recorded AS EUR before the currency fix),
        # and mixing that unconverted entry with the now-converted exit corrupts
        # the return. Derive-if-missing for everyone else.
        entry_eur = new_h.get("entry_price_eur")
        if is_etf or not entry_eur or entry_eur <= 0:
            derived = _to_eur(entry_local, ccy, entry_date_iso, fx_rates)
            if derived is not None:
                entry_eur = derived
                new_h["entry_price_eur"] = round(entry_eur, 4)
        # Resolve the latest close: companies from metric_data, ETFs (negative
        # company_id) from benchmark_price (uniform {target_date, value} shape).
        if cid is not None and cid < 0:
            bp = latest_by_bid.get(-cid)
            latest = (
                {"target_date": bp["target_date"], "numeric_value": bp["price"]}
                if bp else None
            )
        else:
            latest = latest_by_cid.get(cid)
        # Refresh the LOCAL price + exit date from the latest close UNCONDITIONALLY
        # (only needs `latest` + `entry_local`). It used to also require a usable
        # `entry_eur`, so a holding whose EUR basis couldn't be computed — e.g. an
        # ETF hit by the '$'/None currency bug — was skipped ENTIRELY and its price
        # FROZE at the last good re-price while the company holdings kept updating.
        # The EUR mark + EUR return are best-effort on top: use them when both ends
        # convert, else fall back to the local return so the holding never freezes.
        if latest and entry_local:
            current_local = float(latest["numeric_value"])
            target_d = str(latest["target_date"])[:10]
            new_h["exit_price_local"] = current_local
            new_h["exit_date"] = target_d
            current_eur = _to_eur(current_local, ccy, target_d, fx_rates)
            if current_eur is not None and entry_eur and entry_eur > 0:
                new_h["exit_price_eur"] = round(current_eur, 4)
                ret = (current_eur / float(entry_eur) - 1) * 100.0
            else:
                # No usable EUR basis — keep the price current and report the
                # local return rather than dropping the holding from the aggregate.
                ret = ((current_local - float(entry_local)) / float(entry_local)) * 100.0
            new_h["forward_return_pct"] = round(ret, 2)
            if latest_price_date is None or target_d > latest_price_date:
                latest_price_date = target_d
        updated_holdings.append(new_h)

    # Apply the cash sleeve (scales every holding by (1-cash) + appends a flat
    # 0%-return cash holding) so the weights + the period return pick up the cash
    # drag. Then the period return via the SINGLE source of truth — the weighted
    # per-holding `forward_return_pct` (see `momentum.portfolio_math`) — keeps the
    # stored `period_return_pct` exactly equal to the weighted mean of the per-row
    # returns the card displays, so the card Total + header MTD can't diverge.
    from momentum.portfolio_math import apply_cash_allocation, portfolio_eur_return_pct  # noqa: PLC0415
    updated_holdings = apply_cash_allocation(updated_holdings, cash_pct)
    portfolio_return = portfolio_eur_return_pct(updated_holdings)

    new_row = {
        "triggered_by": "auto",
        "as_of_date": rebal["as_of_date"],
        "latest_price_date": latest_price_date,
        "config": rebal.get("config"),
        "holdings": updated_holdings,
        "daily_picks": [],
        "strategy_hash": rebal.get("strategy_hash"),
        "name": rebal.get("name"),
        "kind": "price_update",
        "is_backfill": is_backfill,
        "ingest_run_id": ingest_run_id,
        "scheduled_strategy_id": strategy_id,
        # Weighted aggregate of per-holding returns since the prior
        # rebalance — the % gain so far on this position. Renders on
        # the run-history row.
        "period_return_pct": portfolio_return,
    }
    ins = supabase.table("current_picks_snapshot").insert(new_row).execute()
    if not ins.data:
        return None
    # Best-effort log; signature noise is intentional for debugging later.
    _log.info(
        "[price_update] strategy=%s prior_rebal=%s new=%s portfolio_return=%.2f%% "
        "(backfill=%s)",
        strategy_id, rebal.get("snapshot_id"), ins.data[0].get("snapshot_id"),
        portfolio_return or 0.0, is_backfill,
    )
    return int(ins.data[0]["snapshot_id"])


def apply_sleeves_to_snapshot(
    snapshot_id: int,
    *,
    etf_overlay: list[dict] | None,
    cash_pct: float | None,
) -> int | None:
    """Rewrite a stored snapshot's book to `etf_overlay` + `cash_pct`, IN PLACE.

    ONE writer for both callers — the rebalance (which applies the strategy's
    saved sleeves to a freshly-computed selection) and the hand edit (which
    applies newly-typed ones to the open period). Two writers would be two
    chances for the live book to disagree with what the config says it is.

    The stock sleeve is always rebuilt from the snapshot's own stock holdings,
    renormalized to sum-1 first (see `apply_sleeves`), so the weights are
    re-derived from the UNDERLYING STRATEGY's selection every time rather than
    compounded onto whatever the last edit left behind.

    `etf_overlay` entries are `{benchmark_id, weight_pct}` where `weight_pct` is
    a share of the INVESTED (non-cash) book — the convention the diversifier
    writes and the backtest blend assumes. Returns the new holdings count, or
    None when the snapshot is gone.
    """
    import bisect  # noqa: PLC0415

    from momentum.blend_backtest import make_etf_holding  # noqa: PLC0415
    from momentum.portfolio_math import (  # noqa: PLC0415
        apply_sleeves,
        portfolio_eur_return_pct,
        split_book,
    )

    snap_resp = (
        supabase.table("current_picks_snapshot")
        .select("holdings, as_of_date, latest_price_date")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    )
    if not snap_resp.data:
        return None
    snap = snap_resp.data[0]
    stock_holdings, prior_etfs, _prior_cash = split_book(snap.get("holdings") or [])

    overlay = [o for o in (etf_overlay or []) if o.get("benchmark_id")]
    as_of = str(snap.get("as_of_date") or "")[:10]
    latest = str(snap.get("latest_price_date") or as_of)[:10]

    # ⚠ ETF ENTRY MUST ANCHOR TO THE SAME BAR THE STOCK SLEEVE ENTERED ON — the
    # prior trading day's close the picks are anchored to
    # (`run_current_portfolio` enters stocks at
    # `_price_on_or_before(rebalance_date − 1)`), NOT the raw `as_of`. `as_of` is
    # the nominal rebalance grid date, which can be a FUTURE Monday when the tick
    # fires early (Saturday, for the upcoming Monday). Using it stamped a future
    # ETF entry_date and priced entry against a not-yet-real (or corrupt future)
    # benchmark bar — the SPMO +277% incident. Take the stock sleeve's actual
    # entry_date (they all share the prior-trading-day anchor); fall back to
    # `latest` (the freshest real close, never the future) for a pure-ETF
    # strategy with no stock holdings.
    stock_entry_dates = [
        str(h.get("entry_date"))[:10] for h in stock_holdings if h.get("entry_date")
    ]
    entry_ref = min(stock_entry_dates) if stock_entry_dates else latest
    if latest and entry_ref > latest:
        entry_ref = latest  # never price entry past the latest real close

    etf_holdings: list[dict] = []
    if overlay:
        bids = [int(o["benchmark_id"]) for o in overlay]
        meta_resp = (
            supabase.table("benchmark")
            .select("benchmark_id, ticker, name, sector, currency")
            .in_("benchmark_id", bids)
            .execute()
        )
        meta = {m["benchmark_id"]: m for m in (meta_resp.data or [])}

        # Daily benchmark closes per id, for as-of (last-on-or-before) lookups.
        px: dict[int, tuple[list[str], list[float]]] = {}
        for bid in bids:
            # ⚠⚠ PAGED, AND THE BUG IT FIXES PRINTED A SEVEN-YEAR-OLD PRICE AS TODAY'S ENTRY.
            #
            # This read is ASCENDING and was unpaged. PostgREST caps a response at 1,000 rows on
            # Supabase cloud (10,000 locally) and truncates SILENTLY, so in production the series
            # stopped at the 1,000th BAR — the OLDEST thousand — and `_asof` then answered every
            # recent date with the last row it happened to have.
            #
            # Measured 2026-08-03 on SPMO (Invesco S&P 500 Momentum ETF), 2,716 bars from
            # 2015-10-12: the 1,000-row cut ends at **2019-10-01, price 40.18**. That is exactly
            # what /schedule showed as the entry — "Start (local) 40.18 USD, as of 2026-07-31" —
            # beside a true End of 143.83 on the same date. A +258% return on a position opened
            # days earlier, which then drifted the Current weight to 74.5% against a 45.0% target.
            # Every figure downstream of that entry was wrong, and nothing raised.
            #
            # ⚠ THE EXIT WAS RIGHT, WHICH IS WHY IT LOOKED LIKE A DISPLAY BUG. The daily
            # price-update re-prices the exit through a different path, so the row carried one
            # correct price and one seven-year-old one, both stamped with today's date.
            #
            # 4 of our 5 benchmarks already exceed 1,000 bars (SPY reaches back to 1998), so this
            # was live for every ETF overlay in production and invisible in local dev.
            rows: list[dict] = []
            off = 0
            while True:
                page = (
                    supabase.table("benchmark_price")
                    .select("target_date, price")
                    .eq("benchmark_id", bid)
                    .lte("target_date", latest or as_of)
                    # `(benchmark_id, target_date)` is unique, so a page boundary cannot serve a
                    # row twice or skip one.
                    .order("target_date")
                    .range(off, off + 999)
                    .execute()
                ).data or []
                if not page:
                    break
                rows += page
                off += len(page)      # advance by what came back — correct under any cap
            if rows:
                px[bid] = (
                    [str(r["target_date"])[:10] for r in rows],
                    [float(r["price"]) for r in rows],
                )

        def _asof(bid: int, day: str) -> float | None:
            pair = px.get(bid)
            if not pair or not day:
                return None
            ds, ps = pair
            i = bisect.bisect_right(ds, day) - 1
            return ps[i] if i >= 0 else None

        for o in overlay:
            bid = int(o["benchmark_id"])
            m = meta.get(bid) or {}
            etf_holdings.append(make_etf_holding(
                benchmark_id=bid,
                ticker=m.get("ticker") or f"BM{bid}",
                name=m.get("name") or m.get("ticker") or f"Benchmark {bid}",
                sector=m.get("sector"),
                weight=float(o.get("weight_pct") or 0.0) / 100.0,
                entry_price=_asof(bid, entry_ref),
                exit_price=_asof(bid, latest),
                entry_date=entry_ref or None,
                exit_date=latest or None,
                currency=m.get("currency"),
            ))

    if not etf_holdings and not prior_etfs and not float(cash_pct or 0.0) and not _prior_cash:
        return None  # nothing to apply and nothing to strip — leave the row alone

    merged = apply_sleeves(stock_holdings, etf_holdings, cash_pct)
    supabase.table("current_picks_snapshot").update({
        "holdings": merged,
        # The blended period return via the SINGLE source of truth (weighted
        # per-holding `forward_return_pct`) — identical basis to
        # `compute_and_save_price_update`, so the card's Total cannot disagree
        # with the header MTD.
        "period_return_pct": portfolio_eur_return_pct(merged),
    }).eq("snapshot_id", snapshot_id).execute()
    return len(merged)


def _coerce_as_of_date(raw: str | None) -> str:
    """Backtest period dates are YYYY-MM strings; current_picks_snapshot
    expects YYYY-MM-DD. Convert by appending '-01'."""
    if not raw:
        return date.today().isoformat()
    s = str(raw)
    if len(s) == 7 and s[4] == "-":  # YYYY-MM
        return s + "-01"
    return s[:10]


def _latest_exit_date(rec: dict) -> str | None:
    """Highest `exit_date` across the record's holdings — a reasonable
    proxy for the snapshot's `latest_price_date`."""
    out: str | None = None
    for h in (rec.get("holdings") or []):
        d = h.get("exit_date") or h.get("entry_date")
        if d and (out is None or d > out):
            out = d
    return out


def _seed_snapshot_from_backtest(
    strategy_id: int, backtest_run_id: int | None, name: str, config: dict,
) -> int | None:
    """Seed the strategy's first `current_picks_snapshot` from its saved
    backtest's most-recent period — so it has live holdings the daily price
    refresh can track IMMEDIATELY, with no off-cycle rebalance (no universe
    reprice / template scrape). The holdings ARE the backtest's current
    picks. Then re-price them against the latest DB closes so the seed
    reflects today's data, not the backtest's end date.

    Returns the seeded snapshot_id (or None when there's nothing to seed:
    no backtest run, empty result, or no holdings)."""
    from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

    if not backtest_run_id:
        return None
    result = load_backtest_result_sync(backtest_run_id)
    monthly = (result or {}).get("monthly_records") or []
    if not monthly:
        return None
    last = monthly[-1]
    holdings = last.get("holdings") or []
    if not holdings:
        return None

    row = {
        "triggered_by": "auto",
        # The backtest's last period is the current open period. Anchor the
        # snapshot to its REBALANCE date — the record's `date` field, which is
        # always the exact rebalance Monday (e.g. 2026-06-01). NOT `as_of_date`:
        # the runner sets that to the open period's EXIT date (`open_as_of`, the
        # latest data date) for the blend/curve math, so using it here made the
        # snapshot read "held since <latest price date>" instead of the rebalance.
        "as_of_date": _coerce_as_of_date(last.get("date") or last.get("as_of_date")),
        "latest_price_date": _latest_exit_date(last),
        "config": config,
        "holdings": holdings,
        "daily_picks": [],
        "strategy_hash": None,
        "name": name,
        "kind": "rebalance",
        # Seeded from the backtest, not a live pipeline rebalance.
        "is_backfill": True,
        "scheduled_strategy_id": strategy_id,
        "period_return_pct": last.get("portfolio_return_pct"),
    }
    ins = supabase.table("current_picks_snapshot").insert(row).execute()
    if not ins.data:
        return None
    seeded_id = int(ins.data[0]["snapshot_id"])
    # Re-price against the latest available closes so 'since go-live' starts
    # from current data. Best-effort — the daily refresh would do it anyway.
    try:
        compute_and_save_price_update(strategy_id, ingest_run_id=None, is_backfill=True)
    except Exception as e:
        _log.warning(
            "[seed] strategy=%s post-seed price_update failed: %s: %s",
            strategy_id, type(e).__name__, e,
        )
    _log.info("[seed] strategy=%s seeded snapshot=%s from backtest_run=%s (%s holdings)",
              strategy_id, seeded_id, backtest_run_id, len(holdings))
    return seeded_id
