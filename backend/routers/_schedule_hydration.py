"""Run-history hydration for the schedule list endpoints.

Extracted from `routers.scheduled_strategies`. `_hydrate` attaches each
strategy's latest-snapshot summary + MTD/YTD rollups (walked from the full
snapshot history by `_compute_period_returns`). Pure read-side: queries
`current_picks_snapshot`, no writes.
"""
from __future__ import annotations

import bisect
from datetime import date

from deps import fetch_in_chunks, supabase


def _extract_sectors(holdings: list[dict] | None) -> list[dict]:
    """Distinct sectors from a holdings list, ordered by count desc then
    alpha. Empty list when no holdings or no sectors. Used for the
    /schedule collapsed-row summary."""
    if not holdings:
        return []
    counts: dict[str, int] = {}
    for h in holdings:
        sec = (h.get("sector") or "").strip()
        if not sec:
            continue
        counts[sec] = counts.get(sec, 0) + 1
    if not counts:
        return []
    return [
        {"sector": sec, "count": cnt}
        for sec, cnt in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))
    ]


def _walk_snapshot_curve(
    snapshots: list[dict],
) -> tuple[list[tuple[str, float]], str | None, float]:
    """Walk a strategy's current-picks snapshot history into a relative
    equity curve (base 1.0). The single source of truth for a strategy's
    LIVE forward performance — `period_return_pct` is marked-to-market by
    the price-update job, so this curve always reaches the latest priced
    day (unlike `current_picks_day`, which only advances on a full compute).

    `snapshots` must be ascending by (latest_price_date, created_at).

    Snapshot convention: `period_return_pct` on each row is the running
    return for THAT row's open period as of the row's `latest_price_date`.
    For a BACKFILL rebalance it's the full closed-period return; for a LIVE
    rebalance it's 0% at creation, then refreshed by the price_update flow.

    Walker rules:
      - rebalance: close the prior period at its running return, then open
        a new one whose initial running return = this row's stored value.
      - price_update: refresh the open period's running return.

    Returns `(curve, last_rebalance_eff_date, open_period_start_equity)`,
    where `curve` is `[(effective_date, equity), ...]` (effective_date =
    the day the row's return is marked through). The latter two feed
    `_compute_period_returns`'s month-anchor logic."""
    open_period_return_pct = 0.0
    open_period_start_equity = 1.0
    last_rebalance_eff_date: str | None = None
    curve: list[tuple[str, float]] = []

    for s in snapshots:
        eff_date = str(s.get("latest_price_date") or s.get("as_of_date") or "")[:10]
        if not eff_date:
            continue
        kind = s.get("kind") or "rebalance"
        pct = s.get("period_return_pct")
        if kind == "rebalance":
            open_period_start_equity = open_period_start_equity * (1.0 + open_period_return_pct / 100.0)
            open_period_return_pct = float(pct) if pct is not None else 0.0
            last_rebalance_eff_date = eff_date
        elif pct is not None:
            open_period_return_pct = float(pct)
        # Same-date rows overwrite (last wins) — a later price_update is
        # more current.
        equity_now = open_period_start_equity * (1.0 + open_period_return_pct / 100.0)
        curve.append((eff_date, equity_now))

    return curve, last_rebalance_eff_date, open_period_start_equity


def _compute_period_returns(snapshots: list[dict], today: date) -> dict:
    """MTD + YTD returns for a strategy, read off the snapshot equity curve
    (`_walk_snapshot_curve`).

    Returns {mtd_return_pct, ytd_return_pct, as_of_date} all None-able.
    `as_of_date` is the latest_price_date of the newest snapshot, surfaced
    so the UI can render "+12.7% (as of 2026-05-22)" without a second
    lookup. Used as the fallback when a strategy has no source backtest;
    when it does, `_returns_from_backtest` is preferred (anchored at
    go-live, finer-grained history)."""
    if not snapshots:
        return {"mtd_return_pct": None, "ytd_return_pct": None, "as_of_date": None}

    curve, last_rebalance_eff_date, open_period_start_equity = _walk_snapshot_curve(snapshots)
    if not curve:
        return {"mtd_return_pct": None, "ytd_return_pct": None, "as_of_date": None}

    latest_date, latest_equity = curve[-1]
    month_start = today.replace(day=1).isoformat()
    year_start = today.replace(month=1, day=1).isoformat()

    # YTD anchor: last equity point strictly before year start, else 1.0
    # (strategy started inside the year — measure from inception).
    ytd_anchor = 1.0
    for d, e in curve:
        if d < year_start:
            ytd_anchor = e

    # MTD anchor:
    #   Default — last equity point strictly before month_start.
    #   Override — when the latest rebalance fired IN this month, anchor
    #     at the open-period start equity (post-close of the prior period
    #     by THIS rebalance). That way MTD reads as "return since the
    #     latest rebalance" for cadences where the rebalance landed in
    #     this month (monthly / weekly / daily), rather than including a
    #     chunk of the prior month's open period that we can't cleanly
    #     attribute to either calendar month.
    mtd_anchor = 1.0
    for d, e in curve:
        if d < month_start:
            mtd_anchor = e
    if last_rebalance_eff_date and last_rebalance_eff_date >= month_start:
        mtd_anchor = open_period_start_equity

    def _pct(end: float, start: float) -> float | None:
        if start <= 0:
            return None
        return round((end / start - 1.0) * 100.0, 2)

    return {
        "mtd_return_pct": _pct(latest_equity, mtd_anchor),
        "ytd_return_pct": _pct(latest_equity, ytd_anchor),
        "as_of_date": latest_date,
    }


def _scale_curve_returns(
    pts: list[tuple[str, float]], cash_pct: float | None, *, as_pct: bool,
) -> list[tuple[str, float]]:
    """Apply a cash sleeve to a return curve: scale EVERY period's return by
    `(1 - cash_pct)` and recompound. `pts` = `[(date, level)]`, where `level` is
    cumulative return % when `as_pct` else equity (base 1.0). This is the exact
    cash-drag transform of a curve — annualized return + drawdown shrink, while
    Sharpe/Sortino (mean÷vol, both scaled) are unchanged. No-op when cash ≤ 0."""
    pct = min(max(float(cash_pct or 0.0), 0.0), 1.0)
    if not pts or pct <= 0.0:
        return pts
    scale = 1.0 - pct
    out: list[tuple[str, float]] = []
    prev_eq = 1.0
    cur = 1.0
    first = True
    for d, lv in pts:
        eq_in = (1.0 + float(lv) / 100.0) if as_pct else float(lv)
        r = (eq_in - 1.0) if first else (eq_in / prev_eq - 1.0) if prev_eq else 0.0
        first = False
        cur *= (1.0 + r * scale)
        prev_eq = eq_in
        out.append((d, (cur - 1.0) * 100.0 if as_pct else cur))
    return out


def _curve_stats(pts: list[tuple[str, float]]) -> tuple[float | None, float | None]:
    """`(annualized_return_pct, max_drawdown_magnitude_pct)` from a cumulative-
    return-% curve `[(date, cum_pct)]`. maxdd is a POSITIVE magnitude. Both None
    when the curve is too short. Used to recompute cash-adjusted risk stats."""
    if not pts or len(pts) < 2:
        return None, None
    eqs = [(d, 1.0 + c / 100.0) for d, c in pts]
    peak = eqs[0][1]
    mdd = 0.0
    for _, e in eqs:
        if e > peak:
            peak = e
        if peak > 0:
            mdd = max(mdd, (peak - e) / peak)
    try:
        d0 = date.fromisoformat(eqs[0][0][:10])
        d1 = date.fromisoformat(eqs[-1][0][:10])
        years = max((d1 - d0).days / 365.25, 1e-9)
    except ValueError:
        years = 1.0
    final = eqs[-1][1]
    ann = ((final ** (1.0 / years)) - 1.0) * 100.0 if final > 0 else None
    return ann, mdd * 100.0


def _load_backtest_pts(backtest_run_id: int, cash_pct: float = 0.0) -> list[tuple[str, float]]:
    """The saved backtest's daily equity curve as
    ``[(YYYY-MM-DD, cumulative_return_pct), ...]`` ascending, with the strategy's
    cash sleeve applied (returns scaled by `1-cash_pct`). Empty when the run has
    no stored curve. Best-effort (storage errors → empty)."""
    from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

    res = load_backtest_result_sync(backtest_run_id)
    pts: list[tuple[str, float]] = []
    for d in (res or {}).get("daily_records") or []:
        dt = str(d.get("date") or "")[:10]
        cum = d.get("cumulative_return_pct")
        if dt and cum is not None:
            pts.append((dt, float(cum)))
    pts.sort(key=lambda p: p[0])
    return _scale_curve_returns(pts, cash_pct, as_pct=True)


def _open_basket_live_curve(backtest_run_id: int, cash_pct: float = 0.0) -> list[tuple[str, float]]:
    """Dense DAILY equity curve (base 1.0) of the strategy's OPEN-period basket:
    the source backtest's last-period holdings, re-priced EVERY trading day in
    EUR from their entry through the latest available close.

    Same basis as the Portfolio holdings table's open-period reprice
    (`repriceOpenPeriod`): per-holding `price_eur / entry_price_eur`, weighted-
    mean on the long side minus the short side. This is the single source that
    fills the daily-returns chart past the backtest's saved end — the backtest
    curve freezes on its save date and the per-snapshot tail only had a point
    per price-update, leaving multi-day gaps (e.g. Jun 12 → Jun 22). Marking the
    held basket from daily closes gives a point per trading day, and its open
    month equals the holdings table's figure by construction.

    Returns `[(date, equity), ...]`; empty when the run/holdings/prices are
    unavailable (caller falls back to the sparse snapshot walk)."""
    from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

    res = load_backtest_result_sync(backtest_run_id)
    monthly = (res or {}).get("monthly_records") or []
    if not monthly:
        return []
    holdings = (monthly[-1].get("holdings") or [])

    def _side(want: str) -> list[tuple[int, float, float]]:
        out = []
        for h in holdings:
            cid, entry = h.get("company_id"), h.get("entry_price_eur")
            if cid is None or not entry:
                continue
            if (h.get("side") or "long") != want:
                continue
            w = h.get("weight")
            out.append((int(cid), float(entry), float(w) if w else 1.0))
        return out

    longs, shorts = _side("long"), _side("short")
    if not longs and not shorts:
        return []
    # Companies price from metric_data (+FX→EUR); ETF overlay holdings carry a
    # NEGATIVE company_id (= -benchmark_id) and price from benchmark_price
    # (currency-agnostic — the daily return ratio price/entry is unit-free).
    cids = [c for c, _, _ in (*longs, *shorts) if c > 0]
    bids = [-c for c, _, _ in (*longs, *shorts) if c < 0]
    entry_dates = [str(h["entry_date"])[:10] for h in holdings if h.get("entry_date")]
    start_iso = min(entry_dates) if entry_dates else str(monthly[-1].get("date"))[:10]

    from datetime import date as _date  # noqa: PLC0415

    from momentum.data import (  # noqa: PLC0415
        convert_prices_to_eur, load_all_prices, load_company_currency, load_fx_rates,
    )
    try:
        start = _date.fromisoformat(start_iso)
    except ValueError:
        return []
    today = date.today()

    # Per-cid sorted (date, price) for an asof (last-on-or-before) lookup.
    # Companies are EUR-converted; ETF benchmarks are kept in native price
    # (only their return ratio is used, which is currency-agnostic).
    px: dict[int, tuple[list[str], list[float]]] = {}
    if cids:
        local_df = load_all_prices(supabase, cids, start, today)
        if not local_df.empty:
            cur = load_company_currency(supabase, cids)
            currencies = sorted({c for c in cur.values() if c})
            fx = load_fx_rates(supabase, currencies, start, today) if currencies else {}
            eur_df, _ = convert_prices_to_eur(local_df, cur, fx)
            for cid, group in eur_df.groupby("company_id"):
                g = group.sort_values("target_date")
                ds = [d.isoformat() if hasattr(d, "isoformat") else str(d)[:10] for d in g["target_date"]]
                px[int(cid)] = (ds, [float(p) for p in g["price"]])

    # ETF overlay holdings (negative cid) — daily benchmark closes, keyed by
    # the same negative cid the holdings carry.
    for bid in bids:
        rows: list[dict] = []
        offset = 0
        while True:
            resp = (
                supabase.table("benchmark_price")
                .select("target_date, price")
                .eq("benchmark_id", bid)
                .gte("target_date", start.isoformat())
                .order("target_date")
                .range(offset, offset + 999)
                .execute()
            )
            batch = resp.data or []
            rows.extend(batch)
            if len(batch) < 1000:
                break
            offset += 1000
        if rows:
            ds = [str(r["target_date"])[:10] for r in rows]
            px[-bid] = (ds, [float(r["price"]) for r in rows])

    all_days = sorted({d for ds, _ in px.values() for d in ds})
    if not all_days:
        return []

    def _asof(cid: int, day: str) -> float | None:
        pair = px.get(cid)
        if not pair:
            return None
        ds, ps = pair
        i = bisect.bisect_right(ds, day) - 1
        return ps[i] if i >= 0 else None

    def _side_ret(side: list[tuple[int, float, float]], day: str) -> float | None:
        num = wsum = 0.0
        for cid, entry, w in side:
            p = _asof(cid, day)
            if p is None or entry <= 0:
                continue
            num += w * (p / entry - 1.0)
            wsum += w
        return (num / wsum) if wsum > 0 else None

    curve: list[tuple[str, float]] = []
    for d in all_days:
        lr = _side_ret(longs, d) if longs else None
        sr = _side_ret(shorts, d) if shorts else None
        if lr is None and sr is None:
            continue
        curve.append((d, 1.0 + (lr or 0.0) - (sr or 0.0)))
    # This basket is FULLY invested (the backtest holdings carry no cash), so
    # apply the strategy's cash sleeve here (unlike the snapshot walk, which is
    # already cash-aware via period_return_pct).
    return _scale_curve_returns(curve, cash_pct, as_pct=False)


def _splice_snapshot_tail(
    backtest_pts: list[tuple[str, float]],
    snap_curve: list[tuple[str, float]],
) -> tuple[str | None, list[dict]]:
    """Graft the LIVE held-basket curve onto the (frozen) backtest daily curve,
    with live data taking precedence from where it begins (≈ go-live).

    `backtest_pts` — the backtest curve as
    ``[(YYYY-MM-DD, cumulative_return_pct), ...]`` (any order).
    `snap_curve`   — the snapshot equity curve from `_walk_snapshot_curve`
    (base 1.0), which the price-update job marks to market through the
    latest priced day. This is the single live source — `current_picks_day`
    is deliberately NOT used here: it only advances on a full compute and so
    lags the price-update snapshots.

    Returns ``(cutover_date, tail_points)``:
      * `cutover_date` — the live curve's FIRST day. The caller keeps backtest
        points strictly before it and appends `tail_points`. None only when
        there's nothing to splice (an empty curve on either side).
      * `tail_points` — ``[{"date", "cumulative_return_pct"}, ...]`` on the
        SAME cumulative scale as the backtest curve.

    The live curve REPLACES the backtest curve from its first day onward: the
    backtest is hypothetical *context* for the pre-go-live history, but once a
    strategy is live the held basket is what actually happened — even on
    calendar days the saved backtest happens to cover. The whole live curve is
    rebased to continue from the backtest's cumulative level AT the cutover, so
    no level mismatch shows at the join.

    (Previously only snapshot points dated AFTER the backtest curve's last day
    were grafted. When a saved backtest's horizon ran past go-live, the go-live
    month then showed the backtest's curve instead of the real basket — the
    cause of the /schedule monthly-returns vs holdings-period-return mismatch.)"""
    if not backtest_pts or not snap_curve:
        return None, []
    bt = sorted(backtest_pts, key=lambda p: p[0])
    sc = sorted(snap_curve, key=lambda p: p[0])

    # Cut over to the live curve at its first day (≈ go-live) — that's where
    # the real held basket begins and supersedes the backtest.
    cutover_date = sc[0][0]
    anchor_eq = sc[0][1]
    if anchor_eq <= 0:
        return None, []

    # Backtest cumulative level the live curve continues from: the last backtest
    # point STRICTLY BEFORE the cutover (the prior period's close). The live
    # basket ENTERS on the cutover day with no return yet (its first point ≈ eq
    # 1.0), so it must continue from the day-before close — NOT the cutover-day
    # backtest level. With a dense daily backtest curve (a point ON the cutover
    # day), anchoring on/before would fold that day's BACKTEST move into the
    # rebase, leaking it into MTD/YTD (anchored at the prior period close) and
    # making them disagree with the holdings open-period return — the exact
    # mismatch this splice exists to prevent. Falls back to the backtest's start
    # when the live curve predates the backtest entirely.
    bt_cum_at_cut = bt[0][1]
    for d, cum in bt:
        if d < cutover_date:
            bt_cum_at_cut = cum
        else:
            break

    rebase = (1.0 + bt_cum_at_cut / 100.0) / anchor_eq
    tail = [
        {"date": d, "cumulative_return_pct": round((e * rebase - 1.0) * 100.0, 6)}
        for d, e in sc
    ]
    return cutover_date, tail


def _extended_curve(
    backtest_run_id: int, snapshots: list[dict], cash_pct: float = 0.0,
) -> list[tuple[str, float]]:
    """The strategy's full equity curve: the backtest daily curve with the
    live snapshot tail spliced on (continuous cumulative scale), with the cash
    sleeve applied. The single source of truth shared by the run-history rollups
    (`_returns_from_backtest`) and the detail view's live curve
    (`build_live_curve`). Empty when the run has no stored curve."""
    bt_pts = _load_backtest_pts(backtest_run_id, cash_pct)   # cash-scaled
    if not bt_pts:
        return []
    # Prefer the dense daily open-basket curve (a point per trading day, matches
    # the holdings table's open-period figure); fall back to the sparse
    # per-snapshot walk only when the basket can't be re-priced. The open-basket
    # is fully-invested → cash-scaled; the walk is already cash-aware → left as-is.
    snap_curve = _open_basket_live_curve(backtest_run_id, cash_pct)
    if not snap_curve:
        snap_curve, _, _ = _walk_snapshot_curve(snapshots or [])
    cutover, tail = _splice_snapshot_tail(bt_pts, snap_curve)
    if not cutover or not tail:
        return bt_pts
    # Keep backtest points strictly before the cutover; the live tail supersedes
    # the curve from go-live on (it may start before the backtest's end, so we
    # must drop the overlapping backtest tail — not just append).
    kept = [(d, c) for d, c in bt_pts if d < cutover]
    return kept + [(p["date"], p["cumulative_return_pct"]) for p in tail]


def _returns_from_backtest(
    backtest_run_id: int,
    inception_iso: str,
    today: date,
    snapshots: list[dict] | None = None,
    clamp_calendar_to_inception: bool = True,
    cash_pct: float = 0.0,
) -> dict | None:
    """MTD / YTD / since-inception returns read off the strategy's full
    equity curve (`_extended_curve`), anchored at the go-live date.

    The backtest curve alone goes stale on the day it was saved; splicing
    the live snapshot tail keeps MTD/YTD/since-inception + `as_of_date`
    tracking the latest priced day.

    - MTD  = from the start of the current calendar month
    - YTD  = from the start of the current calendar year
    - since-inception = from the go-live date (`inception_iso`)

    MTD/YTD are calendar-anchored BUT clamped to never reach earlier than the
    go-live date: a strategy is only "real" from go-live on, and the curve
    before that is the hypothetical backtest. So a strategy launched mid-year
    reports a YTD of just its live performance (not the backtest's Jan→launch
    gains); for a strategy live since a prior year the calendar anchors apply
    unchanged. Returns None when the run has no curve."""
    pts = _extended_curve(backtest_run_id, snapshots or [], cash_pct)
    if not pts:
        return None
    latest_date, latest_cum = pts[-1]

    def cum_at(date_iso: str) -> float | None:
        """Cumulative-return level at the last curve point on-or-before `date_iso`."""
        v: float | None = None
        for dt, cum in pts:
            if dt <= date_iso:
                v = cum
            else:
                break
        return v

    def cum_before(date_iso: str) -> float | None:
        """Cumulative-return level at the last curve point STRICTLY before
        `date_iso`. This is the period-boundary anchor the monthly-returns
        chart uses (its month cell = lastCum(thisMonth) / lastCum(prevMonth)),
        so MTD/YTD computed off it match the chart's month/year cells exactly —
        the daily curve is the single basis."""
        v: float | None = None
        for dt, cum in pts:
            if dt < date_iso:
                v = cum
            else:
                break
        return v

    def rel(a: float | None, b: float | None) -> float | None:
        if a is None or b is None:
            return None
        return round(((1 + a / 100.0) / (1 + b / 100.0) - 1) * 100.0, 2)

    # Anchor cumulative levels; when an anchor predates the curve, fall back
    # to the curve's start (earliest data we have). MTD/YTD anchor STRICTLY
    # before the month/year start (= the prior period's last close), matching
    # how the monthly-returns chart chains its cells, so the header agrees with
    # the chart to the cent. since-inception anchors AT the go-live date.
    curve_start_cum = pts[0][1]
    inc_cum = cum_at(inception_iso)
    if inc_cum is None:
        inc_cum = curve_start_cum
    year_start = today.replace(month=1, day=1).isoformat()
    month_start = today.replace(day=1).isoformat()
    # Clamp the MTD/YTD anchors to the go-live date ONLY when an explicit
    # go-live was set (`clamp_calendar_to_inception`). Then a strategy launched
    # mid-period measures live-only — it never reports the pre-go-live backtest
    # curve in its MTD/YTD (e.g. a May go-live wouldn't show the backtest's
    # Jan→May gains in YTD). When no go-live is set (anchor defaulted to
    # created_at), DON'T clamp: use the full calendar month/year off the daily
    # curve, so the header MTD/YTD equal the monthly-returns chart's cells
    # (which never clamp the cell value).
    inc = (inception_iso or "")[:10]
    if clamp_calendar_to_inception and inc and inc >= year_start:
        ytd_cum = inc_cum
    else:
        ytd_cum = cum_before(year_start)
        if ytd_cum is None:
            ytd_cum = curve_start_cum
    if clamp_calendar_to_inception and inc and inc >= month_start:
        mtd_cum = inc_cum
    else:
        mtd_cum = cum_before(month_start)
        if mtd_cum is None:
            mtd_cum = curve_start_cum
    return {
        "mtd_return_pct": rel(latest_cum, mtd_cum),
        "ytd_return_pct": rel(latest_cum, ytd_cum),
        "since_inception_pct": rel(latest_cum, inc_cum),
        "inception_date": inception_iso,
        "as_of_date": latest_date,
    }


def build_live_curve(backtest_run_id: int, snapshots: list[dict], cash_pct: float = 0.0) -> dict | None:
    """The live-extension of a scheduled strategy's backtest curve, for the
    detail view's monthly-returns heatmap + equity curve.

    Splices the snapshot tail (`_splice_snapshot_tail`) onto the backtest
    daily curve — same single source as the run-history rollups. The cash sleeve
    is applied (the tail here is on the SAME rebased scale as the frontend's
    pre-cutover backtest curve, which the frontend cash-scales client-side with
    the `cash_pct` from the /runs response). Returns ``{cutover_date, points,
    as_of_date}`` (the caller keeps backtest points before `cutover_date` and
    appends `points`), or None when there's no backtest curve / no live data."""
    bt_pts = _load_backtest_pts(backtest_run_id, cash_pct)
    if not bt_pts:
        return None
    snap_curve = _open_basket_live_curve(backtest_run_id, cash_pct)
    if not snap_curve:
        snap_curve, _, _ = _walk_snapshot_curve(snapshots or [])
    cutover_date, tail = _splice_snapshot_tail(bt_pts, snap_curve)
    if not cutover_date or not tail:
        return None
    return {
        "cutover_date": cutover_date,
        "points": tail,
        "as_of_date": tail[-1]["date"],
    }


def basket_price_staleness(backtest_run_id: int) -> dict | None:
    """Whether the LATEST plotted point of the strategy's live curve mixes
    stale (carried-forward) prices — and if so, which holdings.

    The monthly-returns heatmap's live tail (`_open_basket_live_curve`) marks
    the source backtest's last-period holdings each trading day using an as-of
    (last-close-on-or-before) lookup, so a holding whose latest close predates
    the freshest close in the basket is silently carried forward at the last
    point. That makes the current-month cell a PARTIAL mark-to-market rather
    than a clean one.

    Reference date = the freshest close across the basket (= the day the last
    curve point / `live_curve.as_of_date` is marked through). A holding is
    "missing" when its own latest close is strictly before the reference (or it
    has no close at all). Holdings marked delisted / out-of-scope / illiquid are
    skipped — they lag by design and are legitimately unpriced — as is cash
    (`company_id == 0`). ETF-overlay holdings (negative `company_id` =
    `-benchmark_id`) are checked against `benchmark_price`.

    Returns ``{reference_date, month, missing:[{company_id, label, ticker,
    last_close}]}`` only when ≥1 holding is missing; None when the basket is a
    clean mark-to-market (or there's no basket / no priced holdings). The caller
    surfaces this so the heatmap can replace the incomplete month cell with a
    warning listing the lagging assets instead of a misleading number."""
    from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

    res = load_backtest_result_sync(backtest_run_id)
    monthly = (res or {}).get("monthly_records") or []
    if not monthly:
        return None
    holdings = monthly[-1].get("holdings") or []
    comp = {
        int(h["company_id"]): h for h in holdings
        if h.get("company_id") is not None and int(h["company_id"]) > 0
    }
    etfs = {
        int(h["company_id"]): h for h in holdings
        if h.get("company_id") is not None and int(h["company_id"]) < 0
    }
    if not comp and not etfs:
        return None

    # Latest close date per company (metric_data) + per ETF benchmark.
    latest_by_cid: dict[int, str] = {}
    for r in fetch_in_chunks(
        list(comp.keys()),
        lambda chunk: supabase.table("metric_data")
        .select("company_id, target_date")
        .eq("metric_code", "close_price")
        .in_("company_id", chunk)
        .order("target_date", desc=True)
        .execute(),
    ):
        cid = int(r["company_id"])
        if cid not in latest_by_cid:
            latest_by_cid[cid] = str(r["target_date"])[:10]

    latest_by_bid: dict[int, str] = {}
    for r in fetch_in_chunks(
        [-c for c in etfs],
        lambda chunk: supabase.table("benchmark_price")
        .select("benchmark_id, target_date")
        .in_("benchmark_id", chunk)
        .order("target_date", desc=True)
        .execute(),
    ):
        bid = int(r["benchmark_id"])
        if bid not in latest_by_bid:
            latest_by_bid[bid] = str(r["target_date"])[:10]

    # Skip companies unpriced BY DESIGN (delisted / out-of-scope / illiquid):
    # they lag the pack legitimately and shouldn't raise a stale-price warning.
    excluded: set[int] = set()
    for r in fetch_in_chunks(
        list(comp.keys()),
        lambda chunk: supabase.table("company")
        .select("company_id, delisted_at, out_of_scope_at, illiquid_at")
        .in_("company_id", chunk)
        .execute(),
    ):
        if r.get("delisted_at") or r.get("out_of_scope_at") or r.get("illiquid_at"):
            excluded.add(int(r["company_id"]))

    per_hold: list[tuple[dict, str | None]] = []
    for cid, h in comp.items():
        if cid in excluded:
            continue
        per_hold.append((h, latest_by_cid.get(cid)))
    for cid, h in etfs.items():
        per_hold.append((h, latest_by_bid.get(-cid)))

    dates = [d for _, d in per_hold if d]
    if not dates:
        return None
    reference = max(dates)

    missing: list[dict] = []
    for h, d in per_hold:
        if d is None or d < reference:
            missing.append({
                "company_id": int(h["company_id"]),
                "label": h.get("company_name") or h.get("ticker") or f"#{h.get('company_id')}",
                "ticker": h.get("ticker"),
                "last_close": d,
            })
    if not missing:
        return None
    missing.sort(key=lambda m: (m["last_close"] or "", m["label"]))
    return {"reference_date": reference, "month": reference[:7], "missing": missing}


def _hydrate(rows: list[dict]) -> list[dict]:
    """Attach the most recent snapshot summary + period-return rollups to
    each row, joined via the `current_picks_snapshot.scheduled_strategy_id`
    FK.

    Two queries (both batched by strategy_id), each pulling only what's
    needed:
      1. Latest-snapshot holdings -- so we can extract sectors + count.
      2. Full snapshot history without holdings -- for the MTD/YTD walk.
    """
    if not rows:
        return []
    sched_ids = [r["id"] for r in rows]

    # Query 1: every snapshot row, no holdings yet (so the historical walk
    # stays cheap). Ordered chronologically; the period-return helper
    # assumes ascending.
    history_resp = (
        supabase.table("current_picks_snapshot")
        .select(
            "snapshot_id, scheduled_strategy_id, ingest_run_id, "
            "kind, as_of_date, latest_price_date, period_return_pct, created_at"
        )
        .in_("scheduled_strategy_id", sched_ids)
        .order("latest_price_date", desc=False)
        .order("created_at", desc=False)
        .execute()
    )
    history_by_sched: dict[int, list[dict]] = {}
    for s in history_resp.data or []:
        sid = s.get("scheduled_strategy_id")
        if sid is None:
            continue
        history_by_sched.setdefault(sid, []).append(s)

    # Query 2: holdings of just the latest snapshot per strategy. Doing
    # this as a separate call (rather than embedding holdings in query 1)
    # avoids hauling the full per-snapshot holdings blob across the wire
    # for every historical row.
    latest_ids: list[int] = []
    for sid, hist in history_by_sched.items():
        if hist:
            latest_ids.append(int(hist[-1]["snapshot_id"]))
    holdings_by_snap: dict[int, list[dict]] = {}
    for hr in fetch_in_chunks(
        latest_ids,
        lambda chunk: supabase.table("current_picks_snapshot")
        .select("snapshot_id, holdings")
        .in_("snapshot_id", chunk)
        .execute(),
    ):
        holdings_by_snap[int(hr["snapshot_id"])] = hr.get("holdings") or []

    today = date.today()

    out: list[dict] = []
    for r in rows:
        hist = history_by_sched.get(r["id"]) or []
        latest = hist[-1] if hist else None
        holdings = holdings_by_snap.get(int(latest["snapshot_id"])) if latest else None
        last_snapshot: dict | None = None
        if latest:
            returns = _compute_period_returns(hist, today)
            since_inception_pct: float | None = None
            # Go-live anchor: the configured `start_date`, else `created_at`
            # (the SAME default the detail view + equity-curve marker use). We
            # default here — rather than gating on `start_date` — so the header
            # MTD/YTD always come from the daily equity curve below, matching
            # the monthly-returns chart. Without this default, no-go-live-date
            # strategies fell back to the sparse snapshot walk, whose MTD
            # disagreed with the chart's current-month cell.
            inception_date = (
                str(r["start_date"])[:10] if r.get("start_date")
                else (str(r["created_at"])[:10] if r.get("created_at") else None)
            )
            # MTD / YTD / since-inception are read off the strategy's daily
            # equity curve (`_returns_from_backtest` → `_extended_curve`) — the
            # SAME curve the monthly-returns chart renders — anchored at the
            # period boundary the chart uses. So the header MTD/YTD equal the
            # chart's month/year cells exactly: the daily curve is the single,
            # most-granular basis. Falls back to the live-snapshot walk
            # (`_compute_period_returns`) only when there's no backtest curve.
            if r.get("backtest_run_id") and inception_date:
                try:
                    bt = _returns_from_backtest(
                        int(r["backtest_run_id"]), inception_date, today,
                        snapshots=hist,
                        # Live-only clamp only when the user set an explicit
                        # go-live; otherwise full calendar (matches the chart).
                        clamp_calendar_to_inception=bool(r.get("start_date")),
                        cash_pct=float((r.get("config") or {}).get("cash_pct") or 0.0),
                    )
                except Exception:
                    bt = None
                if bt:
                    returns = {
                        "mtd_return_pct": bt["mtd_return_pct"],
                        "ytd_return_pct": bt["ytd_return_pct"],
                        "as_of_date": bt["as_of_date"],
                    }
                    since_inception_pct = bt["since_inception_pct"]
            # When the open period started THIS month (the latest rebalance
            # fired in the current calendar month), the whole month-to-date IS
            # that open period — so the header MTD must equal the held basket's
            # actual return, the engine's authoritative per-snapshot
            # `period_return_pct`. This makes the header agree with the
            # Current-portfolio card + the engine to the cent (all read the same
            # EUR held-basket return since the rebalance), instead of the
            # calendar-month slice off the backtest curve which mixed in
            # pre-rebalance days. (Spanning-month open periods keep the
            # curve-based MTD above — that month-to-date isn't the full period.)
            month_start = today.replace(day=1).isoformat()
            last_rebalance = next(
                (s for s in reversed(hist) if s.get("kind") == "rebalance"), None
            )
            snap_period_ret = latest.get("period_return_pct")
            if (
                last_rebalance is not None
                and str(last_rebalance.get("as_of_date") or "")[:10] >= month_start
                and snap_period_ret is not None
            ):
                returns = dict(returns)
                returns["mtd_return_pct"] = round(float(snap_period_ret), 2)
            last_snapshot = {
                "snapshot_id": latest["snapshot_id"],
                "ingest_run_id": latest.get("ingest_run_id"),
                "created_at": latest["created_at"],
                "latest_price_date": latest.get("latest_price_date"),
                "holdings_count": len(holdings or []),
                "sectors": _extract_sectors(holdings),
                "mtd_return_pct": returns["mtd_return_pct"],
                "ytd_return_pct": returns["ytd_return_pct"],
                "since_inception_pct": since_inception_pct,
                "inception_date": inception_date,
                "as_of_date": returns["as_of_date"] or latest.get("latest_price_date"),
            }
        out.append({
            "id": r["id"],
            "name": r.get("name") or f"Strategy #{r['id']}",
            "frequency": r.get("frequency"),
            "config": r.get("config") or {},
            "enabled": r.get("enabled", True),
            "user_visible": bool(r.get("user_visible")),
            "created_at": r.get("created_at"),
            "updated_at": r.get("updated_at"),
            # Configurable go-live date (red dashed equity-curve marker +
            # live cutoff). NULL → frontend defaults to created_at.
            "start_date": r.get("start_date"),
            "last_run_at": r.get("last_run_at"),
            "next_due_at": r.get("next_due_at"),
            "backfill": {
                "status": r.get("backfill_status"),
                "progress_pct": r.get("backfill_progress_pct"),
                "message": r.get("backfill_message"),
                "error": r.get("backfill_error"),
                "started_at": r.get("backfill_started_at"),
                "finished_at": r.get("backfill_finished_at"),
            },
            "last_snapshot": last_snapshot,
        })
    return out
