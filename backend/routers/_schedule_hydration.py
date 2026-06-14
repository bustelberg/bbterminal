"""Run-history hydration for the schedule list endpoints.

Extracted from `routers.scheduled_strategies`. `_hydrate` attaches each
strategy's latest-snapshot summary + MTD/YTD rollups (walked from the full
snapshot history by `_compute_period_returns`). Pure read-side: queries
`current_picks_snapshot`, no writes.
"""
from __future__ import annotations

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


def _load_backtest_pts(backtest_run_id: int) -> list[tuple[str, float]]:
    """The saved backtest's daily equity curve as
    ``[(YYYY-MM-DD, cumulative_return_pct), ...]`` ascending. Empty when the
    run has no stored curve. Best-effort (storage errors → empty)."""
    from routers.momentum.backtest_crud import load_backtest_result_sync  # noqa: PLC0415

    res = load_backtest_result_sync(backtest_run_id)
    pts: list[tuple[str, float]] = []
    for d in (res or {}).get("daily_records") or []:
        dt = str(d.get("date") or "")[:10]
        cum = d.get("cumulative_return_pct")
        if dt and cum is not None:
            pts.append((dt, float(cum)))
    pts.sort(key=lambda p: p[0])
    return pts


def _splice_snapshot_tail(
    backtest_pts: list[tuple[str, float]],
    snap_curve: list[tuple[str, float]],
) -> tuple[str | None, list[dict]]:
    """Graft the live forward tail onto the (frozen) backtest daily curve so
    a strategy's monthly-returns + equity views stay current with the latest
    priced day.

    `backtest_pts` — the backtest curve as
    ``[(YYYY-MM-DD, cumulative_return_pct), ...]`` (any order).
    `snap_curve`   — the snapshot equity curve from `_walk_snapshot_curve`
    (base 1.0), which the price-update job marks to market through the
    latest priced day. This is the single live source — `current_picks_day`
    is deliberately NOT used here: it only advances on a full compute and so
    lags the price-update snapshots.

    Returns ``(cutover_date, tail_points)``:
      * `cutover_date` — the first forward date. The caller keeps backtest
        points strictly before it and appends `tail_points`. None when
        there's nothing to splice (no curve, or live data no fresher than
        the backtest curve's end).
      * `tail_points` — ``[{"date", "cumulative_return_pct"}, ...]`` on the
        SAME cumulative scale as the backtest curve.

    Only snapshot points dated AFTER the backtest curve's last day extend
    it, and their RELATIVE move from that boundary is grafted on — so a
    level mismatch between the two independently-computed curves never shows
    at the join."""
    if not backtest_pts or not snap_curve:
        return None, []
    bt = sorted(backtest_pts, key=lambda p: p[0])
    bt_last_date, bt_last_cum = bt[-1]
    sc = sorted(snap_curve, key=lambda p: p[0])

    forward = [(d, e) for d, e in sc if d > bt_last_date]
    if not forward:
        return None, []

    # Snapshot equity at the backtest boundary: last point on/before it,
    # else the first snapshot point (curve begins after the boundary).
    anchor_eq: float | None = None
    for d, e in sc:
        if d <= bt_last_date:
            anchor_eq = e
        else:
            break
    if anchor_eq is None:
        anchor_eq = sc[0][1]
    if anchor_eq <= 0:
        return None, []

    rebase = (1.0 + bt_last_cum / 100.0) / anchor_eq
    tail = [
        {"date": d, "cumulative_return_pct": round((e * rebase - 1.0) * 100.0, 6)}
        for d, e in forward
    ]
    return forward[0][0], tail


def _extended_curve(
    backtest_run_id: int, snapshots: list[dict],
) -> list[tuple[str, float]]:
    """The strategy's full equity curve: the backtest daily curve with the
    live snapshot tail spliced on (continuous cumulative scale). The single
    source of truth shared by the run-history rollups (`_returns_from_backtest`)
    and the detail view's live curve (`build_live_curve`). Empty when the run
    has no stored curve."""
    bt_pts = _load_backtest_pts(backtest_run_id)
    if not bt_pts:
        return []
    snap_curve, _, _ = _walk_snapshot_curve(snapshots or [])
    _, tail = _splice_snapshot_tail(bt_pts, snap_curve)
    return bt_pts + [(p["date"], p["cumulative_return_pct"]) for p in tail]


def _returns_from_backtest(
    backtest_run_id: int,
    inception_iso: str,
    today: date,
    snapshots: list[dict] | None = None,
) -> dict | None:
    """MTD / YTD / since-inception returns read off the strategy's full
    equity curve (`_extended_curve`), anchored at the go-live date.

    The backtest curve alone goes stale on the day it was saved; splicing
    the live snapshot tail keeps MTD/YTD/since-inception + `as_of_date`
    tracking the latest priced day.

    - MTD  = from the start of the current calendar month
    - YTD  = from the start of the current calendar year
    - since-inception = from the go-live date (`inception_iso`)

    MTD/YTD are calendar-anchored and independent of the go-live date; only
    since-inception moves when the go-live date changes. Returns None when
    the run has no curve."""
    pts = _extended_curve(backtest_run_id, snapshots or [])
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

    def rel(a: float | None, b: float | None) -> float | None:
        if a is None or b is None:
            return None
        return round(((1 + a / 100.0) / (1 + b / 100.0) - 1) * 100.0, 2)

    # Anchor cumulative levels; when an anchor predates the curve, fall back
    # to the curve's start (earliest data we have).
    curve_start_cum = pts[0][1]
    inc_cum = cum_at(inception_iso)
    if inc_cum is None:
        inc_cum = curve_start_cum
    year_start = today.replace(month=1, day=1).isoformat()
    month_start = today.replace(day=1).isoformat()
    ytd_cum = cum_at(year_start)
    if ytd_cum is None:
        ytd_cum = curve_start_cum
    mtd_cum = cum_at(month_start)
    if mtd_cum is None:
        mtd_cum = curve_start_cum
    return {
        "mtd_return_pct": rel(latest_cum, mtd_cum),
        "ytd_return_pct": rel(latest_cum, ytd_cum),
        "since_inception_pct": rel(latest_cum, inc_cum),
        "inception_date": inception_iso,
        "as_of_date": latest_date,
    }


def build_live_curve(backtest_run_id: int, snapshots: list[dict]) -> dict | None:
    """The live-extension of a scheduled strategy's backtest curve, for the
    detail view's monthly-returns heatmap + equity curve.

    Splices the snapshot tail (`_splice_snapshot_tail`) onto the backtest
    daily curve — same single source as the run-history rollups. Returns
    ``{cutover_date, points, as_of_date}`` (the caller keeps backtest points
    before `cutover_date` and appends `points`), or None when there's no
    backtest curve / no live data fresher than the curve's end."""
    bt_pts = _load_backtest_pts(backtest_run_id)
    if not bt_pts:
        return None
    snap_curve, _, _ = _walk_snapshot_curve(snapshots or [])
    cutover_date, tail = _splice_snapshot_tail(bt_pts, snap_curve)
    if not cutover_date or not tail:
        return None
    return {
        "cutover_date": cutover_date,
        "points": tail,
        "as_of_date": tail[-1]["date"],
    }


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
            inception_date = str(r["start_date"])[:10] if r.get("start_date") else None
            # Prefer returns from the backtest equity curve (anchored at
            # go-live) — the live snapshots are too thin for a meaningful
            # MTD/YTD. Falls back to the live-snapshot walk when there's no
            # backtest run or curve.
            if r.get("backtest_run_id") and inception_date:
                try:
                    bt = _returns_from_backtest(
                        int(r["backtest_run_id"]), inception_date, today,
                        snapshots=hist,
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
