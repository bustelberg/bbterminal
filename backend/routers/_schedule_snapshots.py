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


def compute_and_save_price_update(
    strategy_id: int,
    ingest_run_id: int | None,
    is_backfill: bool = False,
    as_of_iso: str | None = None,
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
            return (h.get("currency") or "").upper() or None  # ETF: benchmark ccy
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
        entry_local = h.get("entry_price_local")
        ccy = _hold_ccy(h)
        entry_date_iso = str(h.get("entry_date") or rebal.get("as_of_date") or "")[:10]
        # Entry EUR: trust the rebalance's stored EUR mark for companies; for ETF
        # overlay holdings (no stored EUR) derive it from the benchmark local +
        # entry-date FX, and persist it so the card has a consistent EUR basis.
        entry_eur = h.get("entry_price_eur")
        if not entry_eur or entry_eur <= 0:
            entry_eur = _to_eur(entry_local, ccy, entry_date_iso, fx_rates)
            if entry_eur is not None:
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
        if latest and entry_local and entry_eur and entry_eur > 0:
            current_local = float(latest["numeric_value"])
            target_d = str(latest["target_date"])[:10]
            new_h["exit_price_local"] = current_local
            new_h["exit_date"] = target_d
            current_eur = _to_eur(current_local, ccy, target_d, fx_rates)
            if current_eur is not None:
                new_h["exit_price_eur"] = round(current_eur, 4)
                ret = (current_eur / float(entry_eur) - 1) * 100.0
            else:
                # FX unavailable for this currency — fall back to the local
                # return rather than dropping the holding from the aggregate.
                ret = ((current_local - float(entry_local)) / float(entry_local)) * 100.0
            new_h["forward_return_pct"] = round(ret, 2)
            if latest_price_date is None or target_d > latest_price_date:
                latest_price_date = target_d
        updated_holdings.append(new_h)

    # Period return via the SINGLE source of truth — the weighted per-holding
    # `forward_return_pct` (see `momentum.portfolio_math`). Keeps the stored
    # `period_return_pct` exactly equal to the weighted mean of the per-row
    # returns the card displays, so the card Total + header MTD can't diverge.
    from momentum.portfolio_math import portfolio_eur_return_pct  # noqa: PLC0415
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
        # The backtest's last period is the current open period; its date is
        # the period's first-<weekday> (e.g. 2026-06-01). Anchor the snapshot
        # there so it reads as the current-period rebalance.
        "as_of_date": _coerce_as_of_date(last.get("as_of_date") or last.get("date")),
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
