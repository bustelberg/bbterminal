"""Capture a golden-master fixture for a scheduled strategy's rebalance.

Phase 0 of the engine unification: pin the legacy momentum calculation by
recording the EXACT inputs and outputs of `run_current_portfolio` for a real
scheduled strategy, so any later refactor can be checked against it offline.

It drives the real `_momentum_backtest_stream` (the same code path
`ingest/phases/momentum.py` Branch B uses to rebalance), intercepting
`run_current_portfolio` to:

  * pin `today` to the snapshot's compute date (the engine otherwise reads
    `date.today()`, so a replay would drift into a different month), and
  * truncate every price/volume frame to `[as_of - LOOKBACK_DAYS, latest_price_date]`,
    so the fixture is bounded and the run is reproducible against a DB whose
    price table has since moved forward.

The captured result is then compared to the holdings that ACTUALLY shipped in
`current_picks_snapshot`, which is what makes this a golden master rather than
just a snapshot of today's behavior.

Two modes:

  SNAPSHOT mode (`--snapshot-id`) — anchor `today` to when a real snapshot was
    computed, and additionally diff the replay against the holdings that shipped.

  HISTORICAL mode (`--today` + `--price-through`) — anchor to any past date. Use
    this to reach states a live rebalance never occupies: in steady state the
    engine decides a Monday rebalance from Friday's close, so `as_of` has NO bar
    and the strict `<` cutoff is unobservable. Anchoring `today` to a past Monday
    that already has a close puts a bar exactly ON the cutoff.

Usage (local Supabase, needs SUPABASE_DB_URL for the COPY fast path):

    cd backend && PYTHONPATH=. uv run python scripts/capture_golden_rebalance.py \
        --strategy-id 34 --snapshot-id 829 --out tests/fixtures/golden_rebalance_34.npz

    cd backend && PYTHONPATH=. uv run python scripts/capture_golden_rebalance.py \
        --strategy-id 34 --today 2026-06-01 --price-through 2026-06-01 \
        --out tests/fixtures/golden_rebalance_34_trading_day.npz

Re-run this ONLY to deliberately re-baseline. A failing golden test means the
calculation changed; regenerating the fixture to make it pass defeats the point.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
from dataclasses import asdict, is_dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

import deps  # noqa: F401  — loads .env / .env.local before anything touches Supabase
from deps import supabase

# Calendar days back from as_of. Must cover every lookback the signal set uses:
# 12-1 momentum needs 13 months (~396 calendar days) and above_200ma needs 200
# TRADING days (~288 calendar). `verify_lookback_sufficient()` proves the trim is
# lossless (identical holdings at 2x the window) before anything is written.
DEFAULT_LOOKBACK_DAYS = 450

_FRAME_KEYS = ("prices_df", "prices_local_df", "volumes_df")

# `weight` is persisted to `current_picks_snapshot.holdings` rounded to 4dp, so a
# replay's exact 1/24 can never equal the stored 0.0417. Compare at the stored
# precision when checking against what shipped; the fixture itself keeps full
# precision and the golden test compares it exactly.
_SHIPPED_WEIGHT_DP = 4


def _load_snapshot(snapshot_id: int) -> dict:
    r = (
        supabase.table("current_picks_snapshot")
        .select("snapshot_id, scheduled_strategy_id, kind, as_of_date, "
                "latest_price_date, created_at, holdings, config")
        .eq("snapshot_id", snapshot_id)
        .limit(1)
        .execute()
    )
    rows = r.data or []
    if not rows:
        raise SystemExit(f"snapshot {snapshot_id} not found")
    snap = rows[0]
    if snap["kind"] != "rebalance":
        raise SystemExit(f"snapshot {snapshot_id} is kind={snap['kind']!r}, need 'rebalance'")
    return snap


def _load_strategy_config(strategy_id: int) -> dict:
    r = (
        supabase.table("scheduled_strategy")
        .select("id, name, config")
        .eq("id", strategy_id)
        .limit(1)
        .execute()
    )
    rows = r.data or []
    if not rows:
        raise SystemExit(f"scheduled_strategy {strategy_id} not found")
    return rows[0]


def _build_request(cfg: dict):
    """Mirror ingest/phases/momentum.py Branch B exactly."""
    from routers.momentum.backtest_stream.models import BacktestRequest

    cfg = dict(cfg)
    cfg["mode"] = "current_portfolio"
    cfg["force_recompute"] = True
    cfg["db_only"] = True
    cfg.pop("variants", None)
    cfg.pop("n_trials", None)
    return BacktestRequest(**cfg)


def _trim(df: pd.DataFrame | None, lo: date, hi: date) -> pd.DataFrame | None:
    if df is None or df.empty or "target_date" not in df.columns:
        return df
    d = pd.to_datetime(df["target_date"]).dt.date
    return df.loc[(d >= lo) & (d <= hi)].reset_index(drop=True)


def _holdings_key(holdings: list[dict], *, weight_dp: int = 6) -> list[tuple]:
    """The identity of a selection: who, how much, at what price, ranked how."""
    return sorted(
        (
            int(h["company_id"]),
            round(float(h["weight"]), weight_dp),
            round(float(h["score"]), 4),
            int(h["sector_rank"]),
            int(h["company_rank"]),
            str(h["entry_date"]),
            None if h.get("entry_price_local") is None else round(float(h["entry_price_local"]), 6),
        )
        for h in holdings
    )


def _shrink(df: pd.DataFrame | None) -> pd.DataFrame | None:
    """Narrow the keys (not the values) — ids and dates carry no precision worth
    keeping, prices do. float32 on prices would inject ~6e-8 of noise into a
    fixture whose entire purpose is exact reproduction."""
    if df is None or df.empty:
        return df
    df = df.copy()
    if "company_id" in df.columns:
        df["company_id"] = df["company_id"].astype("int32")
    if "target_date" in df.columns:
        df["target_date"] = pd.to_datetime(df["target_date"]).dt.date
    return df


def capture(
    strategy_id: int,
    lookback_days: int,
    *,
    snapshot_id: int | None = None,
    today: date | None = None,
    price_through: date | None = None,
    quiet: bool = False,
) -> dict:
    """Replay one rebalance, capturing `run_current_portfolio`'s inputs + output.

    Anchored either by a real snapshot (`snapshot_id`) or by an explicit
    `today` / `price_through` pair. Exactly one of the two must be given.
    """
    snap: dict | None = None
    if snapshot_id is not None:
        snap = _load_snapshot(snapshot_id)
        if snap["scheduled_strategy_id"] != strategy_id:
            raise SystemExit(
                f"snapshot {snapshot_id} belongs to strategy "
                f"{snap['scheduled_strategy_id']}, not {strategy_id}"
            )
        anchor = date.fromisoformat(str(snap["as_of_date"]))
        lpd = date.fromisoformat(str(snap["latest_price_date"]))
        # The engine reads `date.today()`; at compute time that was created_at's day.
        computed_on = date.fromisoformat(str(snap["created_at"])[:10])
    else:
        if today is None or price_through is None:
            raise SystemExit("historical mode needs both --today and --price-through")
        # `as_of` is derived by the engine and is <= today, so anchor the lookback
        # on `today`; verify_lookback_sufficient() proves the window is deep enough.
        anchor, lpd, computed_on = today, price_through, today

    strat = _load_strategy_config(strategy_id)
    lo = anchor - timedelta(days=lookback_days)

    if not quiet:
        print(f"strategy  : {strategy_id} {strat['name']!r}")
        if snap:
            print(f"snapshot  : {snapshot_id}  as_of={snap['as_of_date']}  latest_price_date={lpd}")
            print(f"stored    : {len(snap['holdings'])} holdings")
        else:
            print("mode      : historical (no shipped snapshot to compare against)")
        print(f"today     : {computed_on}")
        print(f"price win : {lo} .. {lpd}  ({lookback_days}d lookback)")
        print()

    req = _build_request(strat["config"])

    import routers.momentum.backtest_stream.single_run as single_run
    from routers.momentum.backtest_stream.stream import _momentum_backtest_stream

    original = single_run.run_current_portfolio
    box: dict = {}

    def _capturing(config, prices_df, universe_df, send_event=None, **kw):
        kw = dict(kw)
        for k in _FRAME_KEYS[1:]:          # prices_local_df, volumes_df
            if k in kw:
                kw[k] = _trim(kw[k], lo, lpd)
        prices_df = _trim(prices_df, lo, lpd)
        kw["today"] = computed_on

        box["config"] = config
        box["universe_df"] = universe_df.copy()
        box["prices_df"] = prices_df.copy()
        box["prices_local_df"] = None if kw.get("prices_local_df") is None else kw["prices_local_df"].copy()
        box["volumes_df"] = None if kw.get("volumes_df") is None else kw["volumes_df"].copy()
        box["company_currency"] = dict(kw.get("company_currency") or {})
        box["monthly_eligible"] = kw.get("monthly_eligible")
        box["today"] = computed_on

        result = original(config, prices_df, universe_df, send_event, **kw)
        box["result"] = result
        return result

    single_run.run_current_portfolio = _capturing
    try:
        async def _drain():
            async for _ in _momentum_backtest_stream(req):
                pass
        asyncio.run(_drain())
    finally:
        single_run.run_current_portfolio = original

    if "result" not in box:
        raise SystemExit("run_current_portfolio was never called — the stream bailed early")

    if snap and box["result"].as_of_date != str(snap["as_of_date"]):
        raise SystemExit(
            f"replay derived as_of={box['result'].as_of_date} but snapshot "
            f"{snapshot_id} recorded {snap['as_of_date']} — the anchor is wrong, "
            "so the comparison below would be meaningless"
        )

    box["snapshot"] = snap
    box["strategy"] = strat
    return box


def _as_dicts(holdings) -> list[dict]:
    return [asdict(h) if is_dataclass(h) else h for h in holdings]


def _pack_text(s: str) -> np.ndarray:
    """UTF-8 bytes, not a numpy str — `np.savez` stores `str` as UTF-32 (4 bytes
    per character), which quadrupled the JSON half of this fixture."""
    return np.frombuffer(s.encode("utf-8"), dtype=np.uint8)


def verify_against_shipped(box: dict) -> bool:
    """Does the current engine still reproduce the holdings that shipped?

    Weights are compared at the persisted precision (see `_SHIPPED_WEIGHT_DP`);
    anything else that differs is genuine drift.
    """
    stored = box["snapshot"]["holdings"]
    computed = _as_dicts(box["result"].holdings)

    print("=" * 74)
    print("REPLAY vs SHIPPED SNAPSHOT")
    print("=" * 74)
    print(f"  stored holdings   : {len(stored)}")
    print(f"  replayed holdings : {len(computed)}")

    s_ids = {int(h["company_id"]) for h in stored}
    c_ids = {int(h["company_id"]) for h in computed}
    if s_ids != c_ids:
        print("  !! company set differs")
        print(f"     only in stored   : {sorted(s_ids - c_ids)}")
        print(f"     only in replayed : {sorted(c_ids - s_ids)}")
        return False
    print(f"  company set       : IDENTICAL ({len(s_ids)} names)")

    sk = _holdings_key(stored, weight_dp=_SHIPPED_WEIGHT_DP)
    ck = _holdings_key(computed, weight_dp=_SHIPPED_WEIGHT_DP)
    if sk == ck:
        print("  weights/scores/ranks/entry: IDENTICAL")
        return True

    print("  !! per-holding drift (weights compared at persisted 4dp):")
    by_cid_s = {k[0]: k for k in sk}
    by_cid_c = {k[0]: k for k in ck}
    for cid in sorted(by_cid_s):
        if by_cid_s[cid] != by_cid_c[cid]:
            print(f"     cid {cid}")
            print(f"       shipped {by_cid_s[cid]}")
            print(f"       replay  {by_cid_c[cid]}")
    return False


def verify_lookback_sufficient(box: dict, lookback: int, capture_kwargs: dict) -> bool:
    """Re-run with a 2x window; identical holdings => the trim lost nothing."""
    print()
    print("=" * 74)
    print(f"LOOKBACK SUFFICIENCY ({lookback}d vs {lookback * 2}d)")
    print("=" * 74)
    wide = capture(lookback_days=lookback * 2, quiet=True, **capture_kwargs)
    a = _holdings_key(_as_dicts(box["result"].holdings))
    b = _holdings_key(_as_dicts(wide["result"].holdings))
    ok = a == b
    print(f"  identical holdings at 2x window: {ok}")
    if not ok:
        print(f"  !! a {lookback}d window CHANGES the result — raise --lookback")
        for x, y in zip(a, b):
            if x != y:
                print(f"     {lookback}d  {x}")
                print(f"     {lookback*2}d  {y}")
    return ok


def write_fixture(box: dict, out: Path, lookback: int) -> None:
    out.parent.mkdir(parents=True, exist_ok=True)
    cfg = box["config"]
    texts = {
        "config_json": json.dumps(asdict(cfg) if is_dataclass(cfg) else cfg, default=str),
        "today": box["today"].isoformat(),
        "company_currency_json": json.dumps({str(k): v for k, v in box["company_currency"].items()}),
        "monthly_eligible_json": json.dumps(box["monthly_eligible"], default=str),
        "expected_holdings_json": json.dumps(_as_dicts(box["result"].holdings), default=str),
        # Empty in historical mode — there is no shipped snapshot to diff against.
        "shipped_holdings_json": json.dumps(
            (box["snapshot"] or {}).get("holdings") or [], default=str
        ),
        "meta_json": json.dumps({
            "strategy_id": box["strategy"]["id"],
            "strategy_name": box["strategy"]["name"],
            "snapshot_id": (box["snapshot"] or {}).get("snapshot_id"),
            # The as_of the ENGINE derived — not the snapshot's, so historical
            # captures record it too.
            "as_of_date": box["result"].as_of_date,
            "latest_price_date": box["result"].latest_price_date,
            "today": box["today"].isoformat(),
            "lookback_days": lookback,
        }),
    }
    payload = {f"{k}__utf8": _pack_text(v) for k, v in texts.items()}
    for k in ("universe_df", *_FRAME_KEYS):
        df = _shrink(box[k])
        payload[f"{k}__parquet"] = (
            np.frombuffer(df.to_parquet(engine="pyarrow", index=False, compression="zstd"), dtype=np.uint8)
            if df is not None else np.zeros(0, dtype=np.uint8)
        )
    np.savez_compressed(out, **payload)
    mb = out.stat().st_size / 2**20
    print()
    print(f"fixture written: {out}  ({mb:.2f} MB)")
    for k in ("universe_df", *_FRAME_KEYS):
        df = box[k]
        print(f"  {k:18} {'None' if df is None else f'{len(df):>8,} rows'}")


def report_cutoff_coverage(box: dict) -> bool:
    """Is there a bar exactly ON the signal cutoff?

    The cutoff is `as_of_date` and the guard is a strict `<`. If no company has a
    bar on that date, `searchsorted(side="left")` and `side="right")` agree and
    the fixture cannot tell a strict cutoff from an inclusive one — the exact
    blind spot the trading-day fixture exists to close.
    """
    as_of = pd.Timestamp(box["result"].as_of_date)
    d = pd.to_datetime(box["prices_df"]["target_date"])
    on_cutoff = int((d == as_of).sum())

    print()
    print("=" * 74)
    print("CUTOFF COVERAGE")
    print("=" * 74)
    print(f"  as_of (signal cutoff) : {as_of.date()} ({as_of.day_name()})")
    print(f"  latest bar in window  : {d.max().date()}")
    print(f"  companies with a bar ON the cutoff: {on_cutoff}")
    if on_cutoff:
        print("  -> this fixture DISTINGUISHES a strict `<` cutoff from `<=`.")
    else:
        print("  -> no bar on the cutoff: `<` and `<=` are indistinguishable here.")
    return on_cutoff > 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--strategy-id", type=int, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--snapshot-id", type=int, help="snapshot mode: anchor to a real rebalance")
    p.add_argument("--today", type=date.fromisoformat, help="historical mode: pin date.today()")
    p.add_argument("--price-through", type=date.fromisoformat, help="historical mode: last bar to load")
    p.add_argument("--lookback", type=int, default=DEFAULT_LOOKBACK_DAYS)
    p.add_argument("--skip-lookback-check", action="store_true")
    a = p.parse_args()

    if (a.snapshot_id is None) == (a.today is None):
        raise SystemExit("give exactly one of --snapshot-id or (--today + --price-through)")

    kwargs = (
        {"strategy_id": a.strategy_id, "snapshot_id": a.snapshot_id}
        if a.snapshot_id is not None
        else {"strategy_id": a.strategy_id, "today": a.today, "price_through": a.price_through}
    )

    box = capture(lookback_days=a.lookback, **kwargs)
    reproduced = verify_against_shipped(box) if box["snapshot"] else None
    report_cutoff_coverage(box)
    lookback_ok = (
        True if a.skip_lookback_check
        else verify_lookback_sufficient(box, a.lookback, kwargs)
    )

    if not lookback_ok:
        print("\nABORT: fixture window is too short — nothing written.")
        return 2

    write_fixture(box, a.out, a.lookback)

    print()
    if reproduced is None:
        print("OK — historical fixture written (no shipped snapshot to compare).")
    elif reproduced:
        print("OK — the fixture is anchored to holdings that actually shipped.")
    else:
        print("NOTE — the replay does not match the shipped snapshot exactly.")
        print("The fixture pins the engine's behavior on the CURRENT price data,")
        print("which is the correct refactor baseline. See the drift above: if it")
        print("is explained by bars written into the window AFTER the snapshot")
        print("shipped (GuruFocus publish lag), the engine is fine and the DB moved.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
