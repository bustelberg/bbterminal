"""Profile run_current_portfolio against the local Supabase.

Caches loaded DataFrames in scripts/.cache/ so re-runs are instant. Delete
the cache dir to force a fresh load.

Usage (from backend/):
    uv run python scripts/profile_current_portfolio.py
    uv run python scripts/profile_current_portfolio.py --universe longequity
    uv run python scripts/profile_current_portfolio.py --no-cache
"""
from __future__ import annotations

import argparse
import logging
import os
import pickle
import sys
import time
from datetime import date
from pathlib import Path

# Make the backend root importable when running from anywhere.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from supabase import create_client

from momentum.backtest import BacktestConfig, run_current_portfolio
from momentum.data import (
    convert_prices_to_eur,
    load_all_prices,
    load_all_volumes,
    load_company_currency,
    load_fx_rates,
    load_universe,
    sync_fx_rates_to_db,
)
from momentum.signals import PRICE_SIGNAL_DEFS

CACHE_DIR = Path(__file__).resolve().parent / ".cache"


def load_env():
    backend_dir = Path(__file__).resolve().parents[1]
    load_dotenv(backend_dir / ".env")
    load_dotenv(backend_dir / ".env.local", override=True)


def get_supabase():
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


def load_monthly_eligible(sb, label: str) -> dict[str, dict[int, str | None]]:
    u = sb.table("universe").select("universe_id").eq("label", label).limit(1).execute()
    if not u.data:
        raise ValueError(f"No universe with label {label!r}")
    uid = u.data[0]["universe_id"]
    rows: list[dict] = []
    offset, page_size = 0, 1000
    while True:
        r = (
            sb.table("universe_membership")
            .select("target_month, company_id, sector")
            .eq("universe_id", uid)
            .order("target_month")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = r.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    out: dict[str, dict[int, str | None]] = {}
    for r in rows:
        m = (r.get("target_month") or "")[:7]
        if not m:
            continue
        out.setdefault(m, {})[r["company_id"]] = r.get("sector")
    return out


def build_dataset(sb, *, universe_label: str, price_start: date, price_end: date):
    print(f"[load] monthly_eligible for universe={universe_label!r}...")
    t0 = time.perf_counter()
    monthly_eligible = load_monthly_eligible(sb, universe_label)
    print(f"  -> {len(monthly_eligible)} months, latest={max(monthly_eligible)} ({time.perf_counter() - t0:.1f}s)")

    # Company set: union across the most recent 6 months — keeps load size sane
    # while covering anything currently eligible.
    recent_months = sorted(monthly_eligible)[-6:]
    company_ids = sorted({cid for m in recent_months for cid in monthly_eligible[m]})
    print(f"[load] company_ids in last 6 months: {len(company_ids)}")

    print("[load] universe (company table)...")
    t0 = time.perf_counter()
    universe_df = load_universe(sb)
    universe_df = universe_df[universe_df["company_id"].isin(company_ids)].reset_index(drop=True)
    print(f"  -> {len(universe_df)} rows ({time.perf_counter() - t0:.1f}s)")

    print(f"[load] prices for {len(company_ids)} companies, {price_start}..{price_end}...")
    t0 = time.perf_counter()
    prices_local_df = load_all_prices(sb, company_ids, price_start, price_end)
    print(f"  -> {len(prices_local_df):,} price rows ({time.perf_counter() - t0:.1f}s)")

    print("[load] company currencies...")
    t0 = time.perf_counter()
    company_currency = load_company_currency(sb, company_ids)
    needed_ccy = sorted({c for c in company_currency.values() if c})
    print(f"  -> {len(needed_ccy)} currencies needed ({time.perf_counter() - t0:.1f}s)")

    print(f"[load] FX rates ({needed_ccy})...")
    t0 = time.perf_counter()
    sync_fx_rates_to_db(sb, needed_ccy, price_start, price_end)
    fx_rates = load_fx_rates(sb, needed_ccy, price_start, price_end)
    print(f"  -> {sum(1 for v in fx_rates.values() if v is not None)} series loaded ({time.perf_counter() - t0:.1f}s)")

    print("[load] converting prices to EUR...")
    t0 = time.perf_counter()
    prices_df, fx_stats = convert_prices_to_eur(prices_local_df, company_currency, fx_rates)
    print(f"  -> converted={fx_stats['converted_rows']:,} passthrough={fx_stats['passthrough_rows']:,} dropped_no_currency={fx_stats['dropped_no_currency']:,} dropped_no_fx={fx_stats['dropped_no_fx']:,} ({time.perf_counter() - t0:.1f}s)")

    print("[load] volumes...")
    t0 = time.perf_counter()
    volumes_df = load_all_volumes(sb, company_ids, price_start, price_end)
    print(f"  -> {len(volumes_df):,} volume rows ({time.perf_counter() - t0:.1f}s)")

    return {
        "universe_df": universe_df,
        "prices_df": prices_df,
        "prices_local_df": prices_local_df,
        "volumes_df": volumes_df,
        "monthly_eligible": monthly_eligible,
        "company_currency": company_currency,
    }


def get_dataset(*, universe_label: str, use_cache: bool):
    CACHE_DIR.mkdir(exist_ok=True)
    cache_path = CACHE_DIR / f"dataset_{universe_label}.pkl"
    if use_cache and cache_path.exists():
        print(f"[cache] loading {cache_path.name} ({cache_path.stat().st_size / 1024 / 1024:.1f} MB)...")
        with cache_path.open("rb") as f:
            return pickle.load(f)
    sb = get_supabase()
    today = date.today()
    # Need ~14 months of history for the 12-1 momentum signal, plus a buffer.
    price_start = date(today.year - 2, 1, 1)
    price_end = today
    data = build_dataset(sb, universe_label=universe_label, price_start=price_start, price_end=price_end)
    print(f"[cache] writing {cache_path.name}...")
    with cache_path.open("wb") as f:
        pickle.dump(data, f)
    return data


def run_profile(universe_label: str, *, use_cache: bool):
    data = get_dataset(universe_label=universe_label, use_cache=use_cache)

    today = date.today()
    config = BacktestConfig(
        start_date=date(today.year, today.month, 1),
        end_date=date(today.year, today.month, 1),
        signal_weights={s["key"]: s["default_weight"] for s in PRICE_SIGNAL_DEFS},
        top_n_sectors=4,
        top_n_per_sector=6,
        category_weights={"price": 0.5, "volume": 0.5},
        selection_mode="momentum",
    )

    print()
    print("=" * 70)
    print(f"Running run_current_portfolio (universe={universe_label!r})")
    print("=" * 70)

    t0 = time.perf_counter()
    result = run_current_portfolio(
        config,
        data["prices_df"],
        data["universe_df"],
        send_event=None,  # no SSE; rely on logger
        volumes_df=data["volumes_df"],
        monthly_eligible=data["monthly_eligible"],
        prices_local_df=data["prices_local_df"],
        company_currency=data["company_currency"],
    )
    elapsed = time.perf_counter() - t0
    print()
    print(f"-> {len(result.holdings)} holdings, {len(result.daily_picks)} daily picks, {elapsed:.2f}s wall")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", default="ACWI")
    parser.add_argument("--no-cache", action="store_true")
    args = parser.parse_args()

    load_env()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    run_profile(args.universe, use_cache=not args.no_cache)


if __name__ == "__main__":
    main()
