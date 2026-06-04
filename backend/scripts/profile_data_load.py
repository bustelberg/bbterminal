"""Profile the up-front price+volume load a /backtest pays, against local Supabase.

Mirrors a real full-history run: take the ACWI universe's all-time company set,
then time `load_all_prices` + `load_all_volumes` over the full date range. Reports
per-phase wall time, row counts, PostgREST page counts (round-trips), and rows/sec
so we can see exactly where the load time goes before optimizing.

Usage (from backend/):
    uv run python scripts/profile_data_load.py
    uv run python scripts/profile_data_load.py --universe ACWI --start 2002-01-01
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv
from supabase import create_client

from momentum.data import load_all_prices, load_all_volumes, load_universe


def load_env():
    backend_dir = Path(__file__).resolve().parents[1]
    load_dotenv(backend_dir / ".env")
    load_dotenv(backend_dir / ".env.local", override=True)


def get_supabase():
    return create_client(os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_KEY"])


def universe_company_ids(sb, label: str) -> list[int]:
    u = sb.table("universe").select("universe_id").eq("label", label).limit(1).execute()
    if not u.data:
        raise SystemExit(f"No universe with label {label!r}")
    uid = u.data[0]["universe_id"]
    ids: set[int] = set()
    offset, page = 0, 1000
    while True:
        r = (
            sb.table("universe_membership")
            .select("company_id")
            .eq("universe_id", uid)
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = r.data or []
        ids.update(row["company_id"] for row in batch)
        if len(batch) < page:
            break
        offset += page
    return sorted(ids)


def metric_row_count(sb, metric_code: str) -> int:
    r = (
        sb.table("metric_data")
        .select("company_id", count="exact")
        .eq("metric_code", metric_code)
        .eq("source_code", "gurufocus")
        .limit(1)
        .execute()
    )
    return r.count or 0


class Progress:
    """on_progress callback that prints one line per completed chunk (so the
    run never sits silent) and records the max page number = total round-trips."""

    def __init__(self, label: str):
        self.label = label
        self.pages = 0
        self.chunks_total = 0
        self._last_chunks = 0

    def __call__(self, rows_so_far, page_num, chunks_done=0, chunks_total=0):
        self.pages = max(self.pages, page_num)
        self.chunks_total = chunks_total
        if chunks_done > self._last_chunks:
            self._last_chunks = chunks_done
            print(
                f"  [{self.label}] {rows_so_far:,} rows · {chunks_done}/{chunks_total} chunks · {page_num} pages",
                flush=True,
            )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--universe", default="ACWI")
    ap.add_argument("--start", default="2002-01-01")
    ap.add_argument("--end", default=date.today().isoformat())
    args = ap.parse_args()

    load_env()
    sb = get_supabase()
    start, end = date.fromisoformat(args.start), date.fromisoformat(args.end)

    print("=" * 72)
    print(f"SUPABASE_URL = {os.environ.get('SUPABASE_URL')}")
    print(f"window       = {start} .. {end}")
    print("=" * 72)

    # DB scale probe (cheap exact counts).
    t = time.perf_counter()
    n_price = metric_row_count(sb, "close_price")
    n_vol = metric_row_count(sb, "volume")
    print(f"[scale] metric_data: {n_price:,} close_price + {n_vol:,} volume rows "
          f"(gurufocus) ({time.perf_counter() - t:.1f}s)")

    t = time.perf_counter()
    cids = universe_company_ids(sb, args.universe)
    print(f"[universe] {args.universe}: {len(cids)} companies, all-time ({time.perf_counter() - t:.1f}s)")

    t = time.perf_counter()
    udf = load_universe(sb)
    print(f"[load_universe] {len(udf):,} company rows ({time.perf_counter() - t:.1f}s)")

    # Prices.
    pp = Progress("prices")
    t = time.perf_counter()
    pdf = load_all_prices(sb, cids, start, end, on_progress=pp)
    dt_p = time.perf_counter() - t
    rps_p = len(pdf) / dt_p if dt_p else 0
    print(f"[prices]  {len(pdf):,} rows · {pp.pages} pages · {pp.chunks_total} chunks · "
          f"{dt_p:.1f}s · {rps_p:,.0f} rows/s")

    # Volumes.
    pv = Progress("volumes")
    t = time.perf_counter()
    vdf = load_all_volumes(sb, cids, start, end, on_progress=pv)
    dt_v = time.perf_counter() - t
    rps_v = len(vdf) / dt_v if dt_v else 0
    print(f"[volumes] {len(vdf):,} rows · {pv.pages} pages · {pv.chunks_total} chunks · "
          f"{dt_v:.1f}s · {rps_v:,.0f} rows/s")

    print("=" * 72)
    total_rows = len(pdf) + len(vdf)
    total_pages = pp.pages + pv.pages
    print(f"TOTAL price+volume load: {dt_p + dt_v:.1f}s · {total_rows:,} rows · "
          f"{total_pages} round-trips · {total_rows / (dt_p + dt_v):,.0f} rows/s overall")
    if not pdf.empty:
        print(f"prices date range returned: {pdf['target_date'].min().date()} .. {pdf['target_date'].max().date()}")
    print("=" * 72)


if __name__ == "__main__":
    main()
