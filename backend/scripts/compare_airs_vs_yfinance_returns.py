"""Read-only spike: AIRS book returns vs the yfinance model reconstruction, side by side.

    uv run python scripts/compare_airs_vs_yfinance_returns.py            # every linked pair
    uv run python scripts/compare_airs_vs_yfinance_returns.py --id 34    # one model portfolio
    uv run python scripts/compare_airs_vs_yfinance_returns.py --name Neutraal

WHAT IT ANSWERS — the two questions that decide whether basing /portfolios "Analyse" on AIRS
(the Vermogensoverzicht / VOLK report, stored in `airs_holding`) is worth building:

  1. DO THE NUMBERS LOOK RIGHT?  For every model portfolio paired with an AIRS book, it prints
     the current yfinance YTD (`compute_portfolio_performance`, the model buy-and-hold) beside
     AIRS's own flow-aware book YTD (`cumulatief_rendement`), and the gap in pp. A modest,
     explicable gap (implementation drift, income, fees) is the expected result; a wild one is a
     red flag to chase before committing.

  2. DOES NAME -> ISIN RESOLUTION COVER ENOUGH?  VOLK carries `Fondsomschrijving` (a name), NOT
     an ISIN — so every downstream number (attribution, buckets, benchmark match) rides on
     `resolve_account_isins` (the price-gated matcher). This reports, per book, the share of book
     VALUE that resolves to an ISIN, how many holdings resolve, and how many did so only weakly.
     The unresolved names are listed — those are exactly the holdings that would drop out of an
     AIRS-based attribution.

READ-ONLY. It writes nothing; it only reads Supabase + runs the existing compute functions.
Run it against PROD data (the local DB has no AIRS scrape).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import deps  # noqa: E402, F401  — imported first so env + Supabase client load


def _fmt(v: float | None, suffix: str = "") -> str:
    return f"{v:+.2f}{suffix}" if isinstance(v, (int, float)) else "   —  "


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--id", type=int, help="only this model portfolio id")
    ap.add_argument("--name", type=str, help="only model portfolios whose name contains this")
    ap.add_argument("--show-resolved", action="store_true",
                    help="also list the resolved holdings, not just the unresolved ones")
    args = ap.parse_args()

    from routers._airs_account_links import list_account_links
    from routers._airs_holding_isin import resolve_account_isins
    from routers._airs_portfolio_perf import compute_portfolio_performance

    print("Loading yfinance model performance (this is the heavy step)...", flush=True)
    perf_by_id = {r["portfolio_id"]: r for r in compute_portfolio_performance()}

    accounts = list_account_links()["accounts"]
    # Only pairs where a human (or the guess) has linked the book to a model.
    linked = [a for a in accounts if a.get("model_portfolio_id")]
    if args.id:
        linked = [a for a in linked if a["model_portfolio_id"] == args.id]

    rows_out: list[dict] = []
    for a in linked:
        pid = a["model_portfolio_id"]
        perf = perf_by_id.get(pid)
        model_name = (perf or {}).get("name") or f"id={pid}"
        if args.name and args.name.lower() not in model_name.lower():
            continue

        res = resolve_account_isins(a["portefeuille"])
        hrows = res.get("rows") or []
        total_val = sum((r.get("current_value_eur") or 0) for r in hrows)
        resolved = [r for r in hrows if r.get("isin")]
        resolved_val = sum((r.get("current_value_eur") or 0) for r in resolved)
        weak = [r for r in resolved if r.get("weak_name")]
        unresolved = [r for r in hrows if not r.get("isin")
                      and (r.get("asset_class") != "Cash")]

        # A rough book PRICE return over the resolved sleeve, from VOLK start/current EUR values
        # — NOT flow-aware and excludes income, so it deliberately differs from cumulatief_rendement.
        start_val = sum((r.get("start_value_eur") or 0) for r in resolved
                        if r.get("start_value_eur"))
        cur_for_start = sum((r.get("current_value_eur") or 0) for r in resolved
                            if r.get("start_value_eur"))
        volk_price_ret = ((cur_for_start / start_val - 1.0) * 100.0) if start_val > 0 else None

        rows_out.append({
            "model": model_name,
            "book": a["portefeuille"],
            "yf_ytd": (perf or {}).get("ytd_pct"),
            "yf_since": (perf or {}).get("since_model_pct"),
            "yf_changed": (perf or {}).get("model_changed_in_period"),
            "book_ytd": a.get("ytd_pct"),                 # cumulatief_rendement (flow-aware + income)
            "volk_price_ret": volk_price_ret,             # VOLK start->current, resolved sleeve
            "cover_pct": (resolved_val / total_val * 100.0) if total_val > 0 else None,
            "n_resolved": len(resolved),
            "n_total": len([r for r in hrows if r.get("asset_class") != "Cash"]),
            "n_weak": len(weak),
            "unresolved": unresolved,
            "resolved": resolved,
        })

    if not rows_out:
        print("No linked model<->book pairs matched. "
              "(Link accounts on /portfolios, or check airs_account_links.)")
        return

    rows_out.sort(key=lambda r: r["model"].lower())

    print()
    print(f"{'MODEL':<28} {'BOOK':<26} {'yfYTD':>8} {'bookYTD':>8} {'diff':>7} "
          f"{'VOLKpx':>8} {'cover':>7} {'resolved':>10} {'weak':>5}")
    print("-" * 122)
    for r in rows_out:
        gap = (r["yf_ytd"] - r["book_ytd"]
               if isinstance(r["yf_ytd"], (int, float)) and isinstance(r["book_ytd"], (int, float))
               else None)
        star = " *" if r["yf_changed"] else ""      # yf YTD is a partial year (young model)
        cover = f"{r['cover_pct']:.0f}%" if r["cover_pct"] is not None else "-"
        print(f"{r['model'][:28]:<28} {r['book'][:26]:<26} "
              f"{_fmt(r['yf_ytd']):>8} {_fmt(r['book_ytd']):>8} {_fmt(gap):>7} "
              f"{_fmt(r['volk_price_ret']):>8} {cover:>7} "
              f"{f'{r['n_resolved']}/{r['n_total']}':>10} {r['n_weak']:>5}{star}")

    print("\n  diff = yfYTD - bookYTD (pp).  '*' = yfinance YTD is a PARTIAL year (model younger "
          "than the year), so its window != AIRS's Jan-1 year - the gap there is not comparable.")
    print("  bookYTD = AIRS cumulatief_rendement (flow-aware, includes income/fees).")
    print("  VOLKpx  = start->current EUR over the RESOLVED sleeve (price only, no income) - a "
          "rough proxy, differs from bookYTD by design.")
    print("  cover   = share of book VALUE that resolves to an ISIN.  weak = low-confidence "
          "name matches among the resolved.")

    # The seam, spelled out: which holdings would fall through an AIRS-based attribution.
    for r in rows_out:
        if not r["unresolved"] and not (args.show_resolved and r["resolved"]):
            continue
        print(f"\n{r['model']}  ({r['book']}) - as-of resolution:")
        for h in sorted(r["unresolved"], key=lambda x: -(x.get("current_value_eur") or 0)):
            val = h.get("current_value_eur") or 0
            pct = (val / (sum((x.get('current_value_eur') or 0) for x in r['resolved'] + r['unresolved']) or 1)) * 100
            print(f"   UNRESOLVED  {pct:5.1f}%  {h.get('holding_name')}")
        if args.show_resolved:
            for h in sorted(r["resolved"], key=lambda x: -(x.get("current_value_eur") or 0)):
                flag = " (weak)" if h.get("weak_name") else ""
                print(f"   ok  {h.get('isin')}  {h.get('holding_name')}{flag}")


if __name__ == "__main__":
    main()
