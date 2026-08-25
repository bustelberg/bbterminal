"""Which trading session each risk tracker closes in, and how much of the index closes with it.

⚠⚠ THE TRACKING-ERROR PANEL WARNS THAT DAILY CLOSES ARE NOT SYNCHRONOUS, and the warning names one
cause — "the tracker closes at 16:30 London, a US holding at 21:00". That claim is about the VENUE
we price the tracker on, not about the fund: an ISIN names a fund, and `asset_execution` picks which
listing of it we actually read. So the warning could be right, stale, or wrong, and nothing on
screen says which. This prints the venue behind each tracker and the session split of the index it
is supposed to stand for, which is what decides whether repointing a tracker would help.

⚠ SESSIONS, NOT CLOCK TIMES, ARE THE UNIT THAT MATTERS. Two series are synchronous when their bars
close in the same session; the exact minute is a DST detail that changes twice a year and would give
this report a false precision. Approximate UTC closes are printed as orientation only.

⚠ READ-ONLY. It writes nothing and touches no vendor.

    cd backend && uv run python scripts/diagnose_tracker_clocks.py
    cd backend && uv run python scripts/diagnose_tracker_clocks.py --isins IE00B6R52259,US4642882579
"""
from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import deps  # noqa: E402,F401  (loads .env / .env.local first)
from deps import IN_CHUNK_SIZE, supabase  # noqa: E402
from routers._asset_financials import _BENCHMARK_RISK_ETF  # noqa: E402

# ⚠ BY LISTING COUNTRY, WHICH IS THE VENUE'S — never the issuer's domicile. A US company on a German
# line closes at the German bell, and that is the only fact synchrony cares about. `asset_grid`
# carries both; picking the wrong one is the trap `CLAUDE.md` records for `msci_region`.
# ⚠ `listing_country` HOLDS FULL NAMES ("United States"), NOT ISO-2 CODES — checked against the
# live grid, where a code-keyed table silently bucketed every row as UNMAPPED and the report still
# printed a confident-looking breakdown. Codes stay in as a second key so an ISO-2 column elsewhere
# does not have to be special-cased.
_SESSION = {
    "AMERICAS": ({"UNITED STATES", "US", "CANADA", "CA", "BRAZIL", "BR", "MEXICO", "MX",
                  "ARGENTINA", "AR", "CHILE", "CL", "PERU", "PE", "COLOMBIA", "CO"},
                 "~20:00-21:00 UTC"),
    "EUROPE/ME/AFRICA": ({"UNITED KINGDOM", "GB", "IRELAND", "IE", "NETHERLANDS", "NL", "GERMANY",
                          "DE", "FRANCE", "FR", "SWITZERLAND", "CH", "SWEDEN", "SE", "DENMARK",
                          "DK", "NORWAY", "NO", "FINLAND", "FI", "ITALY", "IT", "SPAIN", "ES",
                          "BELGIUM", "BE", "AUSTRIA", "AT", "PORTUGAL", "PT", "POLAND", "PL",
                          "CZECH REPUBLIC", "CZ", "GREECE", "GR", "ISRAEL", "IL", "SOUTH AFRICA",
                          "ZA", "UNITED ARAB EMIRATES", "AE", "SAUDI ARABIA", "SA", "QATAR", "QA",
                          "TURKEY", "TR", "HUNGARY", "HU", "LUXEMBOURG", "LU"},
                         "~15:30-16:30 UTC"),
    "ASIA-PACIFIC": ({"JAPAN", "JP", "HONG KONG", "HK", "CHINA", "CN", "TAIWAN", "TW",
                      "SOUTH KOREA", "KOREA", "KR", "SINGAPORE", "SG", "AUSTRALIA", "AU",
                      "NEW ZEALAND", "NZ", "INDIA", "IN", "THAILAND", "TH", "MALAYSIA", "MY",
                      "INDONESIA", "ID", "PHILIPPINES", "PH", "VIETNAM", "VN"},
                     "~06:00-08:00 UTC"),
}

_GRID_COLS = ("isin,analysis_id,yahoo_symbol,exchange,listing_country,currency,status,bars,"
              "is_default,price_from,price_to")


def _session(country: str | None) -> str:
    c = (country or "").strip().upper()
    for name, (members, _) in _SESSION.items():
        if c in members:
            return name
    # ⚠ NOT SILENTLY BUCKETED. An unmapped country is a hole in the table above, and folding it into
    # the biggest session would make this report agree with itself while being wrong.
    return f"UNMAPPED({c or '—'})"


def _clock(session: str) -> str:
    return dict((k, v[1]) for k, v in _SESSION.items()).get(session, "unknown")


def _rows_for(isins: list[str]) -> list[dict]:
    out: list[dict] = []
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        chunk = isins[i:i + IN_CHUNK_SIZE]
        res = supabase.table("asset_grid").select(_GRID_COLS).in_("isin", chunk).execute()
        out += res.data or []
    return out


def _report_trackers(isins: dict[str, str]) -> dict[str, str]:
    """Print every listing behind each tracker; return {label: session of the priced listing}."""
    print("\n" + "=" * 96)
    print("RISK TRACKERS — which listing do we actually price?")
    print("=" * 96)
    rows = _rows_for(sorted(set(isins.values())))
    by_isin: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_isin[(r.get("isin") or "").upper()].append(r)

    picked: dict[str, str] = {}
    for label, isin in isins.items():
        found = by_isin.get(isin.upper(), [])
        print(f"\n{label}  ({isin})")
        if not found:
            # ⚠ LOUD. A tracker with no row is a benchmark leg that cannot be priced at all, which
            # is a bigger problem than the clock question this script was written for.
            print("  !! NO asset_grid ROW — this benchmark has no priced listing.")
            continue
        # The pick the price loaders make: a usable listing, `is_default` first, then deepest bars.
        usable = [r for r in found
                  if r.get("status") == "ok" and (r.get("bars") or 0) > 0]
        best = max(usable, key=lambda r: (bool(r.get("is_default")), r.get("bars") or 0),
                   default=None)
        for r in sorted(found, key=lambda r: -(r.get("bars") or 0)):
            sess = _session(r.get("listing_country"))
            mark = "->" if best is not None and r is best else "  "
            print(f"  {mark} {str(r.get('yahoo_symbol') or '?'):<14}"
                  f" {str(r.get('exchange') or '?'):<12}"
                  f" {str(r.get('listing_country') or '?'):<4}"
                  f" {str(r.get('currency') or '?'):<4}"
                  f" {str(r.get('status') or '?'):<10}"
                  f" bars={r.get('bars') or 0:<7}"
                  f" {str(r.get('price_from') or '?')}..{str(r.get('price_to') or '?')}"
                  f"  [{sess}]")
        if best is None:
            print("  !! NO USABLE LISTING (status ok + bars > 0) — nothing to price this leg with.")
            continue
        sess = _session(best.get("listing_country"))
        picked[label] = sess
        print(f"     priced in {sess} ({_clock(sess)})")
    return picked


def _report_index(label: str, tracker_session: str | None) -> None:
    """The index's own constituents by session — the other half of the synchrony question."""
    from routers._asset_benchmark import members  # noqa: PLC0415

    print("\n" + "-" * 96)
    print(f"{label} CONSTITUENTS — what session is the index itself in?")
    print("-" * 96)
    try:
        mem, coverage = members(label)
    except Exception as e:  # noqa: BLE001 — a diagnostic must say why, not vanish
        print(f"  !! members({label!r}) raised {type(e).__name__}: {e}")
        return
    if not mem:
        print("  !! no priced constituents")
        return

    aids = [m["company_id"] for m in mem]           # ⚠ the analysis_id, see `members`' docstring
    country: dict[int, str] = {}
    for i in range(0, len(aids), IN_CHUNK_SIZE):
        res = (supabase.table("asset_grid").select("analysis_id,listing_country")
               .in_("analysis_id", aids[i:i + IN_CHUNK_SIZE]).execute())
        for r in res.data or []:
            country.setdefault(r["analysis_id"], r.get("listing_country"))

    total = sum(float(m.get("market_cap_eur") or 0) for m in mem)
    by_sess: dict[str, float] = defaultdict(float)
    n_by_sess: dict[str, int] = defaultdict(int)
    for m in mem:
        s = _session(country.get(m["company_id"]))
        by_sess[s] += float(m.get("market_cap_eur") or 0)
        n_by_sess[s] += 1

    print(f"  {len(mem)} priced constituents, {coverage.get('covered_pct') or 0:.2f}% of the index")
    for s, w in sorted(by_sess.items(), key=lambda kv: -kv[1]):
        share = (w / total * 100.0) if total > 0 else 0.0
        flag = "  <- tracker's session" if s == tracker_session else ""
        print(f"    {s:<20} {share:6.2f}% of cap   {n_by_sess[s]:>5} names{flag}")

    if tracker_session:
        aligned = (by_sess.get(tracker_session, 0.0) / total * 100.0) if total > 0 else 0.0
        print(f"\n  => {aligned:.2f}% of the index closes WITH the tracker; "
              f"{100 - aligned:.2f}% does not.")
        # ⚠ THE ACTIONABLE COMPARISON. If the largest session is not the tracker's, repointing the
        # tracker to a listing in that session strictly reduces the misaligned weight.
        biggest = max(by_sess, key=lambda k: by_sess[k])
        if biggest != tracker_session:
            best_share = by_sess[biggest] / total * 100.0
            print(f"  => a tracker priced in {biggest} would align {best_share:.2f}% instead "
                  f"(+{best_share - aligned:.2f}pp).")
        else:
            print("  => the tracker is already in the index's largest session; repointing it "
                  "would make the mismatch worse, not better.")


def _report_extra(isins: list[str]) -> None:
    print("\n" + "-" * 96)
    print("EXTRA ISINs")
    print("-" * 96)
    rows = _rows_for(isins)
    if not rows:
        print("  !! none of them have an asset_grid row")
        return
    for r in sorted(rows, key=lambda r: (r.get("isin") or "", -(r.get("bars") or 0))):
        sess = _session(r.get("listing_country"))
        print(f"  {str(r.get('isin')):<14} {str(r.get('yahoo_symbol') or '?'):<14}"
              f" {str(r.get('exchange') or '?'):<12} {str(r.get('listing_country') or '?'):<4}"
              f" {str(r.get('status') or '?'):<10} bars={r.get('bars') or 0:<7} [{sess}]")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--isins", default="",
                    help="extra comma-separated ISINs to look up (e.g. a candidate US listing)")
    ap.add_argument("--index", default="ACWI",
                    help="which index to break down by session (default ACWI)")
    args = ap.parse_args()

    print(f"[1/3] risk trackers: {', '.join(f'{k}={v}' for k, v in _BENCHMARK_RISK_ETF.items())}")
    picked = _report_trackers(_BENCHMARK_RISK_ETF)

    print(f"\n[2/3] index breakdown: {args.index}")
    _report_index(args.index.upper(), picked.get(args.index.upper()))

    extra = [s.strip().upper() for s in args.isins.split(",") if s.strip()]
    print(f"\n[3/3] extra ISINs: {len(extra)}")
    if extra:
        _report_extra(extra)

    print("\nDone. Nothing was written.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
