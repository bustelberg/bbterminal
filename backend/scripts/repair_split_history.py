"""Repair price/volume history the vendor RE-SCALED and we never re-read.

⚠ THE BUG THIS FIXES IS SILENT, AND IT REACHES THE LIVE BOOK.

When a split or reverse split happens, GuruFocus rewrites the WHOLE series to the
new share basis. Our ingest only ever asks for dates NEWER than what we hold
(`ingest/prices.py::_upsert_metric_rows` filters `d > existing_max`), so the
rewrite is never read: the old bars keep the old scale, the new ones arrive on
the new one, and the seam between them is a price move no market ever made.
⚠ `force_refresh=True` does NOT fix it — it re-downloads the full series and then
still writes only the newer rows. Nothing in the normal pipeline can repair this.

Measured 2026-08-02, Worldline SA (`XPAR:WLN`, 1-for-40 reverse split):

    stored 2025-07-31   0.8277        vendor today   33.1068      ×40.00
    stored 2026-06-10   0.2886        stored 06-11   11.1600      ×38.67  <- the seam

    12-1M momentum read  +1142%   (0.8277 → 10.28)
    the truth            −69%     (33.11  → 10.28)

...which put Worldline in all three live strategies' books as a top-scoring
Financial. 12 of the 1,479 Leonteq names carried the same break.

WHAT MAKES THE REPAIR SAFE

A big one-day move is not proof of a split — a stock really can halve. So a
candidate is only repaired when the VENDOR DISAGREES WITH US about a date we both
already have: we compare stored vs live for bars BEFORE the seam. If the vendor's
history matches ours, the jump was real and the row is left alone (reported as
`real-move`). Only a consistent ratio across sampled pre-seam dates is a rewrite.

⚠ VOLUME IS RE-SCALED TOO, INVERSELY. A 1:40 reverse split multiplies price by 40
and divides volume by 40, so `vol_20d_vs_60d` and `vol_trend_3m` are corrupted by
the same event. Both metrics are repaired; repairing only price would leave half
the signals wrong while the obvious symptom disappeared.

Usage (dry run by default — nothing is written without --apply):

    cd backend && PYTHONPATH=. uv run python scripts/repair_split_history.py
    cd backend && PYTHONPATH=. uv run python scripts/repair_split_history.py --apply
    ... --universe "LEONTEQ (as of 2026-06-17)"   # default: every company
    ... --company-id 5608                          # one name
"""
from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import date

import deps  # noqa: F401 — loads .env/.env.local
from common.pg import _run_copy
from deps import supabase
from ingest.prices import (
    _fetch_indicator_from_api,
    _parse_price_series,
    upsert_metric_rows,
)

# A single-session move beyond this is a split seam until proven otherwise. Real
# one-day moves of 3× happen (a biotech readout, a takeover pop) — which is why
# nothing is repaired on this signal alone; it only selects who to ASK about.
SEAM_RATIO = 3.0
# How many pre-seam dates to compare against the vendor. More than one because a
# single date could differ for a mundane reason (a late correction); a CONSISTENT
# ratio across several is a re-scaled history.
SAMPLE_DATES = 5
# Stored/vendor ratios must agree within this to count as one clean re-scale.
RATIO_TOLERANCE = 0.02
# Metrics that get re-scaled by the same corporate action.
METRICS = ("close_price", "volume")


def _members(universe_label: str | None, company_id: int | None) -> list[int]:
    if company_id:
        return [company_id]
    if universe_label:
        u = (supabase.table("universe").select("universe_id")
             .eq("label", universe_label).limit(1).execute()).data
        if not u:
            raise SystemExit(f"no universe labelled {universe_label!r}")
        uid = int(u[0]["universe_id"])
        out, off = [], 0
        while True:
            rows = (supabase.table("universe_membership").select("company_id")
                    .eq("universe_id", uid).range(off, off + 999).execute().data or [])
            out += [int(r["company_id"]) for r in rows]
            if len(rows) < 1000:
                return sorted(set(out))
            off += 1000
    out, off = [], 0
    while True:
        rows = (supabase.table("company").select("company_id")
                .is_("delisted_at", "null").range(off, off + 999).execute().data or [])
        out += [int(r["company_id"]) for r in rows]
        if len(rows) < 1000:
            return sorted(set(out))
        off += 1000


def _stored_series(cids: list[int], metric: str, since: str) -> dict[int, list[tuple[str, float]]]:
    buf = _run_copy(
        "COPY (SELECT company_id, target_date::text, numeric_value FROM metric_data "
        "WHERE metric_code = %s AND source_code = 'gurufocus' "
        "AND company_id = ANY(%s::int[]) AND target_date >= %s "
        "ORDER BY company_id, target_date) TO STDOUT WITH (FORMAT csv)",
        (metric, cids, since),
    )
    if buf is None:
        raise SystemExit("no COPY path — set SUPABASE_DB_URL")
    out: dict[int, list[tuple[str, float]]] = defaultdict(list)
    for line in buf.read().decode().splitlines():
        cid, d, v = line.split(",")
        out[int(cid)].append((d, float(v)))
    return out


def _find_seam(rows: list[tuple[str, float]]) -> tuple[str, float] | None:
    """The LAST single-session jump beyond `SEAM_RATIO`, as (date, ratio). Last,
    not first: a series can carry more than one rewrite, and the newest one is
    the boundary between "our scale" and "the vendor's current scale"."""
    seam = None
    for (_d0, v0), (d1, v1) in zip(rows, rows[1:]):
        if v0 > 0:
            ratio = v1 / v0
            if ratio >= SEAM_RATIO or ratio <= 1 / SEAM_RATIO:
                seam = (d1, ratio)
    return seam


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--universe", default=None, help="restrict to a universe label")
    p.add_argument("--company-id", type=int, default=None)
    p.add_argument("--since", default="2024-01-01", help="scan window for the seam")
    p.add_argument("--apply", action="store_true", help="write the repair (default: dry run)")
    a = p.parse_args()

    cids = _members(a.universe, a.company_id)
    print(f"scanning {len(cids)} companies from {a.since} (seam ≥ {SEAM_RATIO}×)")
    closes = _stored_series(cids, "close_price", a.since)

    meta: dict[int, dict] = {}
    for i in range(0, len(cids), 200):
        for r in (supabase.table("company")
                  .select("company_id, company_name, gurufocus_ticker, "
                          "gurufocus_exchange:gurufocus_exchange(exchange_code)")
                  .in_("company_id", cids[i:i + 200]).execute().data or []):
            meta[int(r["company_id"])] = r

    candidates = []
    for cid, rows in closes.items():
        seam = _find_seam(rows)
        if seam:
            candidates.append((cid, *seam))
    print(f"{len(candidates)} candidate(s) with a seam\n")

    repaired = skipped = failed = 0
    for cid, seam_date, seam_ratio in sorted(candidates, key=lambda c: c[0]):
        m = meta.get(cid) or {}
        tic = m.get("gurufocus_ticker") or "?"
        exch = (m.get("gurufocus_exchange") or {}).get("exchange_code") or "?"
        label = f"{exch}:{tic} {(m.get('company_name') or '?')[:28]}"

        # Ask the vendor for the full series it serves TODAY.
        data, _log, status = _fetch_indicator_from_api(tic, exch, "price")
        vendor = dict(_parse_price_series(data or []))
        if not vendor:
            print(f"  {label:44s} SKIP — vendor returned nothing (HTTP {status})")
            failed += 1
            continue

        pre = [(d, v) for d, v in closes[cid] if d < seam_date][-SAMPLE_DATES:]
        ratios = []
        for d, v in pre:
            live = vendor.get(date.fromisoformat(d))
            if live and v > 0:
                ratios.append(live / v)
        if not ratios:
            print(f"  {label:44s} SKIP — no comparable pre-seam dates")
            failed += 1
            continue
        lo, hi = min(ratios), max(ratios)
        consistent = (hi - lo) / max(abs(hi), 1e-9) <= RATIO_TOLERANCE
        factor = sum(ratios) / len(ratios)

        if abs(factor - 1.0) <= RATIO_TOLERANCE:
            # The vendor's history agrees with ours — the jump was a real move.
            print(f"  {label:44s} real-move at {seam_date} (×{seam_ratio:.3f}) — left alone")
            skipped += 1
            continue
        if not consistent:
            print(f"  {label:44s} ⚠ INCONSISTENT ratios {lo:.3f}–{hi:.3f} at {seam_date} "
                  "— NOT repaired, inspect by hand")
            failed += 1
            continue

        print(f"  {label:44s} RESCALED ×{factor:.4f} before {seam_date} "
              f"(stored {pre[-1][1]:.4f} vs vendor {vendor[date.fromisoformat(pre[-1][0])]:.4f})")
        if not a.apply:
            repaired += 1
            continue

        # Rewrite BOTH metrics from the vendor's current series. ⚠ Straight
        # upserts of the full payload — `load_prices_into_db` would drop every
        # row at-or-before our stored max, which is precisely the history that
        # needs replacing.
        for metric, indicator in (("close_price", "price"), ("volume", "volume")):
            payload = data if indicator == "price" else (
                _fetch_indicator_from_api(tic, exch, "volume")[0] or [])
            series = _parse_price_series(payload)
            rows = [
                {"company_id": cid, "metric_code": metric, "source_code": "gurufocus",
                 "target_date": d.isoformat(), "numeric_value": v}
                for d, v in series if d >= date(1998, 1, 1)
            ]
            if rows:
                n = upsert_metric_rows(supabase, rows, with_retry=True,
                                       description=f"repair {metric} cid={cid}")
                print(f"      {metric}: rewrote {n} row(s)")
        repaired += 1

    print(f"\n{'APPLIED' if a.apply else 'DRY RUN'} — {repaired} re-scaled, "
          f"{skipped} real moves left alone, {failed} unresolved")
    if not a.apply and repaired:
        print("re-run with --apply to write the repair")
    return 0


raise SystemExit(main())
