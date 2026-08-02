"""Force a COMPLETE re-read of price + volume history from GuruFocus.

⚠ THE ONLY WAY OUR HISTORY EVER GETS CORRECTED.

`ingest/prices.py::_upsert_metric_rows` writes only rows with `d > existing_max`,
so once a bar is stored it is never revisited — and `force_refresh=True` does NOT
change that, it re-downloads the full series and still writes only the newer rows.
Every vendor correction to the PAST is therefore invisible to the normal pipeline.

Measured on the 1,479-name Leonteq universe, 2026-08-02:

    173 companies had wrong CLOSE history   (46,969 bars)
    887 companies had wrong VOLUME history  (68,311 bars)

⚠ AND THE BIG ONES ARE NOT THE DANGEROUS ONES. A seam detector finds Worldline's
1-for-40 (0.2886 → 11.16 overnight, a +1142% momentum on a stock that fell 69%).
It cannot find Air Liquide's 1-for-10 free share attribution, which re-scales the
whole history by 10/11 and shows up as a −9.1% step — an ordinary day's move, with
no threshold that separates it from real price action. Only asking the vendor for
every bar finds those, which is why this exists and why it runs on a schedule
rather than on suspicion.

TWO RULES, BOTH LEARNED THE HARD WAY

  * An empty vendor response NEVER deletes anything. A 404 (a delisted ticker
    that no longer resolves — `SATS`), a 403 (unsubscribed region), a throttle:
    all keep the stored series. Trading a stale series for no series is strictly
    worse, and it is exactly what "replace with whatever the API returns" does.
  * Only bars that actually MOVED are written. The vendor's series is ~5,000 bars
    per company; rewriting all of them for 1,479 companies is ~15M row-writes to
    say almost nothing. Diffing first turns that into the few thousand that
    changed — and the diff IS the audit trail.
"""
from __future__ import annotations

import logging
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Callable

from common.pg import _run_copy
from deps import supabase
from ingest.prices import (
    DATA_CUTOFF,
    _fetch_indicator_from_api,
    _parse_price_series,
    upsert_metric_rows,
)

log = logging.getLogger(__name__)

# Same concurrency as the prices phase: fast enough to finish ~1,500 companies in
# minutes, low enough that GuruFocus/Cloudflare doesn't throttle the tail.
WORKERS = 8
# A stored/vendor difference beyond this on a shared date means the history moved.
# Not zero — a float round-trip through JSON differs in the last digit.
CHANGE_TOLERANCE = 1e-6
METRICS = (("close_price", "price"), ("volume", "volume"))
# ── Phantom bars ───────────────────────────────────────────────────
# ⚠ A ROW ON A DAY THE MARKET WAS SHUT. Older GuruFocus responses carried
# exchange holidays with a volume of 0; the vendor now omits those dates
# entirely, but we stored them and an upsert-only refetch can never remove them.
# Measured on VERBUND (WBO): 8 zero-volume bars — 24/25/26 December, 31 December,
# 1 January, Good Friday, Easter Monday, 1 May. Every one a Vienna holiday, and
# every one dragging the mean down for the 20–60 trading days its rolling window
# covers. That is what put `vol_trend_3m` at 39,211,500 and flattened every other
# company's volume score to ~0.
#
# Deleting stored rows is the one operation a re-run cannot undo, so it is fenced
# three ways: only INSIDE the span the vendor returned, only where the vendor's
# own series is DENSE around the date (a vendor gap must not read as "this day
# never traded"), and never beyond `PHANTOM_MAX_SHARE` of a company's rows — a
# wholesale mismatch means something else is wrong (a repointed listing with a
# different trading calendar), not that we hold thousands of phantoms.
PHANTOM_NEIGHBOUR_DAYS = 7
PHANTOM_MAX_SHARE = 0.05
# Bars older than this are the marker: the normal pipeline can never rewrite one
# (it only writes `d > existing_max`), so an OLD bar with a RECENT `recorded_at`
# is proof a full refetch ran. A stored flag would be a second source of truth
# someone has to remember to set; this is structural.
_OLD_BAR_BEFORE = "2024-01-01"


def last_full_refetch(company_ids: list[int]) -> datetime | None:
    """When a full refetch last rewrote history for these companies, or None.

    Reads the newest `recorded_at` among bars whose `target_date` is old enough
    that nothing but a full refetch could have written them."""
    if not company_ids:
        return None
    buf = _run_copy(
        "COPY (SELECT max(recorded_at)::text FROM metric_data "
        "WHERE metric_code = 'close_price' AND source_code = 'gurufocus' "
        "AND company_id = ANY(%s::int[]) AND target_date < %s) TO STDOUT WITH (FORMAT csv)",
        (list(company_ids), _OLD_BAR_BEFORE),
    )
    if buf is None:
        return None
    raw = buf.read().decode().strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace(" ", "T").replace("Z", "+00:00"))
    except ValueError:
        return None


def refetched_this_month(company_ids: list[int], *, now: datetime | None = None) -> bool:
    """Has a full refetch already run for these companies this CALENDAR month?

    The rebalance gate. A monthly strategy is due once a month, so "when due" is
    already once a month — but a weekly strategy would otherwise refetch every
    week, and a Force re-rebalance would refetch on every press."""
    seen = last_full_refetch(company_ids)
    if seen is None:
        return False
    n = now or datetime.now(timezone.utc)
    if seen.tzinfo is None:
        seen = seen.replace(tzinfo=timezone.utc)
    return (seen.year, seen.month) == (n.year, n.month)


def find_phantoms(
    stored_dates: list[str],
    vendor_dates: set[str],
    *,
    neighbour_days: int = PHANTOM_NEIGHBOUR_DAYS,
    max_share: float = PHANTOM_MAX_SHARE,
) -> tuple[list[str], str | None]:
    """Stored dates the vendor does not have, that are safe to delete.

    Returns `(phantoms, refusal_reason)`. Pure — no DB, no network — so the three
    fences are unit-testable without a vendor:

      * INSIDE THE SPAN. Never a date before the vendor's first bar or after its
        last: a vendor that truncated its history must not delete ours.
      * DENSE NEIGHBOURHOOD. The vendor must have a bar within `neighbour_days`
        on BOTH sides. A genuine vendor gap (a missing month) then reads as a gap
        rather than as "none of these days ever traded".
      * BOUNDED. Beyond `max_share` of our rows, refuse the whole company and say
        why. Mass disagreement is a different bug — a repointed listing with its
        own trading calendar — and deleting into it would destroy real history.
    """
    if not vendor_dates or not stored_dates:
        return [], None
    lo, hi = min(vendor_dates), max(vendor_dates)
    ordered = sorted(vendor_dates)
    import bisect  # noqa: PLC0415
    from datetime import date as _d  # noqa: PLC0415

    out: list[str] = []
    for s in stored_dates:
        if s in vendor_dates or s < lo or s > hi:
            continue
        i = bisect.bisect_left(ordered, s)
        before = ordered[i - 1] if i > 0 else None
        after = ordered[i] if i < len(ordered) else None
        if not before or not after:
            continue
        gap_b = (_d.fromisoformat(s) - _d.fromisoformat(before)).days
        gap_a = (_d.fromisoformat(after) - _d.fromisoformat(s)).days
        if gap_b <= neighbour_days and gap_a <= neighbour_days:
            out.append(s)
    if out and len(out) > max_share * len(stored_dates):
        return [], (
            f"{len(out)} of {len(stored_dates)} stored bars are absent from the vendor "
            f"(> {max_share:.0%}) — refusing to delete; this looks like a different "
            "trading calendar (a repointed listing?), not phantom rows"
        )
    return out, None


def _stored(cids: list[int], metric: str) -> dict[int, dict[str, float]]:
    buf = _run_copy(
        "COPY (SELECT company_id, target_date::text, numeric_value FROM metric_data "
        "WHERE metric_code = %s AND source_code = 'gurufocus' "
        "AND company_id = ANY(%s::int[])) TO STDOUT WITH (FORMAT csv)",
        (metric, cids),
    )
    if buf is None:
        raise RuntimeError("no COPY path — set SUPABASE_DB_URL")
    out: dict[int, dict[str, float]] = defaultdict(dict)
    for line in buf.read().decode().splitlines():
        cid, d, v = line.split(",")
        out[int(cid)][d] = float(v)
    return out


def load_symbols(cids: list[int]) -> dict[int, dict]:
    """`company_id → {ticker, exchange, name}` for the fetch."""
    out: dict[int, dict] = {}
    for i in range(0, len(cids), 200):
        for r in (supabase.table("company")
                  .select("company_id, company_name, gurufocus_ticker, "
                          "gurufocus_exchange:gurufocus_exchange(exchange_code)")
                  .in_("company_id", cids[i:i + 200]).execute().data or []):
            out[int(r["company_id"])] = {
                "ticker": r.get("gurufocus_ticker"),
                "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "name": r.get("company_name"),
            }
    return out


def refetch_full_history(
    company_ids: list[int],
    *,
    apply: bool = True,
    prune_phantoms: bool = True,
    on_step: Callable[[str, str], None] | None = None,
) -> dict:
    """Re-read every bar for `company_ids` and write back only what changed.

    `on_step(message, level)` receives a running commentary (the pipeline pipes it
    into the run transcript; the CLI prints it). Returns the counters + the list
    of companies whose history moved."""
    def _say(msg: str, level: str = "info") -> None:
        if on_step:
            on_step(msg, level)
        (log.warning if level in ("warn", "error") else log.info)("[refetch] %s", msg)

    cids = sorted({int(c) for c in company_ids})
    if not cids:
        return {"companies": 0, "counters": {}, "moved": []}
    # ONE timestamp for the whole run, so "was there a refetch this month" reads a
    # single instant rather than a smear across a 20-minute walk.
    _now_iso = datetime.now(timezone.utc).isoformat()
    meta = load_symbols(cids)
    _say(f"Full history refetch: {len(cids)} companies, reading stored series…")
    stored = {m: _stored(cids, m) for m, _ in METRICS}

    lock = threading.Lock()
    counters: dict[str, int] = defaultdict(int)
    moved: list[dict] = []
    phantom_examples: list[str] = []
    done = [0]

    def _one(cid: int) -> None:
        m = meta.get(cid) or {}
        tic, exch = m.get("ticker"), m.get("exchange")
        label = f"{exch}:{tic}"
        if not tic or not exch:
            with lock:
                counters["no_symbol"] += 1
            return
        for metric, indicator in METRICS:
            try:
                data, _log, status = _fetch_indicator_from_api(tic, exch, indicator)
            except Exception as e:  # noqa: BLE001
                with lock:
                    counters["error"] += 1
                _say(f"  {label} {metric}: FETCH FAILED {type(e).__name__}: {e}", "error")
                continue
            series = _parse_price_series(data or [])
            if not series:
                # ⚠ NEVER a delete — see the module header.
                with lock:
                    counters["empty_vendor"] += 1
                _say(f"  {label} {metric}: vendor returned nothing (HTTP {status}) — kept ours",
                     "warn")
                continue

            have = stored[metric].get(cid, {})
            changed: list[dict] = []
            first: tuple[str, float, float] | None = None
            for d, v in series:
                if d < DATA_CUTOFF:
                    continue
                key = d.isoformat()
                old = have.get(key)
                if old is None or abs(old - v) > max(CHANGE_TOLERANCE, abs(old) * 1e-9):
                    if old is not None and first is None:
                        first = (key, old, v)
                    changed.append({
                        "company_id": cid, "metric_code": metric, "source_code": "gurufocus",
                        "target_date": key, "numeric_value": v,
                        # ⚠ STAMPED EXPLICITLY, AND THE MONTH GUARD DEPENDS ON IT.
                        # `recorded_at` defaults on INSERT only; an upsert that
                        # resolves to an UPDATE leaves the original timestamp, so a
                        # corrected 2015 bar would still read "first seen 2026-06"
                        # and `last_full_refetch` would never advance. It is also
                        # the truthful value: this is when we learned THIS number.
                        "recorded_at": _now_iso,
                    })
            with lock:
                counters[f"{metric}_fetched"] += 1
                if changed:
                    counters[f"{metric}_bars_changed"] += len(changed)
                    counters[f"{metric}_companies_changed"] += 1
                    if first:
                        moved.append({"label": label, "metric": metric, "bars": len(changed),
                                      "sample_date": first[0], "was": first[1], "now": first[2]})
            if changed and apply:
                upsert_metric_rows(supabase, changed, with_retry=True,
                                   description=f"refetch {metric} cid={cid}")

            # ── Phantoms: rows we hold on days the vendor never traded ──
            if prune_phantoms:
                vendor_dates = {d.isoformat() for d, _ in series if d >= DATA_CUTOFF}
                ghosts, refusal = find_phantoms(sorted(have.keys()), vendor_dates)
                if refusal:
                    with lock:
                        counters[f"{metric}_prune_refused"] += 1
                    _say(f"  {label} {metric}: {refusal}", "warn")
                elif ghosts:
                    with lock:
                        counters[f"{metric}_phantoms"] += len(ghosts)
                        counters[f"{metric}_phantom_companies"] += 1
                        if len(phantom_examples) < 40:
                            phantom_examples.append(
                                f"{label} {metric}: {len(ghosts)} bar(s) "
                                f"({', '.join(ghosts[:4])}{'…' if len(ghosts) > 4 else ''})"
                            )
                    if apply:
                        # Chunked: a company can carry years of holidays, and the
                        # delete filter goes in the URL.
                        for i in range(0, len(ghosts), 100):
                            supabase.table("metric_data").delete() \
                                .eq("company_id", cid).eq("metric_code", metric) \
                                .eq("source_code", "gurufocus") \
                                .in_("target_date", ghosts[i:i + 100]).execute()
        with lock:
            done[0] += 1
            if done[0] % 250 == 0:
                _say(f"  … {done[0]}/{len(cids)} companies")

    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix="refetch") as ex:
        list(ex.map(_one, cids))

    # ⚠ STAMP THE MARKER EVEN WHEN NOTHING MOVED — the clean run is the common
    # case and it must still count as "we asked this month". Only changed bars are
    # written, so a universe that is already correct would otherwise leave no
    # trace, `last_full_refetch` would never advance, and a weekly strategy would
    # re-ask every week precisely because the data was fine. One row: the oldest
    # bar we already hold, re-written with its own unchanged value and today's
    # `recorded_at` — a true statement (we re-verified it just now), not a flag.
    if apply and counters.get("close_price_fetched"):
        try:
            probe = (
                supabase.table("metric_data")
                .select("company_id, target_date, numeric_value")
                .eq("metric_code", "close_price").eq("source_code", "gurufocus")
                .in_("company_id", cids[:1] or [0])
                .order("target_date").limit(1).execute()
            ).data
            if probe:
                r = probe[0]
                upsert_metric_rows(supabase, [{
                    "company_id": r["company_id"], "metric_code": "close_price",
                    "source_code": "gurufocus", "target_date": r["target_date"],
                    "numeric_value": r["numeric_value"], "recorded_at": _now_iso,
                }], with_retry=True, description="refetch marker")
        except Exception as e:  # noqa: BLE001
            _say(f"could not stamp the refetch marker: {type(e).__name__}: {e} — "
                 "the next rebalance may re-ask", "warn")

    # ⚠ The headline is what MOVED, not what ran. "1,479 companies refetched" is
    # a receipt; "173 had wrong close history" is the finding.
    _say(
        f"{'Rewrote' if apply else 'Would rewrite'} history: "
        f"{counters.get('close_price_companies_changed', 0)} companies' closes "
        f"({counters.get('close_price_bars_changed', 0)} bars), "
        f"{counters.get('volume_companies_changed', 0)} companies' volumes "
        f"({counters.get('volume_bars_changed', 0)} bars); "
        f"{counters.get('empty_vendor', 0)} kept as-is (vendor had nothing), "
        f"{counters.get('error', 0)} errors",
        "warn" if counters.get("close_price_companies_changed") else "info",
    )
    for row in sorted(moved, key=lambda r: -r["bars"])[:25]:
        _say(f"      {row['label']} {row['metric']}: {row['bars']} bar(s), "
             f"e.g. {row['sample_date']} {row['was']:.4f} → {row['now']:.4f}")

    ph_close = counters.get("close_price_phantoms", 0)
    ph_vol = counters.get("volume_phantoms", 0)
    if ph_close or ph_vol or counters.get("close_price_prune_refused") or counters.get("volume_prune_refused"):
        _say(
            f"{'Deleted' if apply else 'Would delete'} phantom bars (dates the vendor never "
            f"traded): {ph_close} close across "
            f"{counters.get('close_price_phantom_companies', 0)} companies, "
            f"{ph_vol} volume across {counters.get('volume_phantom_companies', 0)}; "
            f"{counters.get('close_price_prune_refused', 0) + counters.get('volume_prune_refused', 0)}"
            " company/metric(s) refused as too different to be phantoms",
            "warn" if (ph_close or ph_vol) else "info",
        )
        for ex in phantom_examples[:25]:
            _say(f"      {ex}")
    return {"companies": done[0], "counters": dict(counters), "moved": moved,
            "phantoms": phantom_examples}
