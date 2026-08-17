"""Backfill / correct `company.company_name` from GuruFocus.

The bulk equivalent of the `/companies` "GF name" button: for every company we
fetch the GuruFocus summary name (`summary.company_data.company`) for its
`(ticker, exchange)` and overwrite `company_name` when it genuinely differs —
alphanumeric-insensitive, so punctuation/casing diffs ("Apple Inc" vs "Apple
Inc.") don't churn, but a wrong company entirely (a row stored "TSMC" whose
`TSE:2330` listing GuruFocus calls "Forside Co Ltd") gets corrected. Also fills
in names for nameless stubs that GuruFocus can resolve.

Exchanges outside the GuruFocus subscription (India / UK / Ireland / AU-NZ /
Russia / Africa / LatAm — `is_gf_subscribed_exchange`) are skipped up front
since they only return a 403, so the run spends its calls where a name can
actually come back.

One GuruFocus call per company (the shared `_api_request` client rate-limits to
`_min_interval()`, 0.75s by default since 2026-08-17 and 1.5s before that), so a
full ~3k-company run is ~38 min. Companies GuruFocus can't
resolve (delisted, wrong exchange) are left unchanged.

Observability: emits one flushed line PER company (so a backgrounded run shows
live progress in its log — run with `python -u` or rely on the `flush=True`
callback) plus an explicit line for every rename, then a final summary.

Usage:
    uv run python -m index_universe.backfill_company_names                 # full run
    uv run python -m index_universe.backfill_company_names --only-nameless # only fill blanks
    uv run python -m index_universe.backfill_company_names --limit 30      # cap GF calls (testing)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field

from supabase import Client

from index_universe.acwi.exchange_map import is_gf_subscribed_exchange
from index_universe.backfill_market_cap import (
    _load_companies,
    _name_key,
    gf_company_name_for,
)

_log = logging.getLogger(__name__)


@dataclass
class NameBackfillResult:
    companies_scanned: int = 0
    skipped_have_name: int = 0      # --only-nameless: already had a name
    skipped_unsubscribed: int = 0   # exchange outside the GF subscription → no call
    gurufocus_calls: int = 0
    renamed: int = 0                # name genuinely differed → overwritten
    unchanged: int = 0             # GF name matched the stored one (already correct)
    no_name: int = 0               # call made, GuruFocus returned no usable name
    no_symbol: int = 0             # couldn't build a GF symbol (no ticker)
    renames: list[str] = field(default_factory=list)  # "cid=.. EXCH:TICK: old → new"
    errors: list[str] = field(default_factory=list)


def backfill_company_names(
    supabase: Client,
    *,
    only_nameless: bool = False,
    limit: int | None = None,
    on_progress=None,
) -> NameBackfillResult:
    def emit(msg: str, **fields) -> None:
        _log.info(msg)
        if on_progress:
            on_progress({"message": msg, **fields})

    result = NameBackfillResult()
    companies = _load_companies(supabase)
    result.companies_scanned = len(companies)

    targets: list[dict] = []
    for c in companies:
        if only_nameless and (c.get("company_name") or "").strip():
            result.skipped_have_name += 1
            continue
        targets.append(c)
    if limit is not None:
        targets = targets[:limit]

    total = len(targets)
    emit(f"{len(companies)} companies; checking GuruFocus name for {total} "
         f"(~{round(total * 1.5 / 60)} min at the 1.5s rate limit)…",
         processed=0, total=total, renamed=0)

    for i, c in enumerate(targets, 1):
        cid = int(c["company_id"])
        exch = (c.get("gurufocus_exchange") or {}) or {}
        exch_code = exch.get("exchange_code")
        ticker = c.get("gurufocus_ticker") or ""
        old = c.get("company_name")
        tag = f"{exch_code or '?'}:{ticker}"

        # Skip exchanges GuruFocus doesn't cover — they only 403, so a call
        # would never return a name. Keeps the run's calls where they count.
        if not is_gf_subscribed_exchange(exch_code):
            result.skipped_unsubscribed += 1
            emit(f"  {i}/{total} cid={cid} {tag}: skip (unsubscribed region)",
                 processed=i, total=total, renamed=result.renamed)
            continue

        try:
            res = gf_company_name_for(ticker, exch_code)
        except Exception as e:
            result.errors.append(f"cid={cid} ({tag}) GF error: {type(e).__name__}: {e}")
            emit(f"  {i}/{total} cid={cid} {tag}: ERROR {type(e).__name__}: {e}",
                 processed=i, total=total, renamed=result.renamed)
            continue

        if res.get("symbol") is None:
            result.no_symbol += 1
            emit(f"  {i}/{total} cid={cid} {tag}: no GF symbol (missing ticker)",
                 processed=i, total=total, renamed=result.renamed)
            continue

        result.gurufocus_calls += 1
        gf_name = res.get("name")
        if not gf_name:
            result.no_name += 1
            emit(f"  {i}/{total} cid={cid} {tag}: no name from GF (unresolved)",
                 processed=i, total=total, renamed=result.renamed)
            continue

        if _name_key(gf_name) == _name_key(old):
            result.unchanged += 1
            emit(f"  {i}/{total} cid={cid} {tag}: ok ({gf_name})",
                 processed=i, total=total, renamed=result.renamed)
            continue

        # Genuine mismatch (wrong company entirely, not just punctuation) →
        # overwrite with what GuruFocus reports for this listing.
        try:
            supabase.table("company").update(
                {"company_name": gf_name}
            ).eq("company_id", cid).execute()
            result.renamed += 1
            line = f"cid={cid} {tag}: {old!r} → {gf_name!r}"
            if len(result.renames) < 500:
                result.renames.append(line)
            emit(f"  {i}/{total} RENAME {line}",
                 processed=i, total=total, renamed=result.renamed)
        except Exception as e:
            result.errors.append(f"cid={cid} update failed: {type(e).__name__}: {e}")
            emit(f"  {i}/{total} cid={cid} {tag}: UPDATE FAILED {type(e).__name__}: {e}",
                 processed=i, total=total, renamed=result.renamed)

    emit(f"Done. renamed={result.renamed} unchanged={result.unchanged} "
         f"no-name={result.no_name} no-symbol={result.no_symbol} "
         f"skipped-unsubscribed={result.skipped_unsubscribed} errors={len(result.errors)}",
         processed=total, total=total, renamed=result.renamed)
    return result


def format_summary(r: NameBackfillResult) -> str:
    lines = [
        f"Companies scanned:      {r.companies_scanned}",
        f"  Skipped (named):      {r.skipped_have_name}",
        f"  Skipped unsubscribed: {r.skipped_unsubscribed}",
        f"  GuruFocus calls:      {r.gurufocus_calls}",
        f"  Renamed:              {r.renamed}",
        f"  Unchanged (ok):       {r.unchanged}",
        f"  No name (GF):         {r.no_name}",
        f"  No GF symbol:         {r.no_symbol}",
        f"  Errors:               {len(r.errors)}",
    ]
    if r.renames:
        lines.append("  Renames (first 30):")
        for rn in r.renames[:30]:
            lines.append(f"    {rn}")
    for e in r.errors[:10]:
        lines.append(f"    {e}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys  # noqa: PLC0415

    from deps import supabase  # noqa: PLC0415  -- importing deps loads backend/.env (GuruFocus creds)

    # Output goes through the on_progress print callback below (flushed, so a
    # backgrounded run shows live in its log). Root logging is left at its
    # default WARNING so `_log.info` doesn't ALSO print every line (double).
    # Silence httpx/httpcore so a per-company "HTTP Request: …" line doesn't
    # drown the progress log.
    for noisy in ("httpx", "httpcore", "hpack"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    only_nameless = "--only-nameless" in sys.argv
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])
    res = backfill_company_names(
        supabase, only_nameless=only_nameless, limit=limit,
        on_progress=lambda d: print(d["message"], flush=True),
    )
    print("\n" + format_summary(res))
