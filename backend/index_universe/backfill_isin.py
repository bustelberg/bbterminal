"""Backfill `company.isin` for every company we can resolve one for.

Two sources, cheapest first:

  1. **Leonteq** (free, no API call) — `leonteq_equity.isin` is already
     stored and mapped to `company_id` by the Leonteq template. Copy it
     straight across.
  2. **GuruFocus** (one summary call per remaining company) — the
     `/stock/{symbol}/summary` endpoint returns the issuer ISIN at
     `summary.company_data.isin` for US and non-US listings alike. We
     already hold a GuruFocus symbol (ticker + exchange) for every
     company, so this covers essentially the whole table.

Idempotent: only companies whose `isin` is NULL are touched, so the run
is safe to repeat (e.g. after a universe refresh adds new companies) and
resumable if interrupted. The GuruFocus pass is rate-limited by the
shared client (`_api_request`, 1.5s/call), so a full ~1k-company run
takes ~25-30 minutes — emit progress as we go.

Usage:
    uv run python -m index_universe.backfill_isin                 # full run
    uv run python -m index_universe.backfill_isin --no-gurufocus  # Leonteq only
    uv run python -m index_universe.backfill_isin --limit 50      # cap GF calls (testing)
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from supabase import Client

from ingest.gurufocus_url import US_EXCHANGE_CODES

_log = logging.getLogger(__name__)

# ISIN = 2-letter country code + 9 alphanumeric + 1 check digit.
_ISIN_RE = re.compile(r"^[A-Z]{2}[A-Z0-9]{9}[0-9]$")


@dataclass
class BackfillResult:
    companies_scanned: int = 0
    already_set: int = 0
    set_from_leonteq: int = 0
    set_from_gurufocus: int = 0
    gurufocus_calls: int = 0
    gurufocus_misses: int = 0  # call made, but no valid ISIN came back
    errors: list[str] = field(default_factory=list)


def _clean_isin(raw: object) -> str | None:
    """Normalize + validate an ISIN. Returns None if it doesn't look like one."""
    if not isinstance(raw, str):
        return None
    s = raw.strip().upper()
    return s if _ISIN_RE.match(s) else None


def _gf_symbol(ticker: str, exchange_code: str | None) -> str | None:
    """GuruFocus API symbol: bare ticker for US, else `EXCH:TICK`.
    Mirrors `ingest.gurufocus_url.gurufocus_url`'s exchange convention."""
    t = (ticker or "").strip()
    if not t:
        return None
    e = (exchange_code or "").strip().upper()
    if not e or e in US_EXCHANGE_CODES:
        return t
    return f"{e}:{t}"


def _load_companies(supabase: Client) -> list[dict]:
    """Every company with its current isin + exchange code. Paginated
    (PostgREST caps at db-max-rows; see project_postgrest_max_rows_trap)."""
    out: list[dict] = []
    offset = 0
    page = 1000
    for _ in range(50):
        resp = (
            supabase.table("company")
            .select("company_id, gurufocus_ticker, isin, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .order("company_id")
            .range(offset, offset + page - 1)
            .execute()
        )
        batch = resp.data or []
        if not batch:
            break
        out.extend(batch)
        if len(batch) < page:
            break
        offset += page
    return out


def _leonteq_isin_map(supabase: Client) -> dict[int, str]:
    """`{company_id: isin}` from the Leonteq scrape (free, already in DB)."""
    rows = (
        supabase.table("leonteq_equity")
        .select("company_id, isin")
        .not_.is_("company_id", "null")
        .not_.is_("isin", "null")
        .execute()
        .data
        or []
    )
    out: dict[int, str] = {}
    for r in rows:
        cid = r.get("company_id")
        isin = _clean_isin(r.get("isin"))
        if cid is not None and isin:
            out.setdefault(int(cid), isin)
    return out


def _gurufocus_isin(symbol: str) -> tuple[str | None, str]:
    """Fetch `summary.company_data.isin` for a GuruFocus symbol.
    Returns (isin_or_None, log_message)."""
    # Lazy import: keeps the GuruFocus client (and its boot-time curl_cffi
    # diagnostics) out of the import graph for callers that only use the
    # Leonteq pass.
    from ingest.earnings._api_client import _api_request, _build_api_url  # noqa: PLC0415

    url = _build_api_url(f"stock/{symbol}/summary")
    res = _api_request(url)
    if res.data is None:
        return None, res.log
    data = res.data
    company_data = ((data.get("summary") or {}).get("company_data") or {}) if isinstance(data, dict) else {}
    return _clean_isin(company_data.get("isin")), res.log


def backfill_isin(
    supabase: Client,
    *,
    use_gurufocus: bool = True,
    limit: int | None = None,
    on_progress=None,
) -> BackfillResult:
    """Set `company.isin` for every company missing one. See module docstring.

    `limit` caps the number of GuruFocus calls (the Leonteq pass is always
    full). `on_progress(msg)` is invoked with human-readable status lines."""
    def emit(msg: str) -> None:
        _log.info(msg)
        if on_progress:
            on_progress(msg)

    result = BackfillResult()
    companies = _load_companies(supabase)
    result.companies_scanned = len(companies)
    leonteq = _leonteq_isin_map(supabase)

    missing = [c for c in companies if not (c.get("isin") or "").strip()]
    result.already_set = len(companies) - len(missing)
    emit(f"{len(companies)} companies; {result.already_set} already have an ISIN, "
         f"{len(missing)} missing.")

    # ── Pass 1: Leonteq (free) ──────────────────────────────────────────
    still_missing: list[dict] = []
    for c in missing:
        cid = int(c["company_id"])
        isin = leonteq.get(cid)
        if isin:
            try:
                supabase.table("company").update({"isin": isin}).eq("company_id", cid).execute()
                result.set_from_leonteq += 1
            except Exception as e:
                result.errors.append(f"cid={cid} leonteq update failed: {type(e).__name__}: {e}")
                still_missing.append(c)
        else:
            still_missing.append(c)
    emit(f"Leonteq pass: set {result.set_from_leonteq} ISINs (free). "
         f"{len(still_missing)} still missing.")

    if not use_gurufocus:
        emit("Skipping GuruFocus pass (--no-gurufocus).")
        return result

    # ── Pass 2: GuruFocus summary (one call each) ───────────────────────
    targets = still_missing if limit is None else still_missing[:limit]
    emit(f"GuruFocus pass: resolving {len(targets)} companies "
         f"(~{round(len(targets) * 1.5 / 60)} min at the 1.5s rate limit)…")
    for i, c in enumerate(targets, 1):
        cid = int(c["company_id"])
        ticker = c.get("gurufocus_ticker") or ""
        exch = ((c.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
        symbol = _gf_symbol(ticker, exch)
        if not symbol:
            continue
        result.gurufocus_calls += 1
        try:
            isin, _msg = _gurufocus_isin(symbol)
        except Exception as e:
            result.errors.append(f"cid={cid} ({symbol}) GF error: {type(e).__name__}: {e}")
            continue
        if isin:
            try:
                supabase.table("company").update({"isin": isin}).eq("company_id", cid).execute()
                result.set_from_gurufocus += 1
            except Exception as e:
                result.errors.append(f"cid={cid} GF update failed: {type(e).__name__}: {e}")
        else:
            result.gurufocus_misses += 1
        if i % 50 == 0 or i == len(targets):
            emit(f"  …{i}/{len(targets)} processed "
                 f"({result.set_from_gurufocus} set, {result.gurufocus_misses} no-ISIN)")

    emit(f"Done. Leonteq={result.set_from_leonteq} GuruFocus={result.set_from_gurufocus} "
         f"misses={result.gurufocus_misses} errors={len(result.errors)}")
    return result


def format_summary(r: BackfillResult) -> str:
    lines = [
        f"Companies scanned:      {r.companies_scanned}",
        f"  Already had ISIN:     {r.already_set}",
        f"  Set from Leonteq:     {r.set_from_leonteq}",
        f"  Set from GuruFocus:   {r.set_from_gurufocus}",
        f"  GuruFocus calls:      {r.gurufocus_calls}",
        f"  GuruFocus no-ISIN:    {r.gurufocus_misses}",
        f"  Errors:               {len(r.errors)}",
    ]
    for e in r.errors[:10]:
        lines.append(f"    {e}")
    return "\n".join(lines)


if __name__ == "__main__":
    import sys  # noqa: PLC0415
    from deps import supabase  # noqa: PLC0415

    logging.basicConfig(level=logging.INFO, format="%(message)s")
    use_gf = "--no-gurufocus" not in sys.argv
    limit = None
    if "--limit" in sys.argv:
        idx = sys.argv.index("--limit")
        if idx + 1 < len(sys.argv):
            limit = int(sys.argv[idx + 1])
    res = backfill_isin(supabase, use_gurufocus=use_gf, limit=limit, on_progress=print)
    print("\n" + format_summary(res))
