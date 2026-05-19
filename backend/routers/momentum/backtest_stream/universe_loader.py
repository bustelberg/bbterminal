"""Universe-label and index-universe membership loaders.

Both `universe_label` and `index_universe` resolve to a row in `universe`
and a list of per-month memberships in `universe_membership`; the only
difference is which one is read first. The async-generator wrapper
yields the same progress + error events the inline code used to emit
so the SSE stream stays byte-identical."""
from __future__ import annotations

import asyncio
import json

from deps import supabase


def _load_universe_membership(label: str) -> dict[str, dict[int, str | None]]:
    """Sync read of `universe_membership` for a label. Paginates past
    Supabase's silent 1000-row default. Returns
    {YYYY-MM: {company_id: sector | None}}. Empty dict if label not found."""
    u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
    if not u_resp.data:
        return {}
    universe_id = u_resp.data[0]["universe_id"]
    rows = []
    offset = 0
    page_size = 1000
    while True:
        resp = supabase.table("universe_membership").select(
            "target_month, company_id, sector"
        ).eq("universe_id", universe_id).order(
            "target_month"
        ).order("company_id").range(offset, offset + page_size - 1).execute()
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    result: dict[str, dict[int, str | None]] = {}
    for r in rows:
        # Normalize to "YYYY-MM" — the backtest loop keys on
        # month_date.isoformat()[:7] so any stored "YYYY-MM-DD"
        # value (e.g. from an older longequity_cumulative build)
        # would otherwise never match.
        m = (r.get("target_month") or "")[:7]
        if not m:
            continue
        if m not in result:
            result[m] = {}
        result[m][r["company_id"]] = r.get("sector")
    return result


def _load_index_universe(label: str) -> dict[str, dict[int, str | None]]:
    """Same shape as `_load_universe_membership` — index universes are
    stored as regular universes so the read is structurally identical.
    Kept as a separate function so the calling progress messages can read
    naturally (`Loading index universe ...` vs `Loading universe ...`).

    Resolution order: first try `template_key == label` (so 'ACWI' picks
    up the canonical template-managed row no matter what its `label`
    is), then fall back to `label == label` for index universes still on
    the old static-snapshot model (SP500 today).

    Cached: subsequent backtests against the same template/label re-use
    the indexed `{month: {company_id: sector}}` dict without a 700k-row
    re-fetch, until `UniverseTemplate.refresh()` invalidates the cache
    (or the 60s TTL safety net fires). For a ~290-month ACWI universe
    this turns repeat backtest startup from ~2s into ~5ms."""
    from index_universe.templates._cache import full_universe_cache  # noqa: PLC0415

    cache_key = (label,)
    cached = full_universe_cache.get(cache_key)
    if cached is not None:
        # cached is (last_refreshed_at, dict). Validate against the DB's
        # current timestamp to catch a refresh from another process.
        cached_ts, cached_dict = cached
        try:
            check = (
                supabase.table("universe")
                .select("last_refreshed_at, universe_id")
                .or_(f"template_key.eq.{label},label.eq.{label}")
                .limit(1)
                .execute()
            )
            current_ts = (check.data or [{}])[0].get("last_refreshed_at")
        except Exception:
            current_ts = cached_ts  # On error, prefer stale to a hard fail.
        if current_ts == cached_ts:
            return cached_dict

    u_resp = (
        supabase.table("universe")
        .select("universe_id, last_refreshed_at")
        .eq("template_key", label)
        .limit(1)
        .execute()
    )
    if not u_resp.data:
        u_resp = (
            supabase.table("universe")
            .select("universe_id, last_refreshed_at")
            .eq("label", label)
            .limit(1)
            .execute()
        )
    if not u_resp.data:
        return {}
    universe_id = u_resp.data[0]["universe_id"]
    last_refreshed = u_resp.data[0].get("last_refreshed_at")
    rows: list[dict] = []
    offset = 0
    page_size = 1000
    while True:
        resp = (
            supabase.table("universe_membership")
            .select("target_month, company_id, sector")
            .eq("universe_id", universe_id)
            .order("target_month")
            .range(offset, offset + page_size - 1)
            .execute()
        )
        batch = resp.data or []
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    result: dict[str, dict[int, str | None]] = {}
    for r in rows:
        m = (r.get("target_month") or "")[:7]
        if not m:
            continue
        if m not in result:
            result[m] = {}
        result[m][r["company_id"]] = r.get("sector")

    # Stash for the next backtest. Tuple shape matches the cache
    # contract: `(last_refreshed_at, dict)`.
    full_universe_cache.put(cache_key, (last_refreshed, result))
    return result


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


async def load_monthly_eligible(req):
    """Async generator: resolves `req.universe_label` / `req.index_universe`
    into `monthly_eligible` and yields SSE progress / error events along
    the way.

    Yields:
        SSE event strings.
    Final yield (always): a sentinel `("__result__", monthly_eligible_or_None, did_error)` tuple
        so the orchestrator can pick up the loaded value without a second
        async function.
    """
    monthly_eligible: dict[str, dict[int, str | None]] | None = None

    if req.universe_label:
        yield _emit({"type": "progress", "pct": 6, "message": f"Loading universe '{req.universe_label}'..."})
        monthly_eligible = await asyncio.to_thread(_load_universe_membership, req.universe_label)
        n_months = len(monthly_eligible)
        if n_months == 0:
            yield _emit({"type": "error", "message": f"No universe data for label '{req.universe_label}'"})
            yield ("__result__", None, True)
            return
        avg_pass = sum(len(v) for v in monthly_eligible.values()) // n_months
        yield _emit({"type": "progress", "pct": 7, "message": f"Universe: {n_months} months, ~{avg_pass} companies/month"})

        # Diagnose missing sector data: if no membership row has a sector
        # value, sector-based selection will silently pick zero companies.
        # Fail loudly so the user knows to re-save the universe.
        total_sec = sum(
            1 for month_map in monthly_eligible.values()
            for s in month_map.values() if s
        )
        if total_sec == 0:
            yield _emit({"type": "error", "message": f"Universe '{req.universe_label}' has no sector data in universe_membership — re-save this universe from its source page so sectors are populated."})
            yield ("__result__", None, True)
            return

    if req.index_universe and monthly_eligible is None:
        yield _emit({"type": "progress", "pct": 6, "message": f"Loading index universe '{req.index_universe}'..."})
        monthly_eligible = await asyncio.to_thread(_load_index_universe, req.index_universe)
        n_months = len(monthly_eligible)
        if n_months == 0:
            yield _emit({"type": "error", "message": f"No index universe data for '{req.index_universe}'"})
            yield ("__result__", None, True)
            return
        avg_co = sum(len(v) for v in monthly_eligible.values()) // n_months
        yield _emit({"type": "progress", "pct": 7, "message": f"Index universe: {n_months} months, ~{avg_co} companies/month"})

        total_sec = sum(
            1 for month_map in monthly_eligible.values()
            for s in month_map.values() if s
        )
        if total_sec == 0:
            yield _emit({"type": "error", "message": f"Index universe '{req.index_universe}' has no sector data — re-save this universe from its source page so sectors are populated."})
            yield ("__result__", None, True)
            return

    # Also fail cleanly when no universe was selected at all — the
    # scoring pipeline requires per-company sectors, and `load_universe`
    # leaves them all None in that fallback.
    if monthly_eligible is None and (req.top_n_sectors or 0) > 0:
        yield _emit({"type": "error", "message": "No universe selected. Sector-based selection requires a universe (or index universe) with stored sector data."})
        yield ("__result__", None, True)
        return

    yield ("__result__", monthly_eligible, False)
