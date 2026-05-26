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


def _load_universe_membership(
    label: str, grouping_field: str = "sector",
) -> dict[str, dict[int, str | None]]:
    """Sync read of `universe_membership` for a label. Paginates past
    Supabase's silent 1000-row default. Returns
    {YYYY-MM: {company_id: <grouping_value> | None}}. Empty dict if label
    not found.

    `grouping_field` selects which column carries the grouping label that
    `top_n_sectors` will bucket by. "sector" is universal; "industry" only
    has values on LEONTEQ memberships (callers MUST guard the request)."""
    if grouping_field not in ("sector", "industry"):
        raise ValueError(f"invalid grouping_field={grouping_field!r}")
    u_resp = supabase.table("universe").select("universe_id").eq("label", label).limit(1).execute()
    if not u_resp.data:
        return {}
    universe_id = u_resp.data[0]["universe_id"]
    rows = []
    offset = 0
    page_size = 1000
    while True:
        resp = supabase.table("universe_membership").select(
            f"target_month, company_id, {grouping_field}"
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
        result[m][r["company_id"]] = r.get(grouping_field)
    return result


def _load_index_universe(
    label: str, grouping_field: str = "sector",
) -> dict[str, dict[int, str | None]]:
    """Same shape as `_load_universe_membership` — index universes are
    stored as regular universes so the read is structurally identical.
    Kept as a separate function so the calling progress messages can read
    naturally (`Loading index universe ...` vs `Loading universe ...`).

    Resolution order: first try `template_key == label` (so 'ACWI' picks
    up the canonical template-managed row no matter what its `label`
    is), then fall back to `label == label` for index universes still on
    the old static-snapshot model (SP500 today).

    Cached: subsequent backtests against the same template/label re-use
    the indexed `{month: {company_id: <grouping_value>}}` dict without a
    700k-row re-fetch, until `UniverseTemplate.refresh()` invalidates the
    cache (or the 60s TTL safety net fires). The cache key includes
    `grouping_field` so a sector-grouped fetch doesn't accidentally serve
    an industry-grouped request (or vice versa).

    Special-case ACWI_LEONTEQ + industry: the ACWI_LEONTEQ template wipes
    sector during the intersection build but doesn't propagate Leonteq's
    industry value -- meaning `universe_membership.industry` is NULL for
    every ACWI_LEONTEQ row. Since every company in ACWI_LEONTEQ is by
    construction also in LEONTEQ, we backfill the industry from the
    LEONTEQ membership for the same (month, company_id). Falls back to
    the company's most-recent-known LEONTEQ industry when the parent
    universe doesn't have a row for that specific month."""
    if grouping_field not in ("sector", "industry"):
        raise ValueError(f"invalid grouping_field={grouping_field!r}")
    from index_universe.templates._cache import full_universe_cache  # noqa: PLC0415

    cache_key = (label, grouping_field)
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
            .select(f"target_month, company_id, {grouping_field}")
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
        result[m][r["company_id"]] = r.get(grouping_field)

    # ACWI_LEONTEQ + industry: ACWI_LEONTEQ rows don't carry industry,
    # so the values above are all None. Backfill from LEONTEQ.
    if grouping_field == "industry" and label == "ACWI_LEONTEQ":
        leonteq_map = _load_index_universe("LEONTEQ", grouping_field="industry")
        # Latest-known industry per company (for months where LEONTEQ
        # doesn't have a matching row). LEONTEQ data is monthly-snapshotted
        # but practically the industry classification is stable.
        latest_industry: dict[int, str] = {}
        for m in sorted(leonteq_map.keys()):
            for cid, ind in leonteq_map[m].items():
                if ind:
                    latest_industry[cid] = ind
        for m, by_cid in result.items():
            leon_month = leonteq_map.get(m, {})
            for cid in list(by_cid.keys()):
                if by_cid[cid] is not None:
                    continue
                # Try same-month LEONTEQ first, then last-known.
                ind = leon_month.get(cid) or latest_industry.get(cid)
                by_cid[cid] = ind

    # Stash for the next backtest. Tuple shape matches the cache
    # contract: `(last_refreshed_at, dict)`.
    full_universe_cache.put(cache_key, (last_refreshed, result))
    return result


def _emit(data: dict) -> str:
    return f"data: {json.dumps(data)}\n\n"


_LEONTEQ_GROUPED_UNIVERSES = ("LEONTEQ", "ACWI_LEONTEQ")


async def load_monthly_eligible_for(
    universe_label: str | None,
    index_universe: str | None,
    grouping_field: str,
    *,
    require_universe: bool = True,
):
    """Decoupled-arg variant of `load_monthly_eligible`: takes the inputs
    directly instead of fishing them off a request. Used by the variants
    sweep so each (universe, grouping) combo can be loaded independently.

    Same yields contract: SSE event strings, then a final
    `("__result__", monthly_eligible_or_None, did_error)` sentinel."""
    if grouping_field not in ("sector", "industry"):
        grouping_field = "sector"

    monthly_eligible: dict[str, dict[int, str | None]] | None = None

    if grouping_field == "industry":
        chosen_universe = universe_label or index_universe
        if chosen_universe not in _LEONTEQ_GROUPED_UNIVERSES:
            yield _emit({
                "type": "error",
                "message": (
                    "grouping='industry' is only available for the Leonteq "
                    "universes (LEONTEQ, ACWI_LEONTEQ). Selected universe "
                    f"is {chosen_universe!r}."
                ),
            })
            yield ("__result__", None, True)
            return

    grouping_label = "industry" if grouping_field == "industry" else "sector"

    if universe_label:
        yield _emit({"type": "progress", "pct": 6, "message": f"Loading universe '{universe_label}'..."})
        monthly_eligible = await asyncio.to_thread(
            _load_universe_membership, universe_label, grouping_field,
        )
        n_months = len(monthly_eligible)
        if n_months == 0:
            yield _emit({"type": "error", "message": f"No universe data for label '{universe_label}'"})
            yield ("__result__", None, True)
            return
        avg_pass = sum(len(v) for v in monthly_eligible.values()) // n_months
        yield _emit({"type": "progress", "pct": 7, "message": f"Universe: {n_months} months, ~{avg_pass} companies/month"})

        total_sec = sum(
            1 for month_map in monthly_eligible.values()
            for s in month_map.values() if s
        )
        if total_sec == 0:
            yield _emit({"type": "error", "message": f"Universe '{universe_label}' has no {grouping_label} data in universe_membership — re-save this universe from its source page so {grouping_label}s are populated."})
            yield ("__result__", None, True)
            return

    if index_universe and monthly_eligible is None:
        yield _emit({"type": "progress", "pct": 6, "message": f"Loading index universe '{index_universe}'..."})
        monthly_eligible = await asyncio.to_thread(
            _load_index_universe, index_universe, grouping_field,
        )
        n_months = len(monthly_eligible)
        if n_months == 0:
            yield _emit({"type": "error", "message": f"No index universe data for '{index_universe}'"})
            yield ("__result__", None, True)
            return
        avg_co = sum(len(v) for v in monthly_eligible.values()) // n_months
        yield _emit({"type": "progress", "pct": 7, "message": f"Index universe: {n_months} months, ~{avg_co} companies/month"})

        total_sec = sum(
            1 for month_map in monthly_eligible.values()
            for s in month_map.values() if s
        )
        if total_sec == 0:
            yield _emit({"type": "error", "message": f"Index universe '{index_universe}' has no {grouping_label} data — re-save this universe from its source page so {grouping_label}s are populated."})
            yield ("__result__", None, True)
            return

    if monthly_eligible is None and require_universe:
        yield _emit({"type": "error", "message": "No universe selected. Sector-based selection requires a universe (or index universe) with stored sector data."})
        yield ("__result__", None, True)
        return

    yield ("__result__", monthly_eligible, False)


async def load_monthly_eligible(req):
    """Back-compat wrapper around `load_monthly_eligible_for` for the
    single-run path that still reads everything off `req`."""
    grouping_field = getattr(req, "grouping", "sector") or "sector"
    require = (req.top_n_sectors or 0) > 0
    async for evt in load_monthly_eligible_for(
        req.universe_label, req.index_universe, grouping_field,
        require_universe=require,
    ):
        yield evt
