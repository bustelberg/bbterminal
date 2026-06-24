"""Small system-level endpoints: health check, hello, GuruFocus API usage.

Endpoints:
    GET /api/hello   sanity ping
    GET /api/health  Supabase connectivity probe (used by uptime checks)
    GET /api/items   demo endpoint kept around for the boilerplate page
    GET /api/usage   GuruFocus API call counter for the current month
"""

from __future__ import annotations

import asyncio
import os
from datetime import date

from fastapi import APIRouter, HTTPException, Response

from deps import fetch_in_chunks, supabase
from ingest.api_usage import get_usage
from routers._cache_headers import CACHE_PIPELINE

router = APIRouter(tags=["system"])


@router.get("/api/hello")
def hello():
    return {"message": "Hello from FastAPI + uv!"}


@router.get("/api/health")
def health():
    """Probe Supabase connectivity. Returns {status: ok | error, ...}."""
    try:
        url = os.environ.get("SUPABASE_URL", "NOT SET")
        has_key = "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO"
        resp = supabase.table("company").select("company_id").limit(1).execute()
        return {
            "status": "ok",
            "supabase_url": url,
            "has_service_key": has_key,
            "test_query": "success",
            "rows": len(resp.data or []),
        }
    except Exception as e:
        return {
            "status": "error",
            "supabase_url": os.environ.get("SUPABASE_URL", "NOT SET"),
            "has_service_key": "YES" if os.environ.get("SUPABASE_SERVICE_KEY") else "NO",
            "error": str(e),
        }


@router.get("/api/items")
def get_items():
    try:
        result = supabase.table("items").select("*").execute()
        return {"items": result.data}
    except Exception:
        return {"items": []}


@router.get("/api/usage")
async def api_usage():
    """GuruFocus API usage counter for the current month."""
    return await asyncio.to_thread(get_usage, supabase)


@router.get("/api/data/latest-price-date")
async def latest_price_date(response: Response):
    """Most recent close-price observation across all companies. The
    /backtest page uses this as the default end-date — "test up to
    however current our data is." The `source_code = 'gurufocus'` filter is
    load-bearing: it lets Postgres serve this via the
    `idx_metric_data_source_date` (source_code, target_date) index as a
    backward scan. Without it there's no usable index for
    `metric_code = … ORDER BY target_date DESC`, so prod seq-scans the whole
    `metric_data` table and trips the statement timeout (57014)."""
    response.headers["Cache-Control"] = CACHE_PIPELINE
    def _q() -> dict:
        try:
            resp = (
                supabase.table("metric_data")
                .select("target_date")
                .eq("source_code", "gurufocus")
                .eq("metric_code", "close_price")
                .order("target_date", desc=True)
                .limit(1)
                .execute()
            )
        except Exception as e:
            return {"date": None, "error": f"{type(e).__name__}: {e}"}
        if not resp.data:
            return {"date": None}
        raw = resp.data[0].get("target_date")
        # `target_date` is stored as YYYY-MM-DD; pass through verbatim
        # (slice to 10 chars defensively in case a timestamp ever leaks
        # through).
        return {"date": str(raw)[:10] if raw else None}
    return await asyncio.to_thread(_q)


@router.get("/api/data/price-coverage")
async def price_coverage(response: Response):
    """Freshest + most-stale company by LATEST close-price date — so the
    /schedule month-end refresh can show prices actually moved.

    Reads each company's latest close date from the
    `company_latest_close_price_dates` RPC (the same source the prices phase
    sorts on), then enriches the min/max companies with name / ticker /
    exchange. `newest` = the most recent price held anywhere (should be the last
    trading day right after a refresh); `oldest` = the company whose latest
    price is furthest behind. Both null when no company has prices. Cached (1
    min) since the underlying aggregation isn't cheap."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q() -> dict:
        latest_by_cid: dict[int, str] = {}
        page, offset = 1000, 0
        for _ in range(20):
            try:
                resp = (
                    supabase.rpc("company_latest_close_price_dates", {})
                    .range(offset, offset + page - 1)
                    .execute()
                )
            except Exception as e:
                return {
                    "newest": None, "oldest": None, "priced_companies": 0,
                    "error": f"{type(e).__name__}: {e}",
                }
            batch = resp.data or []
            if not batch:
                break
            for row in batch:
                cid = row.get("company_id")
                d = row.get("latest_target_date")
                if cid is not None and d:
                    latest_by_cid[int(cid)] = str(d)[:10]
            if len(batch) < page:
                break
            offset += page

        if not latest_by_cid:
            return {"newest": None, "oldest": None, "priced_companies": 0}

        # Exclude companies we've marked as not-validly-priced — delisted /
        # acquired (`delisted_at`), out-of-GuruFocus-coverage (`out_of_scope_at`),
        # or illiquid (`illiquid_at`, trades rarely so GuruFocus serves stale
        # prices). Their perpetually-behind "latest close" isn't a valid measure
        # of how fresh our ACTIVE prices are. (Covestro AG was acquired late
        # 2025; Telecom Italia savings shares MIL:TITR are illiquid — both kept
        # surfacing as the "oldest" until marked.)
        excluded: set[int] = set()
        try:
            ex_page, ex_off = 1000, 0
            for _ in range(20):
                ex = (
                    supabase.table("company").select("company_id")
                    .or_("delisted_at.not.is.null,out_of_scope_at.not.is.null,illiquid_at.not.is.null")
                    .range(ex_off, ex_off + ex_page - 1).execute()
                ).data or []
                if not ex:
                    break
                for r in ex:
                    excluded.add(int(r["company_id"]))
                if len(ex) < ex_page:
                    break
                ex_off += ex_page
        except Exception:
            pass  # best-effort — without exclusion the measure is just noisier
        latest_by_cid = {c: d for c, d in latest_by_cid.items() if c not in excluded}
        if not latest_by_cid:
            return {"newest": None, "oldest": None, "priced_companies": 0}

        oldest_cid = min(latest_by_cid, key=lambda c: latest_by_cid[c])
        newest_cid = max(latest_by_cid, key=lambda c: latest_by_cid[c])

        rows = (
            supabase.table("company")
            .select(
                "company_id, company_name, gurufocus_ticker, "
                "gurufocus_exchange:gurufocus_exchange(exchange_code)"
            )
            .in_("company_id", list({oldest_cid, newest_cid}))
            .execute()
        ).data or []
        info: dict[int, dict] = {}
        for r in rows:
            cid = int(r["company_id"])
            info[cid] = {
                "company_id": cid,
                "company_name": r.get("company_name"),
                "ticker": r.get("gurufocus_ticker"),
                "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "date": latest_by_cid[cid],
            }
        return {
            "newest": info.get(newest_cid),
            "oldest": info.get(oldest_cid),
            "priced_companies": len(latest_by_cid),
        }
    return await asyncio.to_thread(_q)


def _latest_metric_dates_for(cids: set[int], metric_code: str) -> dict[int, str]:
    """`{company_id: latest target_date}` for `metric_code`, scoped to `cids`,
    via the `company_latest_metric_dates_for` RPC. Chunked (≤800 ids/call) so
    each call rides the metric/company index and returns under the db-max-rows
    cap."""
    out: dict[int, str] = {}
    cid_list = list(cids)
    for i in range(0, len(cid_list), 800):
        chunk = cid_list[i:i + 800]
        resp = supabase.rpc(
            "company_latest_metric_dates_for",
            {"p_company_ids": chunk, "p_metric_code": metric_code},
        ).execute()
        for row in resp.data or []:
            cid, d = row.get("company_id"), row.get("latest_target_date")
            if cid is not None and d:
                out[int(cid)] = str(d)[:10]
    return out


def _excluded_company_ids() -> set[int]:
    """Companies excluded from freshness measures — delisted / out-of-scope /
    illiquid (their perpetually-behind close isn't a valid freshness signal)."""
    excluded: set[int] = set()
    page, offset = 1000, 0
    try:
        for _ in range(20):
            ex = (
                supabase.table("company").select("company_id")
                .or_("delisted_at.not.is.null,out_of_scope_at.not.is.null,illiquid_at.not.is.null")
                .range(offset, offset + page - 1).execute()
            ).data or []
            if not ex:
                break
            for r in ex:
                excluded.add(int(r["company_id"]))
            if len(ex) < page:
                break
            offset += page
    except Exception:
        pass  # best-effort
    return excluded


@router.get("/api/data/universe-coverage")
async def universe_coverage(response: Response):
    """Per-universe price + volume freshness: for each frozen snapshot and each
    template-managed universe, the min/max LATEST close-price and volume date
    across its active members. Surfaces, on the /schedule month-end refresh, how
    fresh each tradable universe's data is — and which lag (the daily price
    job only touches held names, so between rebalances the rest goes stale).

    Returns `{universes: [{universe_id, label, frozen_from, template_key,
    members, priced/volumed counts, price:{min,max}, volume:{min,max}}]}`,
    ordered by label. Cached 1 min (the RPC scans are not cheap)."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q() -> dict:
        from ingest.phases.planner import _latest_membership_company_ids  # noqa: PLC0415

        # STATIC (frozen) snapshots only — the reproducible, tradable sets. The
        # live template universes (ACWI/Leonteq/LongEquity) are excluded.
        us = (
            supabase.table("universe")
            .select("universe_id, label, frozen_from")
            .not_.is_("frozen_at", "null")
            .execute()
        ).data or []

        members_by_uid: dict[int, set[int]] = {}
        for u in us:
            uid = int(u["universe_id"])
            try:
                members_by_uid[uid] = _latest_membership_company_ids(uid)
            except Exception:
                members_by_uid[uid] = set()
        all_cids: set[int] = set().union(*members_by_uid.values()) if members_by_uid else set()

        close = _latest_metric_dates_for(all_cids, "close_price")
        vol = _latest_metric_dates_for(all_cids, "volume")
        excluded = _excluded_company_ids()

        # Per universe, find the company at the min (most-stale) and max
        # (freshest) latest date for price + volume. Collect those companies to
        # enrich with ticker/exchange/name in one batched lookup.
        def _extremes(members: set[int], dates: dict[int, str]):
            priced = [(c, dates[c]) for c in members if dates.get(c)]
            if not priced:
                return None, None, 0
            lo = min(priced, key=lambda x: x[1])
            hi = max(priced, key=lambda x: x[1])
            return lo, hi, len(priced)

        per_uid: dict[int, dict] = {}
        extreme_cids: set[int] = set()
        for u in us:
            uid = int(u["universe_id"])
            members = members_by_uid.get(uid, set()) - excluded
            if not members:
                continue
            p_lo, p_hi, p_n = _extremes(members, close)
            v_lo, v_hi, v_n = _extremes(members, vol)
            per_uid[uid] = {"members": len(members), "p_lo": p_lo, "p_hi": p_hi,
                            "p_n": p_n, "v_lo": v_lo, "v_hi": v_hi, "v_n": v_n}
            for pair in (p_lo, p_hi, v_lo, v_hi):
                if pair and pair[0] is not None:
                    extreme_cids.add(pair[0])

        info: dict[int, dict] = {}
        for r in fetch_in_chunks(
            list(extreme_cids),
            lambda chunk: supabase.table("company")
            .select("company_id, company_name, gurufocus_ticker, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .in_("company_id", chunk).execute(),
        ):
            info[int(r["company_id"])] = {
                "ticker": r.get("gurufocus_ticker"),
                "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "company_name": r.get("company_name"),
            }

        def _co(pair):
            """A min/max endpoint: the date + the company responsible for it."""
            if not pair or pair[0] is None:
                return None
            cid, dt = pair
            i = info.get(cid, {})
            return {"date": dt, "company_id": cid, "ticker": i.get("ticker"),
                    "exchange": i.get("exchange"), "company_name": i.get("company_name")}

        out: list[dict] = []
        for u in us:
            uid = int(u["universe_id"])
            ex = per_uid.get(uid)
            if not ex:
                continue
            out.append({
                "universe_id": uid,
                "label": u.get("label"),
                "frozen_from": u.get("frozen_from"),
                "members": ex["members"],
                "price": {"min": _co(ex["p_lo"]), "max": _co(ex["p_hi"]), "priced": ex["p_n"]},
                "volume": {"min": _co(ex["v_lo"]), "max": _co(ex["v_hi"]), "priced": ex["v_n"]},
            })
        out.sort(key=lambda x: (x.get("label") or "").lower())
        return {"universes": out}

    return await asyncio.to_thread(_q)


@router.get("/api/data/universe-staleness")
async def universe_staleness(
    response: Response,
    universe_id: int,
    stale_after: int = 3,
):
    """Per-company price/volume freshness for ONE universe — so a manual
    'Refresh' can be VERIFIED: which members are up-to-date and which we
    failed to get recent data for.

    A member is **flagged** when its latest close OR volume is missing, or is
    more than `stale_after` trading days behind the freshest close in the
    universe (the market's last good day). Members marked delisted /
    out-of-scope / illiquid are reported separately as **excluded** — they're
    expected-stale, not refresh failures, and don't set the freshness bar.

    Returns `{universe_id, label, frozen_from, members, reference_date,
    stale_after, counts:{fresh,flagged,excluded}, companies:[…]}`, companies
    sorted worst-first (no-data → most-stale → fresh)."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q() -> dict:
        from ingest.phases.planner import _latest_membership_company_ids  # noqa: PLC0415
        from ingest.staleness import trading_days_between  # noqa: PLC0415

        urow = (
            supabase.table("universe")
            .select("universe_id, label, frozen_from")
            .eq("universe_id", universe_id).limit(1).execute()
        ).data
        if not urow:
            raise HTTPException(404, f"Universe {universe_id} not found")
        u = urow[0]

        members = _latest_membership_company_ids(universe_id)
        base = {
            "universe_id": universe_id, "label": u.get("label"),
            "frozen_from": u.get("frozen_from"), "members": len(members),
            "stale_after": stale_after,
        }
        if not members:
            return {**base, "reference_date": None,
                    "counts": {"fresh": 0, "flagged": 0, "excluded": 0},
                    "companies": []}

        close = _latest_metric_dates_for(members, "close_price")
        vol = _latest_metric_dates_for(members, "volume")

        # Company attributes + price-status markers, one batched lookup.
        info: dict[int, dict] = {}
        for r in fetch_in_chunks(
            list(members),
            lambda chunk: supabase.table("company")
            .select("company_id, company_name, gurufocus_ticker, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code), "
                    "delisted_at, out_of_scope_at, illiquid_at")
            .in_("company_id", chunk).execute(),
        ):
            info[int(r["company_id"])] = r

        def _marker(r: dict) -> str | None:
            if r.get("delisted_at"):
                return "delisted"
            if r.get("out_of_scope_at"):
                return "out_of_scope"
            if r.get("illiquid_at"):
                return "illiquid"
            return None

        # Reference = freshest close among NON-excluded members (so a frozen
        # delisted close can't masquerade as "the market's latest day").
        reference_str = max(
            (close[c] for c in members if close.get(c) and not _marker(info.get(c, {}))),
            default=None,
        )
        ref_date: date | None = None
        if reference_str:
            try:
                ref_date = date.fromisoformat(reference_str[:10])
            except ValueError:
                ref_date = None

        def _behind(dstr: str | None) -> int | None:
            if not dstr or ref_date is None:
                return None
            try:
                return trading_days_between(date.fromisoformat(dstr[:10]), ref_date)
            except ValueError:
                return None

        companies: list[dict] = []
        n_fresh = n_flagged = n_excluded = 0
        for c in members:
            r = info.get(c, {})
            lc, lv = close.get(c), vol.get(c)
            cb, vb = _behind(lc), _behind(lv)
            price_stale = lc is None or (cb is not None and cb > stale_after)
            volume_stale = lv is None or (vb is not None and vb > stale_after)
            marker = _marker(r)
            if marker:
                status, n_excluded = "excluded", n_excluded + 1
            elif price_stale or volume_stale:
                status, n_flagged = "flagged", n_flagged + 1
            else:
                status, n_fresh = "fresh", n_fresh + 1
            companies.append({
                "company_id": c,
                "ticker": r.get("gurufocus_ticker"),
                "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "company_name": r.get("company_name"),
                "latest_close": lc,
                "latest_volume": lv,
                "close_days_behind": cb,
                "volume_days_behind": vb,
                "price_stale": price_stale,
                "volume_stale": volume_stale,
                "marker": marker,
                "status": status,
            })

        # Worst-first within each bucket: no-data (None → treat as most stale),
        # then largest days-behind, so the names needing attention float up.
        bucket = {"flagged": 0, "excluded": 1, "fresh": 2}
        companies.sort(key=lambda x: (
            bucket[x["status"]],
            -(x["close_days_behind"] if x["close_days_behind"] is not None else 10_000),
        ))
        return {**base, "reference_date": reference_str,
                "counts": {"fresh": n_fresh, "flagged": n_flagged, "excluded": n_excluded},
                "companies": companies}

    return await asyncio.to_thread(_q)


# A consecutive-day jump larger than this (in the trailing year) is treated as a
# real data gap — weekends + holiday clusters stay well under it.
_GAP_THRESHOLD_DAYS = 14


@router.get("/api/data/universe-history")
async def universe_history(label: str, response: Response):
    """On-demand depth + gap check for ONE static universe's members: does every
    company have ≥1 year of price/volume history with no missing stretches?

    Returns, per metric (`price`/`volume`): `start` (earliest date across
    members), `covered`/`no_data` counts, `short` (members with <1yr history),
    `gaps` (members with a >14-day hole in the trailing year), and the worst
    offender of each (with its company). Heavier than the polled coverage
    endpoint (full-history scan), so it's its own button-triggered call."""
    response.headers["Cache-Control"] = CACHE_PIPELINE

    def _q() -> dict:
        from datetime import date, timedelta  # noqa: PLC0415

        from ingest.phases.planner import _latest_membership_company_ids  # noqa: PLC0415

        u = (
            supabase.table("universe").select("universe_id, label")
            .eq("label", label).limit(1).execute()
        ).data
        if not u:
            return {"label": label, "error": "universe not found", "members": 0}
        uid = int(u[0]["universe_id"])
        members = _latest_membership_company_ids(uid) - _excluded_company_ids()
        since = (date.today() - timedelta(days=365)).isoformat()
        result: dict = {"label": label, "members": len(members), "since": since}
        if not members:
            return result

        cid_list = list(members)
        # Coverage per company for each metric (chunked to ride the index).
        cov_by_metric: dict[str, dict[int, dict]] = {}
        for metric in ("close_price", "volume"):
            cov: dict[int, dict] = {}
            for i in range(0, len(cid_list), 400):
                resp = supabase.rpc("company_metric_coverage_for", {
                    "p_company_ids": cid_list[i:i + 400],
                    "p_metric_code": metric, "p_since": since,
                }).execute()
                for r in resp.data or []:
                    cov[int(r["company_id"])] = r
            cov_by_metric[metric] = cov

        # Enrich the worst offenders with ticker/exchange/name.
        worst_cids: set[int] = set()
        agg: dict[str, dict] = {}
        for metric, key in (("close_price", "price"), ("volume", "volume")):
            cov = cov_by_metric[metric]
            starts = [r["earliest_target_date"] for r in cov.values() if r.get("earliest_target_date")]
            short = [(c, r) for c, r in cov.items()
                     if (r.get("earliest_target_date") or "9999-99-99") > since]
            gaps = [(c, r) for c, r in cov.items() if (r.get("max_gap_days") or 0) > _GAP_THRESHOLD_DAYS]
            worst_gap = max(gaps, key=lambda x: x[1]["max_gap_days"], default=None)
            worst_short = max(short, key=lambda x: x[1].get("earliest_target_date") or "", default=None)
            if worst_gap:
                worst_cids.add(worst_gap[0])
            if worst_short:
                worst_cids.add(worst_short[0])
            agg[key] = {
                "start": min(starts) if starts else None,
                "covered": len(cov),
                "no_data": len(members) - len(cov),
                "short": len(short),
                "gaps": len(gaps),
                "_worst_gap": worst_gap,
                "_worst_short": worst_short,
            }

        info: dict[int, dict] = {}
        for r in fetch_in_chunks(
            list(worst_cids),
            lambda chunk: supabase.table("company")
            .select("company_id, company_name, gurufocus_ticker, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)")
            .in_("company_id", chunk).execute(),
        ):
            info[int(r["company_id"])] = {
                "ticker": r.get("gurufocus_ticker"),
                "exchange": (r.get("gurufocus_exchange") or {}).get("exchange_code"),
                "company_name": r.get("company_name"),
            }

        def _co(cid: int | None, extra: dict) -> dict | None:
            if cid is None:
                return None
            return {"company_id": cid, **info.get(cid, {}), **extra}

        for key in ("price", "volume"):
            a = agg[key]
            wg, ws = a.pop("_worst_gap"), a.pop("_worst_short")
            a["worst_gap"] = _co(wg[0], {"gap_days": wg[1]["max_gap_days"]}) if wg else None
            a["worst_short"] = _co(ws[0], {"earliest": ws[1].get("earliest_target_date")}) if ws else None
            result[key] = a
        return result

    return await asyncio.to_thread(_q)
