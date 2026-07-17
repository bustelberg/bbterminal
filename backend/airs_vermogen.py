"""Daily AIRS refresh — both reports, both tables.

Re-discovers the CURRENT live portfolio list from AirSPMS each run (the list
changes day-to-day), then for EVERY portfolio downloads + parses + stores both:
  - Rendement (ATT)             → `airs_performance`  (upsert per periode)
  - Vermogensoverzicht (VOLK)   → `airs_holding`      (replace per as-of date)
Both are deduped, so re-running adds no duplicate rows. Another site can read
these two tables straight from Supabase; this job keeps them fresh each day.

Runs as an in-process scheduled job (working days 11:00 Amsterdam — see
scheduler.py) and on-demand from the /airs-portfolio "Refresh now" button.
Reuses the existing scraper + parsers (`scan_portfolios_sync`,
`download_portfolio_sync`/`download_vermogensoverzicht_sync`, `parse_airs_excel`,
and `routers.airs._parse_att_excel`/`_save_performance_to_db`).
"""
from __future__ import annotations

import logging
import threading
from datetime import date, datetime, timezone

from deps import supabase

_log = logging.getLogger(__name__)
_LOCK = threading.Lock()

# Latest in-process run status. The persistent "last successful refresh" is the
# freshest snapshot date in airs_holding (surfaced by get_status()), so the
# status survives a restart even though this dict doesn't.
_STATUS: dict = {
    "running": False,
    "started_at": None,
    "finished_at": None,
    "status": None,            # None | running | ok | error
    "message": None,
    "triggered_by": None,
    "portfolios_found": 0,
    "rendement_stored": 0,     # portfolios whose Rendement (ATT) was stored
    "vermogen_stored": 0,      # portfolios whose Vermogensoverzicht (VOLK) was stored
    "holdings_rows": 0,        # total holding rows stored
    "crm_stored": None,        # True/False — CRM "Alle relaties" export stored this run
    "crm_bytes": 0,            # size of the stored raw CRM export
    "crm_rows": 0,             # relations parsed into airs_crm_relatie this run
    "errors": [],
}


def _discover_portfolios() -> list[str]:
    """Current live AirSPMS portfolio names, scraped fresh (Playwright)."""
    from airs_scanner import scan_portfolios_sync  # noqa: PLC0415

    captured: list[dict] = []

    def _sink(msg_type: str, **kw):
        if msg_type == "portfolios":
            captured.extend(kw.get("data") or [])
        elif msg_type == "error":
            raise RuntimeError(kw.get("message") or "scan error")

    result = scan_portfolios_sync(_sink)
    rows = result if result else captured
    names: list[str] = []
    for r in rows:
        n = (r.get("portefeuille") or "").strip()
        if n:
            names.append(n)
    return names


def _save_holdings(portefeuille: str, as_of: str, holdings) -> int:
    """Replace this portfolio's snapshot for `as_of` with `holdings`. Delete-then-
    insert so a position that dropped out doesn't linger from an earlier run."""
    rows = [
        {
            "portefeuille": portefeuille,
            "as_of_date": as_of,
            "holding_name": h.holding_name,
            "quantity": h.quantity,
            "currency": h.currency,
            "weight": h.weight,
            "start_value_eur": h.start_value_eur,
            "current_value_eur": h.current_value_eur,
            "ytd_return_eur": h.ytd_return_eur,
            "ytd_return_pct": h.ytd_return_pct,
            "ytd_return_local_pct": h.ytd_return_local_pct,
            # AIRS's own columns, as reported. Stored beside ours rather than instead of
            # them: two statements of the same quantity are the cross-check.
            "cost_basis_local": h.cost_basis_local,
            "current_price_local": h.current_price_local,
            "airs_weight": h.airs_weight,
            "fund_result_eur": h.fund_result_eur,
            "fx_result_eur": h.fx_result_eur,
            "airs_result_pct": h.airs_result_pct,
        }
        for h in holdings
        if h.holding_name
    ]
    if not rows:
        return 0
    (
        supabase.table("airs_holding")
        .delete().eq("portefeuille", portefeuille).eq("as_of_date", as_of).execute()
    )
    for i in range(0, len(rows), 200):
        supabase.table("airs_holding").insert(rows[i:i + 200]).execute()
    return len(rows)


def run_airs_vermogen_refresh_sync(triggered_by: str = "manual") -> dict:
    """Discover → download → parse → store, for every live portfolio. Serialized
    via `_LOCK` (a second trigger while one runs returns busy). Returns the final
    status dict. Call from a thread — it does blocking Playwright + DB work."""
    if not _LOCK.acquire(blocking=False):
        return {"status": "busy", "message": "An AIRS refresh is already running"}
    try:
        from airs_scanner import (  # noqa: PLC0415
            download_portfolio_sync, download_vermogensoverzicht_sync,
        )
        from portfolio import parse_airs_excel  # noqa: PLC0415
        from routers.airs import _parse_att_excel, _save_performance_to_db  # noqa: PLC0415

        today = date.today()
        van, tot = f"{today.year}-01-01", today.isoformat()
        _STATUS.update({
            "running": True,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "finished_at": None,
            "status": "running",
            "message": "Discovering active portfolios…",
            "triggered_by": triggered_by,
            "portfolios_found": 0,
            "rendement_stored": 0,
            "vermogen_stored": 0,
            "holdings_rows": 0,
            "crm_stored": None,
            "crm_bytes": 0,
            "crm_rows": 0,
            "errors": [],
        })

        try:
            names = _discover_portfolios()
        except Exception as e:
            _STATUS.update({
                "status": "error",
                "message": f"Portfolio discovery failed: {type(e).__name__}: {e}",
                "finished_at": datetime.now(timezone.utc).isoformat(),
            })
            _log.warning("[airs_vermogen] discovery failed: %s: %s", type(e).__name__, e)
            return dict(_STATUS)

        _STATUS["portfolios_found"] = len(names)
        rendement_ok = vermogen_ok = holdings_total = 0
        for i, name in enumerate(names, 1):
            _STATUS["message"] = f"{i}/{len(names)}: {name}…"
            # Rendement (ATT) → airs_performance. Independent of the holdings
            # fetch so one report failing doesn't lose the other.
            try:
                att = download_portfolio_sync(name, van, tot)
                _save_performance_to_db(name, _parse_att_excel(att))
                rendement_ok += 1
            except Exception as e:
                _STATUS["errors"].append(f"{name} (Rendement): {type(e).__name__}: {e}")
                _log.warning("[airs_vermogen] %s Rendement failed: %s: %s", name, type(e).__name__, e)
            # Vermogensoverzicht (VOLK) → airs_holding.
            try:
                vmo = download_vermogensoverzicht_sync(name, van, tot)
                holdings_total += _save_holdings(name, tot, parse_airs_excel(vmo))
                vermogen_ok += 1
            except Exception as e:
                _STATUS["errors"].append(f"{name} (Vermogensoverzicht): {type(e).__name__}: {e}")
                _log.warning("[airs_vermogen] %s Vermogensoverzicht failed: %s: %s", name, type(e).__name__, e)

        # CRM → Relaties → Alle relaties: one global export, OVERWRITING
        # airs_crm_relatie with the latest snapshot (shared with the dedicated
        # 11:00 daily CRM job — see airs_crm.run_crm_relaties_refresh_sync).
        _STATUS["message"] = "Downloading CRM Alle relaties…"
        crm_bytes = 0
        crm_rows = 0
        crm_ok = False
        try:
            from airs_crm import run_crm_relaties_refresh_sync  # noqa: PLC0415

            res = run_crm_relaties_refresh_sync()
            crm_rows, crm_bytes, crm_ok = res["rows"], res["bytes"], True
        except Exception as e:
            _STATUS["errors"].append(f"CRM relaties: {type(e).__name__}: {e}")
            _log.warning("[airs_vermogen] CRM relaties failed: %s: %s", type(e).__name__, e)

        total = len(names)
        any_stored = rendement_ok or vermogen_ok or crm_ok
        _STATUS.update({
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": "ok" if any_stored else "error",
            "rendement_stored": rendement_ok,
            "vermogen_stored": vermogen_ok,
            "holdings_rows": holdings_total,
            "crm_stored": crm_ok,
            "crm_bytes": crm_bytes,
            "crm_rows": crm_rows,
            "message": (
                f"Rendement {rendement_ok}/{total}, Vermogensoverzicht {vermogen_ok}/{total} "
                f"({holdings_total} holdings); CRM relaties "
                + (f"{crm_rows} relations ({crm_bytes // 1024} KB)" if crm_ok else "failed")
                + (f"; {len(_STATUS['errors'])} report(s) failed" if _STATUS["errors"] else "")
            ),
        })
        _log.info("[airs_vermogen] %s refresh — %s", triggered_by, _STATUS["message"])
        return dict(_STATUS)
    finally:
        _STATUS["running"] = False
        _LOCK.release()


def get_status() -> dict:
    """Current status + the persistent freshest snapshot: its date, distinct
    portfolios, and total holding rows."""
    latest_date = None
    portfolios = 0
    holdings = 0
    try:
        resp = (
            supabase.table("airs_holding")
            .select("as_of_date").order("as_of_date", desc=True).limit(1).execute()
        )
        if resp.data:
            latest_date = resp.data[0]["as_of_date"]
            # Total holding rows at that date.
            cnt = (
                supabase.table("airs_holding")
                .select("id", count="exact")
                .eq("as_of_date", latest_date).limit(0).execute()
            )
            holdings = getattr(cnt, "count", 0) or 0
            # Distinct portfolios — paginate the portefeuille column + dedupe
            # (PostgREST has no DISTINCT count; the row set at one date is small).
            seen: set[str] = set()
            offset, page = 0, 1000
            for _ in range(20):
                rows = (
                    supabase.table("airs_holding")
                    .select("portefeuille").eq("as_of_date", latest_date)
                    .range(offset, offset + page - 1).execute()
                ).data or []
                if not rows:
                    break
                seen.update(r["portefeuille"] for r in rows)
                if len(rows) < page:
                    break
                offset += page
            portfolios = len(seen)
    except Exception:
        pass
    return {**_STATUS, "latest_snapshot_date": latest_date,
            "latest_snapshot_portfolios": portfolios,
            "latest_snapshot_holdings": holdings}
