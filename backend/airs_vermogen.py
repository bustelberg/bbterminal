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
from datetime import date, datetime, timedelta, timezone

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
    "errors": [],
}


# Below this, a discovery is treated as FAILED rather than as "AIRS has very few accounts".
# Measured: the live filtered Front-Office list is 44. A login failure or a changed selector
# returns a handful of rows and no exception, and writing that roster would retire the whole
# table in one pass.
_MIN_ROSTER = 10


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
    _record_roster(names)
    return names


def _record_roster(names: list[str]) -> None:
    """Persist WHICH accounts AIRS listed on this pass — the roster `list_accounts` reads.

    ⚠ WITHOUT THIS THE ANSWER IS THROWN AWAY. The discovery already knows the live set; it just
    used it to drive the scrape and forgot it. `airs_performance` cannot recover it: it says what
    a book made, which stays true long after AIRS stops listing the book.

    ⚠ ONE TIMESTAMP FOR THE WHOLE BATCH, so "the live set" is exactly `last_seen_at = max(...)`.
    Stamping each row with its own now() would make that comparison a race against the write.

    ⚠ AN EMPTY OR SUSPICIOUSLY SMALL DISCOVERY IS NOT WRITTEN. A login failure or a changed
    selector returns few rows, not an error, and recording that would retire the entire table on
    the strength of a failed scrape. Better to keep yesterday's roster than to publish a wrong one.
    """
    if len(names) < _MIN_ROSTER:
        _log.warning("[airs_vermogen] discovery returned %d portfolios (< %d) — roster NOT "
                     "updated; keeping the previous one rather than retiring accounts on a "
                     "possibly-failed scrape", len(names), _MIN_ROSTER)
        return
    stamp = datetime.now(timezone.utc).isoformat()
    rows = [{"portefeuille": n, "last_seen_at": stamp} for n in sorted(set(names))]
    try:
        for i in range(0, len(rows), 200):
            supabase.table("airs_account_roster").upsert(
                rows[i:i + 200], on_conflict="portefeuille").execute()
    except Exception as e:  # noqa: BLE001 — the scrape itself must not fail on bookkeeping
        _log.warning("[airs_vermogen] could not record the account roster: %s: %s",
                     type(e).__name__, e)


def _save_holdings(portefeuille: str, as_of: str, holdings) -> int:
    """Replace this portfolio's snapshot for `as_of` with `holdings`. Delete-then-
    insert so a position that dropped out doesn't linger from an earlier run."""
    rows = [
        {
            "portefeuille": portefeuille,
            "as_of_date": as_of,
            "holding_name": h.holding_name,
            # AIRS's own ISIN (`ISIN-code`, switched on 2026-07-23). None on the cash line and on
            # every snapshot older than that — `_airs_holding_isin` falls back to the name route.
            "isin": h.isin,
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


def _save_mutaties(portefeuille: str, van: str, tot: str) -> int:
    """Download and store this account's Mutaties journal for [van, tot] — its dividend income.

    Delete-then-insert over the WHOLE account, not the window: the window is always "this year so
    far", so a narrower re-scan that only deleted its own range would leave last run's rows for the
    days it no longer covers and double-count them. One account, one current journal.

    ⚠ A book with no dividends yet is an EMPTY journal, which is an answer, not a failure. The
    caller treats a raised error as a failure, so a legitimately empty download must return 0.
    """
    from airs_mutaties import parse_mutaties  # noqa: PLC0415
    from airs_scanner import download_mutaties_sync  # noqa: PLC0415

    try:
        raw = download_mutaties_sync(portefeuille, van, tot)
    except RuntimeError as e:
        # `_download_report_sync` raises "Response too small" for BOTH an unvalued/empty report
        # and a dead session. The fleet loop logs it; we do not invent an empty journal, because
        # "no dividends" and "we could not ask" must not look alike.
        raise RuntimeError(f"Mutaties: {e}") from e
    rows = [{
        "portefeuille": portefeuille,
        "boekdatum": m.boekdatum.isoformat() if m.boekdatum else None,
        "grootboek": m.grootboek,
        "fonds": m.fonds,
        "omschrijving": m.omschrijving or None,
        "amount_eur": m.amount_eur,
        "amount_local": m.amount_local,
        "currency": m.currency,
        "fx_rate": m.fx_rate,
    } for m in parse_mutaties(raw) if m.fonds and m.grootboek]
    supabase.table("airs_mutatie").delete().eq("portefeuille", portefeuille).execute()
    for i in range(0, len(rows), 200):
        supabase.table("airs_mutatie").insert(rows[i:i + 200]).execute()
    return len(rows)


def _save_model_weights(portefeuille: str, van: str, tot: str) -> int:
    """Download and store this book's OWN model weights (`rapport_types=MODEL`).

    ⚠ THIS IS WHAT REPLACES THE FIXED↔DYNAMIC PAIRING. The weights are scoped to the dynamic
    portfolio, so there is no second AirSPMS portfolio to guess a partner for — and no
    mis-pairing that files a book's money under another strategy's name.

    Delete-then-insert per account, so a position dropped from the model disappears instead of
    lingering as a weight nothing holds.
    """
    from airs_model import model_total_pct, parse_model  # noqa: PLC0415
    from airs_scanner import download_model_sync  # noqa: PLC0415

    weights = parse_model(download_model_sync(portefeuille, van, tot))
    if not weights:
        return 0
    total = model_total_pct(weights)
    # ⚠ Measured at EXACTLY 100.000 on every book. A partial sheet understates every weight and
    # looks entirely normal, so it is refused rather than stored.
    if not (95.0 <= total <= 105.0):
        raise RuntimeError(
            f"MODEL percentages sum to {total}, not ~100 — refusing to store a partial model")
    rows = [{
        "portefeuille": portefeuille, "fonds": w.fonds, "model_pct": w.model_pct,
        "actual_pct": w.actual_pct, "drift_pct": w.drift_pct, "drift_eur": w.drift_eur,
        "buy": w.buy, "sell": w.sell, "model_value_eur": w.model_value_eur,
    } for w in weights]
    supabase.table("airs_model_weight").delete().eq("portefeuille", portefeuille).execute()
    for i in range(0, len(rows), 200):
        supabase.table("airs_model_weight").insert(rows[i:i + 200]).execute()
    return len(rows)


def run_airs_vermogen_refresh_sync(triggered_by: str = "manual") -> dict:
    """Discover → download → parse → store, for every live portfolio. Serialized
    via `_LOCK` (a second trigger while one runs returns busy). Returns the final
    status dict. Call from a thread — it does blocking Playwright + DB work."""
    if not _LOCK.acquire(blocking=False):
        return {"status": "busy", "message": "An AIRS refresh is already running"}
    try:
        from airs_scanner import download_portfolio_sync  # noqa: PLC0415
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
        rendement_ok = vermogen_ok = holdings_total = mutaties_total = model_total = 0
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
                # Most recent VALUED date, not today (which AirSPMS has not valued yet).
                v_as_of, vmo = _vermogen_most_recent(name, van)
                holdings_total += _save_holdings(name, v_as_of, parse_airs_excel(vmo))
                vermogen_ok += 1
            except Exception as e:
                _STATUS["errors"].append(f"{name} (Vermogensoverzicht): {type(e).__name__}: {e}")
                _log.warning("[airs_vermogen] %s Vermogensoverzicht failed: %s: %s", name, type(e).__name__, e)
            # Mutaties (MUT) → airs_mutatie. Independent of the other two: a book's dividends are
            # worth having even when its valuation is unavailable, and a shared try would lose them.
            try:
                mutaties_total += _save_mutaties(name, van, tot)
            except Exception as e:
                _STATUS["errors"].append(f"{name} (Mutaties): {type(e).__name__}: {e}")
                _log.warning("[airs_vermogen] %s Mutaties failed: %s: %s", name, type(e).__name__, e)
            # MODEL → airs_model_weight. The book's own strategy weights — this is what replaces
            # the fixed↔dynamic pairing, so it runs for every account, paired or not.
            try:
                model_total += _save_model_weights(name, van, tot)
            except Exception as e:
                _STATUS["errors"].append(f"{name} (Model): {type(e).__name__}: {e}")
                _log.warning("[airs_vermogen] %s Model failed: %s: %s", name, type(e).__name__, e)

        # ⚠ THIS JOB DOES NOT TOUCH CRM. It used to also download CRM → Relaties → Alle
        # relaties inline, which is a different report about different objects (relations, not
        # portfolios) and already has its own daily job at 11:00
        # (`airs_crm.run_crm_relaties_refresh_sync`, wired in `scheduler._fire_crm_relaties`).
        # Running it here meant a second scrape of the same export every time anyone refreshed
        # the holdings, and — worse — a CRM failure was appended to THIS job's `errors` and
        # counted in its "N report(s) failed", so a portfolio refresh reported a fault in a
        # report it was never asked to fetch.
        total = len(names)
        any_stored = rendement_ok or vermogen_ok
        _STATUS.update({
            "finished_at": datetime.now(timezone.utc).isoformat(),
            "status": "ok" if any_stored else "error",
            "rendement_stored": rendement_ok,
            "vermogen_stored": vermogen_ok,
            "holdings_rows": holdings_total,
            "mutatie_rows": mutaties_total,
            "model_weight_rows": model_total,
            "message": (
                f"Rendement {rendement_ok}/{total}, Vermogensoverzicht {vermogen_ok}/{total} "
                f"({holdings_total} holdings)"
                + (f"; {len(_STATUS['errors'])} report(s) failed" if _STATUS["errors"] else "")
            ),
        })
        _log.info("[airs_vermogen] %s refresh — %s", triggered_by, _STATUS["message"])
        return dict(_STATUS)
    finally:
        _STATUS["running"] = False
        _LOCK.release()


def _vermogen_most_recent(name: str, van: str) -> tuple[str, bytes]:
    """The Vermogensoverzicht for the most recent AVAILABLE valuation date, and that date.

    ⚠ AirSPMS VALUES END-OF-DAY. So `today` has no Vermogensoverzicht until its valuation runs, and
    a weekend or holiday never gets one — a request for an unvalued `datum_tot` returns an empty
    ~49-byte body (`Response too small`). The Rendement (ATT) report does NOT share this: it returns
    MONTHLY rows regardless of the exact date, which is why a same-day refresh fails on VOLK alone.

    So walk back from today and take the first date that returns a real file. That date IS the
    snapshot's as_of — the holdings are valued as of THEN, not today (matching what the AirSPMS UI
    shows, which also defaults to the last valued date, e.g. Friday's on a Monday).
    """
    from airs_scanner import download_vermogensoverzicht_sync  # noqa: PLC0415

    last_err: Exception | None = None
    for back in range(0, 7):
        tot = (date.today() - timedelta(days=back)).isoformat()
        try:
            return tot, download_vermogensoverzicht_sync(name, van, tot)
        except RuntimeError as e:
            # Unvalued date → empty body / error page. Try the day before. A real auth failure
            # returns the same on EVERY date, exhausts the loop, and is raised below.
            last_err = e
    raise RuntimeError(f"no valued Vermogensoverzicht in the last 7 days ({last_err})")


def refresh_one_portfolio(portefeuille: str) -> dict:
    """Re-scan ONE portfolio's Rendement (ATT) + Vermogensoverzicht (VOLK) and store both — the
    per-row "Refresh" on the overview table.

    Reuses the exact download → parse → save path the full daily scan uses, so a single row's
    refresh and the whole-fleet refresh can never diverge. Serialized against the full scan (and
    other single refreshes) via `_LOCK` — they share ONE AirSPMS session, which must not be driven
    by two threads at once. A few seconds: two downloads (plus a login only if the session lapsed).
    """
    if not _LOCK.acquire(blocking=False):
        return {"status": "busy", "message": "An AIRS refresh is already running", "portefeuille": portefeuille}
    try:
        from airs_scanner import download_portfolio_sync  # noqa: PLC0415
        from portfolio import parse_airs_excel  # noqa: PLC0415
        from routers.airs import _parse_att_excel, _save_performance_to_db  # noqa: PLC0415

        today = date.today()
        van, tot = f"{today.year}-01-01", today.isoformat()
        errors: list[str] = []
        rendement_ok = vermogen_ok = holdings = mutaties = model_weights = 0
        vermogen_as_of = tot
        # Independent — one report failing must not lose the other (same as the fleet loop).
        try:
            att = download_portfolio_sync(portefeuille, van, tot)
            _save_performance_to_db(portefeuille, _parse_att_excel(att))
            rendement_ok = 1
        except Exception as e:  # noqa: BLE001
            errors.append(f"Rendement: {type(e).__name__}: {e}")
            _log.warning("[airs_vermogen] %s single Rendement failed: %s", portefeuille, e)
        try:
            # Most recent VALUED date, not today — see `_vermogen_most_recent`.
            vermogen_as_of, vmo = _vermogen_most_recent(portefeuille, van)
            holdings = _save_holdings(portefeuille, vermogen_as_of, parse_airs_excel(vmo))
            vermogen_ok = 1
        except Exception as e:  # noqa: BLE001
            errors.append(f"Vermogensoverzicht: {type(e).__name__}: {e}")
            _log.warning("[airs_vermogen] %s single Vermogensoverzicht failed: %s", portefeuille, e)
        # Independent of the other two: a book's dividends are worth having even if its valuation
        # failed, and losing them because the price report was unvalued would be silent.
        try:
            mutaties = _save_mutaties(portefeuille, van, tot)
        except Exception as e:  # noqa: BLE001
            errors.append(f"Mutaties: {type(e).__name__}: {e}")
            _log.warning("[airs_vermogen] %s single Mutaties failed: %s", portefeuille, e)
        try:
            model_weights = _save_model_weights(portefeuille, van, tot)
        except Exception as e:  # noqa: BLE001
            errors.append(f"Model: {type(e).__name__}: {e}")
            _log.warning("[airs_vermogen] %s single Model failed: %s", portefeuille, e)
        return {
            "status": "ok" if (rendement_ok or vermogen_ok) else "error",
            "portefeuille": portefeuille,
            "as_of": vermogen_as_of if vermogen_ok else tot,
            "holdings_rows": holdings,
            "mutatie_rows": mutaties,
            "model_weight_rows": model_weights,
            "rendement_stored": bool(rendement_ok),
            "vermogen_stored": bool(vermogen_ok),
            "errors": errors,
        }
    finally:
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
