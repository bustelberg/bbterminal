"""Historical ACWI universe reconstruction.

For each month between start and end, a feasible holding is in the
universe iff:
  - its earliest matched MSCI ADDED event has effective_date < M
    (or no ADDED event, i.e. "grandfathered" — present from start), AND
  - either it has no post-XLS DELETED event, or that event's
    effective_date >= M.

Holdings without any matched addition are present in every month — they
were in the fund before the announcement archive begins. The DELETED
filter only applies to events post-dating the XLS snapshot (forward
walk): backward semantics stay exactly as before, so a fuzzy DELETED
match against pre-XLS history can't accidentally trim historical
months.

This is the survivorship-biased path the backtester uses when an
explicit `index_universe` snapshot isn't provided. The bias is
documented in CLAUDE.md."""
from __future__ import annotations

from .exchange_map import _ISHARES_TO_GF, FEASIBLE_GF_EXCHANGES, gurufocus_ticker_normalized
from .holdings import load_acwi_holdings
from .net_additions import compute_constituent_changes


def _parse_effective_date(s: str):
    """Parse 'April 10, 2026' style dates. Returns a date or None."""
    from datetime import datetime as _dt
    if not s:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d"):
        try:
            return _dt.strptime(s.strip(), fmt).date()
        except Exception:
            continue
    return None


def _parse_xls_as_of(s: str):
    """Parse the iShares XLS row-0 as-of date ('15-Apr-2026' style)."""
    from datetime import datetime as _dt
    if not s:
        return None
    for fmt in ("%d-%b-%Y", "%Y-%m-%d"):
        try:
            return _dt.strptime(s.strip(), fmt).date()
        except Exception:
            continue
    return None


def feasible_holdings_for_db() -> list[dict]:
    """Return feasible ACWI holdings with DB-facing fields.

    Each dict: {db_exchange, gf_ticker, company_name, sector, symbol,
    ishares_ticker, ishares_exchange, unavailable_reason}.
    symbol is "EXCH:TICK" used as the universe_ticker and lookup key.
    `unavailable_reason` is non-None only when the override marks the
    listing out-of-scope (e.g. Varta on Hamburg) — the company still
    lands in `company` for visibility, but the caller MUST skip
    universe_membership insertion for these rows and stamp
    `out_of_scope_at` + `out_of_scope_reason` on the company row.
    """
    holdings, _ = load_acwi_holdings()
    result: list[dict] = []
    for h in holdings:
        gf = _ISHARES_TO_GF.get(h["Exchange"])
        if gf is None or gf not in FEASIBLE_GF_EXCHANGES:
            continue
        norm = gurufocus_ticker_normalized(h["Ticker"], h["Exchange"])
        if norm is None:
            continue
        db_exch, gf_tick = norm
        sector = (h.get("Sector") or "").strip() or None
        # Lazy import keeps this module's import graph free of
        # exchange_map's helper surface area.
        from .exchange_map import unavailable_reason as _unavailable_reason  # noqa: PLC0415
        result.append({
            "db_exchange": db_exch,
            "gf_ticker": gf_tick,
            "company_name": h.get("Name", ""),
            "sector": sector,
            "symbol": f"{db_exch}:{gf_tick}",
            "ishares_ticker": h["Ticker"],
            "ishares_exchange": h["Exchange"],
            "unavailable_reason": _unavailable_reason(h["Ticker"], h["Exchange"]),
        })
    return result


def reconstruct_monthly_holdings(
    start_date: str,
    end_date: str,
    *,
    extra_holdings: list[dict] | None = None,
) -> tuple[dict[str, set[str]], dict]:
    """Reconstruct monthly feasible-universe ACWI holdings.

    For each month M in [start_date, end_date]:
      - Include a feasible holding iff its earliest matched MSCI ADDED
        event has effective_date < M (grandfathered = no match = always
        included for the addition leg).
      - ALSO exclude any holding whose latest matched MSCI DELETED event
        has effective_date > XLS_as_of AND < M (forward-walk removal).
        The XLS-date guard preserves backward semantics so a fuzzy
        DELETED match against pre-XLS history can't trim months that
        the XLS itself reflects as still-included.

    Returns (monthly, stats) where:
    - monthly: {"YYYY-MM": set of "EXCH:TICKER" symbols (pure ticker for US)}
    - stats: {"feasible_count", "with_addition", "grandfathered", "months",
              "xls_as_of", "forward_removals"}
    """
    from datetime import date as _date

    holdings, xls_as_of_str = load_acwi_holdings()
    changes = compute_constituent_changes()
    xls_as_of_date = _parse_xls_as_of(xls_as_of_str)

    # Filter to feasible holdings as a LIST of (ishares_ticker, symbol) pairs.
    # The previous dict-keyed-by-ticker approach silently dropped one of any
    # two iShares listings that share a ticker (e.g. MRK = Merck & Co on
    # NYSE AND Merck KGaA on Xetra, NEM = Newmont on NYSE AND Nemetschek
    # on Xetra, TEL = TE Connectivity on NYSE AND Telenor on Oslo). The
    # last-written entry won and the loser never made it into a single
    # month of membership. There are ~38 such ticker collisions in the
    # current iShares ACWI file, accounting for the visible gap between
    # 2005 feasible holdings and ~1967 membership rows per month.
    # Lazy import — keeps the top-of-file import set focused on the
    # core reconstruction surface.
    from .exchange_map import unavailable_reason as _unavailable_reason  # noqa: PLC0415

    feasible_holdings: list[tuple[str, str]] = []
    for h in holdings:
        gf = _ISHARES_TO_GF.get(h["Exchange"])
        if gf is None or gf not in FEASIBLE_GF_EXCHANGES:
            continue
        norm = gurufocus_ticker_normalized(h["Ticker"], h["Exchange"])
        if norm is None:
            continue
        # Out-of-scope listings: the company lands in `company` for
        # /companies visibility but must NOT appear in any monthly
        # membership — backtests would otherwise include a ticker the
        # price phase deliberately skipped, producing missing-data
        # warnings every run.
        if _unavailable_reason(h["Ticker"], h["Exchange"]) is not None:
            continue
        db_exch, gf_tick = norm
        symbol = f"{db_exch}:{gf_tick}"
        feasible_holdings.append((h["Ticker"], symbol))

    # Per iShares ticker: earliest ADDED date and latest DELETED date.
    # When multiple listings share the ticker, all listings inherit the
    # same dates — MSCI announcements identify securities by ticker only,
    # so we can't attribute an action to one specific listing.
    feasible_tickers = {t for t, _ in feasible_holdings}
    earliest_added: dict[str, _date] = {}
    latest_deleted: dict[str, _date] = {}
    for ch in changes:
        if not ch.get("matched"):
            continue
        t = ch.get("matched_ticker")
        eff = ch.get("effective_date")
        action = ch.get("action")
        if not t or not eff or t not in feasible_tickers:
            continue
        d = _parse_effective_date(eff)
        if d is None:
            continue
        if action == "ADDED":
            if t not in earliest_added or d < earliest_added[t]:
                earliest_added[t] = d
        elif action == "DELETED":
            if t not in latest_deleted or d > latest_deleted[t]:
                latest_deleted[t] = d

    # Inject forward-walked post-XLS additions. Each carries its own
    # synthetic ticker key (the MSCI announcement href) so it never
    # collides with iShares tickers. Treated like any other holding
    # from here on — included from its eff_date forward.
    extra_count = 0
    for eh in (extra_holdings or []):
        synth_t = eh.get("synthetic_ticker")
        symbol = eh.get("symbol")
        eff_iso = eh.get("eff_date")
        if not synth_t or not symbol or not eff_iso:
            continue
        try:
            eff_d = _date.fromisoformat(eff_iso)
        except Exception:
            continue
        feasible_holdings.append((synth_t, symbol))
        earliest_added[synth_t] = eff_d
        extra_count += 1

    start = _date.fromisoformat(start_date)
    end = _date.fromisoformat(end_date)
    cursor = _date(start.year, start.month, 1)
    end_m = _date(end.year, end.month, 1)

    monthly: dict[str, set[str]] = {}
    while cursor <= end_m:
        month_key = f"{cursor.year:04d}-{cursor.month:02d}"
        included: set[str] = set()
        for t, symbol in feasible_holdings:
            add_d = earliest_added.get(t)
            if add_d is not None and add_d >= cursor:
                continue  # not yet added as of this month
            del_d = latest_deleted.get(t)
            if (
                del_d is not None
                and xls_as_of_date is not None
                and del_d > xls_as_of_date
                and del_d < cursor
            ):
                continue  # post-XLS deletion already in effect
            included.add(symbol)
        monthly[month_key] = included
        cursor = _date(cursor.year + 1, 1, 1) if cursor.month == 12 else _date(cursor.year, cursor.month + 1, 1)

    forward_removals = sum(
        1 for t, _ in feasible_holdings
        if (d := latest_deleted.get(t)) is not None
        and xls_as_of_date is not None
        and d > xls_as_of_date
    )

    stats = {
        "feasible_count": len(feasible_holdings),
        "with_addition": sum(1 for t, _ in feasible_holdings if t in earliest_added),
        "grandfathered": sum(1 for t, _ in feasible_holdings if t not in earliest_added),
        "months": len(monthly),
        "xls_as_of": xls_as_of_str,
        "forward_removals": forward_removals,
        "forward_additions": extra_count,
    }
    return monthly, stats
