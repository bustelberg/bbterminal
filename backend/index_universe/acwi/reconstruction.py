"""Historical ACWI universe reconstruction.

For each month between start and end, a feasible holding is in the
universe iff its earliest matched MSCI ADDED event has an effective
date earlier than that month. Holdings without any matched addition
("grandfathered") are present in every month — they were in the fund
before the announcement archive begins.

This is the survivorship-biased path the backtester uses when an
explicit `index_universe` snapshot isn't provided. The bias is
documented in CLAUDE.md."""
from __future__ import annotations

from .exchange_map import _ISHARES_TO_GF, FEASIBLE_GF_EXCHANGES, gurufocus_ticker_normalized
from .holdings import load_acwi_holdings
from .net_additions import compute_net_additions


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


def feasible_holdings_for_db() -> list[dict]:
    """Return feasible ACWI holdings with DB-facing fields.

    Each dict: {db_exchange, gf_ticker, company_name, sector, symbol, ishares_ticker, ishares_exchange}.
    symbol is "EXCH:TICK" used as the universe_ticker and lookup key.
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
        result.append({
            "db_exchange": db_exch,
            "gf_ticker": gf_tick,
            "company_name": h.get("Name", ""),
            "sector": sector,
            "symbol": f"{db_exch}:{gf_tick}",
            "ishares_ticker": h["Ticker"],
            "ishares_exchange": h["Exchange"],
        })
    return result


def reconstruct_monthly_holdings(start_date: str, end_date: str) -> tuple[dict[str, set[str]], dict]:
    """Reconstruct monthly feasible-universe ACWI holdings.

    For each month M in [start_date, end_date], a feasible holding is included
    iff its earliest matched MSCI addition has effective_date < M. Holdings
    without any matched addition ("grandfathered") are included in every month.

    Returns (monthly, stats) where:
    - monthly: {"YYYY-MM": set of "EXCH:TICKER" symbols (pure ticker for US)}
    - stats: {"feasible_count", "with_addition", "grandfathered", "months"}
    """
    from datetime import date as _date

    holdings, _ = load_acwi_holdings()
    additions = compute_net_additions()

    # Filter to feasible holdings as a LIST of (ishares_ticker, symbol) pairs.
    # The previous dict-keyed-by-ticker approach silently dropped one of any
    # two iShares listings that share a ticker (e.g. MRK = Merck & Co on
    # NYSE AND Merck KGaA on Xetra, NEM = Newmont on NYSE AND Nemetschek
    # on Xetra, TEL = TE Connectivity on NYSE AND Telenor on Oslo). The
    # last-written entry won and the loser never made it into a single
    # month of membership. There are ~38 such ticker collisions in the
    # current iShares ACWI file, accounting for the visible gap between
    # 2005 feasible holdings and ~1967 membership rows per month.
    feasible_holdings: list[tuple[str, str]] = []
    for h in holdings:
        gf = _ISHARES_TO_GF.get(h["Exchange"])
        if gf is None or gf not in FEASIBLE_GF_EXCHANGES:
            continue
        norm = gurufocus_ticker_normalized(h["Ticker"], h["Exchange"])
        if norm is None:
            continue
        db_exch, gf_tick = norm
        symbol = f"{db_exch}:{gf_tick}"
        feasible_holdings.append((h["Ticker"], symbol))

    # Earliest effective_date per iShares ticker (only for tickers that
    # appear in at least one feasible holding). When multiple listings
    # share the ticker, all listings inherit the same earliest date —
    # MSCI announcements identify the security by ticker only, so we
    # can't attribute the addition to one specific listing.
    feasible_tickers = {t for t, _ in feasible_holdings}
    earliest: dict[str, _date] = {}
    for na in additions:
        if not na.get("matched"):
            continue
        t = na.get("matched_ticker")
        eff = na.get("effective_date")
        if not t or not eff or t not in feasible_tickers:
            continue
        d = _parse_effective_date(eff)
        if d is None:
            continue
        if t not in earliest or d < earliest[t]:
            earliest[t] = d

    start = _date.fromisoformat(start_date)
    end = _date.fromisoformat(end_date)
    cursor = _date(start.year, start.month, 1)
    end_m = _date(end.year, end.month, 1)

    monthly: dict[str, set[str]] = {}
    while cursor <= end_m:
        month_key = f"{cursor.year:04d}-{cursor.month:02d}"
        included = {
            symbol for t, symbol in feasible_holdings
            if (d := earliest.get(t)) is None or d < cursor
        }
        monthly[month_key] = included
        cursor = _date(cursor.year + 1, 1, 1) if cursor.month == 12 else _date(cursor.year, cursor.month + 1, 1)

    stats = {
        "feasible_count": len(feasible_holdings),
        "with_addition": sum(1 for t, _ in feasible_holdings if t in earliest),
        "grandfathered": sum(1 for t, _ in feasible_holdings if t not in earliest),
        "months": len(monthly),
    }
    return monthly, stats
