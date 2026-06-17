"""Response-shaping helpers for the admin API.

Extracted from `routers.admin` (the HTTP layer). `_build_portfolio_payload`
turns a `current_picks_snapshot` row into the IBKR-ready order shape (used by
`GET /api/admin/schedules/{id}`); `_fetch_latest_snapshots_for` loads the most
recent snapshot per strategy. Both read the DB.
"""
from __future__ import annotations

from deps import fetch_in_chunks, supabase


def _build_portfolio_payload(snapshot_row: dict) -> dict:
    """Convert a `current_picks_snapshot` row into the IBKR-friendly
    response shape — every field a rebalancing script would need:

        ticker            symbol on the home exchange (GuruFocus form)
        exchange          GuruFocus exchange code (NYSE, NASDAQ, OHEL, …)
        country           listing country name (via the exchange)
        currency          ISO 4217 currency code
        side              "long" or "short"
        target_weight     fractional weight in the portfolio (sum ≈ 1.0)
        company_id        DB id, useful for cross-referencing
        company_name      display name
        sector            GICS sector (for verification)
        isin              ISO 6166 security identifier (null if unknown)
        entry_price_local most recent close in the listing currency
        entry_price_eur   …same converted to EUR
        score             the momentum score at selection time

    The IBKR symbol/exchange mapping isn't done here — callers know
    their own broker conventions and we don't want to lock in any
    particular translation. We just hand back the canonical GuruFocus
    fields and let the script adapt.
    """
    raw_holdings = snapshot_row.get("holdings") or []
    cfg = snapshot_row.get("config") or {}

    # Resolve company → exchange + country. The snapshot's holdings don't
    # carry exchange/country directly (only currency); we look them up via
    # the company table joined to gurufocus_exchange → country.
    cids = [int(h["company_id"]) for h in raw_holdings if h.get("company_id") is not None]
    exchange_by_cid: dict[int, str] = {}
    country_by_cid: dict[int, str | None] = {}
    isin_by_cid: dict[int, str | None] = {}
    for row in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("company")
        .select(
            "company_id, isin, gurufocus_exchange:gurufocus_exchange("
            "exchange_code, country:country(country_name))"
        )
        .in_("company_id", chunk)
        .execute(),
    ):
        exch_info = row.get("gurufocus_exchange") or {}
        cid = int(row["company_id"])
        exchange_by_cid[cid] = exch_info.get("exchange_code") or ""
        country_by_cid[cid] = (exch_info.get("country") or {}).get("country_name")
        isin_by_cid[cid] = row.get("isin")

    from ingest.gurufocus_url import gurufocus_url, pad_hkse_ticker  # noqa: PLC0415

    total_weight = 0.0
    holdings_out: list[dict] = []
    for h in raw_holdings:
        cid = int(h.get("company_id")) if h.get("company_id") is not None else None
        weight = float(h.get("weight") or 0.0)
        total_weight += weight
        exchange = exchange_by_cid.get(cid, "") if cid is not None else ""
        gf_ticker = pad_hkse_ticker(h.get("ticker"), exchange)
        holdings_out.append({
            "company_id": cid,
            "ticker": h.get("ticker"),
            "exchange": exchange,
            "country": country_by_cid.get(cid) if cid is not None else None,
            "currency": h.get("currency"),
            "isin": isin_by_cid.get(cid) if cid is not None else None,
            "side": h.get("side") or "long",
            "target_weight": round(weight, 6),
            "company_name": h.get("company_name"),
            "sector": h.get("sector"),
            "entry_price_local": h.get("entry_price_local"),
            "entry_price_eur": h.get("entry_price_eur"),
            "entry_date": h.get("entry_date"),
            "score": h.get("score"),
            "gurufocus_url": gurufocus_url(gf_ticker, exchange),
        })

    return {
        "snapshot_id": snapshot_row.get("snapshot_id"),
        "as_of_date": snapshot_row.get("as_of_date"),
        "latest_price_date": snapshot_row.get("latest_price_date"),
        "triggered_by": snapshot_row.get("triggered_by"),
        "created_at": snapshot_row.get("created_at"),
        "strategy": {
            "name": snapshot_row.get("name"),
            "selection_mode": cfg.get("selection_mode"),
            "strategy_type": cfg.get("strategy_type", "long_only"),
            "index_universe": cfg.get("index_universe"),
            "top_n_sectors": cfg.get("top_n_sectors"),
            "top_n_per_sector": cfg.get("top_n_per_sector"),
            "rebalance_frequency": cfg.get("rebalance_frequency"),
        },
        "holdings": holdings_out,
        "holdings_count": len(holdings_out),
        "total_weight": round(total_weight, 6),
    }


def _enrich_universe_members(member_rows: list[dict]) -> list[dict]:
    """Turn `universe_membership` rows into the same per-company shape the
    admin holdings endpoint returns — so a universe member reads like a
    portfolio holding minus the position-specific fields.

    Each member row carries `company_id`, `universe_ticker`, `sector`,
    `industry`. We resolve the descriptive attributes from `company`
    (joined to `gurufocus_exchange` → `country`) exactly like
    `_build_portfolio_payload`, attach the latest close (native + EUR via
    the same `fetch_latest_from_db` FX source the /fx-rates page uses), and
    emit:

        company_id, ticker, exchange, country, currency, isin,
        company_name, sector, industry,
        latest_close_local, latest_close_eur, latest_close_date,
        fx_rate_per_eur

    The portfolio-only fields (`side`, `target_weight`, `score`,
    `entry_date`) are omitted — a universe member isn't a position — and
    the holding's `entry_price_*` becomes `latest_close_*` (the current
    close, which is what a fresh pick's entry price would be). Sorted by
    (sector, ticker) for stable output.
    """
    cids = [int(m["company_id"]) for m in member_rows if m.get("company_id") is not None]
    if not cids:
        return []

    # Descriptive attributes from the company table — same join as
    # `_build_portfolio_payload`, plus currency + isin.
    comp: dict[int, dict] = {}
    for row in fetch_in_chunks(
        cids,
        lambda chunk: supabase.table("company")
        .select(
            "company_id, company_name, gurufocus_ticker, isin, "
            "gurufocus_exchange:gurufocus_exchange("
            "exchange_code, currency_code, country:country(country_name))"
        )
        .in_("company_id", chunk)
        .execute(),
    ):
        gfx = row.get("gurufocus_exchange") or {}
        comp[int(row["company_id"])] = {
            "company_name": row.get("company_name"),
            "ticker": row.get("gurufocus_ticker"),
            "isin": row.get("isin"),
            "exchange": gfx.get("exchange_code") or "",
            "currency": gfx.get("currency_code"),
            "country": (gfx.get("country") or {}).get("country_name"),
        }

    # Latest native-currency close per company (fast COPY path; per-company
    # PostgREST fallback when SUPABASE_DB_URL is unset).
    latest_close: dict[int, dict] = {}
    try:
        from momentum.data._pg import load_latest_close_prices_via_copy  # noqa: PLC0415
        fast = load_latest_close_prices_via_copy(cids)
        if fast is not None:
            latest_close = fast
        else:
            for cid in cids:
                r = (
                    supabase.table("metric_data")
                    .select("target_date, numeric_value")
                    .eq("metric_code", "close_price")
                    .eq("company_id", cid)
                    .order("target_date", desc=True)
                    .limit(1)
                    .execute()
                )
                if r.data:
                    val = r.data[0].get("numeric_value")
                    latest_close[cid] = {
                        "date": r.data[0]["target_date"],
                        "price": float(val) if val is not None else None,
                    }
    except Exception:
        latest_close = {}

    # Latest {ccy}/EUR rate per currency — same source as the /fx-rates page.
    fx: dict[str, float] = {}
    try:
        from fx_rates import fetch_latest_from_db  # noqa: PLC0415
        for r in fetch_latest_from_db(supabase):
            code, rate = r.get("currency"), r.get("rate")
            if code and rate:
                fx[code] = float(rate)
    except Exception:
        fx = {}

    from ingest.gurufocus_url import gurufocus_url, pad_hkse_ticker  # noqa: PLC0415

    out: list[dict] = []
    for m in member_rows:
        cid = int(m["company_id"]) if m.get("company_id") is not None else None
        c = comp.get(cid, {}) if cid is not None else {}
        cur = c.get("currency")
        lc = latest_close.get(cid, {}) if cid is not None else {}
        local = lc.get("price")
        rate = 1.0 if cur == "EUR" else (fx.get(cur) if cur else None)
        eur = (
            round(local / rate, 4)
            if rate and local is not None and rate > 0
            else None
        )
        ticker = c.get("ticker") or m.get("universe_ticker")
        exchange = c.get("exchange", "")
        # GuruFocus deep-link to the listing (HKSE tickers zero-padded to match
        # the canonical GuruFocus symbol, same as the /companies + index reads).
        gf_ticker = pad_hkse_ticker(ticker, exchange)
        out.append({
            "company_id": cid,
            "ticker": ticker,
            "exchange": exchange,
            "country": c.get("country"),
            "currency": cur,
            "isin": c.get("isin"),
            "company_name": c.get("company_name"),
            "sector": m.get("sector"),
            "industry": m.get("industry"),
            "latest_close_local": local,
            "latest_close_eur": eur,
            "latest_close_date": lc.get("date"),
            "fx_rate_per_eur": rate,
            "gurufocus_url": gurufocus_url(gf_ticker, exchange),
        })
    out.sort(key=lambda x: ((x.get("sector") or "~"), (x.get("ticker") or "").upper()))
    return out


def _fetch_latest_snapshots_for(strategy_ids: list[int]) -> dict[int, dict]:
    """For each strategy id, return its most-recent snapshot row (or omit
    when none exists). Batches in IN_CHUNK_SIZE chunks to dodge
    Cloudflare 502s on Supabase, same convention as elsewhere."""
    latest: dict[int, dict] = {}
    for row in fetch_in_chunks(
        strategy_ids,
        lambda chunk: supabase.table("current_picks_snapshot")
        .select("*")
        .in_("scheduled_strategy_id", chunk)
        .order("created_at", desc=True)
        .execute(),
    ):
        sid = row.get("scheduled_strategy_id")
        if sid is None or sid in latest:
            continue
        latest[int(sid)] = row
    return latest
