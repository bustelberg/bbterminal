"""A cap-weighted index built in the ASSET world — yfinance prices, joined by ISIN.

WHY A SECOND PATH AT ALL (`_benchmark_index` already rebuilds the S&P)
    That one prices off GuruFocus (`metric_data`, the `company` universe), and GuruFocus SELLS US
    A SUBSCRIPTION WITH HOLES IN IT: no UK, no India, no Ireland, no Australia/NZ, no Africa, no
    LatAm. For the S&P 500 that is invisible — it is a US index. For ACWI it is disqualifying:

        LSE (UK)          72 ACWI members    GuruFocus prices   0
        NSE (India)      160 ACWI members    GuruFocus prices   0
        Australia, Brazil, South Africa, Ireland, Chile...

    ~7.8% of ACWI's published weight sits in countries GuruFocus will never price, and a
    reconstruction that renormalises over the other 92% does not lose that weight — it silently
    redistributes it into everything else. That is a bias, not noise, and no amount of care
    inside the maths removes it.

    yfinance has no such holes. It prices LSE, NSE, ASX, B3. So the fix is not better arithmetic,
    it is a better source — and we already hold those prices in `asset_price`.

THE BRIDGE IS THE ISIN, AND IT IS A JOIN, NOT A COLUMN
    `universe_membership.company_id -> company.isin -> asset_execution.isin`. A membership FLAG
    on `asset_execution` was the obvious alternative and it is a trap: the ACWI universe is
    reconstructed on a schedule, so the flag would have to be re-synced on every refresh, and the
    day it drifts the benchmark is quietly wrong with no error anywhere. Same rule the holdings
    count already follows — *the count is a VIEW, never a column*. The join has no drift because
    it has nothing to keep in sync.

⚠ THE WEIGHTING MATHS IS NOT COPIED — IT IS REUSED (`_benchmark_index._window_rows`).
    Start-of-window cap weights (roll the cap back on the price move; weighting by TODAY's cap is
    look-ahead bias and turned +9.10% into +21.70%), split-adjustment, per-date FX. A second copy
    of that loop is a second place for the bias to grow back. This module's job is only to supply
    the same shape of `members` and `closes` from a different source.

⚠ STILL NOT FLOAT-ADJUSTED. `market_cap_eur` is a FULL market cap, and MSCI weights ACWI on free
    float. That over-weights state- and family-held names (mostly EM and Asia) whichever price
    source we use — it is a property of the weight, not of the price. iShares' own file carries
    the float weights; using them is the next step, not this one.
"""
from __future__ import annotations

import asyncio

from deps import IN_CHUNK_SIZE, supabase
from routers._airs_portfolio_perf import _closes as _asset_closes
from routers._benchmark_index import _fx_to_eur, _window_rows


def _universe_company_ids(label: str) -> list[int]:
    uni = (supabase.table("universe").select("universe_id")
           .eq("label", label).limit(1).execute().data or [])
    if not uni:
        return []
    mem = (supabase.table("universe_membership").select("company_id")
           .eq("universe_id", uni[0]["universe_id"]).execute().data or [])
    return sorted({m["company_id"] for m in mem})


def members(label: str) -> tuple[list[dict], dict]:
    """The index's constituents, priced from the ASSET world. Returns (members, coverage).

    Shaped for `_benchmark_index._window_rows`, which keys prices by `company_id` — here that
    slot carries the `analysis_id`, because the price series is `asset_price`, not `metric_data`.
    The name is the loop's, not ours; what matters is that ONE loop does the weighting.

    ⚠ ONE COMPANY, ONE ROW. Yahoo, like GuruFocus, reports the FULL company market cap on EVERY
    share class — Alphabet is GOOGL *and* GOOG, each carrying the whole cap, so a naive sum counts
    it twice (11.3% of the S&P's weight, fictional). Deduped on the COMPANY name, keeping the
    largest cap, exactly as `_benchmark_index._members` does.
    """
    ids = _universe_company_ids(label)
    if not ids:
        return [], {"universe_members": 0, "priced": 0, "covered_pct": None}

    companies: list[dict] = []
    for i in range(0, len(ids), IN_CHUNK_SIZE):
        companies += (supabase.table("company")
                      .select("company_id,company_name,isin")
                      .in_("company_id", ids[i:i + IN_CHUNK_SIZE])
                      .is_("delisted_at", "null").is_("out_of_scope_at", "null")
                      .execute().data or [])

    isins = sorted({c["isin"] for c in companies if c.get("isin")})
    grid: dict[str, dict] = {}
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        for r in (supabase.table("asset_grid")
                  .select("isin,analysis_id,yahoo_symbol,currency,market_cap_eur,status,bars")
                  .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or []):
            if r.get("status") == "ok" and r.get("analysis_id") and (r.get("bars") or 0) > 0:
                grid[r["isin"]] = r

    by_name: dict[str, dict] = {}
    for c in companies:
        g = grid.get(c.get("isin") or "")
        cap = float((g or {}).get("market_cap_eur") or 0)
        if not g or cap <= 0:
            continue                      # not in the asset grid, or no cap to weight it by
        key = (c.get("company_name") or "").strip().lower()
        prev = by_name.get(key)
        if prev is None or cap > float(prev["market_cap_eur"]):
            by_name[key] = {
                # `_window_rows` looks prices up under this key — here it is the analysis_id.
                "company_id": g["analysis_id"],
                "company_name": c.get("company_name"),
                "gurufocus_ticker": g.get("yahoo_symbol"),   # the loop's field name; a yf symbol
                "isin": c["isin"],
                "currency": g.get("currency"),
                "market_cap_eur": cap,
            }

    out = list(by_name.values())
    coverage = {
        "universe_members": len(ids),
        "priced": len(out),
        # How much of the universe we could actually price. ALWAYS reported: a cap-weighted index
        # renormalised over a fraction of its constituents is exactly the invention the portfolio
        # returns refuse to make, and here the missing names are systematic (a whole country), not
        # random.
        "covered_pct": (len(out) / len(ids) * 100.0) if ids else None,
    }
    return out, coverage


def index_returns(label: str, starts: list[str]) -> dict[str, dict]:
    """Cap-weighted EUR/local return for `label` over several windows — ONE price load, ONE
    weighting (`_window_rows`, shared with the GuruFocus path and with /benchmarks)."""
    from datetime import date, timedelta  # noqa: PLC0415

    mem, coverage = members(label)
    if not mem or not starts:
        return {}
    earliest = min(starts)
    lookback = (date.fromisoformat(earliest) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()

    closes = _asset_closes([m["company_id"] for m in mem], lookback, today)
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in mem}, lookback, today)

    out: dict[str, dict] = {}
    for s in sorted(set(starts)):
        rows, _ = _window_rows(mem, closes, fx, s)
        total = sum(r["start_cap_eur"] for r in rows)
        if not rows or total <= 0:
            out[s] = {"eur_pct": None, "local_pct": None, "members": 0,
                      "start_date": None, **coverage}
            continue
        eur = sum(r["start_cap_eur"] / total * r["return_eur_pct"] for r in rows)
        loc = sum(r["start_cap_eur"] / total * r["return_local_pct"] for r in rows)
        out[s] = {"eur_pct": eur, "local_pct": loc, "members": len(rows),
                  "start_date": min(r["start_date"] for r in rows), **coverage}
    return out


def index_rows(label: str, start: str) -> tuple[list[dict], dict]:
    """The index's CONSTITUENTS over one window — each with its start-of-window weight and its
    EUR return. Returns (rows, coverage).

    Attribution needs the index name by name, not just its total: "did your Technology picks beat
    the index's Technology picks?" is unanswerable from an aggregate. Same `_window_rows`, so the
    weights are the same start-of-window weights the headline return is built from — an
    attribution that reconciles against a DIFFERENT weighting reconciles against nothing.
    """
    from datetime import date, timedelta  # noqa: PLC0415

    mem, coverage = members(label)
    if not mem:
        return [], coverage
    lookback = (date.fromisoformat(start) - timedelta(days=45)).isoformat()
    closes = _asset_closes([m["company_id"] for m in mem], lookback, date.today().isoformat())
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in mem}, lookback,
                    date.today().isoformat())

    rows, _ = _window_rows(mem, closes, fx, start)
    total = sum(r["start_cap_eur"] for r in rows)
    if total <= 0:
        return [], coverage
    for r in rows:
        r["weight_pct"] = r["start_cap_eur"] / total * 100.0
    return rows, coverage


async def index_returns_async(label: str, starts: list[str]) -> dict[str, dict]:
    return await asyncio.to_thread(index_returns, label, starts)
