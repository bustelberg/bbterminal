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
from routers._benchmark_index import INDEX_CAP_PCT, _fx_to_eur, _window_rows, index_weights


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
        # ONE weighting, shared with `index_rows` below and with /benchmarks — an index whose
        # headline return is weighted differently from the constituents behind it reconciles
        # against nothing. This is also where a capped index (AEX) gets its cap.
        w = index_weights(rows, label)
        eur = sum(x / 100.0 * r["return_eur_pct"] for x, r in zip(w, rows))
        loc = sum(x / 100.0 * r["return_local_pct"] for x, r in zip(w, rows))
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
    # Same `index_weights` as `index_returns` — Brinson reconciles the constituents against the
    # index total, so the two MUST be the same weights (capped ones included).
    for r, x in zip(rows, index_weights(rows, label)):
        r["weight_pct"] = x
    return rows, coverage


def compute_index(label: str, year: int | None = None, start: str | None = None) -> dict:
    """The `/benchmarks` panel's index — the ASSET-path twin of `_benchmark_index.compute_index`,
    returning the identical shape.

    ⚠ WHY THE PANEL MOVED HERE (2026-07-16). Its own subtitle promises "same basis as a portfolio,
    so the numbers are comparable" — and every portfolio on that page is priced from `asset_price`
    (yfinance) while the panel was priced from GuruFocus. Two price vendors, two adjustment
    conventions, two FX sources; the difference between them reads as alpha. That is the rule the
    composition modal was built on, applied to the panel that states it out loud.

    The measured cost, stated rather than buried: against SPY's +9.02% USD, the GuruFocus rebuild
    was +9.05% and this one is +9.23% — so for the S&P specifically, the vendor we left was ~0.2pp
    closer. That trade is deliberate. GuruFocus is closer on the ONE index whose constituents it
    fully covers, and structurally unable to price the others:

        ACWI   ~7.8% of published weight in countries GuruFocus will never price
        AEX    31.96% — Shell, Unilever and RELX are all LSE rows with no GuruFocus market cap

    A cap-weighted rebuild does not LOSE that weight, it redistributes it: the GuruFocus AEX
    printed +14.80% against this path's +12.12%, with Prosus capped at 15% — a name that is really
    10.46% of the index, pushed onto the cap by absorbing the missing third. Nothing about that
    output looks wrong. Trading 0.2pp on the S&P for that is not a close call.

    `_benchmark_index.compute_index` stays exactly where it is: it is the SPY cross-check, which
    validates the METHOD (start-of-window weights, split-adjustment, per-date FX) against a real
    ETF. It is no longer any route's basis — with this move its GuruFocus price loader
    (`_benchmark_index._closes`, the last reader of `metric_data` on this page's side of the app)
    has NO production caller left. That is the point, not an oversight: keep the cross-check,
    retire the vendor as a basis.
    """
    from datetime import date, timedelta  # noqa: PLC0415

    year = year or date.today().year
    start_anchor = start or f"{year}-01-01"
    lookback = (date.fromisoformat(start_anchor) - timedelta(days=45)).isoformat()
    today = date.today().isoformat()

    mem, coverage = members(label)
    if not mem:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": f"No universe labelled {label!r}."}

    closes = _asset_closes([m["company_id"] for m in mem], lookback, today)
    fx = _fx_to_eur({(m.get("currency") or "USD") for m in mem}, lookback, today)

    # `adjusted` is KEPT here, not dropped. Our stored closes are not split-adjusted and cannot
    # self-heal; a rescaled price is a CLAIM and the panel shows it. (The `index_returns` /
    # `index_rows` callers above discard it because their surfaces have nowhere to say so — this
    # one does.)
    rows, adjusted = _window_rows(mem, closes, fx, start_anchor)
    if not rows or sum(r["start_cap_eur"] for r in rows) <= 0:
        return {"label": label, "year": year, "members": [], "member_count": 0,
                "ytd_eur_pct": None, "ytd_local_pct": None,
                "note": "No constituent had a price on both ends of the window."}

    # THE SAME `index_weights` as every other surface — capped where the index caps.
    for r, x in zip(rows, index_weights(rows, label)):
        r["weight_pct"] = x
    ytd_eur = sum(r["weight_pct"] / 100.0 * r["return_eur_pct"] for r in rows)
    ytd_loc = sum(r["weight_pct"] / 100.0 * r["return_local_pct"] for r in rows)
    rows.sort(key=lambda r: -r["weight_pct"])

    cap = INDEX_CAP_PCT.get(label)
    note = ("Cap-weighted on FULL market cap (the real index float-adjusts) using "
            "start-of-year weights; membership is a snapshot, so mid-year index changes are not "
            "replayed. Price return, not total return — dividends are not included. "
            "Priced from yfinance, the same source as the portfolios above.")
    if cap:
        note += (f" Capped at {cap:.0f}% per constituent, as the real index is — applied at the "
                 f"window open rather than at the index's review date.")

    return {
        "label": label,
        "year": year,
        "member_count": len(rows),
        # `priced_of_universe` is the honest denominator: `coverage` counts what the UNIVERSE has,
        # `rows` what actually had a price on both ends of this window.
        "priced_of_universe": f"{len(rows)}/{coverage['universe_members']}",
        "as_of": max(r["end_date"] for r in rows),
        "start_date": min(r["start_date"] for r in rows),
        "ytd_eur_pct": ytd_eur,
        "ytd_local_pct": ytd_loc,
        "members": rows,
        "split_adjusted": adjusted,
        "note": note,
    }


async def compute_index_async(label: str, year: int | None = None) -> dict:
    return await asyncio.to_thread(compute_index, label, year)


async def index_returns_async(label: str, starts: list[str]) -> dict[str, dict]:
    return await asyncio.to_thread(index_returns, label, starts)
