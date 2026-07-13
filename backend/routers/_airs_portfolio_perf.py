"""Year-to-date performance of the AIRS model portfolios, in EUR.

⚠ READ THIS BEFORE TRUSTING `ytd_pct`.
    A model portfolio is a COMPOSITION, not an account — AIRS stores what it should hold, not
    a track record. We hold exactly one composition per portfolio (the current one), because
    AirSPMS's snapshot dropdown offers only 2-3 dates and they are not a monthly history:

        BUS_BM_AAND_ww_EUR_2026   ['2026-07-13', '2026-01-06']
        BUS_FTS_OFF_AFS           ['2026-07-13', '2025-03-24', '2025-04-11']

    Those dates are when the model was DEFINED, and `positions_datum` is the latest of them —
    i.e. the composition's EFFECTIVE DATE. That single fact decides what a YTD number means:

      * effective BEFORE Jan 1  (29 of 56) — the model has held these weights all year, so
        buy-and-hold from Jan 1 IS what it earned. A real YTD.
      * effective DURING the year (27 of 56) — the weights we hold were NOT the weights it
        held in January. Applying them back to Jan 1 backtests a basket that was chosen
        knowing how the year had gone. That is hindsight, and it flatters.

    We cannot fix this — the January composition is not recoverable from AIRS. So we compute
    the number, we FLAG it (`model_changed_in_period`), and we also return `since_model_pct`,
    the return since the composition's own effective date, which is always realized and never
    borrows hindsight.

⚠ WEIGHTS ARE THE MODEL'S OWN, RENORMALISED OVER WHAT WE CAN PRICE.
    25 of 248 held ISINs have no price series (structured products, in-house funds — see
    `store_one`'s zero-bar guard). Renormalising assumes the unpriced behave like the priced,
    which is a real assumption, so `covered_pct` is returned and shown. Cash IS priced — at a
    flat 0% — because cash's drag on a portfolio return is a fact, not a gap.
"""
from __future__ import annotations

import asyncio
from datetime import date

from deps import IN_CHUNK_SIZE, supabase
from routers._benchmark_index import _at_or_before, _rate, _split_adjust


# Below this share of the model's weight, we REFUSE to return a return.
#
# Renormalising over what we can price silently assumes the rest behaved the same. At 95%
# that is a rounding error; at 1% it is a fabrication. Measured, before this floor existed:
# TOPS_OFF_BEH reported "+0.00% YTD" — that was its 1% CASH line, renormalised to 100%, while
# the nine structured products making up the other 99% were simply dropped. A confident,
# precise, entirely invented number. The portfolios that trip this are the ones holding
# Leonteq structured products and in-house funds, which have no price series at all (see the
# zero-bar guard in `store_one`) — so this is not a bug to fix, it is a limit to state.
MIN_COVERAGE_PCT = 60.0

# Above this we show it plainly; between the two it is shown but flagged as partial.
GOOD_COVERAGE_PCT = 90.0


def _executions(isins: list[str]) -> dict[str, dict]:
    """ISIN -> its priceable execution row. The bridge between the AIRS world and ours."""
    out: dict[str, dict] = {}
    for i in range(0, len(isins), IN_CHUNK_SIZE):
        rows = (supabase.table("asset_execution")
                .select("isin,analysis_id,currency,yahoo_symbol,name")
                .in_("isin", isins[i:i + IN_CHUNK_SIZE]).execute().data or [])
        for r in rows:
            if r.get("analysis_id") and r["isin"] not in out:
                out[r["isin"]] = r
    return out


def _closes(analysis_ids: list[int], start: str, end: str) -> dict[int, list[tuple[str, float]]]:
    """Yahoo closes per analysis_id, ascending. Local currency."""
    out: dict[int, list[tuple[str, float]]] = {}
    for i in range(0, len(analysis_ids), IN_CHUNK_SIZE):
        chunk = analysis_ids[i:i + IN_CHUNK_SIZE]
        off = 0
        while True:
            # PostgREST caps a response at 1,000 rows and TRUNCATES SILENTLY. 223 holdings ×
            # ~130 trading days is ~29,000 rows — an unpaged read here returns 3% of the data
            # and computes a confident, wrong number off it. (I hit exactly this while
            # probing coverage: it reported 102 priced holdings when the answer is 221.)
            batch = (supabase.table("asset_price")
                     .select("analysis_id,target_date,close")
                     .in_("analysis_id", chunk)
                     .gte("target_date", start).lte("target_date", end)
                     .order("target_date").range(off, off + 999).execute().data or [])
            for r in batch:
                if r["close"] is not None:
                    out.setdefault(r["analysis_id"], []).append(
                        (r["target_date"], float(r["close"])))
            if len(batch) < 1000:
                break
            off += 1000
    for s in out.values():
        s.sort()
    return out


def _fx(currencies: set[str], start: str, end: str) -> dict[str, dict[str, float]]:
    out: dict[str, dict[str, float]] = {}
    cur = sorted(c for c in currencies if c and c != "EUR")
    for i in range(0, len(cur), IN_CHUNK_SIZE):
        rows = (supabase.table("fx_rate")
                .select("currency_code,rate_date,rate")
                .in_("currency_code", cur[i:i + IN_CHUNK_SIZE])
                .gte("rate_date", start).lte("rate_date", end).execute().data or [])
        for r in rows:
            if r["rate"]:
                out.setdefault(r["currency_code"], {})[r["rate_date"]] = float(r["rate"])
    return out


def _eur_return(series: list[tuple[str, float]], ccy: str | None,
                fx: dict[str, dict[str, float]], anchor: str) -> float | None:
    """EUR total price return from the last close on or before `anchor` to the latest close."""
    series, _ = _split_adjust(series)          # Yahoo closes are not guaranteed adjusted either
    first = _at_or_before(series, anchor)
    if not first or not series:
        return None
    d0, p0 = first
    d1, p1 = series[-1]
    if p0 <= 0 or d1 <= d0:
        return None
    r0, r1 = _rate(fx, ccy, d0), _rate(fx, ccy, d1)
    if not r0 or not r1:
        return None
    return ((p1 / r1) / (p0 / r0) - 1.0) * 100.0


def compute_portfolio_performance(year: int | None = None) -> list[dict]:
    """Per-portfolio YTD (EUR), plus the return since the composition's own effective date."""
    year = year or date.today().year
    jan1 = f"{year}-01-01"
    lookback = f"{year - 1}-11-01"            # far enough back to find both anchors
    today = date.today().isoformat()

    ports = (supabase.table("airs_model_portfolio")
             .select("id,name,positions_datum,positions_scanned_at")
             .not_.is_("positions_datum", "null").execute().data or [])
    if not ports:
        return []

    pos = (supabase.table("airs_model_portfolio_position")
           .select("portfolio_id,isin,percentage").execute().data or [])
    by_pf: dict[int, list[dict]] = {}
    for r in pos:
        by_pf.setdefault(r["portfolio_id"], []).append(r)

    isins = sorted({r["isin"] for r in pos if r.get("isin")})
    ex = _executions(isins)
    closes = _closes(sorted({e["analysis_id"] for e in ex.values()}), lookback, today)
    fx = _fx({e.get("currency") for e in ex.values()}, lookback, today)

    out: list[dict] = []
    for p in ports:
        rows = by_pf.get(p["id"], [])
        eff = p["positions_datum"]
        # The composition's own start line. For a model defined mid-year this is the only
        # window in which the number is REALIZED rather than backtested.
        eff_anchor = eff if eff and eff > jan1 else jan1

        ytd_num = ytd_den = eff_num = eff_den = 0.0
        cash_w = 0.0
        priced = unpriced = 0
        total_w = 0.0

        for r in rows:
            w = float(r.get("percentage") or 0)
            if w <= 0:
                continue
            total_w += w
            isin = r.get("isin")
            if not isin:
                # Cash. A 0% return is a FACT — its drag is real, so it is priced, not skipped.
                cash_w += w
                ytd_den += w
                eff_den += w
                priced += 1
                continue
            e = ex.get(isin)
            s = closes.get(e["analysis_id"]) if e else None
            if not e or not s:
                unpriced += 1
                continue
            ccy = e.get("currency")
            ry = _eur_return(s, ccy, fx, jan1)
            re_ = _eur_return(s, ccy, fx, eff_anchor)
            if ry is None:
                unpriced += 1
                continue
            priced += 1
            ytd_num += w * ry
            ytd_den += w
            if re_ is not None:
                eff_num += w * re_
                eff_den += w

        covered = (ytd_den / total_w * 100.0) if total_w > 0 else 0.0
        # Too little of the portfolio is priceable for a renormalised number to mean anything.
        # Return NOTHING rather than a precise-looking invention — see MIN_COVERAGE_PCT.
        enough = covered >= MIN_COVERAGE_PCT and ytd_den > 0

        out.append({
            "portfolio_id": p["id"],
            "name": p["name"],
            "model_effective": eff,
            # The composition we hold was NOT the one it held in January — so applying these
            # weights back to Jan 1 backtests a basket picked with hindsight. Say so.
            "model_changed_in_period": bool(eff and eff > jan1),
            "ytd_pct": (ytd_num / ytd_den) if enough else None,
            "since_model_pct": (eff_num / eff_den) if (enough and eff_den > 0) else None,
            "priced_holdings": priced,
            "unpriced_holdings": unpriced,
            # How much of the model's weight we could price. Always returned, even when we
            # refuse the number — it is the reason we refused.
            "covered_pct": covered if total_w > 0 else None,
            "low_coverage": not enough,
            "partial_coverage": enough and covered < GOOD_COVERAGE_PCT,
            "cash_pct": cash_w,
        })

    out.sort(key=lambda r: (r["ytd_pct"] is None, -(r["ytd_pct"] or 0)))
    return out


async def compute_portfolio_performance_async(year: int | None = None) -> list[dict]:
    return await asyncio.to_thread(compute_portfolio_performance, year)
