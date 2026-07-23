"""Which of a portfolio's holdings can have fundamentals at all — and why the rest cannot.

WHY THIS IS ITS OWN ANSWER, NOT A FOOTNOTE
    A "portfolio fundamentals" view blends the individual companies' figures. Every holding it
    cannot reach is weight that silently drops out of the blend, and a blended P/E over 40% of a
    book presented as the book's P/E is a fabrication of the same shape as the coverage floors
    already guarding the AIRS returns (`MIN_COVERAGE_PCT`): the number looks entirely normal and
    describes something else.

    So coverage is computed FIRST, reported as weight rather than as a count, and every exclusion
    names its own reason. A reader must be able to see that a figure spans 61% of the book.

THE REASONS ARE NOT INTERCHANGEABLE, AND THAT IS THE POINT
    cash          no ISIN at all — not an instrument, nothing to look up
    not_equity    a bond, future, FX or crypto line. A coupon is not an earnings stream; the
                  question does not apply, and no API call is spent asking it.
    fund          an ETF or fund. It HOLDS companies rather than being one, so it has no income
                  statement of its own — `stock/QQQ/financials` returns null. Looking through to
                  its constituents is a different feature, not a gap in this one.
    unsubscribed  a real company on an exchange outside the GuruFocus subscription (India, UK,
                  Ireland, Russia, Africa, LatAm, AU/NZ). ⚠ THE DATA EXISTS AND WE CANNOT BUY IT —
                  the only reason on this list that is about our subscription rather than about
                  the instrument, and the only one a purchase would fix.
    no_company    an equity we simply have no `company` row for. A gap in OUR ingest, fixable by
                  adding it — unlike `unsubscribed`, which no amount of ingesting will fix.
    no_metrics    a company row EXISTS and no fundamentals have been ingested for it. ⚠ THIS USED
                  TO BE REPORTED AS `covered`, which claimed "fundamentals can be fetched" on the
                  strength of a company row alone. Measured 2026-07-23: 2,776 company rows, SEVEN
                  of them carrying any `annuals__` metric — so a portfolio read 100% covered and
                  would have charted nothing. Fixable by running the earnings ingest.
    covered       a company row exists AND the fundamentals are actually there.

⚠ `unsubscribed` AND `no_company` MUST NOT BE MERGED. They look identical on screen ("no
    fundamentals for this one") and have opposite remedies: one is a purchase decision, the other
    is a five-minute ingest. Collapsing them turns an actionable gap into a shrug. `no_metrics` is
    a third remedy again — the company is there and the ingest has not run for it.

⚠ "COVERED" MUST MEAN THE DATA IS THERE, NOT THAT THE ROW IS. Every other reason on the list is a
    reason something CANNOT be fetched; `covered` is the one that promises it can, and it is the
    denominator every blended figure is renormalised over. A `covered` that only checked for a
    company row made that promise on evidence it never had.
"""
from __future__ import annotations

import asyncio

from deps import supabase

# A bond pays coupons and a future has no accounts: the question does not apply, so no call is
# spent asking it. Mirrors `_asset_dividends._NON_EQUITY_PRODUCTS`.
_NON_EQUITY_PRODUCTS = {"BONDS", "BOND", "FUTURE", "FUTURES", "FX", "CRYPTO_CURRENCY", "CRYPTO"}
# asset_grid.asset_class values that mean "a fund wrapper" — it holds companies, it is not one.
_FUND_CLASSES = {"etf", "fund", "etc", "etp"}

# The one metric probed to answer "does this company have fundamentals?". It is the line the
# blended charts are built on, so a company that has it can actually be charted.
_SENTINEL_METRIC = "annuals__Cashflow Statement__Free Cash Flow"


def classify_holding(isin: str | None, grid: dict | None, has_company: bool,
                     subscribed: bool | None, has_metrics: bool = False) -> str:
    """One holding's coverage reason. Pure — every input is already resolved by the caller.

    ⚠ ORDER IS THE RULE. `not_equity` and `fund` come BEFORE the company/subscription tests,
    because a bond on an unsubscribed exchange is not an unsubscribed company — reporting it as
    one would put it on the list of things a subscription would fix, and it never would.
    """
    if not isin:
        return "cash"
    g = grid or {}
    if (g.get("leonteq_product_type") or "").strip().upper() in _NON_EQUITY_PRODUCTS:
        return "not_equity"
    if (g.get("asset_class") or "").strip().lower() in _FUND_CLASSES:
        return "fund"
    if has_company:
        # ⚠ The company existing is not the fundamentals existing. See the module docstring.
        return "covered" if has_metrics else "no_metrics"
    # No company row. Distinguish "we cannot buy this data" from "we have not ingested it".
    return "unsubscribed" if subscribed is False else "no_company"


def coverage_for(members: list[dict]) -> dict:
    """`members` = [{isin, name, weight}] (weight a fraction or a percent — only ratios are used).

    Returns the per-holding verdict plus the WEIGHT each reason accounts for, because a count is
    the wrong unit: nine covered minnows and one uncovered giant is not 90% coverage.
    """
    from asset_pipeline.isin_alias import canonical_map  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    # ⚠ RESOLVE THE ALIAS FIRST. `company`/`gurufocus_listing` are keyed on the RAW ISIN, so an
    # aliased row reads as "not ingested" while its canonical sits there fully covered — measured
    # on the TSMC ADR (US8740391003 -> TW0002330008, company 3223).
    raw = sorted({m["isin"] for m in members if m.get("isin")})
    alias = canonical_map(raw)
    isins = sorted(set(alias.values()))
    grid: dict[str, dict] = {}
    companies: dict[str, dict] = {}
    for i in range(0, len(isins), 100):
        chunk = isins[i:i + 100]
        for g in (supabase.table("asset_grid")
                  .select("isin,asset_class,leonteq_product_type")
                  .in_("isin", chunk).execute().data or []):
            grid[g["isin"]] = g
        for c in (supabase.table("company")
                  .select("company_id,company_name,isin,"
                          "gurufocus_exchange:gurufocus_exchange(exchange_code)")
                  .in_("isin", chunk).execute().data or []):
            companies[c["isin"]] = c

    # Which of those companies have fundamentals AT ALL.
    #
    # ⚠ PROBED WITH ONE SENTINEL METRIC, NOT `LIKE 'annuals__%'`. A company carries ~85 annual
    # codes x ~25 years, so a wildcard over 20 companies is ~40,000 rows against PostgREST's
    # SILENT 1,000-row cap — and every company past the cut-off would come back `no_metrics`,
    # inventing a gap. One code x 20 companies is ~600 rows and cannot truncate.
    #
    # The sentinel is the Free Cash Flow line: it is what the blend actually charts, so "has
    # metrics" means "has the metric this view needs" rather than the weaker "has some row".
    with_metrics: set[int] = set()
    cids = [c["company_id"] for c in companies.values() if c.get("company_id")]
    for i in range(0, len(cids), 20):
        for m in (supabase.table("metric_data").select("company_id")
                  .in_("company_id", cids[i:i + 20])
                  .eq("metric_code", _SENTINEL_METRIC).limit(1000).execute().data or []):
            with_metrics.add(m["company_id"])

    # An exchange we do not subscribe to. Only meaningful where we HAVE a company row to read an
    # exchange off; elsewhere it stays unknown (None) rather than being guessed as subscribed.
    rows: list[dict] = []
    by_reason: dict[str, float] = {}
    total_w = sum(abs(float(m.get("weight") or 0)) for m in members) or 1.0
    for m in members:
        isin = (m.get("isin") or "").strip() or None
        # The instrument that actually serves it; identical unless this ISIN is aliased.
        lookup = alias.get(isin or "", isin)
        comp = companies.get(lookup or "")
        subscribed: bool | None = None
        if comp:
            code = ((comp.get("gurufocus_exchange") or {}) or {}).get("exchange_code")
            subscribed = is_gf_subscribed_exchange(code) if code else None
        reason = classify_holding(isin, grid.get(lookup or ""), bool(comp), subscribed,
                                  has_metrics=(comp or {}).get("company_id") in with_metrics)
        w = abs(float(m.get("weight") or 0))
        by_reason[reason] = by_reason.get(reason, 0.0) + w
        rows.append({
            "isin": isin, "name": m.get("name"), "weight_pct": round(100 * w / total_w, 3),
            "reason": reason, "company_id": (comp or {}).get("company_id"),
            "company_name": (comp or {}).get("company_name"),
            # Surfaced so a reader can see WHY an ISIN they know is uncovered came back covered.
            "served_by": (lookup if lookup != isin else None),
        })
    rows.sort(key=lambda r: -r["weight_pct"])
    return {
        "holdings": len(rows),
        "covered_pct": round(100 * by_reason.get("covered", 0.0) / total_w, 2),
        "by_reason_pct": {k: round(100 * v / total_w, 2) for k, v in sorted(by_reason.items())},
        "rows": rows,
    }


async def coverage_for_async(members: list[dict]) -> dict:
    return await asyncio.to_thread(coverage_for, members)
