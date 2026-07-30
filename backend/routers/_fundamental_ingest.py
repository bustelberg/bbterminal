"""Ingest the GuruFocus fundamentals a coverage row is missing — one holding at a time.

The coverage table (`_fundamental_coverage`) names, per holding, exactly why a fundamentals view
cannot reach it. TWO of those reasons are a gap on OUR side, not a purchase decision, and both are
fixable from a single GuruFocus fetch:

    no_metrics   a `company` row exists; the earnings ingest has simply never run for it. Fetch
                 its financials and load them.
    no_company   no `company` row at all. Resolve the ISIN to a GuruFocus listing (the SAME
                 `isin/{ISIN}` bridge the dividends column uses — see `_gf_listing`), CREATE the
                 company, then fetch.

Every other reason is REFUSED, and the refusal is the honest answer rather than a failure:

    unsubscribed the data exists and we do not subscribe to the exchange — a fetch would 403.
    fund         an ETF has no income statement of its own (`stock/QQQ/financials` is null).
    not_equity   a bond / future / FX line has no earnings stream.
    cash         no ISIN, nothing to look up.

⚠ THE OUTCOME IS NEVER GUESSED FROM THE INPUT. A `no_company` ISIN can still turn out to be an
unsubscribed listing (GuruFocus knows a Bombay line and we can't buy it) or one GuruFocus has no
financials for; the outcome is whatever the fetch actually returns, reported per its OWN status,
so a row that could not be ingested says why — it does not silently read as "done".
"""
from __future__ import annotations

# The two reasons a fetch can fix. Everything else is a purchase decision or a category the
# question does not apply to. Kept in lock-step with `_fundamental_coverage.classify_holding`.
INGESTABLE_REASONS = frozenset({"no_company", "no_metrics"})


def is_ingestable(reason: str | None) -> bool:
    """Can a GuruFocus fetch plausibly close this gap? (The frontend shows a button iff so.)"""
    return (reason or "") in INGESTABLE_REASONS


def classify_fetch_outcome(rows_loaded: int, metrics_found: int,
                           is_forbidden: bool, error: str | None) -> tuple[str, str]:
    """The (status, detail) a completed `fetch_financials` result maps to. Pure — so the mapping
    is unit-tested rather than discovered in production.

    ⚠ `no_data` IS NOT `error`, AND `unsubscribed` IS NOT EITHER. A 403 means the exchange is out
    of subscription (the fetch was well-formed, the answer was "you can't have it"); an empty load
    with no error means GuruFocus simply has no fundamentals for that listing. Both are answers,
    not faults, and collapsing them into "error" would send the reader chasing a bug that isn't
    there.
    """
    if is_forbidden:
        return "unsubscribed", "exchange is an unsubscribed region on GuruFocus"
    if rows_loaded > 0 or metrics_found > 0:
        return "ingested", f"loaded {rows_loaded} rows, {metrics_found} metrics"
    if error:
        return "error", error
    return "no_data", "GuruFocus returned no fundamentals for this listing"


def reusable_same_listing(matches: list, exchange: str):
    """Of the pre-insert matches, the ONE that is the very listing we resolved — or None.

    ⚠ THIS IS THE FIX FOR "✓ INGESTED BUT NO GF EXCHANGE". `find_canonical_match` returns two
    buckets: same (canonical ticker, exchange) AND same NAME across any exchange. Only the first
    is the same security — a "Constellation Software" on TSX is a DIFFERENT listing from the OTC
    line we just resolved (different exchange, currency and ISIN). Reusing that cross-exchange
    name match, fetching the resolved listing's data into it, and stamping our ISIN corrupts that
    row — and because the stamp is exactly what coverage keys on, it silently fails and the holding
    reads `no_company` for ever while showing a ✓. So we adopt a match ONLY when it sits on the
    resolved exchange; everything else gets a clean new row keyed by its own ISIN.
    """
    ex = (exchange or "").strip().upper()
    for m in matches:
        if (getattr(m, "exchange_code", None) or "").strip().upper() == ex:
            return m
    return None


def _create_or_reuse_company(isin: str, name: str, ticker: str, exchange: str) -> tuple[int | None, str | None]:
    """Get a `company_id` for a newly-resolved listing, creating the row if need be.

    Reuses ONLY the existing row that is the SAME listing (same exchange) — see
    `reusable_same_listing` for why a cross-exchange name match must not be adopted. That row is
    unambiguously this security, so its ISIN is set to the resolved one (a listing has exactly one
    ISIN; an older differing value is a data error this corrects). Otherwise a fresh row is
    inserted, keyed by its own ISIN so coverage can find it — a distinct listing of the same issuer
    is a distinct row, exactly as the ADR/home-line cases already are.
    """
    from deps import supabase  # noqa: PLC0415
    from ingest.dedupe import canonical_ticker, find_canonical_match  # noqa: PLC0415
    from routers.companies import _resolve_exchange_id  # noqa: PLC0415

    exch_id = _resolve_exchange_id(exchange)
    if exch_id is None:
        return None, f"unknown exchange {exchange!r} — add it to gurufocus_exchange first"
    norm = canonical_ticker(ticker, exchange)

    match = reusable_same_listing(find_canonical_match(supabase, name, norm, exchange), exchange)
    if match is not None:
        supabase.table("company").update({"isin": isin}) \
            .eq("company_id", match.company_id).execute()
        return match.company_id, None

    ins = supabase.table("company").insert({
        "company_name": name, "gurufocus_ticker": norm,
        "exchange_id": exch_id, "isin": isin,
    }).execute()
    if not ins.data:
        return None, "company insert failed"
    return ins.data[0]["company_id"], None


def _repoint_company(company_id: int, ticker: str, exchange: str) -> str | None:
    """Move a company onto a different GuruFocus listing (ticker + exchange). Returns an error
    string or None.

    ⚠ THIS ALSO CHANGES WHERE ITS PRICES COME FROM. `close_price` refreshes read the company's
    stored ticker/exchange, so repointing Shopify TSX→NASDAQ switches its future price feed from
    CAD to USD — a deliberate, accepted consequence (the alternative is leaving the whole company
    permanently unreachable on a leg we can't fetch). Only ever called for a company already on an
    UNSUBSCRIBED exchange, i.e. one whose feed is broken anyway.
    """
    from deps import supabase  # noqa: PLC0415
    from ingest.dedupe import canonical_ticker  # noqa: PLC0415
    from routers.companies import _resolve_exchange_id  # noqa: PLC0415

    exch_id = _resolve_exchange_id(exchange)
    if exch_id is None:
        return f"unknown exchange {exchange!r} — add it to gurufocus_exchange first"
    supabase.table("company").update({
        "gurufocus_ticker": canonical_ticker(ticker, exchange),
        "exchange_id": exch_id,
    }).eq("company_id", company_id).execute()
    return None


# GuruFocus listing-resolution statuses that are an answer, not a listing → their (status, detail).
_LISTING_REFUSALS = {
    "not_applicable": ("not_equity", "not an equity — no earnings to fetch"),
    "not_found": ("not_found", "GuruFocus does not resolve this ISIN to any listing"),
    "unsubscribed": ("unsubscribed", "listings exist but none on a subscribed exchange"),
    "error": ("error", "GuruFocus ISIN lookup failed (server error) — try again"),
}


def _resolve_primary(isin: str, canon: str, *, force: bool,
                     company_id: int | None = None) -> tuple[str | None, str | None, dict | None]:
    """Resolve the ISIN to its primary SUBSCRIBED GuruFocus listing.

    Returns `(ticker, exchange, None)` when one is found, or `(None, None, refusal_dict)` when the
    ISIN resolves to no usable subscribed listing — the refusal carries the honest status so the
    row says why (not_found / unsubscribed / not_equity / a retriable server error).
    """
    from routers._asset_dividends import _resolve_listing  # noqa: PLC0415

    listing = _resolve_listing(canon, force=force)
    status = listing.get("status")
    base = {"isin": isin}
    if company_id is not None:
        base["company_id"] = company_id
    if status in _LISTING_REFUSALS:
        s, d = _LISTING_REFUSALS[status]
        return None, None, {**base, "status": s, "detail": d}
    ticker, exch = listing.get("gurufocus_ticker"), listing.get("exchange_code")
    if not ticker or not exch:
        return None, None, {**base, "status": "error",
                            "detail": f"listing resolved with status {status!r} but no ticker/exchange"}
    return ticker, exch, None


def _fetch_and_report(company_id: int, ticker: str, exchange: str, *,
                      isin: str, force: bool, created: bool) -> dict:
    from deps import supabase  # noqa: PLC0415
    from ingest.earnings import fetch_financials  # noqa: PLC0415

    r = fetch_financials(supabase, company_id, ticker, exchange, force_refresh=force)
    status, detail = classify_fetch_outcome(
        r.rows_loaded, r.metrics_found, getattr(r, "is_forbidden", False), r.error)
    return {
        "isin": isin, "status": status, "detail": detail,
        "company_id": company_id, "ticker": ticker, "exchange": exchange,
        "created_company": created,
        "rows_loaded": r.rows_loaded, "metrics": r.metrics_found,
    }


def ingest_fundamentals_for_isin(isin: str, name: str | None = None, *,
                                 force: bool = False) -> dict:
    """Attempt to fetch + load one holding's GuruFocus fundamentals.

    Returns `{isin, status, detail, ...}` where `status` is one of:
        ingested · no_data · unsubscribed · not_found · not_equity · error
    A company row is CREATED when the ISIN had none and resolves to a subscribed listing.
    """
    from deps import supabase  # noqa: PLC0415

    from asset_pipeline.isin_alias import canonical  # noqa: PLC0415
    from index_universe.acwi.exchange_map import is_gf_subscribed_exchange  # noqa: PLC0415

    isin = (isin or "").strip()
    if not isin:
        return {"isin": isin, "status": "error", "detail": "no ISIN"}
    canon = canonical(isin)

    # 1. Company already present (the `no_metrics` case).
    co = (supabase.table("company")
          .select("company_id,gurufocus_ticker,"
                  "gurufocus_exchange:gurufocus_exchange(exchange_code)")
          .eq("isin", canon).limit(1).execute().data or [])
    if co:
        row = co[0]
        cid = row["company_id"]
        ticker = row.get("gurufocus_ticker")
        exch = (row.get("gurufocus_exchange") or {}).get("exchange_code")
        # 1a. Already on an exchange we can fetch from → fetch straight into it.
        if ticker and exch and is_gf_subscribed_exchange(exch):
            return _fetch_and_report(cid, ticker, exch, isin=isin, force=force, created=False)
        # 1b. Pinned to an UNSUBSCRIBED (or unknown) exchange — Shopify on TSX. Detect the primary
        # subscribed listing (NASDAQ:SHOP), REPOINT the company to it, then fetch. If nothing is
        # subscribed, the resolver's refusal is the honest answer.
        new_ticker, new_exch, refusal = _resolve_primary(isin, canon, force=force, company_id=cid)
        if refusal is not None:
            return refusal
        rerr = _repoint_company(cid, new_ticker, new_exch)
        if rerr:
            return {"isin": isin, "status": "error", "company_id": cid, "detail": rerr}
        out = _fetch_and_report(cid, new_ticker, new_exch, isin=isin, force=force, created=False)
        out["repointed_from"] = exch  # so the caller can see the listing changed
        return out

    # 2. No company (the `no_company` case) — resolve to the primary listing, create, then fetch.
    ticker, exch, refusal = _resolve_primary(isin, canon, force=force)
    if refusal is not None:
        return refusal
    cid, err = _create_or_reuse_company(canon, name or f"{ticker}.{exch}", ticker, exch)
    if err:
        return {"isin": isin, "status": "error", "detail": err}
    return _fetch_and_report(cid, ticker, exch, isin=isin, force=force, created=True)
