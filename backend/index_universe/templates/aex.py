"""AEX template — the 25 Euronext Amsterdam large-caps, sourced from Wikipedia.

WHY WIKIPEDIA, AND NOT THE TWO ROUTES THAT LOOK EASIER
    There is no free automated route to the AEX's composition.
      * The iShares path ACWI uses is NOT A FETCHER. iShares blocks automated downloads (region
        cookie + JS challenge), so `iShares-MSCI-ACWI-ETF_fund.xls` is COMMITTED to the repo and
        is only ever as fresh as the last person who remembered to commit one. Pointing it at an
        AEX product would inherit that — a stale index that is silently wrong.
      * Euronext is the source of record (and publishes the OFFICIAL capped weights, which would
        let us drop our cap approximation entirely) but we have established no open endpoint.
    Wikipedia's AEX article carries the whole 25-name table and is edited within days of a review.

⚠ THE PAGE CARRIES ITS OWN AS-OF DATE, AND IT IS NOT TODAY.
    The composition table is introduced by "...as of 31 December 2024" — measured 2026-07-16, i.e.
    ~18 months stale. "Self-updating" means "as fresh as Wikipedia's editors", NOT "current". So
    the as-of date is PARSED and becomes the snapshot's `target_month`, which `universe.as_of_date`
    then reports. Stamping today's month instead would assert a freshness we have not got, and the
    staleness would be invisible precisely when it matters (just after a review, when the index
    has changed and the page has not). If the date cannot be parsed we REFUSE: a composition whose
    date we do not know is not a dated snapshot, and this universe model is built on dated
    snapshots.

⚠ THE TABLE IS FOUND BY ITS HEADERS, NOT ITS POSITION.
    The page has THREE wikitables (annual returns, contract specs, composition) and the
    composition is index 2 today. `scrape_sp500` addresses its tables positionally; here a single
    page edit that adds or reorders a table would silently hand us the wrong one — and an "AEX"
    built from the annual-returns table would fail loudly, but one built from a future second
    composition table would not.

RESOLUTION IS TWO TIERS, AND THE SECOND ONE IS STRUCTURAL
    Tier 1 — bare ticker + XAMS. `ASML.AS` -> `ASML` on Amsterdam. 22 of 25 land here, free.
    Tier 2 — the three that do not are EXACTLY the three constituents with a GB ISIN (Shell, RELX,
             Unilever): our pipeline resolved each to its LONDON listing, so both the ticker and
             the exchange differ (`SHELL`->`SHEL`/LSE, `REN`->`REL`/LSE, `UNA`->`ULVR`/LSE).

    ⚠ A NAME MATCH CANNOT DO TIER 2. `same_company("Unilever", "HINDUSTAN UNILEVER LTD")` is
      **True** — `token_set_ratio` scores a subset at 100, the same false friend that put 20 bad
      links in the portfolio-links module. Name-matching the AEX would enlist an Indian company.
      Worse, "Unilever" also matches our NYSE ADR row (`UL`, US9047678035), which is a different
      ISIN and a different price series.

    So tier 2 asks OPENFIGI, and the question it asks is definitional rather than fuzzy: an AEX
    constituent is, by construction, the ISIN that TRADES ON EURONEXT AMSTERDAM under that ticker.
    Candidates come from a cheap name net, but the ACCEPTANCE is `lookup_isins(...)` reporting a
    listing with `(ticker=<bare>, exchCode="NA")`. Measured 2026-07-16:

        GB00BP6MXD84  Shell             -> ('SHELL','NA')   accepted
        GB00B2B0DG97  RELX              -> ('REN','NA')     accepted
        GB00BVZK7T90  Unilever ordinary -> ('UNA','NA')     accepted
        US9047678035  Unilever ADR      -> []               REJECTED, structurally

    The ADR is refused because it genuinely has no Amsterdam line, not because a score fell under
    a threshold; Hindustan Unilever has no ISIN and so is never a candidate at all. Same rule as
    the ETF repointer: enumerate from OpenFIGI's listings of the ONE ISIN — the safety is
    structural, not a heuristic.

    Anything still unresolved goes to `diff.unresolved_additions` and surfaces on /schedule. This
    template never CREATES a company: an AEX name we cannot find is a data gap worth a human
    looking at it, not a stub to be conjured.
"""
from __future__ import annotations

import logging
import re
from datetime import date, datetime

import requests
from supabase import Client

from .base import (
    ProgressCallback,
    RefreshResult,
    TemplateDiff,
    UniverseTemplate,
)

log = logging.getLogger(__name__)

_WIKI_URL = "https://en.wikipedia.org/wiki/AEX_index"
_AMSTERDAM_EXCHANGE = "XAMS"
_OPENFIGI_AMSTERDAM = "NA"          # OpenFIGI's exchCode for Euronext Amsterdam
_AEX_SIZE = 25                      # the AEX is a 25-name index, by definition

# "The index is composed of the following listings as of 31 December 2024."
_AS_OF_RE = re.compile(r"as of\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})", re.IGNORECASE)


def _parse_as_of(html: str) -> date | None:
    for raw in _AS_OF_RE.findall(html):
        for fmt in ("%d %B %Y", "%d %b %Y"):
            try:
                return datetime.strptime(raw.strip(), fmt).date()
            except ValueError:
                continue
    return None


def scrape_aex() -> tuple[date, list[dict]]:
    """(as_of, constituents) from Wikipedia. Each constituent is
    `{ticker, bare, name, sector, weight_pct}`.

    `ticker` is the page's own (`ASML.AS`); `bare` is it without the venue suffix, which is what
    our `company.gurufocus_ticker` holds. Raises if the page shape has moved — a scraper that
    quietly returns fewer names would shrink the index rather than fail.
    """
    resp = requests.get(_WIKI_URL, headers={"User-Agent": "bbterminal/1.0"}, timeout=30)
    resp.raise_for_status()

    from index_universe.sp500.scraping import _WikiTableParser  # noqa: PLC0415

    parser = _WikiTableParser()
    parser.feed(resp.text)

    # BY HEADERS, NOT POSITION — see the module docstring.
    table = next((t for t in parser.tables
                  if "Ticker" in t["headers"] and "Company" in t["headers"]), None)
    if not table:
        raise ValueError(
            f"No AEX composition table on {_WIKI_URL} — expected a wikitable with 'Ticker' and "
            f"'Company' headers, saw: {[t['headers'][:3] for t in parser.tables]}")

    h = table["headers"]
    ti, ci = h.index("Ticker"), h.index("Company")
    si = h.index("ICB Sector") if "ICB Sector" in h else None
    wi = next((i for i, x in enumerate(h) if x.startswith("Index weighting")), None)

    out: list[dict] = []
    for row in table["rows"]:
        if len(row) <= max(ti, ci):
            continue
        ticker = row[ti].strip()
        name = row[ci].strip()
        if not ticker or not name:
            continue
        weight = None
        if wi is not None and len(row) > wi:
            try:
                weight = float(row[wi].strip().replace(",", "."))
            except ValueError:
                weight = None
        out.append({
            "ticker": ticker,
            # `ASML.AS` -> `ASML`. Wikipedia writes the venue suffix; our company rows do not.
            "bare": ticker.split(".")[0].strip().upper(),
            "name": name,
            "sector": (row[si].strip() if si is not None and len(row) > si else ""),
            "weight_pct": weight,
        })

    if not out:
        raise ValueError(f"AEX composition table on {_WIKI_URL} parsed to zero rows.")
    # Not an assertion about Wikipedia's tidiness — the AEX IS 25 names. A parse that yields 19 has
    # gone wrong in a way that would otherwise ship as a real, smaller index.
    if len(out) != _AEX_SIZE:
        log.warning("[templates.aex] expected %s constituents, parsed %s — page shape may have "
                    "changed", _AEX_SIZE, len(out))

    as_of = _parse_as_of(resp.text)
    if not as_of:
        raise ValueError(
            f"Could not parse the composition's as-of date from {_WIKI_URL}. Refusing rather than "
            f"stamping today: an undated composition presented as current is exactly the failure "
            f"this template exists to avoid.")
    return as_of, out


def _resolve_companies(supabase: Client, constituents: list[dict],
                       emit=lambda _m, _p=None: None) -> tuple[dict[str, int], dict[str, str],
                                                               list[dict]]:
    """(company_lookup, sector_lookup, unresolved), each keyed by the BARE ticker.

    Tier 1 is a ticker+exchange read; tier 2 is the OpenFIGI Amsterdam-listing gate. See the
    module docstring — the tiers are not interchangeable, and tier 2 is deliberately not a name
    match.
    """
    bare = [c["bare"] for c in constituents]
    ex = (supabase.table("gurufocus_exchange").select("exchange_id")
          .eq("exchange_code", _AMSTERDAM_EXCHANGE).limit(1).execute().data or [])
    xams = ex[0]["exchange_id"] if ex else None

    rows = (supabase.table("company")
            .select("company_id,gurufocus_ticker,company_name,exchange_id,isin")
            .in_("gurufocus_ticker", bare)
            .is_("delisted_at", "null").is_("out_of_scope_at", "null")
            .execute().data or [])
    by_ticker: dict[str, list[dict]] = {}
    for r in rows:
        by_ticker.setdefault(r["gurufocus_ticker"], []).append(r)

    lookup: dict[str, int] = {}
    sectors: dict[str, str] = {}
    tail: list[dict] = []
    for c in constituents:
        if c["sector"]:
            sectors[c["bare"]] = c["sector"]
        hit = next((r for r in by_ticker.get(c["bare"], []) if r["exchange_id"] == xams), None)
        if hit:
            lookup[c["bare"]] = hit["company_id"]
        else:
            tail.append(c)

    emit(f"AEX: {len(lookup)}/{len(constituents)} resolved on {_AMSTERDAM_EXCHANGE} by ticker.")
    if not tail:
        return lookup, sectors, []

    # --- tier 2: the Amsterdam-listing gate -------------------------------------------------
    emit(f"AEX: resolving {len(tail)} cross-listed name(s) via OpenFIGI…")
    cands: dict[str, list[dict]] = {}
    for c in tail:
        # A cheap NET, not the decision — the OpenFIGI gate below decides. A loose net costs an
        # extra ISIN in one batched call; a loose GATE costs a wrong constituent.
        found = (supabase.table("company")
                 .select("company_id,company_name,isin")
                 .ilike("company_name", f"%{c['name'].split()[0]}%")
                 .is_("delisted_at", "null").is_("out_of_scope_at", "null")
                 .execute().data or [])
        cands[c["bare"]] = [r for r in found if r.get("isin")]

    isins = sorted({r["isin"] for rs in cands.values() for r in rs})
    figi: dict[str, list[dict]] = {}
    if isins:
        from asset_pipeline import openfigi  # noqa: PLC0415
        try:
            figi = openfigi.lookup_isins(isins)
        except Exception as e:  # noqa: BLE001
            # A missing tier 2 costs three constituents and says so; a WRONG tier 2 is silent.
            log.warning("[templates.aex] OpenFIGI lookup failed (%s) — %s name(s) unresolved",
                        e, len(tail))

    unresolved: list[dict] = []
    for c in tail:
        accepted = [r for r in cands.get(c["bare"], [])
                    if any(f.get("exchCode") == _OPENFIGI_AMSTERDAM
                           and (f.get("ticker") or "").upper() == c["bare"]
                           for f in figi.get(r["isin"], []))]
        if len(accepted) == 1:
            lookup[c["bare"]] = accepted[0]["company_id"]
            emit(f"AEX: {c['ticker']} -> {accepted[0]['company_name']} "
                 f"({accepted[0]['isin']}) via OpenFIGI.")
        else:
            # 0 = no company of ours trades that ticker in Amsterdam; 2+ = ambiguous, and a coin
            # flip between two ISINs is two different price series. Both are for a human.
            unresolved.append({
                "ticker": c["ticker"], "name": c["name"],
                "reason": ("no company with an Amsterdam listing under this ticker"
                           if not accepted else
                           f"ambiguous — {len(accepted)} companies match"),
            })
    return lookup, sectors, unresolved


class AEXTemplate(UniverseTemplate):
    template_key = "AEX"
    label = "AEX"
    description = (
        "AEX — the 25 Euronext Amsterdam large-caps, sourced from Wikipedia's composition table. "
        "A single dated snapshot: the page states its own as-of date and that date is what the "
        "snapshot carries, so staleness is visible rather than assumed away. Capped at 15% per "
        "constituent when priced as a benchmark (see `_benchmark_index.INDEX_CAP_PCT`)."
    )
    earliest_date = date(1983, 1, 3)     # AEX inception; decorative for a single-snapshot template

    def refresh(self, supabase: Client, *,
                on_progress: ProgressCallback | None = None) -> RefreshResult:
        emit = on_progress or (lambda _m, _p=None: None)

        # Before `store_index_membership` wipes it — otherwise every refresh reports all 25 as
        # additions and a real change (a name entering at the March review) is lost in the noise.
        universe_id = self.ensure_universe_row(supabase)
        before = {r["company_id"] for r in
                  (supabase.table("universe_membership").select("company_id")
                   .eq("universe_id", universe_id).execute().data or [])}

        emit("Scraping the AEX composition…", 10)
        as_of, constituents = scrape_aex()
        stale_days = (date.today() - as_of).days
        emit(f"AEX composition as of {as_of.isoformat()} "
             f"({len(constituents)} names, {stale_days}d old).", 30)

        lookup, sectors, unresolved = _resolve_companies(supabase, constituents, emit)
        if not lookup:
            raise ValueError("AEX: not one constituent resolved to a company — refusing to write "
                             "an empty index.")

        month = as_of.strftime("%Y-%m")
        emit(f"Writing {len(lookup)} members at {month}…", 70)
        from index_universe.sp500.persistence import store_index_membership  # noqa: PLC0415

        store_index_membership(
            supabase, self.label, {month: set(lookup)}, [], lookup,
            on_progress=lambda m: emit(m), sector_lookup=sectors,
        )

        after = {r["company_id"] for r in
                 (supabase.table("universe_membership").select("company_id")
                  .eq("universe_id", universe_id).execute().data or [])}
        names = {v: k for k, v in lookup.items()}
        diff = TemplateDiff(
            template_key=self.template_key, universe_id=universe_id,
            this_month=month, prev_month=None,
            additions_count=len(after - before), removals_count=len(before - after),
            renames_count=0,
            additions=[{"company_id": i, "ticker": names.get(i)} for i in sorted(after - before)],
            removals=[{"company_id": i} for i in sorted(before - after)],
            renames=[],
            unresolved_additions=unresolved,
        )

        self.mark_refreshed(supabase, universe_id)
        log.info("[templates.aex] refresh complete: universe_id=%s as_of=%s members=%s "
                 "diff=+%s/-%s unresolved=%s",
                 universe_id, as_of, len(lookup), diff.additions_count, diff.removals_count,
                 len(unresolved))
        return RefreshResult(template_key=self.template_key, universe_id=universe_id,
                             months_written=1, diff=diff)
