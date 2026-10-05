"""Template-managed Quality universe from the two committed source workbooks."""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import date
from pathlib import Path

from supabase import Client

from index_universe.acwi.exchange_map import gurufocus_exchange_for_db
from index_universe.acwi.yahoo_map import yahoo_symbol
from index_universe.quality import QualityCandidate, load_quality_candidates

from .base import ProgressCallback, RefreshResult, TemplateDiff, UniverseTemplate

log = logging.getLogger(__name__)
_YAHOO_OVERRIDES_FILE = (
    Path(__file__).resolve().parents[2] / "data" / "company-lists" / "quality_yahoo_overrides.json"
)


def _manual_yahoo_overrides() -> dict[str, str]:
    """Explicitly reviewed source-ticker -> Yahoo-symbol corrections.

    Workbook identifiers can be Bloomberg/vendor codes (for example
    ``2299955D`` for Constellation Software), so they must never be treated as
    Yahoo symbols. Every value in this file is an intentional reviewed mapping.
    """
    with _YAHOO_OVERRIDES_FILE.open(encoding="utf-8") as handle:
        raw = json.load(handle)
    if not isinstance(raw, dict) or not all(isinstance(k, str) and isinstance(v, str) for k, v in raw.items()):
        raise ValueError("quality_yahoo_overrides.json must be an object of string ticker mappings")
    return {ticker.upper(): symbol.upper() for ticker, symbol in raw.items()}


def _resolve_candidates(
    supabase: Client,
    candidates: list[QualityCandidate],
    *,
    tag_sources: bool = True,
) -> tuple[dict[str, int], dict[str, str], list[dict]]:
    """Resolve source candidates to verified database companies.

    A bare ticker is ambiguous across listings.  For an ETF candidate we
    require its mapped exchange; for a Compounders-only candidate we accept a
    bare ticker only when exactly one active company row has it.  This keeps
    the universe from silently linking an ADR or another country's listing.
    """
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for row in (supabase.table("company")
                .select("company_id,gurufocus_ticker,exchange_id")
                .in_("gurufocus_ticker", [c.ticker for c in candidates])
                .is_("delisted_at", "null")
                .is_("out_of_scope_at", "null")
                .execute().data or []):
        by_ticker[str(row["gurufocus_ticker"]).upper()].append(row)

    exchange_ids = {
        str(r["exchange_code"]): int(r["exchange_id"])
        for r in (supabase.table("gurufocus_exchange")
                  .select("exchange_id,exchange_code").execute().data or [])
    }
    resolved: dict[str, int] = {}
    sectors: dict[str, str] = {}
    unresolved: list[dict] = []
    source_tags: dict[int, set[str]] = defaultdict(set)

    for candidate in candidates:
        matches = by_ticker.get(candidate.ticker, [])
        expected_exchange = (
            gurufocus_exchange_for_db(candidate.ishares_exchange)
            if candidate.ishares_exchange else None
        )
        selected = None
        if expected_exchange and expected_exchange in exchange_ids:
            selected = next((r for r in matches if r.get("exchange_id") == exchange_ids[expected_exchange]), None)
        # Only Compounders-only candidates lack a source listing venue.  The
        # ETF's venue is a stronger identifier than a coincident bare ticker,
        # so never replace it with (for example) a US ADR of the same issuer.
        if expected_exchange is None and len(matches) == 1:
            selected = matches[0]
        if selected is None:
            reason = "no matching database company" if not matches else "ambiguous database listing"
            if expected_exchange and expected_exchange not in exchange_ids:
                reason = f"source exchange {expected_exchange} is not configured"
            unresolved.append({"ticker": candidate.ticker, "name": candidate.name, "reason": reason})
            continue
        company_id = int(selected["company_id"])
        resolved[candidate.ticker] = company_id
        if candidate.sector:
            sectors[candidate.ticker] = candidate.sector
        source_tags[company_id].update(f"quality_{source}" for source in candidate.sources)

    if tag_sources:
        for company_id, tags in source_tags.items():
            for source_code in tags:
                supabase.table("company_source").upsert(
                    {"company_id": company_id, "source_code": source_code},
                    on_conflict="company_id,source_code", ignore_duplicates=True,
                ).execute()
    return resolved, sectors, unresolved


def _source_yahoo_symbols(supabase: Client, candidates: list[QualityCandidate]) -> dict[str, str]:
    """Return only Yahoo symbols supported by an exact listing or source venue.

    The workbook ticker is a source identifier, not necessarily Yahoo's symbol.
    Prefer the stored Yahoo execution for a database-matched company; otherwise
    derive a Yahoo spelling only when the iShares source gives a known exchange.
    """
    symbols: dict[str, str] = {}
    try:
        resolved, _sectors, _unresolved = _resolve_candidates(supabase, candidates, tag_sources=False)
        company_ids = list(set(resolved.values()))
        by_company_id: dict[int, str] = {}
        if company_ids:
            for row in (supabase.table("asset_grid").select("company_id,yahoo_symbol")
                        .in_("company_id", company_ids).execute().data or []):
                company_id, symbol = row.get("company_id"), row.get("yahoo_symbol")
                if company_id is not None and symbol:
                    by_company_id.setdefault(int(company_id), str(symbol).upper())
        symbols = {
            candidate.ticker: by_company_id[company_id]
            for candidate in candidates
            if (company_id := resolved.get(candidate.ticker)) in by_company_id
        }
    except Exception:
        # The source table is still useful when the database is temporarily
        # unavailable. Its exchange-derived symbols remain safe to display.
        log.exception("Could not enrich Quality source rows with stored Yahoo symbols")

    for candidate in candidates:
        symbols.setdefault(
            candidate.ticker,
            yahoo_symbol(candidate.ticker, candidate.ishares_exchange or "", candidate.country or "") or "",
        )
    # Overrides win over all source-derived forms. In particular, a numeric
    # vendor identifier may otherwise look like a valid Toronto ticker.
    for ticker, symbol in _manual_yahoo_overrides().items():
        symbols[ticker] = symbol
    return {ticker: symbol for ticker, symbol in symbols.items() if symbol}


def source_companies(supabase: Client) -> dict:
    """The full source union for the Quality Universe screen.

    This deliberately returns every source company, including those not yet
    mapped to a canonical database listing. It remains complete if enrichment
    cannot reach the database; only the optional Yahoo ticker is then absent.
    """
    candidates, as_of = load_quality_candidates()
    yahoo_symbols = _source_yahoo_symbols(supabase, candidates)
    return {
        "as_of_date": as_of.isoformat(),
        "count": len(candidates),
        "companies": [
            {
                "ticker": candidate.ticker,
                "yahoo_ticker": yahoo_symbols.get(candidate.ticker),
                "company_name": candidate.name,
                "sector": candidate.sector,
                "country": candidate.country,
                "sources": sorted(candidate.sources),
            }
            for candidate in candidates
        ],
    }


class QualityUniverseTemplate(UniverseTemplate):
    template_key = "QUALITY"
    label = "Quality universe"
    description = (
        "Union of the Global Compounders database and the iShares MSCI World Quality "
        "Factor UCITS ETF holdings. Members are linked only to verified company listings."
    )
    earliest_date = date(2025, 12, 1)

    def refresh(self, supabase: Client, *, on_progress: ProgressCallback | None = None) -> RefreshResult:
        emit = on_progress or (lambda _m, _p=None: None)
        universe_id = self.ensure_universe_row(supabase)
        before = {
            int(r["company_id"])
            for r in (supabase.table("universe_membership").select("company_id")
                      .eq("universe_id", universe_id).execute().data or [])
        }
        candidates, as_of = load_quality_candidates()
        emit(f"Loaded {len(candidates)} unique companies from the two Quality sources.", 20)
        lookup, sectors, unresolved = _resolve_candidates(supabase, candidates)
        if not lookup:
            raise ValueError("Quality universe: no source companies resolved to database listings.")
        emit(f"Matched {len(lookup)}/{len(candidates)} candidates to verified company listings.", 55)

        from index_universe.sp500.persistence import store_index_membership  # noqa: PLC0415
        month = as_of.strftime("%Y-%m")
        store_index_membership(
            supabase, self.label, {month: set(lookup)}, [], lookup,
            on_progress=lambda message: emit(message, 75), sector_lookup=sectors,
        )
        after = {
            int(r["company_id"])
            for r in (supabase.table("universe_membership").select("company_id")
                      .eq("universe_id", universe_id).execute().data or [])
        }
        self.mark_refreshed(supabase, universe_id)
        diff = TemplateDiff(
            template_key=self.template_key, universe_id=universe_id,
            this_month=month, prev_month=None,
            additions_count=len(after - before), removals_count=len(before - after), renames_count=0,
            additions=[{"company_id": company_id} for company_id in sorted(after - before)],
            removals=[{"company_id": company_id} for company_id in sorted(before - after)],
            renames=[], unresolved_additions=unresolved,
        )
        emit(f"Quality universe refreshed: {len(after)} linked members; {len(unresolved)} need listing review.", 100)
        return RefreshResult(template_key=self.template_key, universe_id=universe_id, months_written=1, diff=diff)
