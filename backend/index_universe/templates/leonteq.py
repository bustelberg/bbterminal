"""LeonteqTemplate — universe-template wrapper around the Leonteq
underlyings scraper (`backend/leonteq/scraper.py`).

Differs from `ACWITemplate` in one important way: **no historical
reconstruction**. Leonteq publishes a "what we trade structured
products on right now" list — there's no audit trail of past
membership. Every `refresh()` writes today's snapshot to:

  1. `leonteq_equity` — the rich scrape (name, ticker, isin,
     sector, industry, gurufocus_url, optional company_id). Backs
     the /leonteq page's sector → industry → company overview.
  2. `universe_membership` — keyed by `target_month = YYYY-MM` of
     today. Lets the universe-template machinery + /schedule pick
     it up like any other template. Industry data is not preserved
     on these rows (the table has no `industry` column).

`earliest_date` is fixed at 2026-05-01 — the month we started
capturing. Backtests selecting LEONTEQ before then will find no
membership.
"""
from __future__ import annotations

import logging
from datetime import date

from supabase import Client

from .base import (
    ProgressCallback,
    RefreshResult,
    UniverseTemplate,
)

log = logging.getLogger(__name__)


class LeonteqTemplate(UniverseTemplate):
    template_key = "LEONTEQ"
    label = "Leonteq Underlyings"
    description = (
        "Equities Leonteq lists as underlyings for structured products. "
        "Snapshot-only — no historical reconstruction. Each refresh "
        "captures today's published list, the per-equity sector + "
        "industry classification, and (where resolvable) a link to "
        "the canonical `company` row + GuruFocus URL."
    )
    # The month we started capturing. Earlier dates have no data.
    earliest_date = date(2026, 5, 1)

    def refresh(
        self,
        supabase: Client,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> RefreshResult:
        """Scrape Leonteq → reconcile to `company` → persist
        `leonteq_equity` rows → write current-month
        `universe_membership` for the canonical LEONTEQ universe."""
        import os  # noqa: PLC0415

        universe_id = self.ensure_universe_row(supabase)

        def emit(msg: str, pct: int | None = None) -> None:
            if on_progress is not None:
                on_progress(msg, pct)

        # Default path: hit Leonteq's underlying API directly (~3s).
        # Fallback: the Playwright DOM scraper, behind an env var so
        # we can flip back instantly if the API ever changes shape or
        # the hardcoded JWT expires. Both produce the same dict shape
        # `[{name, ticker, isin, sector, industry, ...}]`.
        if os.environ.get("LEONTEQ_USE_PLAYWRIGHT") == "1":
            from leonteq.scraper import scrape_underlyings  # noqa: PLC0415
            emit("Starting Leonteq Playwright scrape (LEONTEQ_USE_PLAYWRIGHT=1)…", 0)
            scraped = scrape_underlyings(on_progress=on_progress)
        else:
            from leonteq.api_client import fetch_underlyings_with_isin  # noqa: PLC0415
            emit("Starting Leonteq API fetch…", 0)
            scraped = fetch_underlyings_with_isin(on_progress=on_progress)
        if not scraped:
            emit("Scrape returned no rows — skipping persistence", 100)
            today_month = date.today().strftime("%Y-%m")
            empty_diff = self.compute_month_diff(
                supabase=supabase,
                universe_id=universe_id,
                prev_month=None,
                this_month=today_month,
            )
            self.mark_refreshed(supabase, universe_id)
            return RefreshResult(
                template_key=self.template_key,
                universe_id=universe_id,
                months_written=0,
                diff=empty_diff,
            )

        emit(f"Reconciling {len(scraped)} equities to company rows", 60)
        # Match each scrape row to a `company` by ticker. Anything we
        # can't match goes into `leonteq_equity` with company_id=NULL —
        # still visible on /leonteq, just not in the universe.
        company_by_ticker = self._load_company_index(supabase)
        for row in scraped:
            cid = self._match_company(row, company_by_ticker)
            row["company_id"] = cid
            row["gurufocus_url"] = self._gurufocus_url(row, cid, company_by_ticker)

        # Replace the leonteq_equity table contents wholesale — the
        # scrape IS the snapshot, no merging.
        emit("Persisting leonteq_equity (replace-all)…", 75)
        try:
            supabase.table("leonteq_equity").delete().neq("id", 0).execute()
        except Exception as e:
            log.warning("[leonteq] failed to clear table: %s: %s", type(e).__name__, e)
        rows_to_insert = [
            {
                "name": r["name"],
                "ticker": r.get("ticker"),
                "isin": r.get("isin"),
                "sector": r.get("sector"),
                "industry": r.get("industry"),
                "gurufocus_url": r.get("gurufocus_url"),
                "company_id": r.get("company_id"),
            }
            for r in scraped
            if r.get("name")
        ]
        # Batch by 500 — Supabase's PostgREST endpoint chokes on huge
        # multi-row inserts (URL length / timeout).
        persisted = 0
        for i in range(0, len(rows_to_insert), 500):
            chunk = rows_to_insert[i:i + 500]
            try:
                supabase.table("leonteq_equity").insert(chunk).execute()
                persisted += len(chunk)
            except Exception as e:
                log.warning(
                    "[leonteq] chunk %s insert failed: %s: %s",
                    i // 500, type(e).__name__, e,
                )

        # Write current-month universe_membership for resolvable equities.
        today_month = date.today().strftime("%Y-%m")
        emit(f"Writing universe_membership for {today_month}", 85)
        try:
            supabase.table("universe_membership").delete()\
                .eq("universe_id", universe_id)\
                .eq("target_month", today_month)\
                .execute()
        except Exception as e:
            log.warning(
                "[leonteq] failed to clear membership for %s: %s: %s",
                today_month, type(e).__name__, e,
            )
        membership_rows = [
            {
                "universe_id": universe_id,
                "target_month": today_month,
                "company_id": r["company_id"],
                "universe_ticker": r.get("ticker") or "",
                "sector": r.get("sector"),
            }
            for r in scraped
            if r.get("company_id") is not None
        ]
        if membership_rows:
            for i in range(0, len(membership_rows), 500):
                chunk = membership_rows[i:i + 500]
                try:
                    supabase.table("universe_membership").upsert(
                        chunk, on_conflict="universe_id,company_id,target_month",
                    ).execute()
                except Exception as e:
                    log.warning(
                        "[leonteq] membership chunk %s upsert failed: %s: %s",
                        i // 500, type(e).__name__, e,
                    )

        emit("Computing diff vs previous month…", 95)
        # Look up "previous month" — for a snapshot template this is
        # whatever earlier month we have membership for. compute_month_diff
        # handles None gracefully by treating everything as additions.
        prev_month = self._previous_captured_month(supabase, universe_id, today_month)
        diff = self.compute_month_diff(
            supabase=supabase,
            universe_id=universe_id,
            prev_month=prev_month,
            this_month=today_month,
        )

        self.mark_refreshed(supabase, universe_id)

        log.info(
            "[leonteq] refresh complete: scraped=%s persisted=%s "
            "membership=%s diff=+%s/-%s/r%s",
            len(scraped), persisted, len(membership_rows),
            diff.additions_count, diff.removals_count, diff.renames_count,
        )
        emit("Refresh complete", 100)
        return RefreshResult(
            template_key=self.template_key,
            universe_id=universe_id,
            months_written=len(self.available_months(supabase)),
            diff=diff,
        )

    # ── Reconciliation helpers ──────────────────────────────────

    def _load_company_index(self, supabase: Client) -> dict[str, dict]:
        """Build a {ticker: company_row} index covering every company.
        Used for ticker-based matching of scraped equities. Returns a
        flat dict — duplicate tickers (rare; would mean two companies
        share the symbol on different exchanges) collide and the last
        wins."""
        out: dict[str, dict] = {}
        offset = 0
        page_size = 1000
        while True:
            resp = (
                supabase.table("company")
                .select(
                    "company_id, gurufocus_ticker, company_name, "
                    "gurufocus_exchange:gurufocus_exchange(exchange_code)"
                )
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = resp.data or []
            if not batch:
                break
            for r in batch:
                tkr = (r.get("gurufocus_ticker") or "").strip().upper()
                if tkr:
                    out[tkr] = r
            if len(batch) < page_size:
                break
            offset += page_size
        return out

    def _match_company(self, row: dict, by_ticker: dict[str, dict]) -> int | None:
        """Best-effort ticker match. Returns the company_id or None."""
        tkr = (row.get("ticker") or "").strip().upper()
        if not tkr:
            return None
        match = by_ticker.get(tkr)
        if match is None:
            return None
        return int(match["company_id"])

    def _gurufocus_url(
        self,
        row: dict,
        company_id: int | None,
        by_ticker: dict[str, dict],
    ) -> str | None:
        """Build the canonical GuruFocus URL from the matched company's
        exchange + ticker (`https://gurufocus.com/stock/{EX}:{TKR}/summary`).
        Returns None when we couldn't resolve the company."""
        if company_id is None:
            return None
        match = by_ticker.get((row.get("ticker") or "").strip().upper())
        if not match:
            return None
        exch = ((match.get("gurufocus_exchange") or {}).get("exchange_code")) or ""
        tkr = (match.get("gurufocus_ticker") or "").strip()
        if not exch or not tkr:
            return None
        return f"https://www.gurufocus.com/stock/{exch}:{tkr}/summary"

    def _previous_captured_month(
        self, supabase: Client, universe_id: int, this_month: str,
    ) -> str | None:
        """Most recent target_month for this universe that's strictly
        before `this_month`. Used as the diff baseline. Returns None
        when this is the very first refresh."""
        resp = (
            supabase.table("universe_membership")
            .select("target_month")
            .eq("universe_id", universe_id)
            .lt("target_month", this_month)
            .order("target_month", desc=True)
            .limit(1)
            .execute()
        )
        if not resp.data:
            return None
        return resp.data[0].get("target_month")
