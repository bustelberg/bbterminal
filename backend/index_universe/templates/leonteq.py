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
    label = "Leonteq"
    description = (
        "Equities Leonteq lists as underlyings for structured products. "
        "Snapshot-only at the source — no per-month historical truth — "
        "but the current snapshot is replicated across every month from "
        "the earliest_date backstop to today, so the momentum backtester "
        "can use the present-day list as a survivorship-biased universe "
        "for the full history. Each refresh captures today's published "
        "list, the per-equity sector + industry classification, and "
        "(where resolvable) a link to the canonical `company` row + "
        "GuruFocus URL."
    )
    # Hard backstop. The universe doesn't actually go back to 2002, but
    # we replicate today's snapshot across every month from this date
    # forward so backtests on the full history see a consistent set.
    # Matches the LongEquity + ACWI cumulative backstops.
    earliest_date = date(2002, 1, 1)

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
        # Three-pass reconciliation:
        #   1. Ticker match against existing `company` rows.
        #   2. ISIN match against the previous `leonteq_equity` snapshot
        #      (stable across refreshes even when the Leonteq ticker
        #      doesn't equal the GuruFocus ticker).
        #   3. OpenFIGI ISIN resolution → auto-create new `company`
        #      rows for the long tail. Greyed-out chips on /leonteq
        #      turn white on the next refresh.
        company_by_ticker, company_by_id = self._load_company_index(supabase)

        for row in scraped:
            row["company_id"] = self._match_company(row, company_by_ticker)

        prior_isin_map = self._load_prior_isin_company_map(supabase)
        prior_hits = 0
        for row in scraped:
            if row.get("company_id") is None and row.get("isin"):
                cid = prior_isin_map.get(row["isin"])
                if cid is not None and cid in company_by_id:
                    row["company_id"] = cid
                    prior_hits += 1

        auto_created = self._auto_create_via_openfigi(supabase, scraped, on_progress)
        if auto_created > 0:
            # Pick up the freshly-inserted companies for URL building.
            _by_ticker, company_by_id = self._load_company_index(supabase)

        for row in scraped:
            row["gurufocus_url"] = self._gurufocus_url(
                row.get("company_id"), company_by_id,
            )

        matched = sum(1 for r in scraped if r.get("company_id") is not None)
        emit(
            f"Resolved {matched}/{len(scraped)} equities to companies "
            f"(prior-isin={prior_hits}, openfigi-autoresolved={auto_created}).",
            72,
        )

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

        # Replicate today's snapshot across every month from
        # `earliest_date` to today so the momentum backtester sees the
        # same set on every period (survivorship-biased universe over
        # the full history; matches the cumulative LongEquity model).
        today = date.today().replace(day=1)
        months: list[str] = []
        cur = date(self.earliest_date.year, self.earliest_date.month, 1)
        while cur <= today:
            months.append(cur.strftime("%Y-%m"))
            cur = (
                date(cur.year + 1, 1, 1)
                if cur.month == 12
                else date(cur.year, cur.month + 1, 1)
            )

        emit(f"Replicating membership across {len(months)} months ({months[0]} -> {months[-1]})", 85)

        # Wipe ALL existing memberships for this universe — past runs
        # may have written only current-month rows, and we don't want
        # stragglers if the company set has changed since.
        try:
            for _attempt in range(20):
                supabase.table("universe_membership").delete().eq(
                    "universe_id", universe_id,
                ).execute()
                check = (
                    supabase.table("universe_membership")
                    .select("company_id", count="exact", head=True)
                    .eq("universe_id", universe_id)
                    .execute()
                )
                if (check.count or 0) == 0:
                    break
        except Exception as e:
            log.warning(
                "[leonteq] failed to clear membership: %s: %s",
                type(e).__name__, e,
            )

        # Dedupe by company_id BEFORE building rows. Multiple Leonteq
        # scrape entries can resolve to the same `company_id` (dual-listed
        # names: BHP on LSE+ASX, Shell on LSE+NYSE, Samsung's share
        # classes, …). Without this dedupe, the upsert batch contains
        # duplicate (universe_id, company_id, target_month) tuples and
        # Postgres rejects the entire chunk with "ON CONFLICT DO UPDATE
        # command cannot affect row a second time" — the prior code
        # caught the exception silently, so zero memberships persisted.
        seen_company_ids: set[int] = set()
        per_company: list[dict] = []
        for r in scraped:
            cid = r.get("company_id")
            if cid is None:
                continue
            cid_int = int(cid)
            if cid_int in seen_company_ids:
                continue
            seen_company_ids.add(cid_int)
            per_company.append({
                "company_id": cid_int,
                "universe_ticker": r.get("ticker") or "",
                "sector": r.get("sector"),
                "industry": r.get("industry"),
            })

        # Replicate per-company entries across every month.
        membership_rows: list[dict] = []
        for m in months:
            for entry in per_company:
                membership_rows.append({
                    "universe_id": universe_id,
                    "target_month": m,
                    **entry,
                })
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
        # Now that we replicate across history, every refresh writes to
        # every month — so "this month" is the most recent one we just
        # wrote (always today's month) and "prev_month" is the one
        # before it in the replication.
        this_month = months[-1]
        prev_month = months[-2] if len(months) >= 2 else None
        diff = self.compute_month_diff(
            supabase=supabase,
            universe_id=universe_id,
            prev_month=prev_month,
            this_month=this_month,
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

    def _load_company_index(
        self, supabase: Client,
    ) -> tuple[dict[str, dict], dict[int, dict]]:
        """Build {ticker: company_row} + {company_id: company_row}
        indexes covering every company. The ticker map drives pass-1
        matching of scraped equities; the id map backs URL building
        (so OpenFIGI-resolved rows whose Leonteq ticker differs from
        the GuruFocus ticker still get a working link). Duplicate
        tickers (rare — same symbol on two exchanges) collide and the
        last wins in `by_ticker` only."""
        by_ticker: dict[str, dict] = {}
        by_id: dict[int, dict] = {}
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
                    by_ticker[tkr] = r
                cid = r.get("company_id")
                if cid is not None:
                    by_id[int(cid)] = r
            if len(batch) < page_size:
                break
            offset += page_size
        return by_ticker, by_id

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
        company_id: int | None,
        by_id: dict[int, dict],
    ) -> str | None:
        """Build the canonical GuruFocus URL via the shared
        `ingest.gurufocus_url` helper. Returns None when we couldn't
        resolve the company."""
        from ingest.gurufocus_url import gurufocus_url  # noqa: PLC0415
        if company_id is None:
            return None
        match = by_id.get(int(company_id))
        if not match:
            return None
        exch = (match.get("gurufocus_exchange") or {}).get("exchange_code")
        tkr = match.get("gurufocus_ticker")
        return gurufocus_url(tkr, exch)

    # ── Auto-resolution helpers ─────────────────────────────────

    def _load_prior_isin_company_map(self, supabase: Client) -> dict[str, int]:
        """Read ISIN → company_id from the existing `leonteq_equity`
        rows. Lets a refresh reuse OpenFIGI resolutions from prior
        snapshots without re-querying OpenFIGI every time, AND keeps
        the link stable even when Leonteq's raw ticker differs from
        the canonical GuruFocus ticker we resolved last time."""
        out: dict[str, int] = {}
        offset = 0
        page_size = 1000
        while True:
            resp = (
                supabase.table("leonteq_equity")
                .select("isin, company_id")
                .not_.is_("isin", "null")
                .not_.is_("company_id", "null")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            batch = resp.data or []
            if not batch:
                break
            for r in batch:
                isin = (r.get("isin") or "").strip()
                cid = r.get("company_id")
                if isin and cid is not None:
                    out[isin] = int(cid)
            if len(batch) < page_size:
                break
            offset += page_size
        return out

    def _load_exchange_id_map(self, supabase: Client) -> dict[str, int]:
        """exchange_code → exchange_id from gurufocus_exchange. An
        OpenFIGI resolution to an exchange that's not in this map is
        an exchange we don't trade (Russia, AU/NZ, …) — skip silently."""
        resp = (
            supabase.table("gurufocus_exchange")
            .select("exchange_id,exchange_code")
            .limit(1000)
            .execute()
        )
        return {r["exchange_code"]: r["exchange_id"] for r in (resp.data or [])}

    def _auto_create_via_openfigi(
        self,
        supabase: Client,
        scraped: list[dict],
        on_progress: ProgressCallback | None,
    ) -> int:
        """Resolve the ISINs of still-unmatched rows via OpenFIGI and
        upsert new rows into `company` so the link pass picks them up.
        Returns the number of scraped rows whose `company_id` was set
        as a result.

        If OpenFIGI returns an exchange code that's not in
        `gurufocus_exchange`, we log the code (with row counts) so the
        mapping in `resolve_tickers._EXCHCODE_MAP` or the
        `gurufocus_exchange` table can be extended — Leonteq's entire
        underlying list sits on exchanges we support, so an
        unmapped code is a mapping bug rather than an "unsupported
        region" filter."""
        unresolved = [
            r for r in scraped
            if r.get("company_id") is None and r.get("isin")
        ]
        if not unresolved:
            return 0

        if on_progress is not None:
            try:
                on_progress(
                    f"Auto-resolving {len(unresolved)} unknown equities via OpenFIGI…",
                    65,
                )
            except Exception:
                pass

        from ingest.resolve_tickers import resolve_isins_via_openfigi  # noqa: PLC0415

        resolutions = resolve_isins_via_openfigi(
            [
                {"isin": r["isin"], "country": r.get("country")}
                for r in unresolved
            ],
            on_progress=on_progress,
            progress_start=65,
            progress_end=70,
        )
        if not resolutions:
            log.info("[leonteq] OpenFIGI returned no resolutions for %s ISINs.", len(unresolved))
            return 0

        exchange_id_map = self._load_exchange_id_map(supabase)
        isin_to_name = {r["isin"]: r.get("name") for r in unresolved if r.get("isin")}
        isin_to_country = {r["isin"]: r.get("country") for r in unresolved if r.get("isin")}
        isin_to_lq_ticker = {r["isin"]: r.get("ticker") for r in unresolved if r.get("isin")}

        new_rows: list[dict] = []
        isin_to_resolution: dict[str, dict] = {}
        unmapped_exchange_counts: dict[str, int] = {}
        unmapped_examples: dict[str, list[str]] = {}
        for res in resolutions:
            exch_code = res["gurufocus_exchange"]
            exch_id = exchange_id_map.get(exch_code)
            if exch_id is None:
                unmapped_exchange_counts[exch_code] = (
                    unmapped_exchange_counts.get(exch_code, 0) + 1
                )
                # Keep up to 3 example rows per unmapped code for the
                # SSE emit — names + Leonteq tickers + countries are
                # what the user needs to chase down the mapping bug.
                bucket = unmapped_examples.setdefault(exch_code, [])
                if len(bucket) < 3:
                    name = isin_to_name.get(res["isin"]) or "?"
                    lq_ticker = isin_to_lq_ticker.get(res["isin"]) or "?"
                    country = isin_to_country.get(res["isin"]) or "?"
                    raw_exch = res.get("openfigi_exch_code") or "?"
                    bucket.append(
                        f"{name} [lq:{lq_ticker} → gf:{res['gurufocus_ticker']} "
                        f"raw_exch:{raw_exch} country:{country}]"
                    )
                continue
            from ingest.dedupe import canonical_ticker  # noqa: PLC0415
            isin_to_resolution[res["isin"]] = res
            new_rows.append({
                # Canonical form (HKSE zero-padded, Nordic share-class
                # punctuation normalized) so two iShares/Leonteq sources
                # for the same security can't drift into separate rows.
                "gurufocus_ticker": canonical_ticker(
                    res["gurufocus_ticker"], exch_code,
                ),
                "company_name": isin_to_name.get(res["isin"]),
                "exchange_id": exch_id,
            })

        if unmapped_exchange_counts:
            summary = ", ".join(
                f"{code}={n}"
                for code, n in sorted(
                    unmapped_exchange_counts.items(), key=lambda kv: -kv[1],
                )
            )
            total_unmapped = sum(unmapped_exchange_counts.values())
            log.warning(
                "[leonteq] OpenFIGI returned %s rows on exchange codes not in "
                "gurufocus_exchange — extend resolve_tickers._EXCHCODE_MAP or "
                "the gurufocus_exchange table: %s",
                total_unmapped, summary,
            )
            if on_progress is not None:
                try:
                    on_progress(
                        f"{total_unmapped} resolutions on unmapped exchanges: {summary}",
                        None,
                    )
                    for code, examples in sorted(
                        unmapped_examples.items(), key=lambda kv: -unmapped_exchange_counts[kv[0]],
                    ):
                        for ex in examples:
                            on_progress(f"  [{code}] {ex}", None)
                except Exception:
                    pass

        if not new_rows:
            log.info(
                "[leonteq] OpenFIGI: queried=%s resolutions=%s none had a "
                "mapped exchange_id.",
                len(unresolved), len(resolutions),
            )
            return 0

        # Idempotent upsert — existing `(ticker, exchange)` rows are
        # left alone (ignore_duplicates). We re-fetch the index after
        # to pick up the new ids.
        try:
            supabase.table("company").upsert(
                new_rows,
                on_conflict="gurufocus_ticker,exchange_id",
                ignore_duplicates=True,
            ).execute()
        except Exception as e:
            log.warning(
                "[leonteq] auto-create company upsert failed: %s: %s",
                type(e).__name__, e,
            )
            return 0

        by_ticker, _by_id = self._load_company_index(supabase)

        created = 0
        for row in scraped:
            if row.get("company_id") is not None:
                continue
            isin = row.get("isin")
            if not isin:
                continue
            res = isin_to_resolution.get(isin)
            if res is None:
                continue
            match = by_ticker.get(res["gurufocus_ticker"].strip().upper())
            if match is None:
                continue
            cid = int(match["company_id"])
            row["company_id"] = cid
            # Tag with company_source for auditability ("which template
            # created this company row") — best-effort, never fatal.
            try:
                supabase.table("company_source").upsert(
                    {"company_id": cid, "source_code": "leonteq"},
                    on_conflict="company_id,source_code",
                    ignore_duplicates=True,
                ).execute()
            except Exception:
                pass
            created += 1

        log.info(
            "[leonteq] OpenFIGI auto-resolution: queried=%s resolutions=%s "
            "mapped=%s linked=%s",
            len(unresolved), len(resolutions), len(new_rows), created,
        )
        if on_progress is not None:
            try:
                on_progress(
                    f"OpenFIGI auto-resolved {created}/{len(unresolved)} unknown equities.",
                    70,
                )
            except Exception:
                pass
        return created

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
