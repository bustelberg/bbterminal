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

from deps import chunked

from .base import (
    ProgressCallback,
    RefreshResult,
    UniverseTemplate,
)

log = logging.getLogger(__name__)


# ── Cross-exchange ticker collision: disambiguation tables ───────────
#
# Leonteq's /underlyings rows carry `country` and `ric` (Reuters
# Identification Code, e.g. "2801.T" for Tokyo or "2801.TW" for
# Taipei). Either signal lets us resolve which of two same-ticker
# companies (TSE:2801 Kikkoman vs TPE:2801 Chang Hwa) the row actually
# describes. Without this, `_match_company` would fall back to bare-
# ticker lookup and silently pick whichever company loaded last.
#
# Mappings cover the exchanges in `gurufocus_exchange` — anything else
# falls through to the next tier (country → ISIN → bare ticker).

# Suffix after the dot in a RIC → our `gurufocus_exchange.exchange_code`.
# Refinitiv assigns suffixes per Market Information Code (MIC).
_RIC_SUFFIX_TO_EXCHANGE: dict[str, str] = {
    "T": "TSE",       # Tokyo
    "OS": "TSE",
    "TW": "TPE",      # Taipei
    "HK": "HKSE",     # Hong Kong
    "L": "LSE",       # London
    "PA": "XPAR",     # Paris
    "DE": "XTER",     # Xetra
    "F": "FRA",       # Frankfurt
    "MI": "MIL",      # Milan
    "MC": "XMAD",     # Madrid
    "AS": "XAMS",     # Amsterdam
    "BR": "XBRU",     # Brussels
    "LS": "XLIS",     # Lisbon
    "HE": "OHEL",     # Helsinki
    "ST": "OSTO",     # Stockholm
    "OL": "OSL",      # Oslo
    "CO": "OCSE",     # Copenhagen
    "IS": "IST",      # Istanbul
    "WA": "WAR",      # Warsaw
    "BU": "BUD",      # Budapest
    "PR": "XPRA",     # Prague
    "VI": "WBO",      # Vienna
    "SW": "XSWX",     # Zurich (SIX)
    "S": "XSWX",
    "I": "DUB",       # Dublin
    "AT": "ATH",      # Athens
    "TA": "XTAE",     # Tel Aviv
    "MX": "MEX",      # Mexico City
    "SA": "BSP",      # B3 Brazil
    "SN": "XSGO",     # Santiago
    "JO": "JSE",      # Johannesburg
    "CA": "CAI",      # Cairo
    "QA": "DSMD",     # Doha
    "DU": "DFM",      # Dubai
    "AD": "ADX",      # Abu Dhabi
    "KW": "KUW",      # Kuwait
    "SR": "SAU",      # Saudi (Tadawul)
    "BK": "BKK",      # Bangkok
    "SI": "SGX",      # Singapore
    "SS": "SHSE",     # Shanghai
    "SZ": "SZSE",     # Shenzhen
    "PS": "PHS",      # Philippines
    "KS": "XKRX",     # Korea KOSPI
    "KQ": "XKRX",     # Korea KOSDAQ
    "KL": "XKLS",     # Kuala Lumpur
    "NS": "NSE",      # NSE India
    "BO": "BSE",      # BSE India
    "NZ": "NZSE",     # New Zealand
    "N": "NYSE",
    "O": "NASDAQ",
    "P": "NYSE",      # NYSE Arca / Amex sometimes carries P
    "MX_OTC": "NASDAQ",
}

# Leonteq country → list of candidate exchange_codes in order of
# preference. Used when RIC is missing or its suffix isn't mapped.
# A list (not a single code) so multi-exchange countries (US, China,
# India, Canada) try each in turn — the first hit on
# `(ticker, exchange)` wins.
_COUNTRY_TO_EXCHANGES: dict[str, list[str]] = {
    "United States": ["NYSE", "NASDAQ", "CBOE"],
    "Japan": ["TSE"],
    "Taiwan": ["TPE", "ROCO"],
    "Hong Kong": ["HKSE"],
    "United Kingdom": ["LSE"],
    "France": ["XPAR"],
    "Germany": ["XTER", "FRA"],
    "Italy": ["MIL"],
    "Spain": ["XMAD"],
    "Netherlands": ["XAMS"],
    "Belgium": ["XBRU"],
    "Portugal": ["XLIS"],
    "Finland": ["OHEL"],
    "Sweden": ["OSTO"],
    "Norway": ["OSL"],
    "Denmark": ["OCSE"],
    "Turkey": ["IST"],
    "Poland": ["WAR"],
    "Hungary": ["BUD"],
    "Czech Republic": ["XPRA"],
    "Austria": ["WBO"],
    "Switzerland": ["XSWX"],
    "Ireland": ["DUB"],
    "China": ["SHSE", "SZSE"],
    "South Korea": ["XKRX"],
    "Malaysia": ["XKLS"],
    "Thailand": ["BKK"],
    "Singapore": ["SGX"],
    "Philippines": ["PHS"],
    "India": ["NSE", "BSE"],
    "Greece": ["ATH"],
    "Israel": ["XTAE"],
    "Mexico": ["MEX"],
    "Brazil": ["BSP"],
    "Chile": ["XSGO"],
    "South Africa": ["JSE"],
    "Egypt": ["CAI"],
    "Qatar": ["DSMD"],
    "United Arab Emirates": ["ADX", "DFM"],
    "Kuwait": ["KUW"],
    "Saudi Arabia": ["SAU"],
    "Canada": ["TSX", "TSXV"],
    "New Zealand": ["NZSE"],
}


def _exchange_from_ric(ric: str | None) -> str | None:
    """Pull the GuruFocus exchange_code out of a Reuters RIC. Returns
    None when the RIC is empty, malformed, or its suffix isn't in our
    mapping (which is fine — the caller falls back to country)."""
    if not ric:
        return None
    parts = ric.rsplit(".", 1)
    if len(parts) != 2:
        return None
    return _RIC_SUFFIX_TO_EXCHANGE.get(parts[1].strip().upper())


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
        # Three-pass reconciliation, in order of declining reliability:
        #   1. `_match_company`: (ticker, exchange) via RIC suffix,
        #      then via country, then ISIN-from-prior-snapshot, then
        #      bare ticker as a last-resort with ambiguity warning.
        #   2. OpenFIGI ISIN resolution → auto-create new `company`
        #      rows for the long tail. Greyed-out chips on /leonteq
        #      turn white on the next refresh.
        company_by_te, company_by_bare, company_by_id = self._load_company_index(supabase)
        # Load prior ISIN→company_id mapping BEFORE pass-1 so the
        # matcher can use it as its ISIN tier (was previously a
        # separate fallback pass — the inversion is what fixes the
        # bare-ticker collision bug, see _match_company docstring).
        prior_isin_map = self._load_prior_isin_company_map(supabase)

        for row in scraped:
            row["company_id"] = self._match_company(
                row, company_by_te, company_by_bare, company_by_id, prior_isin_map,
            )

        # Audit: flag matches whose scraped name has zero token overlap with
        # the matched company's name. That's almost always a cross-exchange
        # ticker collision (Kikkoman/Chang Hwa) that slipped through the
        # tiers. We collect a one-line "lq → gf" diff for each so they're
        # surfaced in full in the user-visible progress log below (not just
        # the server log) — visible, not auto-rejected; the next refresh can
        # review.
        mismatch_details: list[str] = []
        for row in scraped:
            cid = row.get("company_id")
            if cid is None:
                continue
            matched_company = company_by_id.get(int(cid))
            if not matched_company:
                continue
            if not self._name_token_overlap(
                row.get("name", ""), matched_company.get("company_name", ""),
            ):
                gf_exch = (matched_company.get("gurufocus_exchange") or {}).get("exchange_code") or ""
                gf_tkr = matched_company.get("gurufocus_ticker") or "?"
                mismatch_details.append(
                    f"lq:{row.get('ticker') or '?'} \"{row.get('name') or '?'}\" "
                    f"→ gf:{(gf_exch + ':') if gf_exch else ''}{gf_tkr} "
                    f"\"{matched_company.get('company_name') or '?'}\" "
                    f"(isin {row.get('isin') or '?'}, {row.get('country') or '?'}, cid {cid})"
                )
        name_mismatches = len(mismatch_details)
        if name_mismatches:
            log.warning(
                "[leonteq] %d name-mismatch mapping(s) — likely ticker collisions: %s",
                name_mismatches, " | ".join(mismatch_details[:10]),
            )

        auto_created = self._auto_create_via_openfigi(supabase, scraped, on_progress)
        if auto_created > 0:
            # Pick up the freshly-inserted companies for URL building.
            company_by_te, company_by_bare, company_by_id = self._load_company_index(supabase)

        for row in scraped:
            row["gurufocus_url"] = self._gurufocus_url(
                row.get("company_id"), company_by_id,
            )

        matched = sum(1 for r in scraped if r.get("company_id") is not None)
        emit(
            f"Resolved {matched}/{len(scraped)} equities to companies "
            f"(openfigi-autoresolved={auto_created}, name-mismatches={name_mismatches}).",
            72,
        )
        if name_mismatches > 0:
            emit(
                f"⚠ {name_mismatches} match(es) where the scraped name doesn't overlap the "
                f"matched company name — likely ticker-collision mismaps, review each:",
                None,
            )
            for detail in mismatch_details[:50]:
                emit(f"   • {detail}", None)
            if name_mismatches > 50:
                emit(f"   … and {name_mismatches - 50} more (full list in the server log).", None)

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
        for ci, chunk in enumerate(chunked(rows_to_insert, 500)):
            try:
                supabase.table("leonteq_equity").insert(chunk).execute()
                persisted += len(chunk)
            except Exception as e:
                log.warning(
                    "[leonteq] chunk %s insert failed: %s: %s",
                    ci, type(e).__name__, e,
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
                # Existence check, not count -- a `SELECT 1 LIMIT 1` short-
                # circuits as soon as one row is found, whereas
                # `count="exact"` runs a full COUNT(*) over the matched set.
                check = (
                    supabase.table("universe_membership")
                    .select("company_id")
                    .eq("universe_id", universe_id)
                    .limit(1)
                    .execute()
                )
                if not check.data:
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
            # Skip rows the new auto-create marked override-unavailable.
            # Their `company` row exists (so /companies shows the badge),
            # but they don't belong in Leonteq's universe_membership.
            if r.get("_out_of_scope"):
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

        # Defensive filter: if `_match_company` (the ticker-based
        # match) linked a scraped row to an existing company that
        # already carries `out_of_scope_at` (e.g. flagged by an
        # earlier retroactive sweep or by a previous Leonteq refresh
        # that ran with overrides), drop it here too. Without this,
        # a stamped row would still slip into Leonteq membership and
        # backtests would fail to find prices for it.
        if seen_company_ids:
            try:
                cid_list = list(seen_company_ids)
                # Chunk to stay under Cloudflare URL limits.
                oos_set: set[int] = set()
                for chunk in chunked(cid_list, 50):
                    oos_resp = (
                        supabase.table("company")
                        .select("company_id")
                        .in_("company_id", chunk)
                        .not_.is_("out_of_scope_at", "null")
                        .execute()
                    )
                    for row in (oos_resp.data or []):
                        oos_set.add(int(row["company_id"]))
                if oos_set:
                    per_company = [c for c in per_company if c["company_id"] not in oos_set]
                    log.info(
                        "[leonteq] excluded %s out-of-scope companies from membership",
                        len(oos_set),
                    )
            except Exception as e:
                log.warning(
                    "[leonteq] out-of-scope membership filter failed (%s: %s) — "
                    "membership may include stamped rows.",
                    type(e).__name__, e,
                )

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
            for ci, chunk in enumerate(chunked(membership_rows, 500)):
                try:
                    supabase.table("universe_membership").upsert(
                        chunk, on_conflict="universe_id,company_id,target_month",
                    ).execute()
                except Exception as e:
                    log.warning(
                        "[leonteq] membership chunk %s upsert failed: %s: %s",
                        ci, type(e).__name__, e,
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
    ) -> tuple[dict[tuple[str, str], dict], dict[str, list[dict]], dict[int, dict]]:
        """Build three indexes of every company row, used by the
        matching tiers in `_match_company`:

          - by_ticker_exchange : ``{(ticker, exchange_code): row}``
              primary lookup — both keys uppercase + stripped. Same
              ticker on different exchanges no longer collides.
          - by_bare_ticker     : ``{ticker: [row, ...]}``
              every candidate per bare ticker, so the fallback path
              can warn loudly when there's ambiguity.
          - by_id              : ``{company_id: row}``
              unchanged; backs URL building + diagnostic logging."""
        by_te: dict[tuple[str, str], dict] = {}
        by_bare: dict[str, list[dict]] = {}
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
                exch = ((r.get("gurufocus_exchange") or {}).get("exchange_code") or "").strip().upper()
                if tkr and exch:
                    by_te[(tkr, exch)] = r
                if tkr:
                    by_bare.setdefault(tkr, []).append(r)
                cid = r.get("company_id")
                if cid is not None:
                    by_id[int(cid)] = r
            if len(batch) < page_size:
                break
            offset += page_size
        return by_te, by_bare, by_id

    def _match_company(
        self,
        row: dict,
        by_te: dict[tuple[str, str], dict],
        by_bare: dict[str, list[dict]],
        by_id: dict[int, dict],
        prior_isin_map: dict[str, int],
    ) -> int | None:
        """Multi-tier company matcher, tried in order of reliability.

          1. ``(ticker, exchange)`` derived from the row's RIC suffix.
             Refinitiv's RIC suffix is a per-MIC exchange identifier,
             so this resolves cross-exchange collisions deterministically
             (TSE:2801 Kikkoman vs TPE:2801 Chang Hwa).
          2. ``(ticker, exchange)`` derived from the row's country.
             Catches rows where RIC is empty or the suffix isn't in
             our mapping. Multi-exchange countries (US, China, India,
             Canada) try each candidate exchange in turn.
          3. ISIN lookup against the prior leonteq_equity snapshot.
             Carries forward correct mappings from earlier refreshes,
             even when Leonteq's raw ticker differs from the GuruFocus
             ticker we previously resolved.
          4. Bare ticker (last resort). Logs a WARNING when the bare
             ticker resolves to multiple companies — that's exactly
             the Kikkoman/Chang Hwa case, and surfacing it lets the
             user investigate ambiguous mappings."""
        tkr = (row.get("ticker") or "").strip().upper()
        if not tkr:
            return None

        def _by_te(t: str, e: str | None) -> dict | None:
            if not e:
                return None
            m = by_te.get((t, e))
            if m is None and e == "HKSE" and t.isdigit() and len(t) < 5:
                m = by_te.get((t.zfill(5), e))
            return m

        # Tier 1: RIC-derived exchange.
        ric_exch = _exchange_from_ric(row.get("ric"))
        match = _by_te(tkr, ric_exch)
        if match:
            return int(match["company_id"])

        # Tier 2: country-derived candidates.
        country = (row.get("country") or "").strip()
        for cand_exch in _COUNTRY_TO_EXCHANGES.get(country, []):
            match = _by_te(tkr, cand_exch)
            if match:
                return int(match["company_id"])

        # Tier 3: ISIN via prior snapshot. Only trust mappings whose
        # company_id still exists (`by_id` covers every current row).
        isin = (row.get("isin") or "").strip()
        if isin:
            cid = prior_isin_map.get(isin)
            if cid is not None and int(cid) in by_id:
                return int(cid)

        # Tier 4: bare ticker (last resort). A bare-ticker hit only counts
        # when its NAME plausibly matches the scraped name — that both
        # disambiguates cross-exchange collisions (TSE:2801 Kikkoman vs
        # TPE:2801 Chang Hwa: pick the one whose name overlaps) AND rejects a
        # lone wrong-issuer collision (US Autoliv's ALV grabbing German
        # Allianz), the dominant source of bad mappings. No name overlap →
        # return None so the OpenFIGI auto-resolver finds the right company
        # from the (authoritative) ISIN instead.
        candidates = by_bare.get(tkr) or []
        if not candidates and tkr.isdigit() and len(tkr) < 5:
            candidates = by_bare.get(tkr.zfill(5)) or []
        if not candidates:
            return None

        def _preview(cands: list[dict]) -> str:
            return ", ".join(
                f"{c.get('company_name')!s} on "
                f"{(c.get('gurufocus_exchange') or {}).get('exchange_code', '?')}"
                for c in cands[:4]
            )

        named = [
            c for c in candidates
            if self._name_token_overlap(row.get("name", ""), c.get("company_name", ""))
        ]
        if not named:
            log.warning(
                "[leonteq] bare-ticker %s (country=%s ric=%s isin=%s) rejected — "
                "no name overlap with %d candidate(s): %s. Deferring to OpenFIGI/ISIN.",
                tkr, country or "?", row.get("ric") or "?", isin or "?",
                len(candidates), _preview(candidates),
            )
            return None
        if len(named) > 1:
            log.warning(
                "[leonteq] AMBIGUOUS bare-ticker %s (country=%s ric=%s isin=%s) — "
                "%d name-overlapping candidates: %s. Picking the first.",
                tkr, country or "?", row.get("ric") or "?", isin or "?",
                len(named), _preview(named),
            )
        return int(named[0]["company_id"])

    @staticmethod
    def _name_token_overlap(a: str, b: str) -> bool:
        """True when the scraped name and the matched company name plausibly
        name the same issuer. Used BOTH to flag suspicious matches and to
        accept/reject Tier-4 bare-ticker matches (so it must be lenient on
        cosmetic differences but strict on genuinely different issuers).

        Matches when they share a non-trivial token (>= 3 chars) OR one is the
        token-initial acronym of the other ('BMW' ↔ 'Bayerische Motoren
        Werke', 'LSEG' ↔ 'London Stock Exchange Group'). Names are normalized
        first: diacritics stripped, apostrophes/hyphens/dots removed (so
        "L'Oreal" ≡ "LOreal", "Argen-X" ≡ "argenx"), '&' → 'and', corporate
        suffixes / filler dropped. Genuinely different issuers (Autoliv vs
        Allianz, C3.ai vs Air Liquide) still don't match → still rejected."""
        import re  # noqa: PLC0415
        import unicodedata  # noqa: PLC0415

        # Legal suffixes are dropped everywhere; filler words are dropped for
        # token matching but KEPT for the acronym (LSEG = London Stock
        # Exchange Group includes the "G" from Group).
        _LEGAL = {
            "ag", "sa", "plc", "inc", "ltd", "co", "corp", "nv", "se", "spa",
            "ab", "oyj", "asa", "as", "adr",
        }
        _FILLER = {"the", "and", "de", "of", "holding", "holdings", "group", "grp", "company"}

        def words(s: str, *, drop_filler: bool = True) -> list[str]:
            s = unicodedata.normalize("NFKD", s or "")
            s = "".join(c for c in s if not unicodedata.combining(c)).lower()
            s = s.replace("&", " and ")
            for ch in ("'", "’", "-", "."):
                s = s.replace(ch, "")
            stop = _LEGAL | (_FILLER if drop_filler else set())
            return [w for w in re.findall(r"[a-z0-9]+", s) if w not in stop]

        wa, wb = words(a), words(b)
        ta = {w for w in wa if len(w) >= 3}
        tb = {w for w in wb if len(w) >= 3}
        if not ta or not tb:
            return True  # no comparable token on a side — can't tell, don't flag
        if ta & tb:
            return True
        # Acronym: one side is a single short token equal to the other's
        # token initials (>= 2 letters), e.g. BMW / Bayerische Motoren Werke.
        wa2, wb2 = words(a, drop_filler=False), words(b, drop_filler=False)
        for short_ws, long_ws in ((wa2, wb2), (wb2, wa2)):
            if len(short_ws) == 1 and len(long_ws) >= 2:
                token = short_ws[0]
                if 2 <= len(token) <= 6 and token == "".join(w[0] for w in long_ws):
                    return True
        return False

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

        # Lazy import to avoid an import-order foot-gun (this module
        # is loaded during template registry build; the acwi package
        # has heavier deps).
        from index_universe.acwi.exchange_map import apply_company_override  # noqa: PLC0415

        import datetime as _dt  # noqa: PLC0415
        _now_iso = _dt.datetime.now(_dt.timezone.utc).isoformat()

        new_rows: list[dict] = []
        # For each ISIN, remember the OPENFIGI-RESOLVED tuple so the
        # downstream link pass below can hit the override-corrected
        # cid by looking up `(target_exch, target_tick)` rather than
        # the OpenFIGI-raw `(exch_code, ticker)`.
        isin_to_resolution: dict[str, dict] = {}
        unmapped_exchange_counts: dict[str, int] = {}
        unmapped_examples: dict[str, list[str]] = {}
        # Track ISINs whose override said "unavailable" so the link
        # pass can recognize them and (a) link to the inserted row
        # (so /companies shows the OUT OF SCOPE badge), but (b) skip
        # them when building per-Leonteq universe_membership — those
        # tickers don't belong in any membership.
        unavailable_isins: set[str] = set()
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
            canonical_tick = canonical_ticker(res["gurufocus_ticker"], exch_code)
            # Apply `gf_ticker_overrides.json` to the OpenFIGI-resolved
            # (exchange, ticker) BEFORE insert. Three outcomes (see
            # apply_company_override docstring):
            #   - no override: insert with OpenFIGI's values.
            #   - remap: insert with the override's target values.
            #   - unavailable: insert with OpenFIGI's values + stamp
            #     out_of_scope_at, then exclude from membership.
            override = apply_company_override(exch_code, canonical_tick)
            final_exch_code = override.target_exchange
            final_tick = override.target_ticker
            final_exch_id = exchange_id_map.get(final_exch_code, exch_id)
            row = {
                "gurufocus_ticker": final_tick,
                "company_name": isin_to_name.get(res["isin"]),
                "exchange_id": final_exch_id,
            }
            if override.unavailable_reason is not None:
                row["out_of_scope_at"] = _now_iso
                row["out_of_scope_reason"] = override.unavailable_reason
                if res.get("isin"):
                    unavailable_isins.add(res["isin"])
            # Stash the FINAL resolved (exch_code, ticker) on the
            # resolution dict so the link pass can find the row we
            # actually inserted, not the OpenFIGI-raw key.
            res = dict(res)
            res["final_gurufocus_exchange"] = final_exch_code
            res["final_gurufocus_ticker"] = final_tick
            isin_to_resolution[res["isin"]] = res
            new_rows.append(row)

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

        by_te, _by_bare, _by_id = self._load_company_index(supabase)

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
            # Look up by the FINAL (override-applied) (exchange, ticker),
            # not the OpenFIGI-raw one. Falls back to the raw values for
            # resolutions that pre-date the override code path (e.g.
            # older legacy mappings whose `isin_to_resolution` entries
            # don't carry `final_*`). Exchange-aware lookup keeps this
            # path safe from the same bare-ticker collision that bit
            # the main matcher.
            lookup_tick = (res.get("final_gurufocus_ticker") or res["gurufocus_ticker"]).strip().upper()
            lookup_exch = (res.get("final_gurufocus_exchange") or res.get("gurufocus_exchange") or "").strip().upper()
            match = by_te.get((lookup_tick, lookup_exch)) if lookup_exch else None
            if match is None:
                continue
            cid = int(match["company_id"])
            row["company_id"] = cid
            # Mark the scraped row when its company is override-
            # unavailable so the membership-build step downstream can
            # skip it. (The cid is still set so the equity is
            # discoverable in /companies with the OUT OF SCOPE badge.)
            if isin in unavailable_isins:
                row["_out_of_scope"] = True
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
