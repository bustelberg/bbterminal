"""ACWILeonteqTemplate — the intersection of ACWI and LEONTEQ.

A derived template. No external data source: each refresh reads the
ACWI and LEONTEQ canonical universes' per-month memberships and
writes the intersection (companies present in BOTH for that month)
to this template's own `universe` row.

Useful as a backtest universe when you want "large global names that
Leonteq is willing to write structured products on" — typically ~few-
hundred names rather than ACWI's ~2.7k.

Ordering matters: register AFTER `ACWITemplate` and `LeonteqTemplate`
in `templates/__init__.py` so the pipeline's templates phase refreshes
the two parents before this template reads from them. Otherwise an
intersection would be computed against the previous tick's parent
state, lagging by one cadence.

`earliest_date`: the later of the two parents (both 2002-01-01 today).
"""
from __future__ import annotations

import logging
from datetime import date

from supabase import Client

from deps import chunked

from .base import ProgressCallback, RefreshResult, UniverseTemplate

log = logging.getLogger(__name__)


class ACWILeonteqTemplate(UniverseTemplate):
    template_key = "ACWI_LEONTEQ"
    label = "ACWI ∩ Leonteq"
    description = (
        "Companies present in BOTH the ACWI index AND the Leonteq "
        "underlyings list, per month. Derived universe — refreshed by "
        "intersecting ACWI's and Leonteq's canonical memberships, no "
        "external data source. Typically a few hundred names; useful for "
        "backtests on the global subset Leonteq writes structured "
        "products on."
    )
    # Both parents carry 2002-01-01 as their hard backstop. Take the
    # later one in case either ever moves forward independently.
    earliest_date = date(2002, 1, 1)

    # Parent template keys, in priority order for sector resolution
    # (ACWI's MSCI-sourced sector wins; LEONTEQ's is the fallback).
    _PARENT_KEYS = ("ACWI", "LEONTEQ")

    def refresh(
        self,
        supabase: Client,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> RefreshResult:
        universe_id = self.ensure_universe_row(supabase)

        def emit(msg: str, pct: int | None = None) -> None:
            if on_progress is not None:
                on_progress(msg, pct)

        # Step 1 — resolve parent universe ids. Missing parent = empty
        # intersection (still considered a successful refresh — the
        # template just has nothing to offer until the parents land).
        emit("Resolving parent universes (ACWI + LEONTEQ)…", 5)
        parent_ids: dict[str, int] = {}
        for key in self._PARENT_KEYS:
            resp = (
                supabase.table("universe")
                .select("universe_id")
                .eq("template_key", key)
                .limit(1)
                .execute()
            )
            if not resp.data:
                emit(
                    f"Parent template '{key}' has no universe row yet — "
                    f"intersection will be empty until it's refreshed.",
                    100,
                )
                self._wipe_membership(supabase, universe_id)
                self.mark_refreshed(supabase, universe_id)
                diff = self.compute_month_diff(
                    supabase=supabase,
                    universe_id=universe_id,
                    prev_month=None,
                    this_month=date.today().strftime("%Y-%m"),
                )
                return RefreshResult(
                    template_key=self.template_key,
                    universe_id=universe_id,
                    months_written=0,
                    diff=diff,
                )
            parent_ids[key] = int(resp.data[0]["universe_id"])

        acwi_id = parent_ids["ACWI"]
        leonteq_id = parent_ids["LEONTEQ"]

        # Step 2 — pull both parents' memberships into RAM. Each parent
        # has ~290 months × ~hundreds-to-thousands of companies, so the
        # combined working set is single-digit MBs. Indexed as
        # `{month: {company_id: sector}}` (same shape the universe
        # loader produces).
        emit("Loading ACWI memberships…", 15)
        acwi = self._load_membership(supabase, acwi_id)
        emit(f"ACWI: {sum(len(v) for v in acwi.values())} rows across {len(acwi)} months.", 35)

        emit("Loading LEONTEQ memberships…", 40)
        leonteq = self._load_membership(supabase, leonteq_id)
        emit(f"LEONTEQ: {sum(len(v) for v in leonteq.values())} rows across {len(leonteq)} months.", 55)

        # Step 3 — compute intersection per month. Months present in
        # both parents are candidates; we union all parent months and
        # for each compute the set of company_ids that appear in BOTH.
        emit("Computing intersection per month…", 60)
        intersection: dict[str, dict[int, dict]] = {}
        months_in_both = sorted(set(acwi.keys()) & set(leonteq.keys()))
        for month in months_in_both:
            acwi_month = acwi[month]
            leonteq_month = leonteq[month]
            common_cids = set(acwi_month.keys()) & set(leonteq_month.keys())
            if not common_cids:
                continue
            intersection[month] = {
                cid: {
                    # ACWI sector wins (MSCI-sourced is canonical);
                    # fall back to LEONTEQ when ACWI is null.
                    "sector": acwi_month[cid].get("sector") or leonteq_month[cid].get("sector"),
                    # Universe_ticker: ACWI carries the exchange-native
                    # ticker we use for the rest of the universe, so
                    # prefer it.
                    "universe_ticker": (
                        acwi_month[cid].get("universe_ticker")
                        or leonteq_month[cid].get("universe_ticker")
                    ),
                }
                for cid in common_cids
            }

        total_rows = sum(len(v) for v in intersection.values())
        emit(
            f"Intersection: {total_rows} rows across "
            f"{len(intersection)} months "
            f"(skipped {len(months_in_both) - len(intersection)} months with empty overlap).",
            70,
        )

        # Step 4 — wipe + bulk write. Same destructive-replace pattern
        # the LEONTEQ template uses: simpler than diffing, and our
        # ~few-hundred-thousand-row write fits comfortably in a single
        # pipeline tick.
        emit("Replacing universe_membership rows…", 75)
        self._wipe_membership(supabase, universe_id)

        membership_rows: list[dict] = []
        for month, by_cid in intersection.items():
            for cid, attrs in by_cid.items():
                membership_rows.append({
                    "universe_id": universe_id,
                    "company_id": cid,
                    "target_month": month,
                    "universe_ticker": attrs.get("universe_ticker") or "",
                    "sector": attrs.get("sector"),
                })

        # Batch by 500 — matches the LEONTEQ template's batching
        # (Supabase PostgREST chokes on huge multi-row inserts; URL
        # length + per-request timeout).
        persisted = 0
        for ci, chunk in enumerate(chunked(membership_rows, 500)):
            try:
                supabase.table("universe_membership").upsert(
                    chunk, on_conflict="universe_id,company_id,target_month",
                ).execute()
                persisted += len(chunk)
            except Exception as e:
                log.warning(
                    "[acwi_leonteq] membership chunk %s upsert failed: %s: %s",
                    ci, type(e).__name__, e,
                )

        emit(f"Persisted {persisted}/{len(membership_rows)} membership rows.", 90)

        # Step 5 — diff vs previous month. `intersection` is sorted by
        # month-key order, so the last two are the most recent pair.
        emit("Computing diff vs previous month…", 95)
        sorted_months = sorted(intersection.keys())
        this_month = sorted_months[-1] if sorted_months else date.today().strftime("%Y-%m")
        prev_month = sorted_months[-2] if len(sorted_months) >= 2 else None
        diff = self.compute_month_diff(
            supabase=supabase,
            universe_id=universe_id,
            prev_month=prev_month,
            this_month=this_month,
        )

        self.mark_refreshed(supabase, universe_id)

        log.info(
            "[acwi_leonteq] refresh complete: months=%s rows=%s diff=+%s/-%s/r%s",
            len(intersection), persisted,
            diff.additions_count, diff.removals_count, diff.renames_count,
        )
        emit("Refresh complete.", 100)

        return RefreshResult(
            template_key=self.template_key,
            universe_id=universe_id,
            months_written=len(intersection),
            diff=diff,
        )

    # ── Helpers ────────────────────────────────────────────────────

    def _load_membership(
        self, supabase: Client, universe_id: int,
    ) -> dict[str, dict[int, dict]]:
        """Page through `universe_membership` for one parent universe and
        return `{month: {company_id: {sector, universe_ticker}}}`. Same
        shape `routers/momentum/backtest_stream/universe_loader.py`
        produces, lifted here so we don't add a cross-package import."""
        out: dict[str, dict[int, dict]] = {}
        offset = 0
        page = 1000
        while True:
            resp = (
                supabase.table("universe_membership")
                .select("target_month, company_id, sector, universe_ticker")
                .eq("universe_id", universe_id)
                .order("target_month")
                .order("company_id")
                .range(offset, offset + page - 1)
                .execute()
            )
            batch = resp.data or []
            for r in batch:
                month = (r.get("target_month") or "")[:7]
                if not month:
                    continue
                cid_raw = r.get("company_id")
                if cid_raw is None:
                    continue
                cid = int(cid_raw)
                out.setdefault(month, {})[cid] = {
                    "sector": r.get("sector"),
                    "universe_ticker": r.get("universe_ticker"),
                }
            if len(batch) < page:
                break
            offset += page
        return out

    def _wipe_membership(self, supabase: Client, universe_id: int) -> None:
        """Delete every row in `universe_membership` for our canonical
        universe. Same loop the LEONTEQ template uses — Supabase's
        delete-by-filter occasionally needs a couple of attempts on a
        very large set (>100k rows) to clear under load."""
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
                "[acwi_leonteq] failed to clear membership: %s: %s",
                type(e).__name__, e,
            )
