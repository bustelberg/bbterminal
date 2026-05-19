"""ACWI template — wraps the existing iShares + MSCI reconstruction
pipeline behind the `UniverseTemplate` interface.

The actual reconstruction logic (parsing the bundled iShares XLS,
fuzzy-matching MSCI announcements, walking months and deciding
membership) lives in `routers/index_universe/acwi.py::run_acwi_save_universe`
— we delegate to it rather than duplicate. The template's job is the
abstraction: ensure the canonical universe row exists with
`template_key='ACWI'` set, run the reconstruction with the canonical
label, compute the month-over-month diff, return a `RefreshResult`.

Refresh strategy: full rebuild from `earliest_date` (2002-01-01) every
time. The reconstruction logic underneath wipes-and-reinserts all
membership rows for the universe. Slow (~30-60s per pipeline run) but
deterministic and correct — late-arriving MSCI announcements never
get stranded. Optimize to incremental updates later if the pipeline's
weekly latency becomes a bottleneck.
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


def _previous_month_str(today: date) -> str:
    """'YYYY-MM' of the calendar month before `today`."""
    if today.month == 1:
        return date(today.year - 1, 12, 1).strftime("%Y-%m")
    return date(today.year, today.month - 1, 1).strftime("%Y-%m")


class ACWITemplate(UniverseTemplate):
    template_key = "ACWI"
    label = "ACWI"
    description = (
        "iShares MSCI ACWI ETF — feasible-universe reconstruction "
        "(Europe + Asia + Middle East + USA, where GuruFocus coverage exists). "
        "Monthly memberships from 2002-01 onward, refreshed continuously by the pipeline."
    )
    earliest_date = date(2002, 1, 1)

    def refresh(
        self,
        supabase: Client,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> RefreshResult:
        # 1. Make sure the canonical universe row exists with the
        #    template_key set. `run_acwi_save_universe` would otherwise
        #    create a row with just `label='ACWI'` (no template_key),
        #    which would then not be findable by the template loader.
        universe_id = self.ensure_universe_row(supabase)

        # 2. Reconstruct from earliest_date → today. Delegates to the
        #    existing sync worker; we just pass the canonical label so
        #    its lookup-by-label finds the row we already ensured.
        from routers.index_universe.acwi import (  # noqa: PLC0415 (heavy module)
            run_acwi_save_universe,
        )

        today = date.today()
        run_acwi_save_universe(
            self.label,
            self.earliest_date.isoformat(),
            today.isoformat(),
            on_progress=on_progress,
        )

        # 3. Diff the just-written current month against the previous
        #    one. Both should exist after the reconstruction; if not,
        #    `prev_month` falls back to None and the diff treats the
        #    current month's full membership as additions.
        this_month = today.strftime("%Y-%m")
        prev_month = _previous_month_str(today)

        diff = self.compute_month_diff(
            supabase=supabase,
            universe_id=universe_id,
            prev_month=prev_month,
            this_month=this_month,
        )

        months = self.available_months(supabase)

        # Mark refreshed (bumps `universe.last_refreshed_at` and
        # invalidates the in-process caches for this template). Must
        # happen AFTER the writes so cached reads can't beat the
        # serialized timestamp.
        self.mark_refreshed(supabase, universe_id)

        log.info(
            "[templates.acwi] refresh complete: universe_id=%s months=%s "
            "diff=+%s/-%s/r%s",
            universe_id, len(months),
            diff.additions_count, diff.removals_count, diff.renames_count,
        )

        return RefreshResult(
            template_key=self.template_key,
            universe_id=universe_id,
            months_written=len(months),
            diff=diff,
        )
