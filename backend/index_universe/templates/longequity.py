"""LongEquity template — wraps the existing LongEquity ingest behind the
`UniverseTemplate` interface so it appears alongside ACWI / LEONTEQ in
the /schedule Template Universes card and the /backtest universe picker.

The existing LongEquity ingest (`routers.longequity.run_longequity_ingest_sync`)
already does all the heavy lifting: probes upstream for the latest
available month via `check_latest_available_month`, downloads any
newer-than-loaded files, flattens + resolves + enriches + transforms,
loads through `load_prepared_into_supabase(universe_label='LongEquity')`,
and rebuilds the cumulative `universe_membership` rows. This template
is a thin abstraction that lets the pipeline + UI treat LongEquity the
same way they treat ACWI: one entry in `TEMPLATES`, one row in the
/schedule card, one option in /backtest, one diff per pipeline run.

Pre-existing row: an environment seeded before the template machinery
has `universe` row with `label='LongEquity'` and `template_key=NULL`.
`ensure_universe_row` (in base.py) detects this case via the
label-match fallback and stamps `template_key='LONGEQUITY'` on the
existing row in place — no migration needed, no data churn.

Cadence: LongEquity upstream releases new monthly snapshots
sporadically. `last_refreshed_at` ticks every pipeline run regardless
(reflects "we checked"), and the diff comes out 0/0/0 on the dominant
no-new-data path.
"""
from __future__ import annotations

import logging
from datetime import date

from supabase import Client

from .base import (
    ProgressCallback,
    RefreshResult,
    TemplateDiff,
    UniverseTemplate,
)

log = logging.getLogger(__name__)


def _previous_month_str(this_month: str) -> str | None:
    """'YYYY-MM' of the calendar month before `this_month`. Returns None
    when `this_month` is malformed."""
    try:
        y, m = (int(x) for x in this_month.split("-")[:2])
    except (ValueError, TypeError):
        return None
    if m == 1:
        return f"{y - 1:04d}-12"
    return f"{y:04d}-{m - 1:02d}"


class LongEquityTemplate(UniverseTemplate):
    template_key = "LONGEQUITY"
    label = "LongEquity"
    description = (
        "LongEquity research-source universe — monthly snapshots of the "
        "research-recommended equity universe, loaded from the LongEquity "
        "report archive. Idempotent: a refresh that finds nothing new "
        "upstream is a no-op."
    )
    # The cumulative LongEquity universe goes back to 2002-01 (every
    # company ever in a snapshot has a 2002-01 membership row by the
    # current `store_index_membership` convention). Matches ACWI's hard
    # backstop so the /backtest default start picker behaves consistently.
    earliest_date = date(2002, 1, 1)

    def refresh(
        self,
        supabase: Client,
        *,
        on_progress: ProgressCallback | None = None,
    ) -> RefreshResult:
        # 1. Adopt or create the canonical row. The existing pre-template
        #    row (label='LongEquity', template_key=NULL) is picked up by
        #    `ensure_universe_row`'s label-match fallback and gets its
        #    `template_key` stamped in place.
        universe_id = self.ensure_universe_row(supabase)

        # 2. Run the existing sync ingest. It probes upstream, short-
        #    circuits to no-op when nothing's newer than loaded, otherwise
        #    downloads + flattens + loads each new month and rebuilds
        #    `universe_membership`. `run_longequity_ingest_sync`'s
        #    `on_progress` takes (msg) only; bridge to ProgressCallback's
        #    (msg, pct) shape with a `None` pct.
        from routers.longequity import run_longequity_ingest_sync  # noqa: PLC0415

        def _emit_msg(msg: str) -> None:
            if on_progress is not None:
                on_progress(msg, None)

        try:
            le_result = run_longequity_ingest_sync(supabase, on_progress=_emit_msg)
        except Exception as e:
            # The ingest itself crashed (network / parser blowup / etc.).
            # Wrap in the same shape downstream code expects so the
            # diff path below still runs over whatever's already in the
            # DB — better to report 0/0/0 with a warning than to abort
            # the entire pipeline phase.
            log.warning(
                "[templates.longequity] run_longequity_ingest_sync crashed: %s: %s",
                type(e).__name__, e,
            )
            le_result = {"status": "error", "error": str(e), "months_loaded": []}

        # 3. Diff the most recent two captured months. `available_months`
        #    is ascending; we want the last pair. When fewer than two
        #    months exist (brand-new env), prev_month=None — the diff
        #    helper treats this_month's full membership as additions.
        months = self.available_months(supabase)
        this_month: str | None = months[-1] if months else None
        prev_month: str | None = None
        if len(months) >= 2:
            prev_month = months[-2]
        elif this_month:
            # Try the calendar-month predecessor even if it's not in the
            # universe — matches the ACWI template's behavior. The diff
            # helper handles "prev_month doesn't exist" gracefully.
            prev_month = _previous_month_str(this_month)

        if this_month is None:
            # Brand-new env: no membership rows yet. Synthesize a zero
            # diff so the RefreshResult contract (non-optional `diff`)
            # holds and the caller's summary array still has the right
            # shape. Subsequent ticks once data lands compute real diffs.
            log.info(
                "[templates.longequity] no LongEquity months loaded yet; "
                "synthesizing zero diff for empty universe."
            )
            diff = TemplateDiff(
                template_key=self.template_key,
                universe_id=universe_id,
                this_month="",
                prev_month=None,
                additions_count=0,
                removals_count=0,
                renames_count=0,
            )
        else:
            diff = self.compute_month_diff(
                supabase=supabase,
                universe_id=universe_id,
                prev_month=prev_month,
                this_month=this_month,
            )

        # 4. Mark refreshed unconditionally — `last_refreshed_at` reflects
        #    "we tried", not "we got new data". The /schedule card uses
        #    it to flag templates that have NEVER been refreshed; that
        #    semantic only fires when no run has ever completed.
        self.mark_refreshed(supabase, universe_id)

        log.info(
            "[templates.longequity] refresh complete: universe_id=%s status=%s "
            "months_loaded=%s diff=+%s/-%s/r%s",
            universe_id, le_result.get("status"),
            le_result.get("months_loaded"),
            diff.additions_count, diff.removals_count, diff.renames_count,
        )

        return RefreshResult(
            template_key=self.template_key,
            universe_id=universe_id,
            months_written=len(months),
            diff=diff,
        )
