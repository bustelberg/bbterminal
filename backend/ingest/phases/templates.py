"""Phase 1 — template-managed universe refresh.

Walks every registered `UniverseTemplate` (ACWI, Leonteq, LongEquity,
…) and refreshes each in turn. Per-template failures are isolated into
`status='error'` entries in the result array rather than bringing down
the whole phase (same isolation pattern as the momentum phase). The
accumulated array is the run's `templates_summary` JSONB; the phase
raises a summary error at the end if any template failed so the outer
pipeline marks the run `error`.
"""
from __future__ import annotations

from deps import supabase

from .runlog import _Throttle, _update_run


def _run_templates_phase(run_id: int) -> None:
    """Phase 1 — refresh every registered `UniverseTemplate` (currently
    just ACWI). Each template's `refresh()` is delegated to in turn;
    per-template failures are captured in the result array as
    `status='error'` entries (instead of bringing down the whole phase),
    matching the per-strategy isolation pattern in the momentum phase.

    The final `templates_summary` JSONB is the array of per-template
    diff entries; `current_picks_snapshot.backtest_run_id` ties momentum
    output back to its source strategy."""
    from index_universe.templates import all_templates  # noqa: PLC0415
    from index_universe.templates import _refresh_status  # noqa: PLC0415

    templates = all_templates()
    if not templates:
        _update_run(run_id, templates_summary=[])
        return

    throttle = _Throttle()
    summaries: list[dict] = []
    errors: list[str] = []

    for idx, t in enumerate(templates, start=1):
        prefix = f"[{idx}/{len(templates)} {t.label}]"
        _update_run(run_id, current_message=f"{prefix} starting refresh…")

        def on_progress(message: str, _pct: int | None = None, _prefix=prefix) -> None:
            if message and throttle.should_write():
                _update_run(run_id, current_message=f"{_prefix} {message}")

        try:
            # tracked_refresh mirrors live progress into the in-process
            # status registry so the /schedule template rows show a busy
            # spinner + progress bar for scheduled (not just manual) runs.
            result = _refresh_status.tracked_refresh(
                t, supabase, extra_on_progress=on_progress,
            )
            summaries.append(result.diff.to_summary_entry())
        except Exception as e:
            msg = f"{type(e).__name__}: {e}"
            errors.append(f"[{t.label}] {msg}")
            # Stub entry so the UI can still render which template
            # was attempted, with an inline error.
            summaries.append({
                "template_key": t.template_key,
                "universe_id": t.universe_id(supabase),
                "this_month": None,
                "prev_month": None,
                "additions_count": 0,
                "removals_count": 0,
                "renames_count": 0,
                "additions": [],
                "removals": [],
                "renames": [],
                "error": msg,
            })

        # Persist incrementally — multi-template runs let the UI see
        # each template land independently rather than waiting for the
        # whole phase to finish.
        _update_run(run_id, templates_summary=summaries)

    if errors:
        raise RuntimeError(
            f"{len(errors)} of {len(templates)} templates failed: "
            + " | ".join(errors[:3])
        )
