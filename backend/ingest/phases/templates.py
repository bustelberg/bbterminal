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

import logging

from deps import supabase

from .runlog import _Throttle, _update_run

_log = logging.getLogger(__name__)


def templates_needing_refresh() -> set[str]:
    """`template_key`s whose memberships need (re)building independent of any
    scheduled-strategy demand: never refreshed in this env (no `universe` row,
    or `last_refreshed_at IS NULL`) OR behind the current calendar month.

    `/backtest` + `/acwi` + the universe dropdown read `universe_membership`
    directly, so template-managed universes must stay maintained even with
    ZERO enabled scheduled strategies. The smart daily tick refreshes this set
    every run; it's a no-op once every template is current, and fires ~monthly
    at the rollover (or once on a fresh DB). On a probe failure we refresh
    defensively (err toward fresh data)."""
    from datetime import datetime, timezone  # noqa: PLC0415
    from index_universe.templates import all_templates  # noqa: PLC0415

    current_month = datetime.now(timezone.utc).strftime("%Y-%m")
    out: set[str] = set()
    for t in all_templates():
        try:
            if t.universe_id(supabase) is None or t.last_refreshed_at(supabase) is None:
                out.add(t.template_key)  # never refreshed in this env
                continue
            months = t.available_months(supabase)
            latest = months[-1] if months else None
            if latest is None or latest < current_month:
                out.add(t.template_key)  # row exists but behind the rollover
        except Exception as e:
            _log.warning(
                "[templates] staleness probe for %s failed: %s: %s — will refresh",
                t.template_key, type(e).__name__, e,
            )
            out.add(t.template_key)
    return out


def _run_templates_phase(run_id: int, only_keys: set[str] | None = None) -> int:
    """Phase 1 — refresh registered `UniverseTemplate`s. Each template's
    `refresh()` is delegated to in turn; per-template failures are captured
    in the result array as `status='error'` entries (instead of bringing
    down the whole phase), matching the per-strategy isolation pattern in
    the momentum phase.

    `only_keys` scopes the refresh to a subset of `template_key`s (the
    smart pipeline passes just the universes its enabled strategies use);
    `None` refreshes every registered template (full/bootstrap pipeline).
    The registry order is preserved so a derived template's parents refresh
    first. Returns the number of templates actually refreshed.

    The final `templates_summary` JSONB is the array of per-template
    diff entries; `current_picks_snapshot.backtest_run_id` ties momentum
    output back to its source strategy."""
    from index_universe.templates import all_templates  # noqa: PLC0415
    from index_universe.templates import _refresh_status  # noqa: PLC0415

    templates = all_templates()
    if only_keys is not None:
        templates = [t for t in templates if t.template_key in only_keys]
    if not templates:
        _update_run(run_id, templates_summary=[])
        return 0

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
    return len(templates)
