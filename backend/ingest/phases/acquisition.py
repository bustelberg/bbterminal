"""Phase 0 — source acquisition.

Probes upstream sources for new data before Phase 1 rebuilds universes
against them. Today this is only the ACWI iShares XLS staleness check
(the file is committed manually — iShares blocks automated downloads —
so all we can do server-side is surface its age). LongEquity ingest
moved into its template (`LongEquityTemplate.refresh`), and Leonteq is
refreshed from inside the templates phase via its API; both are no-ops
here. Acquisition failures never abort the run — Phase 1 rebuilds
against whatever's already on disk.
"""
from __future__ import annotations

from .runlog import _Throttle, _update_run


def _run_acquisition_phase(run_id: int, needed_keys: set[str] | None = None) -> None:
    """Phase 0 — pull fresh source data from upstream before Phase 1
    rebuilds universes against it.

    `needed_keys` scopes the per-source probes to the templates the smart
    pipeline is actually refreshing this tick (the ACWI XLS check only
    runs when an enabled strategy uses ACWI). `None` probes every source
    (full/bootstrap pipeline).

    Today this phase only carries the ACWI iShares XLS staleness
    check: the file is committed manually (iShares blocks automated
    downloads via region-cookie + JS challenge), and Phase 1's
    template refresh reads from whatever's on disk. Surfacing the
    file's age here lets /schedule recent-runs flag a stale XLS
    before it silently affects months of reconstructed memberships.

    LongEquity: moved into the templates phase as `LongEquityTemplate`
    — `run_longequity_ingest_sync` runs there now (`templates/
    longequity.py::refresh`). One source of truth per universe, no
    duplicate-call problem.

    Leonteq: nothing to do — the template refresh in Phase 1 hits the
    Leonteq API directly.
    """
    throttle = _Throttle()
    log_lines: list[str] = []

    def emit(msg: str) -> None:
        log_lines.append(msg)
        if msg and throttle.should_write():
            _update_run(run_id, current_message=f"[acquisition] {msg}")

    # ── ACWI XLS age check ────────────────────────────────────
    # Only relevant when ACWI is being refreshed this tick.
    if needed_keys is not None and "ACWI" not in needed_keys:
        _update_run(run_id, current_message="[acquisition] no ACWI refresh needed — skipping XLS age check.")
        return
    try:
        from index_universe.acwi.holdings import _FILE as _ACWI_XLS_PATH  # noqa: PLC0415
        import os as _os  # noqa: PLC0415
        from datetime import datetime as _dt, timezone as _tz  # noqa: PLC0415
        mtime = _os.path.getmtime(_ACWI_XLS_PATH)
        age_days = (_dt.now(_tz.utc) - _dt.fromtimestamp(mtime, _tz.utc)).days
        if age_days >= 14:
            emit(
                f"ACWI XLS is {age_days} days old — commit a fresh "
                f"`iShares-MSCI-ACWI-ETF_fund.xls` (iShares blocks "
                f"automated downloads)."
            )
        else:
            emit(f"ACWI XLS age: {age_days} day(s).")
    except Exception as e:
        emit(f"ACWI XLS age check failed: {type(e).__name__}: {e}")

    # Final phase message picks up the last log line.
    if log_lines:
        _update_run(run_id, current_message=f"[acquisition done] {log_lines[-1]}")
