"""Shared best-effort progress reporting for scheduled and manual workflows."""
import logging

log = logging.getLogger(__name__)


def reporter(ctx):
    """Send progress to the run receipt and, when present, the live job stream.

     A REPORTER MUST NEVER BE THE REASON A SCAN FAILS — the rule `airs_vermogen._step` already
    states. The work is the scan; the line on screen is a courtesy, and a listener that raised
    would lose a refresh that had already downloaded everything.

    """
    def step(done, total, message) -> None:
        try:
            from job_runlog import record_progress

            event = (int(done or 0), int(total or 0), str(message)[:300])
            record_progress(*event)
            if ctx is not None:
                ctx.progress(*event)
        except Exception:  # noqa: BLE001
            log.debug("[scheduler] progress listener raised", exc_info=True)
    return step
