"""08:00 Amsterdam source preparation for single-request valuation reads."""
from scheduler_runtime.context import LogContext
from scheduler_runtime.execution import cancel_requested, require_complete


def run(ctx=None, *, report=None):
    from services.valuation_dashboard import prepare_all
    context = LogContext(cancelled=lambda: cancel_requested(ctx), step=report, label='Portfolio valuation')
    summary = prepare_all(context)
    if summary['errors']:
        require_complete('Daily portfolio preparation incomplete; completed data was retained.', summary)
    return f"Prepared {len(summary['saved'])} portfolios", summary
