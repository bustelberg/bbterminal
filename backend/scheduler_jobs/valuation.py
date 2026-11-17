"""08:00 Amsterdam source preparation for single-request valuation reads."""
from scheduler_runtime.context import LogContext
from scheduler_runtime.execution import cancel_requested, require_complete


def run(ctx=None, *, report=None):
    from services.valuation_dashboard import prepare_all
    from leonteq.management import refresh_returns
    context = LogContext(cancelled=lambda: cancel_requested(ctx), step=report, label='Portfolio valuation')
    returns = refresh_returns(context)
    summary = prepare_all(context)
    summary['leonteq_returns'] = returns
    if returns['errors']:
        summary['errors']['leonteq_returns'] = returns['errors']
    if summary['errors']:
        require_complete('Daily portfolio preparation incomplete; completed data was retained.', summary)
    return f"Prepared {len(summary['saved'])} portfolios", summary
