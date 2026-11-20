"""08:00 Amsterdam source preparation for single-request valuation reads."""
from scheduler_runtime.context import LogContext
from scheduler_runtime.execution import cancel_requested, require_complete


def run(ctx=None, *, report=None):
    from services.valuation_dashboard import prepare_all
    from leonteq.management import prepare_dashboard_overview, refresh_returns
    from services.toppenberg import configured_entries
    from routers._benchmark_etf import ensure_fresh
    context = LogContext(cancelled=lambda: cancel_requested(ctx), step=report, label='Portfolio valuation')
    context.check()
    benchmark_error = None
    try:
        if ensure_fresh('ACWI', force=True, strict=True) is None:
            benchmark_error = 'No Yahoo Finance ACWI benchmark prices are available.'
    except Exception as exc:
        benchmark_error = f'Yahoo Finance ACWI refresh failed ({type(exc).__name__}).'
    returns = refresh_returns(context, configured=configured_entries())
    summary = prepare_all(context)
    if not prepare_dashboard_overview():
        summary['errors']['leonteq_dashboard'] = 'The prepared TopSelecties dashboard was not saved.'
    summary['leonteq_returns'] = returns
    if benchmark_error:
        summary['errors']['acwi_benchmark'] = benchmark_error
    if returns['errors']:
        summary['errors']['leonteq_returns'] = returns['errors']
    if summary['errors']:
        require_complete('Daily portfolio preparation incomplete; completed data was retained.', summary)
    return f"Prepared {len(summary['saved'])} portfolios", summary
