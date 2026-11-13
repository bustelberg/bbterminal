"""Daily database-only valuation publication, with per-portfolio atomic saves."""
from scheduler_runtime.context import LogContext
from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete


def run(ctx=None, *, report=None):
    from services.portfolio_valuation import portfolios, publish

    def stop():
        return cancel_requested(ctx)

    context = LogContext(cancelled=stop, step=report, label='Portfolio valuation')
    roster = portfolios()
    # Several dashboard accounts can share one Fixed model. Publish it once.
    by_model = {}
    for row in roster:
        if row['id'] is not None:
            by_model.setdefault(row['id'], row)
    selected = list(by_model.values())
    if not selected:
        require_complete('No linked dashboard Fixed models found; refresh the dashboard roster and connect its accounts to AIRS Fixed models first.',
                         {'errors': {'roster': 'no linked models'}, 'unavailable_portfolios': roster})
    summary = {'saved': [], 'errors': {}, 'unavailable_portfolios': [
        {'name': row['name'], 'account': row.get('account'), 'reason': row.get('unavailable_reason')}
        for row in roster if row['id'] is None]}
    inputs = {}
    for index, portfolio in enumerate(selected):
        context.check()
        if report:
            report(index, len(selected), f"Preparing valuation: {portfolio['name']}")
        try:
            payload = publish(portfolio['id'], check_cancel=context.check, input_cache=inputs)
            summary['saved'].append({'portfolio_id': portfolio['id'], 'companies': len(payload['rows']),
                                     'calculated': sum(row['status'] == 'ok' for row in payload['rows'])})
        except Cancelled:
            raise
        except Exception as exc:
            summary['errors'][str(portfolio['id'])] = f"{portfolio['name']}: {type(exc).__name__}: {exc}"
    if summary['errors']:
        require_complete(f"Daily valuation incomplete: {summary['errors']}. Completed portfolio publications remain saved; retry portfolio_valuation_daily.", summary)
    return f"Valuation saved for {len(summary['saved'])} portfolios", summary
