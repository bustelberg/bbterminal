"""08:00 Amsterdam source preparation for single-request valuation reads."""
from scheduler_runtime.context import LogContext
from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete


def run(ctx=None, *, report=None):
    from services import portfolio_valuation as V, portfolio_membership as M, valuation_dashboard as D
    context = LogContext(cancelled=lambda: cancel_requested(ctx), step=report, label='Portfolio valuation')
    summary = {'saved': [], 'errors': {}}
    try:
        M.refresh_holdings(context, source='all')
    except Cancelled:
        raise
    except Exception as exc:
        summary['errors']['holdings'] = str(exc)
    roster = V.portfolios()
    selected = {row['id']: row for row in roster if row.get('id') is not None}
    summary['unavailable_portfolios'] = [{'name': row['name'], 'account': row.get('account'), 'reason': row.get('unavailable_reason')} for row in roster if row.get('id') is None]
    source_cache = {}
    for index, (portfolio_id, portfolio) in enumerate(selected.items()):
        context.check()
        if report:
            report(index, len(selected), f"Preparing portfolio: {portfolio['name']}")
        try:
            payload = D.refresh(portfolio_id,
                lambda event: context.progress(event.get('done', 0), event.get('total', 0), event.get('message', '')),
                context.check, refresh_holdings=False, source_cache=source_cache)
            summary['saved'].append(portfolio_id)
            if payload.get('errors'):
                summary['errors'][str(portfolio_id)] = payload['errors']
        except Cancelled:
            raise
        except Exception as exc:
            summary['errors'][str(portfolio_id)] = type(exc).__name__
    if not selected:
        summary['errors']['roster'] = 'No linked dashboard portfolios found.'
    if summary['errors']:
        require_complete('Daily portfolio preparation incomplete; completed data was retained.', summary)
    return f"Prepared {len(summary['saved'])} portfolios", summary
