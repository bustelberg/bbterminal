"""Shared weekly history collection and daily database-only bundle publication."""
from datetime import date

from scheduler_runtime.context import LogContext
from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete


def run(ctx=None, *, collect_history=True, report=None):
    from management_portfolios import is_dashboard_account
    from routers._airs_accounts import list_accounts
    from routers._airs_instrument_returns import collect_all
    from services.weekly_returns import publish
    def stop():
        return cancel_requested(ctx)
    context = LogContext(cancelled=stop, step=report, label='Weekly returns')
    accounts = sorted({row['portefeuille'] for row in list_accounts()
                       if row.get('holdings') and is_dashboard_account(row.get('portefeuille'))})
    result = {'accounts': [], 'errors': {}}
    for index, account in enumerate(accounts):
        context.check()
        if report:
            report(index, len(accounts), f'Preparing all weekly instrument returns: {account}')
        try:
            if collect_history:
                collect_all(account, context, date(2025, 2, 1), stop_on_flat=False)
            saved = publish(account)
            result['accounts'].append({'portefeuille': account, 'instruments': len(saved['instruments']),
                                       'through': saved['through'], 'prepared_at': saved['prepared_at']})
        except Cancelled:
            # Collection commits a week at a time and releases its locks on exit.
            try:
                publish(account)
            except Exception:
                pass  # Preserve cancellation even if database publication is unavailable.
            raise
        except Exception as exc:
            result['errors'][account] = f'{type(exc).__name__}: {exc}'
    if result['errors']:
        require_complete(f"Weekly returns preparation incomplete: {result['errors']}. Previously published bundles remain available; retry airs_weekly_returns.", result)
    return f"Weekly returns bundles saved for {len(result['accounts'])} portfolios", result
