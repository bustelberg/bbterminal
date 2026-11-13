"""Small daily factsheet preparation; never rebuild company universes."""
from scheduler_runtime.execution import Cancelled, cancel_requested


def run(ctx=None):
    from services.benchmark_factsheet import prepare
    if cancel_requested(ctx):
        raise Cancelled('Factsheet preparation cancelled before provider work')
    result = prepare(force=True)
    return 'ACWI and AGG EUR factsheet saved', {
        'prepared_at': result['prepared_at'],
        'period_years': result['period_years'],
        'rows': [{key: row.get(key) for key in ('ticker', 'period', 'available_history', 'reason')}
                 for row in result['rows']],
    }
