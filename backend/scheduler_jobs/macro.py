"""Refresh each Macro Dashboard series independently."""
from scheduler_runtime.execution import Cancelled, cancel_requested, require_complete


def run(ctx=None, *, report):
    from routers.macro_dashboard import SERIES, refresh_snapshot

    summary = {'saved': {}, 'errors': {}}
    for index, (key, (name, _, _)) in enumerate(SERIES.items()):
        if cancel_requested(ctx):
            raise Cancelled('Macro refresh cancelled. Completed series were saved.')
        report(index, len(SERIES), f'Refreshing {name}')
        try:
            snapshot = refresh_snapshot(key)
        except Exception as exc:
            summary['errors'][key] = f'{type(exc).__name__}: {exc}'
        else:
            summary['saved'][key] = {'observed': snapshot['points'][-1]['date'],
                                     'fetched_at': snapshot['fetched_at']}
    message = f"Refreshed {len(summary['saved'])}/{len(SERIES)} macro indicators"
    if summary['errors']:
        require_complete(f"{message}. Failed: {', '.join(summary['errors'])}. Previous observations are retained.", summary)
    return message, summary
