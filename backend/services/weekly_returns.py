"""Publish complete portfolio returns with one atomic database write; reads never scrape AIRS."""
from datetime import datetime, timezone

from deps import supabase

TABLE = 'airs_weekly_returns_bundle'


def publish(account):
    from routers._airs_instrument_returns import bundle
    payload = bundle(account)
    stamp = datetime.now(timezone.utc).isoformat()
    supabase.table(TABLE).upsert({'portefeuille': account, 'prepared_at': stamp,
                                 'payload': payload}, on_conflict='portefeuille').execute()
    return {**payload, 'prepared_at': stamp, 'ready': True}


def read(account):
    from routers._airs_monthly_report import _account
    _account(account)
    try:
        rows = (supabase.table(TABLE).select('payload,prepared_at').eq('portefeuille', account)
                .limit(1).execute().data or [])
    except Exception as exc:
        from fastapi import HTTPException
        raise HTTPException(503, 'Saved weekly returns could not be read. Check database availability '
                            'and apply migration 20261113170000_weekly_returns_bundles.sql if it is missing; '
                            f'then run airs_weekly_returns. Database error: {type(exc).__name__}') from exc
    if not rows:
        return {'portefeuille': account, 'ready': False,
                'detail': 'Weekly returns have not been prepared. Run airs_weekly_returns or wait for the next daily AIRS preparation.',
                'options': [], 'instruments': {}}
    row = rows[0]
    return {**row['payload'], 'ready': True, 'prepared_at': row['prepared_at']}


def collect_and_publish(account, work):
    # The collector releases AIRS locks before this publication. Committed partial
    # observations remain useful after a provider failure or cancellation.
    try:
        result = work()
    except BaseException:
        try:
            publish(account)
        except Exception:
            import logging
            logging.getLogger(__name__).exception('Could not publish committed weekly observations for %s', account)
        raise
    else:
        publish(account)
        return result
