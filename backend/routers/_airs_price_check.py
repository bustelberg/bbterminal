"""Persist price validation during refreshes; dashboard reads never run validation."""
from datetime import datetime, timezone

from deps import supabase


def save_price_check(account: str) -> dict:
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415

    result = resolve_account_isins(account, freshen=True)
    # Keep overview payloads compact: analysis/classification details are fetched only on expand.
    fields = {'holding_name', 'isin', 'bucket', 'currency', 'airs_price_local', 'our_price_local',
              'our_price_currency', 'our_price_date', 'our_instrument', 'recent_close_range',
              'served_by', 'verdict'}
    result = {'portefeuille': account, 'as_of': result.get('as_of'),
              'rows': [{key: value for key, value in row.items() if key in fields}
                       for row in result.get('rows') or []]}
    saved = {'portefeuille': account, 'checked_at': datetime.now(timezone.utc).isoformat(),
             'result': result}
    supabase.table('airs_price_check').upsert(saved, on_conflict='portefeuille').execute()
    return saved


def saved_price_checks(accounts: list[str], *, client=None) -> dict[str, dict]:
    if not accounts:
        return {}
    client = client if client is not None else supabase
    rows = []
    for offset in range(0, len(accounts), 100):
        rows.extend(client.table('airs_price_check').select('portefeuille,checked_at,result')
                    .in_('portefeuille', accounts[offset:offset + 100]).execute().data or [])
    return {row['portefeuille']: row for row in rows}


def refresh_price_checks(accounts=None, should_stop=None) -> dict:
    """Serial bounded vendor work after source refresh; individual failures stay visible."""
    if accounts is None:
        from routers._airs_accounts import list_accounts  # noqa: PLC0415
        accounts = [row['portefeuille'] for row in list_accounts() if row.get('holdings')]
    result = {'saved': 0, 'errors': {}}
    for account in dict.fromkeys(accounts):
        if should_stop and should_stop():
            break
        try:
            save_price_check(account)
            result['saved'] += 1
        except Exception as exc:  # noqa: BLE001 — keep other accounts moving, retain last success
            result['errors'][account] = f'{type(exc).__name__}: {exc}'
    return result
