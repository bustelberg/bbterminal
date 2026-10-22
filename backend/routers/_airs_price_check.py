"""Persist price validation during refreshes; dashboard reads never run validation."""
from datetime import datetime, timezone

from deps import supabase


def _is_toppenberg(account: str) -> bool:
    """Only the four Toppenberg books are certificate-of-strategy portfolios."""
    return (account or "").strip().upper().startswith("TOPS_")


def _checked_rows(account: str, *, freshen: bool) -> tuple[dict, list[dict], list[str]]:
    """Rows that have a meaningful AIRS-versus-Yahoo price comparison.

    A Toppenberg book holds Leonteq certificates, whose quoted certificates are not the listed
    investments the strategy owns.  Resolve each mapped certificate to its Dynamic strategy and
    compare that strategy's real instruments instead.  We intentionally do not fall back to the
    certificate when a child cannot be read: a made-up wrapper comparison is worse than an
    explicitly absent check.
    """
    from routers._airs_holding_isin import resolve_account_isins  # noqa: PLC0415

    parent = resolve_account_isins(account, freshen=freshen)
    if not _is_toppenberg(account):
        return parent, list(parent.get("rows") or []), []

    from routers._airs_strategy_map import dynamic_account_for_holding  # noqa: PLC0415

    rows: list[dict] = []
    child_accounts: list[str] = []
    for row in parent.get("rows") or []:
        child = dynamic_account_for_holding(row.get("holding_name"))
        if not child:
            rows.append({**row, "airs_price_date": parent.get("as_of")})
            continue
        if child in child_accounts:
            continue
        child_accounts.append(child)
        child_result = resolve_account_isins(child, freshen=freshen)
        rows.extend({**child_row, "airs_price_date": child_result.get("as_of"),
                     "certificate_holding": row.get("holding_name")}
                    for child_row in child_result.get("rows") or [])

    # The same company can arrive through several certificates. One listed instrument has one
    # price identity, so show/check it once; retain the first child row's own AIRS valuation date.
    unique: dict[str, dict] = {}
    for row in rows:
        isin = (row.get("isin") or "").strip().upper()
        key = isin or f"name:{row.get('holding_name') or ''}"
        unique.setdefault(key, row)
    return parent, list(unique.values()), child_accounts


def _apply_symbol_overrides() -> int:
    """Make a forced price check honour reviewed ticker pins immediately after deployment."""
    from asset_pipeline.symbol_override import apply_symbol_overrides  # noqa: PLC0415
    return apply_symbol_overrides()


def _save_price_check(account: str, *, freshen: bool) -> dict:
    """Build and persist one account's compact comparison snapshot."""
    result, checked_rows, child_accounts = _checked_rows(account, freshen=freshen)
    # Keep overview payloads compact: analysis/classification details are fetched only on expand.
    fields = {'holding_name', 'isin', 'bucket', 'currency', 'airs_price_local', 'our_price_local',
              'our_price_currency', 'our_price_date', 'our_instrument', 'recent_close_range',
              'yahoo_symbol', 'served_by', 'verdict', 'airs_price_date', 'is_etf',
              'certificate_holding'}
    result = {'portefeuille': account, 'as_of': result.get('as_of'),
              'looked_through_certificates': bool(child_accounts),
              'looked_through_accounts': child_accounts,
              'rows': [{key: value for key, value in row.items() if key in fields}
                       for row in checked_rows]}
    saved = {'portefeuille': account, 'checked_at': datetime.now(timezone.utc).isoformat(),
             'result': result}
    supabase.table('airs_price_check').upsert(saved, on_conflict='portefeuille').execute()
    return saved


def save_price_check(account: str, *, enforce_symbol_overrides: bool = True) -> dict:
    if enforce_symbol_overrides:
        _apply_symbol_overrides()
    return _save_price_check(account, freshen=True)


def recheck_price_check_row(account: str, isin: str) -> dict:
    """Force Yahoo to reload one displayed holding, then save the account snapshot.

    The membership check happens against ``_checked_rows`` rather than directly against the
    parent account: Toppenberg's displayed equities belong to mapped child strategies, not to the
    certificate wrapper in the parent book.  We deliberately use the pinned execution symbol and
    ``store_series`` (not stale detection), because this is an operator-requested recheck and is
    commonly used immediately after correcting a listing.
    """
    normalized = (isin or '').strip().upper()
    if not normalized:
        raise ValueError('ISIN is required')
    _apply_symbol_overrides()
    _parent, rows, _children = _checked_rows(account, freshen=False)
    if normalized not in {(row.get('isin') or '').strip().upper() for row in rows}:
        raise ValueError(f'{normalized} is not a displayed holding of {account}')
    execution = (supabase.table('asset_execution')
                 .select('analysis_id,yahoo_symbol')
                 .eq('isin', normalized).limit(1).execute().data or [])
    if not execution or not execution[0].get('analysis_id') or not execution[0].get('yahoo_symbol'):
        raise ValueError(f'{normalized} has no resolved Yahoo listing')
    from asset_pipeline import store  # noqa: PLC0415
    row = execution[0]
    # Rechecking a row normally means "get the newest close", not "rewrite decades of bars".
    # Start with the overlapping incremental window; only a missing COPY/statistics path falls
    # back to the full, slower reload used for a genuinely unpriced instrument.
    latest = (supabase.table('asset_price').select('target_date')
              .eq('analysis_id', row['analysis_id']).not_.is_('close', 'null')
              .order('target_date', desc=True).limit(1).execute().data or [])
    if latest and latest[0].get('target_date'):
        if store.extend_series(row['analysis_id'], row['yahoo_symbol'], latest[0]['target_date']) is None:
            store.store_series(row['analysis_id'], row['yahoo_symbol'], None)
    else:
        store.store_series(row['analysis_id'], row['yahoo_symbol'], None)
    # Do not freshen every sibling after the selected row has completed. The saved snapshot reads
    # the new close and preserves the other rows exactly as they were until separately refreshed.
    return _save_price_check(account, freshen=False)


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
    # Pins are global and idempotent. Apply once per batch, not once per account: it both makes a
    # just-deployed correction visible to the very next Force recheck and avoids N identical scans.
    if not (should_stop and should_stop()):
        try:
            _apply_symbol_overrides()
        except Exception as exc:  # noqa: BLE001 — retain existing check results if pin repair fails
            result['errors']['symbol_overrides'] = f'{type(exc).__name__}: {exc}'
            return result
    for account in dict.fromkeys(accounts):
        if should_stop and should_stop():
            break
        try:
            save_price_check(account, enforce_symbol_overrides=False)
            result['saved'] += 1
        except Exception as exc:  # noqa: BLE001 — keep other accounts moving, retain last success
            result['errors'][account] = f'{type(exc).__name__}: {exc}'
    return result
