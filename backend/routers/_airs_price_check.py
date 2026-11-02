"""Persist price validation during refreshes; dashboard reads never run validation."""
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

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


def _build_price_check(account: str, *, freshen: bool) -> dict:
    """Build one comparison without writing, so a batch can share its source reads."""
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
    return saved


def _save_price_check(account: str, *, freshen: bool) -> dict:
    saved = _build_price_check(account, freshen=freshen)
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


def _price_check_version() -> str | None:
    """Use the existing source-counter fingerprint, excluding discovery-only roster writes.

    Discovery updates last_seen_at even on a no-op scan. Comparison inputs, including child
    holdings, prices, mappings and FX, must invalidate checks; discovery alone must not.
    """
    import hashlib
    import json
    from common.pg import _run_copy
    from routers._analysis_cache import _STAMP_SQL, _WATCHED
    from routers._prepared_analyse import code_version
    from asset_pipeline.symbol_override import load_symbol_overrides, alias_price_override_isins
    from asset_pipeline.isin_alias import load_aliases
    try:
        stamp = _run_copy(_STAMP_SQL, ([table for table in _WATCHED if table != 'airs_account_roster'],))
        if stamp is None:
            return None
        config = json.dumps([code_version(), load_symbol_overrides(), load_aliases(),
                             sorted(alias_price_override_isins())], sort_keys=True)
        return hashlib.sha256(stamp.getvalue() + config.encode()).hexdigest()
    except Exception:
        return None  # Unverified sources always miss the cache.


def _current_check(saved: dict, version: str | None) -> bool:
    payload = saved.get('result')
    if version is None or not isinstance(payload, dict) or payload.get('input_version') != version:
        return False
    if not isinstance(saved.get('checked_at'), str):
        return False
    try:
        checked = datetime.fromisoformat(saved['checked_at'].replace('Z', '+00:00'))
        now = datetime.now(ZoneInfo('Europe/Amsterdam'))
        return checked.tzinfo is not None and checked <= now and checked.astimezone(now.tzinfo).date() == now.date()
    except (KeyError, TypeError, ValueError):
        return False


def refresh_price_checks(accounts=None, should_stop=None, *, freshen: bool = True,
                         only_if_changed: bool = False, on_step=None) -> dict:
    """Reuse current comparisons or build a shared-source batch with visible progress.

    Normal dashboard refreshes set freshen=False: the market jobs own Yahoo fetching.
    Explicit force/recheck callers can still request freshen=True.
    """
    if should_stop and should_stop():
        return {'saved': 0, 'skipped': 0, 'errors': {}, 'cancelled': True}
    if accounts is None:
        from routers._airs_accounts import list_accounts  # noqa: PLC0415
        from management_portfolios import is_dashboard_account, dashboard_refresh_key
        accounts = sorted((row['portefeuille'] for row in list_accounts()
                           if row.get('holdings') and is_dashboard_account(row.get('portefeuille'))),
                          key=dashboard_refresh_key)
    accounts = list(dict.fromkeys(accounts))
    result = {'saved': 0, 'skipped': 0, 'errors': {}}
    if not accounts:
        return result
    def stopped():
        return bool(should_stop and should_stop())
    def step(done, total, message):
        if on_step:
            on_step(done, total, message)
    if stopped():
        return {**result, 'cancelled': True}
    # Pins are global and idempotent. Apply once per batch, not once per account: it both makes a
    # just-deployed correction visible to the very next Force recheck and avoids N identical scans.
    if freshen:
        try:
            step(0, 0, 'Checking reviewed Yahoo listing corrections…')
            _apply_symbol_overrides()
        except Exception as exc:  # noqa: BLE001 — retain existing check results if pin repair fails
            result['errors']['symbol_overrides'] = f'{type(exc).__name__}: {exc}'
            return result
    step(0, 0, 'Checking which saved price comparisons need updating…')
    version = _price_check_version() if only_if_changed else None
    previous = saved_price_checks(accounts) if only_if_changed else {}
    todo = [account for account in accounts if not (only_if_changed and _current_check(previous.get(account) or {}, version))]
    result['skipped'] = len(accounts) - len(todo)
    if not todo:
        step(0, 0, f'All {len(accounts)} saved price checks are already current')
        return result
    from common.read_cache import read_cache
    from management_portfolios import dashboard_progress_label
    pending = []
    # Build first, persist second: each upsert clears read_cache. Keeping those writes outside
    # this scope lets Toppenberg and the building blocks reuse the same child resolution.
    with read_cache('batch price comparisons'):
        for index, account in enumerate(todo):
            if stopped():
                break
            label = dashboard_progress_label(account)
            step(index, 2 * len(todo), f'{label} · comparing AIRS with {"Yahoo" if freshen else "stored"} prices…')
            try:
                saved = _build_price_check(account, freshen=freshen)
                if version is not None:
                    saved['result']['input_version'] = version
                pending.append(saved)
                step(index + 1, 2 * len(todo), f'{label} · comparison ready to save')
            except Exception as exc:  # noqa: BLE001 — keep other accounts moving, retain last success
                result['errors'][account] = f'{type(exc).__name__}: {exc}'
                step(index + 1, 2 * len(todo), f'{label} · comparison failed: {exc}')
    # Preserve completed work even when cancellation stopped the next comparison.
    for index, saved in enumerate(pending, 1):
        account = saved['portefeuille']
        step(len(todo) + index - 1, 2 * len(todo), f'{dashboard_progress_label(account)} · saving price check…')
        try:
            supabase.table('airs_price_check').upsert(saved, on_conflict='portefeuille').execute()
            result['saved'] += 1
        except Exception as exc:
            result['errors'][account] = f'{type(exc).__name__}: {exc}'
    result['cancelled'] = stopped()
    completed = len(pending) + len(result['errors']) + result['saved']
    step(completed if result['cancelled'] else 2 * len(todo), 2 * len(todo),
         f"{'Cancelled; ' if result['cancelled'] else ''}price checks: {result['saved']} saved, "
         f"{result['skipped']} already current, {len(result['errors'])} failed")
    return result
