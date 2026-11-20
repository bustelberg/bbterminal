"""Manual, tab-scoped refresh of dashboard source returns only."""
from __future__ import annotations


def refresh(ctx, collection: str) -> str:
    from jobs import UserFacingJobError

    if collection == 'toppenberg':
        from leonteq.management import refresh_returns, prepare_dashboard_overview
        from services.toppenberg import configured_entries
        result = refresh_returns(ctx, configured=configured_entries())
        ctx.check()
        if not prepare_dashboard_overview():
            result['errors']['dashboard'] = 'Updated returns could not be published.'
        if result['errors']:
            raise UserFacingJobError('Toppenberg return sources incomplete: ' + '; '.join(
                f'{name}: {reason}' for name, reason in result['errors'].items()))
        return f"Refreshed {len(result['saved'])} underlying Leonteq return series"

    if collection == 'topselecties':
        from leonteq.management import refresh_returns, prepare_dashboard_overview

        def publish_row(done, total, account):
            if prepare_dashboard_overview():
                ctx.progress(done, total, f'{account} — MTD and YTD updated',
                             account=account, account_done=True)

        result = refresh_returns(ctx, on_saved=publish_row)
        ctx.check()
        if not prepare_dashboard_overview():
            result['errors']['dashboard'] = 'Updated returns could not be published.'
        if result['errors']:
            raise UserFacingJobError('Return refresh incomplete: ' + '; '.join(
                f'{name}: {reason}' for name, reason in result['errors'].items()))
        return f"MTD and YTD updated for {len(result['saved'])} portfolios"

    from airs_vermogen import _LOCK, _roster_names, latest_completed_market_date
    from airs_scanner import download_portfolio_sync
    from management_portfolios import _account_details, dashboard_refresh_key, is_dashboard_account
    from routers.airs import _parse_att_excel, _save_performance_to_db

    if not _LOCK.acquire(blocking=False):
        raise UserFacingJobError('Another AIRS refresh is running. Retry when it finishes.')
    try:
        names = sorted({name for name in _roster_names()
                        if is_dashboard_account(name) and _account_details(name)[0] == collection},
                       key=dashboard_refresh_key)
        if not names:
            raise UserFacingJobError('No saved portfolios found for this tab.')
        through = latest_completed_market_date()
        errors = []
        for index, name in enumerate(names):
            ctx.check()
            _, label = _account_details(name)
            ctx.progress(index, len(names), f'{label} — refreshing MTD and YTD')
            try:
                rows = _parse_att_excel(download_portfolio_sync(name, through[:4] + '-01-01', through))
                if not rows:
                    raise ValueError('AIRS returned no performance rows')
                _save_performance_to_db(name, rows, prune_after=through)
            except Exception as exc:
                errors.append(f'{label}: {exc}')
            else:
                ctx.progress(index + 1, len(names), f'{label} — MTD and YTD updated',
                             account=name, account_done=True)
        ctx.check()
        if errors:
            raise UserFacingJobError('Return refresh incomplete: ' + '; '.join(errors))
        return f'MTD and YTD updated for {len(names)} portfolios'
    finally:
        _LOCK.release()
