"""One tracked, cancellable job for the dashboard's forced price-check batch."""


def force_price_checks(accounts, ctx):
    from routers._airs_full_refresh import refresh_portfolio_fully  # noqa: PLC0415
    failures = []
    accounts = list(dict.fromkeys(accounts))
    ctx.progress(0, len(accounts), 'Refreshing AIRS data and price checks')
    for index, account in enumerate(accounts):
        ctx.check()
        result = refresh_portfolio_fully(portefeuille=account, cascade=False,
                                        should_stop=lambda: ctx.cancelled,
                                        on_step=lambda _d, _t, message: ctx.progress(index, len(accounts), message))
        ctx.check()
        if result.get('status') != 'ok':
            failures.append(account)
        ctx.progress(index + 1, len(accounts), f'Price check saved: {account}' if result.get('status') == 'ok'
                     else f'Price check failed: {account}')
    if failures:
        raise RuntimeError(f'{len(failures)} price check(s) failed: {", ".join(failures)}')
    return f'Price checks refreshed for {len(accounts)} portfolios'
