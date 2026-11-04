"""Monthly benchmark recovery, shared by the daily job and UI recovery."""
from datetime import date, timedelta


class BenchmarkMissing(RuntimeError):
    pass


def ensure(account, month, ctx=None, *, repair=True):
    from routers._airs_monthly_report import _account, month_window, _monthly_rows, _weighted_months, benchmark_policy
    _account(account)
    start, end, _ = month_window(month)
    if not benchmark_policy(account):
        return {'applicable': False}
    rows = [row for row in _monthly_rows(account, int(month[:4])) if row['month'] == month]
    if not rows:
        raise RuntimeError('AIRS has not reported a return for this month yet.')
    result = _weighted_months(account, rows).get(month)
    if result:
        return {'applicable': True, 'benchmark': result, 'repaired': False}
    if not repair:
        raise BenchmarkMissing('Monthly benchmark data is missing.')
    from routers._benchmark_etf import ensure_fresh
    from momentum.data import sync_fx_rates_to_db
    from deps import supabase
    for index, label in enumerate(('ACWI', 'BONDS')):
        if ctx:
            ctx.check()
            ctx.progress(index, 3, 'Updating benchmark…')
        ensure_fresh(label, force=True, bypass_daily_guard=True, full_history=True, strict=True)
    if ctx:
        ctx.check()
        ctx.progress(2, 3, 'Updating benchmark…')
    sync_fx_rates_to_db(supabase, ['USD'], date.fromisoformat(start) - timedelta(days=7), date.fromisoformat(end))
    result = _weighted_months(account, rows).get(month)
    if not result:
        raise RuntimeError('Benchmark data is still incomplete. Please try again later.')
    return {'applicable': True, 'benchmark': result, 'repaired': True}


def ensure_daily(accounts, ctx=None):
    from jobs import JobCancelled
    from airs_vermogen import latest_completed_market_date
    from datetime import datetime
    from zoneinfo import ZoneInfo
    month = latest_completed_market_date(datetime.now(ZoneInfo('Europe/Amsterdam')).date())[:7]
    results, errors, missing = {}, {}, []
    for account in accounts:
        if ctx:
            ctx.check()
        try:
            results[account] = ensure(account, month, ctx, repair=False)
        except BenchmarkMissing:
            missing.append(account)
        except JobCancelled:
            raise
        except Exception as exc:
            errors[account] = str(exc)
    if missing:
        try:
            # All policies use the same two ETFs/FX. Repair their shared sources
            # once, then verify every portfolio without repeated vendor calls.
            results[missing[0]] = ensure(missing[0], month, ctx)
            for account in missing[1:]:
                try:
                    results[account] = ensure(account, month, ctx, repair=False)
                except JobCancelled:
                    raise
                except Exception as exc:
                    errors[account] = str(exc)
        except JobCancelled:
            raise
        except Exception as exc:
            errors.update({account: str(exc) for account in missing})
    return {'month': month, 'accounts': results, 'errors': errors}
