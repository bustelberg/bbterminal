"""Background refreshes using the existing source readers and shared job popups."""
from fastapi import HTTPException
import threading
import time

from jobs import JobCancelled, UserFacingJobError
from leonteq.progress import observe

_results: dict[str, tuple[float, dict]] = {}
_lock = threading.Lock()


def result_for(job_id: str) -> dict | None:
    with _lock:
        cutoff = time.monotonic() - 15 * 60
        for key in [key for key, (stamp, _) in _results.items() if stamp < cutoff]:
            _results.pop(key, None)
        return _results.get(job_id, (0, None))[1]


def _save_result(ctx, status, payload):
    # Holdings belong on the admin-only result route, never on the globally readable job stream.
    result_for(ctx.job.id)
    with _lock:
        _results[ctx.job.id] = (time.monotonic(), {'status': status, 'payload': payload})

_FAILURES = {
    'busy': 'Another refresh is using this source. Please try again shortly.',
    'missing_credentials': 'The source connection is not configured. Ask an administrator to check it.',
    'login_rejected': 'The source rejected the login. Check the account before retrying.',
    'verification_required': 'The source needs additional login verification.',
    'timeout': 'The source took too long to respond. Saved data remains available.',
    'browser_error': 'The source connection could not complete. Saved data remains available.',
}


def run(ctx, operation: str, *, name: str | None = None, portfolio_id: int | None = None,
        datum: str | None = None, lang: str = 'en', source=None, isin=None, holding_name=None, days=7):
    from leonteq import portfolio_cache, composition_cache, airs_models, monthly_performance

    def say(en, nl=None):
        ctx.check()
        ctx.progress(0, 0, (nl or en) if lang == 'nl' else en)

    say('Connecting to the source; your saved data stays visible.',
        'Verbinding maken met de bron; opgeslagen gegevens blijven zichtbaar.')
    try:
        with observe(say):
            if operation == 'list':
                payload = portfolio_cache.portfolios(refresh=True)
                count = len(payload.get('portfolios') or [])
                summary = f'{count} portfolios refreshed from Leonteq.'
                if lang == 'nl':
                    summary = f'{count} portefeuilles vernieuwd uit Leonteq.'
            elif operation == 'composition':
                payload = composition_cache.composition(name, refresh=True)
                count = sum(len(group.get('positions') or []) for group in payload.get('groups') or [])
                summary = f'{name}: {count} holdings refreshed from Leonteq.'
                if lang == 'nl':
                    summary = f'{name}: {count} posities vernieuwd uit Leonteq.'
            elif operation == 'airs':
                say(f'Reading the latest AIRS holdings for {name or portfolio_id}…',
                    f'Nieuwste AIRS-posities ophalen voor {name or portfolio_id}…')
                payload = airs_models.composition(portfolio_id, datum, refresh=True)
                summary = f'{name or payload.get("portfolio")}: {len(payload.get("rows") or [])} AIRS holdings refreshed.'
                if lang == 'nl':
                    summary = f'{name or payload.get("portfolio")}: {len(payload.get("rows") or [])} AIRS-posities vernieuwd.'
            elif operation == 'monthly':
                payload = monthly_performance.comparison(name, portfolio_id, refresh=True)
                summary = f'{name}: monthly returns and differences refreshed.'
                if lang == 'nl':
                    summary = f'{name}: maandrendementen en verschillen vernieuwd.'
            elif operation == 'transactions':
                from leonteq.recent_transactions import run as recent
                payload = recent(ctx, name, portfolio_id, days)
                summary = f'{len(payload["transactions"])} booked trades ready for individual weight review.'
            elif operation == 'prices':
                from leonteq.price_checks import run as check_prices
                payload = check_prices(ctx, name=name, portfolio_id=portfolio_id, source=source,
                                       isin=isin, holding_name=holding_name) if source else check_prices(ctx)
                counts = payload['summary']
                summary = (f'{counts["portfolios"]} linked portfolios · {counts["checked"]} prices checked · '
                           f'{counts["alerts"]} differences over 5% · {counts["unavailable"]} uncheckable.')
            else:
                raise HTTPException(400, 'Unknown refresh operation')
        # Keep a recoverable result even when a source returned its older saved data.
        _save_result(ctx, 200, payload)
        failed = (payload.get('success') is False or payload.get('refresh_error')
                  or payload.get('refresh_errors') or payload.get('snapshot_saved') is False)
        if failed:
            reason = payload.get('refresh_error') or payload.get('status')
            if operation == 'prices':
                raise UserFacingJobError(summary + ' Open Prices to review uncheckable instruments and source errors.')
            raise UserFacingJobError(_FAILURES.get(reason,
                'The refresh is incomplete. Saved data remains available; review the source details and retry.'))
        ctx.progress(1, 1, 'Updated data is ready.' if lang != 'nl' else 'Bijgewerkte gegevens zijn gereed.')
        return summary
    except HTTPException as exc:
        _save_result(ctx, exc.status_code, {'detail': exc.detail})
        raise UserFacingJobError(str(exc.detail)) from None
    except (JobCancelled, UserFacingJobError):
        raise
    except Exception as exc:
        ctx.emit('diagnostic', 'The source request could not complete.', error_type=type(exc).__name__)
        raise UserFacingJobError('The source could not be refreshed. Saved data remains available; please retry.') from None
