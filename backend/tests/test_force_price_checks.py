import pytest

from routers._airs_force_price_checks import force_price_checks


class Context:
    cancelled = False
    def __init__(self):
        self.updates = []
    def progress(self, done, total, message):
        self.updates.append((done, total, message))
    def check(self):
        if self.cancelled:
            raise RuntimeError('cancelled')


def test_batch_uses_full_refresh_and_reports_generic_progress(monkeypatch):
    seen = []
    def refresh(**kwargs):
        assert kwargs['cascade'] is False
        seen.append(kwargs['portefeuille'])
        return {'status': 'ok'}
    monkeypatch.setattr('routers._airs_full_refresh.refresh_portfolio_fully', refresh)
    ctx = Context()
    assert force_price_checks(['A', 'A', 'B'], ctx) == 'Price checks refreshed for 2 portfolios'
    assert seen == ['A', 'B']
    assert ctx.updates[-1][:2] == (2, 2)


def test_failed_accounts_do_not_stop_batch_and_do_not_report_success(monkeypatch):
    monkeypatch.setattr('routers._airs_full_refresh.refresh_portfolio_fully', lambda **kwargs: {'status': 'error'})
    ctx = Context()
    with pytest.raises(RuntimeError, match='2 price check'):
        force_price_checks(['A', 'B'], ctx)
    assert ctx.updates[-1][:2] == (2, 2)


def test_cancel_prevents_next_refresh(monkeypatch):
    ctx = Context()
    ctx.cancelled = True
    monkeypatch.setattr('routers._airs_full_refresh.refresh_portfolio_fully', lambda **kwargs: pytest.fail('cancelled'))
    with pytest.raises(RuntimeError, match='cancelled'):
        force_price_checks(['A'], ctx)
