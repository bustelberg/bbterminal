from routers import earnings as e
from tests._fake_supabase import FakeSupabase


def test_recent_chart_points_use_one_batched_read_and_only_requested_metrics(monkeypatch):
    code = e._metric_codes('revenue')[0]
    quarterly = code.replace('annuals__', 'quarterly__')
    class Counted(FakeSupabase):
        reads = 0
        def table(self, name):
            self.reads += 1
            return super().table(name)
    fake = Counted({'metric_data': [
        {'company_id': 1, 'metric_code': quarterly, 'target_date': day, 'numeric_value': value}
        for day, value in [('2025-03-31', 10), ('2025-06-30', 20),
                           ('2025-09-30', 30), ('2025-12-31', 40), ('2026-03-31', 50)]
    ]})
    monkeypatch.setattr(e, 'supabase', fake)
    rows = e._ltm_rows(1, [{'metric_code': code, 'target_date': '2025-12-31', 'numeric_value': 100}], ['revenue'])
    assert fake.reads == 1
    assert len(rows) == 1
    assert rows[0]['numeric_value'] == 140
    assert rows[0]['target_date'] == '2026-03-31'
    assert rows[0]['metric_code'] == code.replace('annuals__', 'ltm__')


def test_quarterly_batch_keeps_windows_crossing_page_boundaries(monkeypatch):
    quarterly = e._metric_codes('revenue')[0].replace('annuals__', 'quarterly__')
    fake = FakeSupabase({'metric_data': [
        {'company_id': 1, 'metric_code': quarterly, 'target_date': day, 'numeric_value': 10}
        for day in ['2024-03-31', '2024-06-30', '2024-09-30', '2024-12-31',
                    '2025-03-31', '2025-06-30', '2025-09-30', '2025-12-31']
    ]})
    monkeypatch.setattr(e, 'supabase', fake)
    monkeypatch.setattr(e, '_PAGE', 3)
    rows = e._ttm_metric_rows(1, ['revenue'])
    assert len(rows) == 5
    assert all(row['numeric_value'] == 40 for row in rows)
    assert rows[-1]['target_date'] == '2025-12-31'


def test_narrow_growth_payload_retains_fiscal_prices_without_shipping_daily_closes(monkeypatch):
    revenue = e._metric_codes('revenue')[0]
    fake = FakeSupabase({
        'metric_data': [{'company_id': 1, 'source_code': 'gurufocus', 'metric_code': revenue,
                         'target_date': '2025-06-30', 'numeric_value': 100}],
        # FakeSupabase does not apply select aliases (numeric_value:close).
        'company_yahoo_price': [{'company_id': 1, 'target_date': '2025-06-27', 'numeric_value': 25},
                                {'company_id': 1, 'target_date': '2026-06-26', 'numeric_value': 30}],
    })
    monkeypatch.setattr(e, 'supabase', fake)
    rows = e.load_company_metric_rows(1, ['price_ps', 'revenue'])
    assert not any(row['metric_code'] == 'close_price' for row in rows)
    prices = {row['target_date']: row['numeric_value'] for row in rows if row['metric_code'] in e._metric_codes('price_ps')}
    assert prices['2025-06-30'] == 25
    assert prices['2026-06-26'] == 30
