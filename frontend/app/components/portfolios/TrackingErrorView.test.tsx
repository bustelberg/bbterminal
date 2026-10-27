// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { beforeEach, afterEach, describe, expect, it, vi } from 'vitest';
import type { TrackingError } from '../../../lib/types/api';
import TrackingErrorView from './TrackingErrorView';
import { startJob } from '../../../lib/stores/jobs';

const state = vi.hoisted(() => ({ response: {} as TrackingError }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('./useRiskResult', () => ({ riskRequestKey: () => 'simulation', useRiskResult: () => ({ data: state.response, error: null }) }));
vi.mock('../InfoTip', () => ({ default: () => null }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: vi.fn(() => new Promise(() => {})) }));

describe('shared hypothetical performance view', () => {
  let root: Root;
  let host: HTMLDivElement;
  beforeEach(() => {
    vi.mocked(startJob).mockClear();
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
    host = document.createElement('div'); document.body.append(host); root = createRoot(host);
    state.response = { available: true, benchmark: '60% ACWI + 40% AGGH.AS', observations: 24,
      frequency: 'monthly', periods_per_year: 12, history_limited: true,
      covered_portfolio_pct: 60, simulation_scope: 'Yahoo-only direct equities',
      excluded_holdings: [{ isin: 'ETF', name: 'Excluded ETF', weight_pct: 20, reason: 'ETF excluded' }],
      window_from: '2024-09-30', window_to: '2026-09-30', requested_window_from: '2021-09-30', requested_window_to: '2026-09-30',
      portfolio_return_ann_pct: 4, benchmark_return_ann_pct: 5, tracking_error_pct: 2,
      mean_active_per_period_pct: -.1, active_return_std_per_period_pct: .577,
      historical_comparison: { available: true, portfolio_cagr_pct: 4, benchmark_cagr_pct: 5, active_return_pp: -1, years_elapsed: 2 },
      benchmark_history: { cagr_pct: 5, start_date: '2024-09-30', end_date: '2026-09-30', complete: false },
      history_issues: [{ isin: 'FUND', name: 'New bond ETF', kind: 'holding', weight_pct: 12,
        first_price_date: '2024-09-01', last_price_date: '2026-10-26', missing_months: 36, reason: 'Missing month-end EUR prices' }],
    };
  });
  afterEach(async () => { await act(async () => root.unmount()); host.remove(); });
  const render = () => act(async () => { root.render(<TrackingErrorView holdings={[{ isin: 'FUND', name: 'New bond ETF', weight_pct: 12 }, { isin: 'OLD', name: 'Old holding', weight_pct: 88 }]} benchmark="ACWI" portfolioName="Portfolio" portfolioSource="airs_volk" />); });

  it('shows the shortened period, both CAGRs, monthly TE and the holding causing missing history', async () => {
    await render();
    expect(host.textContent).toContain('Available-period equity simulation');
    expect(host.textContent).toContain('covers 60.00% of today’s portfolio');
    expect(host.textContent).toContain('Excluded ETF');
    expect(host.textContent).toContain('30 September 2024 to 30 September 2026');
    expect(host.textContent).toContain('24 completed monthly returns');
    expect(host.textContent).toContain('4.00%');
    expect(host.textContent).toContain('5.00%');
    expect(host.textContent).toContain('-1.00pp');
    expect(host.textContent).toContain('New bond ETF');
    expect(host.textContent).not.toContain('preliminary');
    expect(startJob).toHaveBeenCalledTimes(1);
    const [url, , request] = vi.mocked(startJob).mock.calls[0];
    expect(url).toContain('/tracking-error/repair/job');
    expect(url).toContain('full_history=true');
    expect(JSON.parse(String(request?.body)).holdings.map((holding: { isin: string }) => holding.isin)).toEqual(['FUND']);
  });
  it('still shows missing holdings when too little shared history prevents calculation', async () => {
    state.response = { ...state.response, available: false, reason: 'At least 12 complete monthly returns are required.' };
    await render();
    expect(host.textContent).toContain('At least 12 complete monthly returns');
    expect(host.textContent).toContain('New bond ETF');
    expect(host.textContent).toContain('Hypothetical tracking error');
    expect(host.textContent).not.toContain('4.00%');
    expect(host.querySelector('details')?.open).toBe(false);
  });
});
