// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import PortfolioAnalysisModal from './PortfolioAnalysisModal';
import { apiFetch } from '../../../lib/apiFetch';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../../lib/hooks/useEffectiveRole', () => ({ useIsAdmin: () => false }));
vi.mock('./BookReturnChart', () => ({ default: ({ series, daily }: { series?: { return_pct: number }; daily?: boolean }) =>
  <div data-testid="leonteq-return-chart" data-daily={String(daily)}>{series?.return_pct}</div> }));
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

it.each([false, true, 'estimated'])('keeps the same source-only holdings table with accounting available=%s', async (mode) => {
  const available = mode !== false, estimated = mode === 'estimated';
  const paths: string[] = [];
  vi.mocked(apiFetch).mockImplementation(async input => {
    const path = String(input);
    paths.push(path);
    if (!path.includes('/api/leonteq/management/topselecties/AITopSelectie/analysis')) {
      throw new Error(`Unexpected data source: ${path}`);
    }
    return new Response(JSON.stringify({
      name: 'AITopSelectie', source: 'Leonteq', benchmark: 'ACWI', weight_basis: 'book',
      holdings: 1, holdings_as_of: '2026-11-12', holdings_fetched_at: '2026-11-16T10:00:00Z',
      returns: { source: 'leonteq', portfolio_ytd_pct: 8.78, benchmark_ytd_pct: 5,
        ytd_excess_pct: 3.78, ytd_from: '2026-06-08', portfolio_as_of: '2026-11-12',
        benchmark_ytd_from: '2026-06-08', benchmark_ytd_as_of: '2026-11-12', book_reason: null },
      allocation: [{ bucket: 'Equity', pct: 75, holdings: 1, contribution_pct: available ? estimated ? 8.63 : 8.78 : null },
        { bucket: 'Cash', pct: 25, holdings: 1, contribution_pct: available ? 0 : null }],
      return_series: { returns: [{ date: '2026-06-01', cum_pct: 0 }, { date: '2026-11-12', cum_pct: 8.78 }], return_pct: 8.78 },
      axes: [], book_note: available ? 'Leonteq daily linked contributions.' : 'Leonteq opening valuation is unavailable for 2026-06-08',
      realised: { available, estimated, note: estimated ? 'Opening-capital estimates; difference shown separately.' : null,
        positions: [], certificate_adjustments: estimated ? [{ name: 'Leonteq AMC fees', label: 'Leonteq AMC fees',
          unrealised_eur: 0, realised_result_eur: -24, result_eur: -24, contribution_pct: -.24 }] : [], book_ytd_pct: available ? 8.78 : null,
        basis_eur: available ? 10000 : null, reconciles: available, comparable: available, book_as_of: '2026-11-12' },
      book_holdings: [
        { name: 'Correct Leonteq stock', isin: 'SOURCE', bucket: 'Equity', is_fund: false,
          currency: 'USD', weight_now_pct: 77.018, current_value_eur: 8378, sector: 'Technology', sources: [], via_names: [],
          own_return_source: available ? 'leonteq_transactions' : 'leonteq_average', own_return_pct: available ? 11.7067 : -12.34,
          start_value_eur: available ? 7500 : null, avg_capital_eur: available ? 7500 : null,
          result_eur: available ? 878 : null, contribution_pct: available ? estimated ? 8.63 : 8.78 : null },
        { name: 'EUR cash', isin: null, bucket: 'Cash', is_fund: false,
          currency: 'EUR', weight_now_pct: 22.982, current_value_eur: 2500, sources: [], via_names: [],
          own_return_source: 'leonteq_transactions', own_return_pct: available ? 0 : null,
          start_value_eur: available ? 2500 : null, avg_capital_eur: available ? 2500 : null,
          result_eur: available ? 0 : null, contribution_pct: available ? 0 : null },
      ],
    }), { status: 200 });
  });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => { root.render(<PortfolioAnalysisModal name="AITopSelectie" leonteqPortfolio="AITopSelectie"
      refreshSeq={estimated ? 2 : available ? 1 : 0} onClose={() => {}} />); });
    const text = document.body.textContent!;
    expect(text).toContain('Correct Leonteq stock');
    expect(text).toContain('EUR cash');
    expect(text).toContain('+8.78%');
    expect(text).toContain('+5.00%');
    expect(text).toContain('+3.78pp'); // Opening holdings are not required for aligned index returns.
    expect(host.querySelector('[data-testid="leonteq-return-chart"]')?.textContent).toBe('8.78');
    expect(host.querySelector('[data-testid="leonteq-return-chart"]')?.getAttribute('data-daily')).toBe('true');
    expect(text).toContain('Leonteq return');
    const navigation = document.querySelector('nav[aria-label="Further analysis"]');
    expect([...navigation!.querySelectorAll('button')].map(button => button.textContent)).toEqual(['Performance', 'Risk']);
    expect([...document.querySelectorAll('th')].some(cell => cell.textContent?.includes('AIRS return'))).toBe(false);
    expect(text).not.toContain('No valued positions');
    expect(text).not.toContain('Opened as a basket');
    if (estimated) {
      expect(text).not.toContain('Estimated contributions');
      expect(text).not.toContain('Unallocated source difference: 0.15 pp');
      expect([...document.querySelectorAll('th')].some(cell => cell.textContent?.includes('Contribution ≈'))).toBe(true);
      expect(text).toContain('Difference to reported return');
      expect(host.querySelector('[aria-label="Fees"]')?.textContent).toContain('-0.24pp');
      expect(host.querySelector('tfoot tr:last-child td:last-child')?.textContent).toContain('+8.39pp');
    }
    if (available) {
      expect(text).toContain('+11.71%');
      expect(text).toMatch(estimated ? /8\.39\s*pp/ : /8\.78\s*pp/);
    } else {
      expect(text).toContain('-12.34%');
    }
    expect(paths.length).toBeGreaterThan(0);
    expect(paths.every(path => path.includes('/api/leonteq/management/topselecties/'))).toBe(true);
  } finally {
    await act(async () => { root.unmount(); });
    host.remove();
  }
});
