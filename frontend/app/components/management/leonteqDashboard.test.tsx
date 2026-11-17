// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, expect, it, vi } from 'vitest';
import PortfolioOverviewPanel from '../PortfolioOverviewPanel';
import { apiFetch } from '../../../lib/apiFetch';
import { startJob } from '../../../lib/stores/jobs';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../../lib/hooks/useEffectiveRole', () => ({ useIsAdmin: () => false }));
vi.mock('../PortfoliosPanel', () => ({ LinkCell: () => null }));
vi.mock('next/dynamic', () => ({ default: () => (props: { basket?: unknown; reportAccount?: string; leonteqPortfolio?: string }) =>
  <div data-testid="analysis" data-basket={JSON.stringify(props.basket)} data-account={props.reportAccount}
    data-leonteq={props.leonteqPortfolio} /> }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: vi.fn(), cancelJob: vi.fn(), isAirsBusy: () => false }));
Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });

afterEach(() => { vi.clearAllMocks(); window.sessionStorage.clear(); });

it('uses the identical overview table and Analyse control with Leonteq data and refresh only', async () => {
  const source = { name: 'AITopSelectie', dynamic_portefeuille: 'AITopSelectie',
    management_group: 'topselecties', link_source: 'leonteq', holdings: 2,
    ytd_pct: 8.78, latest_month_pct: 1.82,
    periode: `${new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Amsterdam', year: 'numeric', month: '2-digit' }).format(new Date())}-01`,
    ytd_from: '2026-06-01', as_of: '2026-11-12' };
  const requests: string[] = [];
  vi.mocked(apiFetch).mockImplementation(async input => {
    const path = String(input);
    requests.push(path);
    const body = path.endsWith('/dashboard') ? [source]
      : path.endsWith('/AITopSelectie') ? { success: true, composition: { groups: [{ positions: [
        { isin: 'CORRECT', name: 'Leonteq stock', weight: '75.00%' },
        { isin: null, name: 'Cash', weight: '25.00%' },
      ] }] } }
      : path.endsWith('/api/airs/portfolios/overview') ? [{ ...source, management_group: 'toppenberg',
        name: 'Toppenberg Offensief', dynamic_portefeuille: 'TOPS_OFF', fixed_portfolio_id: 42 }]
      : null;
    if (!body) throw new Error(`Unexpected source request: ${path}`);
    return new Response(JSON.stringify(body), { status: 200 });
  });
  vi.mocked(startJob).mockResolvedValue({ id: 'test', done: Promise.resolve({ status: 'done' }), body: {} } as Awaited<ReturnType<typeof startJob>>);
  window.sessionStorage.setItem('bbterminal:airs-overview:v1', JSON.stringify({ cachedAt: Date.now(),
    rows: [{ ...source, name: 'WRONG AIRS TOPSELECTIE', ytd_pct: 999 }] }));
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<PortfolioOverviewPanel key="leonteq" collection="topselecties" />));
    expect(host.textContent).toContain('+8.78%');
    expect(host.textContent).toContain('+1.82%');
    expect(host.textContent).toContain('1 June 2026');
    expect(host.textContent).toContain('12 November 2026');
    expect(host.textContent).not.toContain('WRONG AIRS');
    const table = host.querySelector('table')!;
    const headers = [...table.querySelectorAll('thead th')].map(cell => cell.textContent);
    const tableClass = table.className;
    expect(headers).toEqual(['#', '', 'Name▲', 'Month-to-date▾', 'Year-to-date▾', 'Since', 'To']);
    const findButton = (text: string) => [...host.querySelectorAll('button')].find(button => button.textContent?.trim() === text)!;
    await act(async () => findButton('Analyse').click());
    const modal = host.querySelector('[data-testid="analysis"]')!;
    expect(modal.hasAttribute('data-basket')).toBe(false);
    expect(modal.getAttribute('data-leonteq')).toBe('AITopSelectie');
    expect(modal.hasAttribute('data-account')).toBe(false);
    await act(async () => findButton('Refresh all').click());
    expect(vi.mocked(startJob).mock.calls[0][0]).toContain('/api/leonteq/management/topselecties/AITopSelectie/refresh/job');
    expect(requests.every(path => path.includes('/api/leonteq/management/topselecties'))).toBe(true);
    await act(async () => root.render(<PortfolioOverviewPanel key="airs" collection="toppenberg" />));
    expect(host.querySelector('table')!.className).toBe(tableClass);
    expect([...host.querySelectorAll('thead th')].map(cell => cell.textContent)).toEqual(
      headers.map(header => header === 'Name▲' ? 'Name▼' : header));
    expect(host.textContent).toContain('Toppenberg Offensief');
    expect(requests.at(-1)).toContain('/api/airs/portfolios/overview');
  } finally {
    await act(async () => root.unmount());
    host.remove();
  }
});
