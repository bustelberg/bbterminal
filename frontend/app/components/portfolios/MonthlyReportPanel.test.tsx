// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot, type Root } from 'react-dom/client';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { startJob } from '../../../lib/stores/jobs';
import MonthlyReportPanel from './MonthlyReportPanel';

vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: vi.fn() }));

const observation = { periode: '2026-10-23', rendement: -2, beleggingsresultaat: -200,
  beginvermogen: 10000, eindvermogen: 10800, stortingen: 1000, onttrekkingen: 0,
  koersresultaat: -210, opbrengsten: 10, kosten: 0, mutatie_opgelopen_rente: 0 };
const response = () => ({ portefeuille: 'BOOK', month: '2026-10', start: '2026-10-01',
  end: '2026-10-23', opening_date: '2026-09-30', months: [{ month: '2026-10', ...observation }],
  needs_refresh: true, warning: null, fetched_at: null, snapshot: null });
const completeResponse = () => ({ ...response(), needs_refresh: false, snapshot: {
  start: '2026-10-01', end: '2026-10-23', fetched_at: '2026-10-26T12:00:00Z',
  positions: [], sections: { performance: { status: 'ok', data: observation } },
} });
const job = (status: 'done' | 'failed', summary = '') => ({ id: 'job', body: {}, done: Promise.resolve({
  id: 'job', kind: 'report', label: 'Report', title: 'Report', status, done: 4, total: 4, summary,
} as Awaited<Awaited<ReturnType<typeof startJob>>['done']>) });

describe('AIRS monthly report', () => {
  let root: Root;
  let host: HTMLDivElement;
  beforeEach(() => {
    vi.resetAllMocks();
    vi.setSystemTime(new Date('2026-10-26T12:00:00Z'));
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
    host = document.createElement('div'); document.body.appendChild(host);
    root = createRoot(host);
  });
  afterEach(async () => { await act(async () => root.unmount()); host.remove(); vi.useRealTimers(); });
  const openOctober = async () => {
    const button = host.querySelectorAll<HTMLButtonElement>('nav[aria-label="Months"] button')[9];
    await act(async () => button.click());
  };

  it('opens a month with only its reported return and individual positions', async () => {
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...completeResponse(), weighted_benchmark: {
      '2026-10': { return_pct: 1.23, start_date: '2026-09-30', end_date: '2026-10-23', source: 'ACWI ETF · EUR' },
    } })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    expect(host.textContent).toContain('-2.00%');
    expect(host.querySelectorAll('nav button')[9].textContent).toContain('Weighted benchmark · EUR +1.23%');
    expect(host.querySelector('nav[aria-label="Months"] button')?.textContent).toContain('Jan 2026');
    expect(host.querySelector('input[type="month"]')).toBeNull();
    expect(host.querySelector('#monthly-report-details')).toBeNull();
    await openOctober();
    expect(host.querySelector('#monthly-report-details')).not.toBeNull();
    expect(host.querySelector('#monthly-report-details')?.textContent).toContain('Weighted benchmark return · EUR');
    expect(host.querySelector('#monthly-report-details')?.textContent).toContain('+1.23%');
    expect(host.textContent).not.toContain('€');
    expect(host.textContent).toContain('Individual positions');
    expect(host.textContent).toContain('No positions were found');
    for (const removed of ['Price result', 'Income and costs', 'Investment result', 'Deposits', 'opening capital']) {
      expect(host.textContent).not.toContain(removed);
    }
    expect(host.textContent).not.toContain('Selection');
    expect(startJob).not.toHaveBeenCalled();
    expect(vi.mocked(apiFetch).mock.calls[0][0]).toContain('/accounts/BOOK/monthly-report?month=2026-10');
  });

  it('highlights only the toggled-on tile and clears the highlight when closed', async () => {
    vi.mocked(apiFetch).mockImplementation(async (url) => {
      const month = new URL(String(url), 'http://localhost').searchParams.get('month');
      return new Response(JSON.stringify({ ...completeResponse(), month }));
    });
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    const tiles = host.querySelectorAll<HTMLButtonElement>('nav button');
    expect(host.querySelector('nav button[aria-pressed="true"]')).toBeNull();
    await openOctober();
    expect(tiles[9].getAttribute('aria-pressed')).toBe('true');
    expect(tiles[9].className).toContain('bg-accent-500/10');
    await act(async () => tiles[8].click());
    expect(tiles[9].getAttribute('aria-pressed')).toBe('false');
    expect(tiles[9].className).not.toContain('bg-accent-500/10');
    expect(tiles[8].getAttribute('aria-pressed')).toBe('true');
    await act(async () => tiles[8].click());
    expect(host.querySelector('nav button[aria-pressed="true"]')).toBeNull();
  });

  it('reserves the loaded detail height when toggling the selected month closed', async () => {
    const bounds = vi.spyOn(HTMLElement.prototype, 'getBoundingClientRect').mockReturnValue(new DOMRect(0, 0, 800, 640));
    try {
      vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(completeResponse())));
      await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
      await openOctober();
      const space = host.querySelector<HTMLElement>('[data-month-detail-space]');
      const nav = host.querySelector('nav');
      expect(space?.style.minHeight).toBe('640px');
      await openOctober();
      expect(host.querySelector('#monthly-report-details')).toBeNull();
      expect(host.querySelector('nav')).toBe(nav);
      expect(space?.style.minHeight).toBe('640px');
      await openOctober();
      expect(space?.style.minHeight).toBe('640px');
    } finally { bounds.mockRestore(); }
  });

  it('keeps the month tiles mounted while another month loads without showing the previous positions', async () => {
    let resolveReport!: (response: Response) => void;
    vi.mocked(apiFetch).mockResolvedValueOnce(new Response(JSON.stringify({ ...completeResponse(), snapshot: {
      ...completeResponse().snapshot, positions: [{ name: 'October only', return_pct: 1, status: 'held', contribution_calculation: null }],
    } }))).mockImplementationOnce(() => new Promise((resolve) => { resolveReport = resolve; }));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    await openOctober();
    const nav = host.querySelector('nav');
    const details = host.querySelector('#monthly-report-details');
    expect(host.textContent).toContain('October only');
    await act(async () => host.querySelectorAll<HTMLButtonElement>('nav button')[8].click());
    expect(host.querySelector('nav')).toBe(nav);
    expect(host.querySelector('#monthly-report-details')).toBe(details);
    expect(host.textContent).not.toContain('October only');
    expect(details?.getAttribute('aria-busy')).toBe('true');
    await act(async () => resolveReport(new Response(JSON.stringify({ ...completeResponse(), month: '2026-09' }))));
    expect(host.querySelector('nav')).toBe(nav);
    expect(details?.getAttribute('aria-busy')).toBe('false');
  });

  it('uses the Analyse YTD source rather than inventing a second headline from monthly returns', async () => {
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(completeResponse())));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio"
      overviewReturns={{ portfolio: 3.754, benchmark: 9.614, asOf: '2026-10-23', from: '2026-01-01' }} />));
    const summary = host.querySelector('[aria-label="Compounded returns"]');
    expect(summary?.textContent).toContain('Return (YTD) · EUR');
    expect(summary?.textContent).toContain('+3.75%');
    expect(summary?.textContent).toContain('+9.61%');
    expect(summary?.textContent).toContain('-5.86pp');
    expect(summary?.textContent).not.toContain('Compounded portfolio');
  });

  it('keeps January monthly returns separate from a January-to-October YTD headline', async () => {
    const data = { ...completeResponse(), months: [
      { ...observation, month: '2026-01', periode: '2026-01-30', rendement: -0.05 },
      { ...observation, month: '2026-10', periode: '2026-10-29' },
    ], weighted_benchmark: {
      '2026-01': { return_pct: 1.38, start_date: '2025-12-31', end_date: '2026-01-30', source: 'Policy' },
    } };
    vi.mocked(apiFetch).mockImplementation(async (url) => new Response(JSON.stringify({
      ...data, month: new URL(String(url), 'http://localhost').searchParams.get('month'),
    })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Bustelberg Offensief"
      overviewReturns={{ portfolio: 6.64, benchmark: 19.26, asOf: '2026-10-29', from: '2026-01-01' }} />));
    expect(host.querySelector('[aria-label="Compounded returns"]')?.textContent).toContain('+19.26%');
    const january = host.querySelector<HTMLButtonElement>('nav[aria-label="Months"] button')!;
    expect(january.textContent).toContain('Weighted benchmark · EUR +1.38%');
    expect(january.textContent).not.toContain('+19.26%');
    await act(async () => january.click());
    const details = host.querySelector('#monthly-report-details');
    expect(details?.textContent).toContain('+1.38%');
    expect(details?.textContent).not.toContain('+19.26%');
  });

  it('does not substitute YTD when its end date is unknown', async () => {
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...completeResponse(), weighted_benchmark: {
      '2026-10': { return_pct: 2.05, source: 'Policy' },
    } })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio"
      overviewReturns={{ portfolio: -1.7, benchmark: 1.55, from: '2026-10-15' }} />));
    expect(host.querySelectorAll('nav[aria-label="Months"] button')[9].textContent)
      .toContain('Weighted benchmark · EUR +2.05%');
  });

  it('uses the AIRS funding window for a portfolio that starts during the selected month', async () => {
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...completeResponse(), weighted_benchmark: {
      '2026-10': { return_pct: 2.05, start_date: '2026-09-30', end_date: '2026-10-29', source: 'Policy' },
    } })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio"
      overviewReturns={{ portfolio: -1.7, benchmark: 1.55, asOf: '2026-10-29', from: '2026-10-15' }} />));
    const summary = host.querySelector('[aria-label="Compounded returns"]');
    expect(summary?.textContent).toContain('+1.55%');
    expect(summary?.textContent).toContain('15 okt 2026 to Thursday 29 okt 2026');
    expect(host.querySelectorAll<HTMLButtonElement>('nav[aria-label="Months"] button')[9].textContent)
      .toContain('Weighted benchmark · EUR +1.55%');
  });

  it('compounds monthly portfolio and benchmark returns rather than adding them', async () => {
    const months = Array.from({ length: 10 }, (_, i) => ({ ...observation,
      month: `2026-${String(i + 1).padStart(2, '0')}`, rendement: i === 0 ? 10 : i === 1 ? -10 : 0,
    }));
    const weighted_benchmark = Object.fromEntries(months.map((row, i) => [row.month, {
      return_pct: i < 2 ? 5 : 0, start_date: '2026-01-01', end_date: '2026-10-23', source: 'Policy',
    }]));
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...response(), months, weighted_benchmark })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    const summary = host.querySelector('[aria-label="Compounded returns"]');
    expect(summary?.textContent).toContain('-1.00%');
    expect(summary?.textContent).toContain('+10.25%');
    expect(summary?.children).toHaveLength(3);
    expect(summary?.lastElementChild?.textContent).toContain('Excess-11.25pp');
    expect(summary?.compareDocumentPosition(host.querySelector('nav')!)).toBe(Node.DOCUMENT_POSITION_FOLLOWING);
  });

  it('automatically loads missing results on selection without fabricating zero returns', async () => {
    vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify({ ...response(), months: [] })));
    vi.mocked(startJob).mockResolvedValue(job('failed', 'AIRS unavailable'));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    await openOctober();
    expect(host.textContent).toContain('Return unavailable');
    expect(host.textContent).not.toContain('€0.00');
    expect(host.textContent).not.toContain('0.00%');
    expect(startJob).toHaveBeenCalledOnce();
  });

  it('retains a partial-download failure after reloading the saved result', async () => {
    vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(response())));
    vi.mocked(startJob).mockResolvedValue({ id: 'job', body: {}, done: Promise.resolve({
      id: 'job', kind: 'report', label: 'Report', title: 'Report', status: 'failed',
      done: 5, total: 5, summary: 'Closing valuation could not be downloaded',
    } as Awaited<Awaited<ReturnType<typeof startJob>>['done']>) });
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    await openOctober();
    expect(startJob).toHaveBeenCalledOnce();
    expect(host.textContent).toContain('Closing valuation could not be downloaded');
    expect(apiFetch).toHaveBeenCalledTimes(3); // strip, saved detail, post-job result
  });

  it('refreshes a stale saved report automatically, then displays the saved replacement', async () => {
    let refreshed = false;
    vi.mocked(apiFetch).mockImplementation(async () => new Response(JSON.stringify(refreshed
      ? completeResponse() : { ...completeResponse(), needs_refresh: true })));
    vi.mocked(startJob).mockImplementation(async () => { refreshed = true; return job('done'); });
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    expect(startJob).not.toHaveBeenCalled();
    await openOctober();
    expect(startJob).toHaveBeenCalledOnce();
    expect(apiFetch).toHaveBeenCalledTimes(3); // strip, saved detail, post-job result
    expect(host.textContent).toContain('No positions were found');
    await openOctober();
    await openOctober();
    expect(startJob).toHaveBeenCalledOnce();
  });

  it('does not launch AIRS when persistent storage cannot be read', async () => {
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...response(), warning: 'Storage unavailable' })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    await openOctober();
    expect(host.textContent).toContain('Storage unavailable');
    expect(startJob).not.toHaveBeenCalled();
  });

  it('reopens a completed historical month from memory without another DB or AIRS request', async () => {
    vi.mocked(apiFetch).mockImplementation(async (url) => {
      const selected = new URL(String(url), 'http://localhost').searchParams.get('month')!;
      return new Response(JSON.stringify({ ...completeResponse(), month: selected }));
    });
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    const selectSeptember = async () => {
      await act(async () => host.querySelectorAll<HTMLButtonElement>('nav[aria-label="Months"] button')[8].click());
    };
    await selectSeptember();
    expect(apiFetch).toHaveBeenCalledTimes(2);
    await openOctober();
    expect(apiFetch).toHaveBeenCalledTimes(2);
    await selectSeptember();
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(startJob).not.toHaveBeenCalled();
  });

  it('loads only the month strip first, then reads saved positions before considering AIRS', async () => {
    vi.mocked(apiFetch).mockImplementation(async (url) => new Response(JSON.stringify(
      String(url).includes('include_details=false') ? response() : completeResponse())));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    expect(String(vi.mocked(apiFetch).mock.calls[0][0])).toContain('include_details=false');
    expect(startJob).not.toHaveBeenCalled();
    await openOctober();
    expect(String(vi.mocked(apiFetch).mock.calls[1][0])).toContain('include_details=true');
    expect(startJob).not.toHaveBeenCalled();
    await openOctober();
    await openOctober();
    expect(apiFetch).toHaveBeenCalledTimes(2);
  });

  it('shows the actual contribution calculation and dated trades in its info popover', async () => {
    const data = completeResponse();
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ ...data, snapshot: { ...data.snapshot,
      positions: [{ name: 'Bought stock', isin: 'TEST', status: 'bought', return_pct: 5,
        contribution_pp: .5, opening_weight_pct: 0, contribution_calculation: {
          opening_value_eur: 0, closing_value_eur: 1050, sales_eur: 0, purchases_eur: 1000,
          result_eur: 50, portfolio_opening_capital_eur: 10000,
          opening_date: '2026-09-30', closing_date: '2026-10-23',
          trades: [{ date: '2026-10-10', kind: 'buy', amount_eur: 1000 }],
        } }],
    } })));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    await openOctober();
    const contribution = host.querySelector('tbody tr')!.querySelectorAll('td')[3];
    await act(async () => contribution.querySelector<HTMLElement>('span span')!.click());
    expect(document.body.textContent).toContain('Contribution calculation');
    expect(document.body.textContent).toContain('− Purchases');
    expect(document.body.textContent).toContain('€50.00 / €10,000.00 × 100 = +0.50pp');
    expect(document.body.textContent).toContain('10 okt 2026 · Bought');
    expect(document.body.textContent).toContain('not yet reconciled');
  });

  it('shows monthly position percentages and holding dates without raw amounts or extra sections', async () => {
    const section = <T,>(data: T) => ({ status: 'ok', data });
    const valuation = { as_of: '2026-10-23', requested_date: '2026-10-23', rows: [
      { name: 'Sample stock', isin: 'TEST', currency: 'EUR', quantity: 987, value_eur: 43210, weight_pct: 25 },
    ] };
    const data = { ...response(), needs_refresh: false, snapshot: { start: '2026-10-01', end: '2026-10-23',
      fetched_at: '2026-10-26T12:00:00Z', positions: [
        { name: 'Sample stock', isin: 'TEST', return_pct: 5, opening_weight_pct: 20, contribution_pp: 1, start_date: '2026-09-30', end_date: '2026-10-23', status: 'held' },
      ], reconciliation: { status: 'not_reconciled', monthly_return_pct: -2, contribution_sum_pp: 1,
        difference_pp: -3, adjustments: [], issues: ['return_mismatch'] }, sections: {
        performance: section(observation), opening: section(valuation), closing: section(valuation),
        transactions: section({ columns: ['Amount'], kinds: { Amount: 'number' }, rows: [{ Amount: 1234 }],
          readable: true, activity: [{ name: 'Sample stock', kind: 'buy', date: '2026-10-10', amount_eur: 1234, quantity: 987 }] }),
        journal: section([{ boekdatum: '2026-10-10', fonds: 'Sample stock', grootboek: 'Dividend', omschrijving: 'Raw amount 1234', amount_eur: 12 }]),
      } } };
    vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify(data)));
    await act(async () => root.render(<MonthlyReportPanel portefeuille="BOOK" name="Client portfolio" />));
    await openOctober();
    expect(host.textContent).toContain('+5.00%');
    expect(host.textContent).toContain('Opening weight (%)');
    expect(host.textContent).toContain('Contribution (pp)');
    expect(host.textContent).toContain('20.00%');
    expect(host.textContent).toContain('+1.00pp');
    expect(host.textContent).toContain('Unexplained difference');
    expect(host.textContent).toContain('-3.00pp');
    expect(host.textContent).toContain('Not reconciled:');
    expect(host.textContent).toContain('Return in EUR (%)');
    expect(host.textContent).toContain('Sample stock');
    expect(host.textContent).toContain('Wednesday 30 sept 2026 to Friday 23 okt 2026');
    for (const forbidden of ['€', '43,210', '43210', '1,234', '987', 'Raw amount']) expect(host.textContent).not.toContain(forbidden);
    expect(host.textContent).not.toContain('Month overview');
    await openOctober();
    expect(host.querySelector('#monthly-report-details')).toBeNull();
  });
});
