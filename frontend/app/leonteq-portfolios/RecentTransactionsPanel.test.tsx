// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import RecentTransactionsPanel from './RecentTransactionsPanel';
import { refreshRequest } from './refreshProgress';
import { apiFetch } from '../../lib/apiFetch';
import type { TransactionBundle } from './recentWeightPlan';

vi.mock('./refreshProgress', () => ({ refreshRequest: vi.fn() }));
vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/stores/jobs', () => ({
  ExpectedJobFailure: class extends Error {},
  startLocalJob: (_title: string, _kind: string, run: (signal: AbortSignal, report: () => void) => Promise<unknown>) => {
    void run(new AbortController().signal, () => {}).catch(() => {}); return 'local-job';
  },
}));

function example(): TransactionBundle {
  return { success: true, from: '2026-11-04', to: '2026-11-10', retrieved_at: 'now', rules: [],
    transactions: [{ id: '1:1', date: '2026-11-10', event: 'Buy', name: 'Apple', isin: 'US0378331005', quantity: 10, price: 100, currency: 'USD' }],
    leonteq: { success: true, status: 'success', retrieved_at: '2026-11-10T12:00:00Z', groups: [{ name: 'Share', weight: '100%', positions: [
      { name: 'Apple', isin: 'US0378331005', weight: '5.20%', total_value: null, units: null, price: null, currency: null, performance: null },
    ] }] },
    airs: { portfolio: 'Fixed', portfolio_id: 1, datum: '2026-11-10', dates: ['2026-11-10'], rows: [
      { id: 1, Fonds: 'Apple', ISINCode: 'US0378331005', Percentage: 4, valuta: null, Beleggingscategorie: null, Beleggingssector: null, regio: null, afmCategorie: null },
    ] },
  };
}

it('keeps other Execute buttons enabled and queues their writes independently', async () => {
  vi.mocked(refreshRequest).mockReset(); vi.mocked(apiFetch).mockReset();
  const data = example();
  data.transactions.push({ ...data.transactions[0], id: '2:1', name: 'Microsoft', isin: 'US5949181045' });
  data.leonteq.groups![0].positions.push({ ...data.leonteq.groups![0].positions[0], name: 'Microsoft', isin: 'US5949181045' });
  data.airs.rows.push({ ...data.airs.rows[0], id: 2, Fonds: 'Microsoft', ISINCode: 'US5949181045' });
  let finish!: (response: Response) => void;
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(data)))
    .mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }))
    .mockImplementation(async () => new Response(JSON.stringify(data)));
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => {
    if (init?.method === 'PATCH') {
      const id = String(_url).includes('/positions/1/') ? 1 : 2;
      data.airs.rows = data.airs.rows.map(row => row.id === id ? { ...row, Percentage: 5.2 } : row);
      return new Response(JSON.stringify(data.airs));
    }
    return new Response(JSON.stringify({ links: [{ leonteq_name: 'Fund', airs_name: 'Fixed', airs_portfolio_id: 1 }] }));
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const action = (name: string) => host.querySelector<HTMLButtonElement>(`button[aria-label="Execute ${name}"]`)!;
  try {
    await act(async () => root.render(<RecentTransactionsPanel />));
    await act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === 'Fetch transactions')!.click());
    await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
    await act(async () => action('Apple').click());
    expect(action('Apple').textContent).toBe('Working…');
    expect(action('Apple').disabled).toBe(true);
    expect(action('Microsoft').disabled).toBe(false);
    await act(async () => action('Microsoft').click());
    expect(action('Microsoft').textContent).toBe('Working…');
    expect(refreshRequest).toHaveBeenCalledTimes(2);
    await act(async () => finish(new Response(JSON.stringify(data))));
    expect(vi.mocked(apiFetch).mock.calls.filter(([, init]) => init?.method === 'PATCH')).toHaveLength(2);
    expect(host.textContent).not.toContain('Working…');
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it.each(['dummy', 'live', 'changed', 'uncertain'] as const)('reviews and applies one recent-trade weight in %s mode', async mode => {
  vi.mocked(refreshRequest).mockReset();
  vi.mocked(apiFetch).mockReset();
  const data = example();
  const fresh = structuredClone(data);
  if (mode === 'changed') fresh.leonteq.groups![0].positions[0].weight = '6%';
  vi.mocked(refreshRequest).mockResolvedValueOnce(new Response(JSON.stringify(data)))
    .mockResolvedValueOnce(new Response(JSON.stringify(fresh)));
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => new Response(JSON.stringify(init?.method === 'PATCH'
    ? { ...data.airs, rows: [{ ...data.airs.rows[0], Percentage: mode === 'uncertain' ? 4 : 5.2 }] }
    : { links: [{ leonteq_name: 'Fund', airs_name: 'Fixed', airs_portfolio_id: 1 }] })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const click = async (label: string) => act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === label)!.click());
  try {
    await act(async () => root.render(<RecentTransactionsPanel />));
    expect(refreshRequest).not.toHaveBeenCalled();
    await click('Fetch transactions');
    expect(host.textContent).toContain('10 units');
    if (mode !== 'dummy') await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
    expect([...host.querySelectorAll('button')].some(button => button.textContent === 'Review')).toBe(false);
    await click('Execute');
    const writes = vi.mocked(apiFetch).mock.calls.filter(([, init]) => init?.method === 'PATCH');
    if (mode === 'dummy') {
      expect(writes).toHaveLength(0);
      expect(host.textContent).toContain('Simulated');
      expect(refreshRequest).toHaveBeenCalledTimes(1);
    } else if (mode === 'changed') {
      expect(writes).toHaveLength(0);
      expect(host.textContent).toContain('changed. Fetch transactions');
    } else {
      expect(writes).toHaveLength(1);
      expect(JSON.parse(String(writes[0][1]?.body))).toEqual({ datum: '2026-11-10', expected_percentage: 4, percentage: 5.2 });
      expect(host.textContent).toContain(mode === 'live' ? 'Applied and verified' : 'outcome is uncertain');
    }
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it.each(['dummy', 'live', 'unregistered'] as const)('adds a new holding in %s mode without confusing it with a weight update', async mode => {
  vi.mocked(refreshRequest).mockReset(); vi.mocked(apiFetch).mockReset();
  const data = example();
  const newRow = { ...data.airs.rows[0], id: 10, Fonds: 'Apple registered name', Percentage: 5.2 };
  data.airs.rows = [{ ...data.airs.rows[0], id: 9, Fonds: 'Liquiditeiten', ISINCode: null, Percentage: 100 }];
  vi.mocked(refreshRequest).mockImplementation(async () => new Response(JSON.stringify(data)));
  vi.mocked(apiFetch).mockImplementation(async (_url, init) => {
    if (init?.method === 'POST') return mode === 'unregistered'
      ? new Response(JSON.stringify({ detail: 'Instrument is not registered in AIRS. Register it manually, then fetch transactions again.' }), { status: 422 })
      : new Response(JSON.stringify({ ...data.airs, rows: [...data.airs.rows, newRow] }));
    return new Response(JSON.stringify({ links: [{ leonteq_name: 'Fund', airs_name: 'Fixed', airs_portfolio_id: 1 }] }));
  });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const click = async (text: string) => act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === text)!.click());
  try {
    await act(async () => root.render(<RecentTransactionsPanel />));
    await click('Fetch transactions');
    expect(host.textContent).toContain('New position');
    expect(host.textContent).toContain('105.20%');
    if (mode !== 'dummy') {
      await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
      }
    await click('Execute');
    const writes = vi.mocked(apiFetch).mock.calls.filter(([, init]) => init?.method === 'POST');
    expect(writes).toHaveLength(mode === 'dummy' ? 0 : 1);
    expect(vi.mocked(apiFetch).mock.calls.some(([, init]) => init?.method === 'PATCH')).toBe(false);
    if (mode === 'dummy') expect(host.textContent).toContain('Simulated add');
    else {
      expect(writes[0][0]).toMatch(/model-portfolios\/1\/positions$/);
      expect(JSON.parse(String(writes[0][1]?.body))).toEqual({ datum: '2026-11-10', isin: 'US0378331005', percentage: 5.2, leonteq_name: 'Fund' });
      expect(host.textContent).toContain(mode === 'live' ? 'Applied and verified' : 'Register it manually');
    }
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
