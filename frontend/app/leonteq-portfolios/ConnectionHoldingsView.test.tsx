// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import ConnectionHoldingsView, { type ConnectionHoldingsCache } from './ConnectionHoldingsView';
import type { HoldingRule } from './matchHoldings';
import { apiFetch } from '../../lib/apiFetch';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('./refreshProgress', async (importOriginal) => ({
  ...await importOriginal<typeof import('./refreshProgress')>(),
  refreshRequest: (url: string, init: Parameters<typeof apiFetch>[1]) => apiFetch(url, init),
}));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../lib/stores/jobs', () => ({
  ExpectedJobFailure: class extends Error {},
  startLocalJob: (_title: string, _kind: string, run: (signal: AbortSignal, report: () => void) => Promise<unknown>) => {
    void run(new AbortController().signal, () => {}).catch(() => {}); return 'job';
  },
}));

it('balances Fixed cash from a fresh preview, serializes additions and updates the saved comparison', async () => {
  vi.mocked(apiFetch).mockReset();
  const link = { leonteq_name: 'Bonds', airs_portfolio_id: 7, airs_name: 'Bonds FX' };
  const cache: ConnectionHoldingsCache = new Map([
    ['leonteq:Bonds', { rows: [{ isin: 'FR001400Z2L7', name: 'France', weight: '1.62%' }] }],
    ['airs:security-v2:7', { datum: '2026-08-12', rows: [{ isin: null, name: 'Liquiditeiten', weight: '10%' }] }],
  ]);
  const rulesCache = new Map<string, HoldingRule[]>([[JSON.stringify(['Bonds', 7]), []]]);
  const composition = { portfolio_id: 7, portfolio: 'Bonds FX', datum: '2026-08-12', dates: [],
    rows: [{ id: 1, Fonds: 'Liquiditeiten', Percentage: 8.38, ISINCode: null }] };
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }))
    .mockResolvedValueOnce(new Response(JSON.stringify({ composition })));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  const click = async (text: string) => act(async () => [...host.querySelectorAll('button')].find(button => button.textContent === text)!.click());
  try {
    await act(async () => root.render(<ConnectionHoldingsView link={link} cache={cache} rulesCache={rulesCache} onBack={() => {}} />));
    expect(apiFetch).not.toHaveBeenCalled();
    await click('Read latest total');
    expect(host.querySelector<HTMLButtonElement>('button[aria-label="Add France to AIRS Fixed"]')!.disabled).toBe(true);
    await act(async () => finish(new Response(JSON.stringify({ composition, total: 101.62, displayed_total: 101.62,
      row_id: 1, cash_name: 'Liquiditeiten', current: 10, target: 8.38, datum: composition.datum, balanced: false }))));
    expect(host.textContent).toContain('10.00% → 8.38%');
    await click('Execute');
    const [url, request] = vi.mocked(apiFetch).mock.calls[1];
    expect(url).toContain('/model-portfolios/7/cash-balance');
    expect(request?.method).toBe('POST');
    expect(JSON.parse(String(request?.body))).toMatchObject({ total: 101.62, current: 10, target: 8.38 });
    expect(cache.get('airs:security-v2:7')?.rows[0].weight).toBe('8.38%');
    expect(host.textContent).toContain('Cash saved and verified');
  } finally { await act(async () => root.unmount()); host.remove(); vi.mocked(apiFetch).mockReset(); }
});

it.each(['success', 'failure'] as const)('queues multiple additions sequentially and preserves verified work: %s', async outcome => {
  const link = { leonteq_name: 'Bonds', airs_portfolio_id: 7, airs_name: 'Bonds FX' };
  const rows = [
    { isin: 'FR001400Z2L7', name: 'France', weight: '1.62%' },
    { isin: 'XS2599169922', name: 'Heineken', weight: '3.43%' },
    { isin: 'NL0015074E19', name: 'Netherlands', weight: '1.70%' },
  ];
  const cache: ConnectionHoldingsCache = new Map([
    ['leonteq:Bonds', { rows }], ['airs:security-v2:7', { datum: '2026-08-12', rows: [] }],
  ]);
  const rulesCache = new Map<string, HoldingRule[]>([[JSON.stringify(['Bonds', 7]), []]]);
  const finish: ((response: Response) => void)[] = [];
  vi.mocked(apiFetch).mockImplementation(() => new Promise(resolve => { finish.push(resolve); }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  const reply = (count: number) => new Response(JSON.stringify({ datum: '2026-08-12', rows: rows.slice(0, count).map((row, index) => ({
    id: index + 1, Fonds: row.name, ISINCode: row.isin, Percentage: Number(row.weight.replace('%', '')),
  })) }));
  try {
    await act(async () => root.render(<ConnectionHoldingsView link={link} cache={cache} rulesCache={rulesCache} onBack={() => {}} />));
    const buttons = rows.map(row => host.querySelector<HTMLButtonElement>(`button[aria-label="Add ${row.name} to AIRS Fixed"]`)!);
    await act(async () => buttons[0].click());
    expect(buttons[1].disabled).toBe(false);
    await act(async () => { buttons[1].click(); buttons[2].click(); buttons[1].click(); });
    expect(apiFetch).toHaveBeenCalledOnce();
    expect(buttons.map(button => button.textContent)).toEqual(['Adding…', 'Queued…', 'Queued…']);
    await act(async () => finish[0](reply(1)));
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(host.querySelector('button[aria-label="Add Heineken to AIRS Fixed"]')?.textContent).toBe('Adding…');
    if (outcome === 'failure') {
      await act(async () => finish[1](new Response(JSON.stringify({ detail: 'Save unverified' }), { status: 502 })));
      expect(apiFetch).toHaveBeenCalledTimes(2);
      expect(host.textContent).toContain('Queue stopped');
      expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('France');
      expect(host.querySelector<HTMLButtonElement>('button[aria-label="Add Netherlands to AIRS Fixed"]')!.disabled).toBe(true);
    } else {
      await act(async () => finish[1](reply(2)));
      expect(apiFetch).toHaveBeenCalledTimes(3);
      await act(async () => finish[2](reply(3)));
      expect(cache.get('airs:security-v2:7')?.rows).toHaveLength(3);
      expect(host.querySelector('section[aria-label="Leonteq · Unmatched holdings"]')?.textContent).toContain('No unmatched holdings');
    }
    expect(vi.mocked(apiFetch).mock.calls.map(call => JSON.parse(String(call[1]?.body)).isin)).toEqual(rows.slice(0, outcome === 'success' ? 3 : 2).map(row => row.isin));
  } finally { await act(async () => root.unmount()); host.remove(); vi.mocked(apiFetch).mockReset(); vi.clearAllMocks(); }
});

it.each(['success', 'failure', 'unverified', 'dynamic', 'missing-date', 'manual-rule'] as const)(
  'adds one unmatched Leonteq holding with verified auto-connection: %s', async scenario => {
    const link = { leonteq_name: 'ObligatieTopSelectie', airs_portfolio_id: 7, airs_name: 'OBTopselectie FX' };
    const cache: ConnectionHoldingsCache = new Map([
      ['leonteq:ObligatieTopSelectie', { rows: [{ isin: 'FR001400Z2L7', name: 'FRTR 2.7 02/25/31', weight: '1,62%' }] }],
      ['airs:security-v2:7', { datum: scenario === 'missing-date' ? null : '2026-08-12', rows: [] }],
    ]);
    const rulesCache = new Map<string, HoldingRule[]>([[JSON.stringify(['ObligatieTopSelectie', 7]),
      scenario === 'manual-rule' ? [{ leonteq_key: 'isin:FR001400Z2L7', airs_key: null }] : []]]);
    let finish!: (response: Response) => void;
    vi.mocked(apiFetch).mockReturnValueOnce(new Promise(resolve => { finish = resolve; }));
    Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
    const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
    try {
      const view = <ConnectionHoldingsView link={link} kind={scenario === 'dynamic' ? 'dynamic' : 'fixed'} cache={cache} rulesCache={rulesCache} onBack={() => {}} />;
      await act(async () => root.render(view));
      const button = host.querySelector<HTMLButtonElement>('button[aria-label="Add FRTR 2.7 02/25/31 to AIRS Fixed"]');
      if (scenario === 'dynamic') { expect(button).toBeNull(); expect(apiFetch).not.toHaveBeenCalled(); return; }
      if (scenario === 'missing-date' || scenario === 'manual-rule') { expect(button!.disabled).toBe(true); expect(apiFetch).not.toHaveBeenCalled(); return; }
      await act(async () => { button!.click(); button!.click(); });
      expect(apiFetch).toHaveBeenCalledOnce();
      expect(button!.disabled).toBe(true);
      const [url, request] = vi.mocked(apiFetch).mock.calls[0];
      expect(url).toContain('/model-portfolios/7/positions');
      expect(request?.method).toBe('POST');
      expect(JSON.parse(String(request?.body))).toEqual({ datum: '2026-08-12', isin: 'FR001400Z2L7', percentage: '1.62', leonteq_name: 'ObligatieTopSelectie' });
      const response = scenario === 'failure' ? new Response(JSON.stringify({ detail: 'New position save is unverified.' }), { status: 502 })
        : new Response(JSON.stringify({ datum: '2026-08-12', rows: [{ id: 91, Fonds: 'France 2.7 2031',
          ISINCode: 'FR001400Z2L7', Percentage: scenario === 'unverified' ? 2 : 1.62 }] }));
      await act(async () => finish(response));
      if (scenario !== 'success') {
        expect(host.textContent).toContain('Select Refresh both before retrying');
        expect(cache.has('airs:security-v2:7')).toBe(false);
        expect(button!.disabled).toBe(true);
        expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('No connected holdings');
        return;
      }
      expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('France 2.7 2031');
      expect(host.querySelector('section[aria-label="Leonteq · Unmatched holdings"]')?.textContent).not.toContain('FR001400Z2L7');
      expect(host.textContent).toContain('added at 1.62% and connected');
      expect(cache.get('airs:security-v2:7')?.rows[0].weight).toBe('1.62%');
      await act(async () => root.render(null));
      await act(async () => root.render(view));
      expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('France 2.7 2031');
      expect(apiFetch).toHaveBeenCalledOnce(); // ISIN matching needs no manual-rule write.
    } finally { await act(async () => root.unmount()); host.remove(); vi.mocked(apiFetch).mockReset(); vi.clearAllMocks(); }
  },
);

it('disconnects a match into leftovers, reconnects it and keeps the saved rules on reopen', async () => {
  const link = { leonteq_name: 'AI', airs_portfolio_id: 1, airs_name: 'Fixed model' };
  const cache: ConnectionHoldingsCache = new Map([
    ['leonteq:AI', { rows: [{ isin: 'US0378331005', name: 'APPLE INC', weight: '4.9%' }] }],
    ['airs:1', { rows: [{ isin: 'US0378331005', name: 'OLD INTEREST ROW', weight: '0.10%' }] }],
    ['airs:security-v2:1', { rows: [{ isin: 'US0378331005', name: 'Apple', weight: '4.95%' }] }],
  ]);
  const rulesCache = new Map<string, HoldingRule[]>([[JSON.stringify(['AI', 1]), []]]);
  vi.mocked(apiFetch).mockResolvedValueOnce({ ok: true, json: async () => ({ rules: [{ leonteq_key: 'isin:US0378331005', airs_key: null }] }) } as Response)
    .mockResolvedValueOnce({ ok: true, json: async () => ({ rules: [{ leonteq_key: 'isin:US0378331005', airs_key: 'isin:US0378331005' }] }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const view = <ConnectionHoldingsView link={link} cache={cache} rulesCache={rulesCache} onBack={() => {}} />;
  try {
    await act(async () => root.render(view));
    expect(host.textContent).not.toContain('OLD INTEREST ROW');
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Disconnect holdings for APPLE INC"]')!.click());
    expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('No connected holdings');
    expect(host.querySelector('section[aria-label="Leonteq · Unmatched holdings"]')?.textContent).toContain('APPLE INC');
    const inputs = host.querySelectorAll<HTMLInputElement>('[role=combobox]');
    for (const input of inputs) {
      await act(async () => input.focus());
      await act(async () => input.dispatchEvent(new KeyboardEvent('keydown', { key: 'Enter', bubbles: true })));
    }
    await act(async () => host.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/portfolio-links/holdings'), {
      method: 'PUT', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ leonteq_name: 'AI', airs_portfolio_id: 1, leonteq_key: 'isin:US0378331005', airs_key: 'isin:US0378331005' }),
    });
    expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('Manually connected');
    await act(async () => root.render(null));
    await act(async () => root.render(view));
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(host.querySelector('section[aria-labelledby=matched-holdings-heading]')?.textContent).toContain('Manually connected');
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});
