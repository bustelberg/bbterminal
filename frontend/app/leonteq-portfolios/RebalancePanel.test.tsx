// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import RebalancePanel from './RebalancePanel';
import { rebalancePlan, weight } from './rebalancePlan';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('./refreshProgress', async (importOriginal) => ({
  ...await importOriginal<typeof import('./refreshProgress')>(),
  refreshRequest: (url: string, init: Parameters<typeof apiFetch>[1]) => apiFetch(url, init),
}));
it('uses the same saved preview in both modes without upgrading metadata or fetching live data', async () => {
  vi.mocked(apiFetch).mockImplementation(async (url) => ({ ok: true, json: async () => {
    if (url.endsWith('/portfolio-links?kind=fixed')) return { links: [{ leonteq_name: 'Source', airs_name: 'Fixed', airs_portfolio_id: 1 }] };
    if (url.includes('/holdings?')) return { rules: [] };
    if (url.includes('/airs/')) return { datum: '2026-07-05', cache_upgrade_available: !url.includes('refresh=true'),
      rows: [{ id: url.includes('refresh=true') ? 99 : null, Fonds: 'Apple', ISINCode: 'US0378331005', Percentage: url.includes('refresh=true') ? 4.5 : 4 }] };
    return { success: true, groups: [{ positions: [{ name: 'Apple', isin: 'US0378331005', weight: '5%' }] }] };
  } }) as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<RebalancePanel />));
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/portfolio-links?kind=fixed'), { cache: 'no-store', noReadCache: true });
    await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
    await act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent === 'Prepare action list')!.click());
    expect(vi.mocked(apiFetch).mock.calls.filter(([url]) => url.includes('composition?refresh=true'))).toHaveLength(0);
    const beforeToggle = vi.mocked(apiFetch).mock.calls.length;
    await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
    expect(apiFetch).toHaveBeenCalledTimes(beforeToggle);
    expect(host.querySelector('tbody')!.textContent).toContain('Apple');
    await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
    expect(host.querySelector('tbody')!.textContent).toContain('Apple');
    expect(host.textContent).not.toContain('Incomplete data');
    expect(vi.mocked(apiFetch).mock.calls.some(([, options]) => options?.method === 'PATCH')).toBe(false);
    expect(Array.from(host.querySelectorAll('button')).find((button) => button.textContent === 'Start live rebalance')!.disabled).toBe(false);
    await act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent === 'Start live rebalance')!.click());
    expect(vi.mocked(apiFetch).mock.calls.filter(([url]) => url.includes('composition?refresh=true'))).toHaveLength(1);
    expect(host.textContent).toContain('AIRS holding changed: Apple');
    expect(vi.mocked(apiFetch).mock.calls.some(([, options]) => options?.method === 'PATCH')).toBe(false);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
it('reloads connections when reopened and preserves the selected pair after reordering', async () => {
  const original = { leonteq_name: 'Original', airs_name: 'Original FX', airs_portfolio_id: 1 };
  const added = { leonteq_name: 'Added', airs_name: 'Added FX', airs_portfolio_id: 2 };
  vi.mocked(apiFetch).mockResolvedValueOnce({ ok: true, json: async () => ({ links: [original] }) } as Response)
    .mockResolvedValueOnce({ ok: true, json: async () => ({ links: [added, original] }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host); const root = createRoot(host);
  try {
    await act(async () => root.render(<RebalancePanel active />));
    expect(host.querySelector('select')!.options).toHaveLength(1);
    await act(async () => root.render(<RebalancePanel active={false} />));
    expect(apiFetch).toHaveBeenCalledTimes(1);
    await act(async () => root.render(<RebalancePanel active />));
    const select = host.querySelector('select')!;
    expect(select.options).toHaveLength(2);
    expect(select.selectedOptions[0].textContent).toContain('Original');
    expect(apiFetch).toHaveBeenCalledTimes(2);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});
it('rejects invalid weights and ambiguous or unmatched rows, preserving explicit disconnections', () => {
  expect(weight('0%')).toBe(0);
  expect(weight('5,25%')).toBe(5.25);
  expect(weight('')).toBeNull();
  expect(weight('101%')).toBeNull();
  const row = { isin: 'US0378331005', name: 'Apple', weight: '5%' };
  expect(rebalancePlan([row, row], [row], []).issues).toContain('Ambiguous match: US0378331005');
  expect(rebalancePlan([row], [row], [{ leonteq_key: 'isin:US0378331005', airs_key: null }]).issues).not.toHaveLength(0);
});
it.each(['complete', 'limit', 'stop-before-first', 'stop-after-first', 'live-limit', 'live-failure', 'live-stop', 'live-revert', 'live-revert-already', 'live-revert-failure', 'live-single'])('runs with mocked APIs: %s', async (scenario) => {
  vi.useFakeTimers();
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const names = ['Apple', 'Microsoft', 'Nvidia'];
  const isins = ['US0378331005', 'US5949181045', 'US67066G1040'];
  let finishWrite!: () => void;
  vi.mocked(apiFetch).mockImplementation(async (url, options) => {
    if (scenario === 'live-stop' && options?.method === 'PATCH') await new Promise<void>((resolve) => { finishWrite = resolve; });
    const percentage = options?.method === 'PATCH' ? JSON.parse(options.body as string).percentage : null;
    return { ok: !(options?.method === 'PATCH' && (scenario === 'live-failure' || (scenario === 'live-revert-failure' && percentage === 4))), json: async () => {
    if (options?.method === 'PATCH') {
      const id = Number(url.match(/positions\/(\d+)/)?.[1] ?? 1);
      return { datum: '2026-07-05', already_at_target: scenario === 'live-revert-already' && percentage === 4, rows: [{ id, Fonds: names[id - 1], ISINCode: isins[id - 1], Percentage: percentage }] };
    }
    if (url.endsWith('/portfolio-links?kind=fixed')) return { links: [{ leonteq_name: 'Source', airs_name: 'Target', airs_portfolio_id: 1 }] };
    if (url.includes('/holdings?')) return { rules: [] };
    if (url.includes('/airs/')) return { datum: '2026-07-05', rows: names.map((Fonds, i) => ({ id: i + 1, Fonds, ISINCode: isins[i], Percentage: 4 })) };
    return { success: true, groups: [{ positions: names.map((name, i) => ({ name, isin: isins[i], weight: '5%' })) }] };
  } } as Response; });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const click = async (text: string) => act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent === text)!.click());
  try {
    await act(async () => root.render(<RebalancePanel />));
    expect(host.querySelector<HTMLInputElement>('input[type=checkbox]')!.checked).toBe(true);
    if (scenario.startsWith('live-')) await act(async () => host.querySelector<HTMLInputElement>('input[type=checkbox]')!.click());
    await click('Prepare action list');
    expect(vi.mocked(apiFetch).mock.calls.some(([url]) => url.includes('refresh=true'))).toBe(false);
    const sourceRead = vi.mocked(apiFetch).mock.calls.find(([url]) => url.endsWith('/portfolios/composition'))!;
    expect(JSON.parse(sourceRead[1]!.body as string).refresh).toBe(false);
    expect(host.querySelectorAll('tbody tr')).toHaveLength(3);
    expect(host.querySelector('tbody tr')!.textContent).toContain('4%5%+1 pp');
    expect(host.textContent).not.toContain('Simulate failure');
    const limit = host.querySelector<HTMLInputElement>('input[type=number]')!;
    if (scenario === 'limit') await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(limit, '1');
      limit.dispatchEvent(new Event('input', { bubbles: true }));
      limit.dispatchEvent(new Event('change', { bubbles: true }));
    });
    if (scenario.startsWith('live-')) {
      if (scenario === 'live-single') {
        await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Execute Microsoft"]')!.click());
        let writes = vi.mocked(apiFetch).mock.calls.filter(([, options]) => options?.method === 'PATCH');
        expect(writes).toHaveLength(1);
        expect(writes[0][0]).toContain('/positions/2/percentage');
        expect(host.querySelector('tbody tr')!.textContent).toContain('Microsoft');
        expect(host.querySelector('tbody tr')!.textContent).toContain('success');
        expect(host.querySelector<HTMLButtonElement>('button[aria-label="Execute Apple"]')!.disabled).toBe(false);
        expect(host.querySelector('button[aria-label="Execute Microsoft"]')).toBeNull();
        await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Execute Apple"]')!.click());
        writes = vi.mocked(apiFetch).mock.calls.filter(([, options]) => options?.method === 'PATCH');
        expect(writes).toHaveLength(2);
        expect(writes[1][0]).toContain('/positions/1/percentage');
        expect(host.querySelector('tbody tr')!.textContent).toContain('Apple');
        return;
      }
      if (scenario === 'live-limit' || scenario.startsWith('live-revert')) {
        const limitInput = host.querySelectorAll<HTMLInputElement>('input[type=number]')[0];
        await act(async () => {
          Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(limitInput, '1');
          limitInput.dispatchEvent(new Event('input', { bubbles: true }));
        });
      }
      await click('Start live rebalance');
      if (scenario === 'live-stop') {
        await click('Stop');
        expect(host.textContent).toContain('Stopping. Verifying submitted write');
        await act(async () => finishWrite());
        expect(host.textContent).toContain('Submitted write verified');
      }
      const writes = vi.mocked(apiFetch).mock.calls.filter(([, options]) => options?.method === 'PATCH');
      expect(writes).toHaveLength(1);
      expect(writes[0][0]).toContain('/positions/1/percentage');
      expect(JSON.parse(writes[0][1]!.body as string)).toEqual({ datum: '2026-07-05', expected_percentage: 4, percentage: 5 });
      expect(host.querySelectorAll('tbody tr')[0].textContent).toContain(scenario === 'live-failure' ? 'failure' : 'success');
      expect(host.querySelectorAll('tbody tr')[1].textContent).toContain('Not executed');
      if (scenario.startsWith('live-revert')) {
        await click('Revert');
        const writesAfterRevert = vi.mocked(apiFetch).mock.calls.filter(([, options]) => options?.method === 'PATCH');
        expect(writesAfterRevert).toHaveLength(2);
        expect(JSON.parse(writesAfterRevert[1][1]!.body as string)).toEqual({ datum: '2026-07-05', expected_percentage: 5, percentage: 4 });
        expect(host.textContent).toContain(scenario === 'live-revert' ? 'restored to 4% and verified' : scenario === 'live-revert-already' ? 'already restored to 4% in AIRS' : 'Revert failed');
        expect(Array.from(host.querySelectorAll('button')).some((button) => button.textContent === 'Revert')).toBe(false);
        return;
      }
      if (scenario !== 'live-stop') expect(host.textContent).toContain(scenario === 'live-limit' ? 'maximum reached' : 'Stopped at action 1');
      return;
    }
    await click('Start dummy rebalance');
    if (scenario === 'stop-before-first') {
      await click('Stop');
      await act(async () => vi.advanceTimersByTimeAsync(2000));
      expect(host.textContent).toContain('Stopped by you.');
      expect(Array.from(host.querySelectorAll('tbody tr')).every((row) => row.textContent!.includes('Not executed'))).toBe(true);
      expect(host.textContent).not.toContain('Simulated success');
      return;
    }
    await act(async () => vi.advanceTimersByTimeAsync(400));
    expect(host.querySelector('tbody tr')!.textContent).toContain('Simulated success');
    if (scenario === 'stop-after-first') {
      await click('Stop');
      await act(async () => vi.advanceTimersByTimeAsync(2000));
      expect(host.textContent).toContain('Stopped by you.');
      expect(host.querySelectorAll('tbody tr')[0].textContent).toContain('Simulated success');
      expect(host.querySelectorAll('tbody tr')[1].textContent).toContain('Not executed');
      expect(host.querySelectorAll('tbody tr')[2].textContent).toContain('Not executed');
      return;
    }
    if (scenario === 'limit') {
      await act(async () => vi.advanceTimersByTimeAsync(2400));
      expect(host.textContent).toContain('Stopped automatically after 1 successful simulated change');
      expect(host.querySelectorAll('tbody tr')[1].textContent).toContain('Not executed');
      expect(host.querySelectorAll('tbody tr')[2].textContent).toContain('Not executed');
      expect(host.querySelectorAll('tbody tr')[0].textContent).toContain('Apple');
      const count = vi.mocked(apiFetch).mock.calls.length;
      await click('Revert');
      expect(host.textContent).toContain('Simulated change reverted for Apple');
      expect(vi.mocked(apiFetch).mock.calls).toHaveLength(count);
      return;
    }
    await act(async () => vi.advanceTimersByTimeAsync(2000));
    const rows = host.querySelectorAll('tbody tr');
    expect(Array.from(rows).every((row) => row.textContent!.includes('Simulated success'))).toBe(true);
    expect(rows[0].textContent).toContain('Nvidia');
    expect(host.textContent).toContain('Dummy complete');
    expect(vi.mocked(apiFetch).mock.calls.every(([, options]) => !['PATCH', 'PUT', 'DELETE'].includes(options?.method ?? ''))).toBe(true);
  } finally { await act(async () => root.unmount()); host.remove(); vi.useRealTimers(); vi.clearAllMocks(); }
});
