// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import { startJob } from '../../lib/stores/jobs';
import type { AirsPortfolioOverview } from '../../lib/types/api';
import AirsCheckPage from './page';

vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/stores/jobs', () => ({ startJob: vi.fn(), startLocalJob: vi.fn() }));

const result = () => ({ portefeuille: 'BOOK', as_of: '2026-10-29', checked_at: '2026-10-30T11:00:00Z', rows: [
  { holding_name: 'ASML', isin: 'NL0000334118', bucket: 'Equity', currency: 'EUR',
    airs_price_local: 100, our_price_local: 94, our_price_currency: 'EUR',
    airs_price_date: '2026-10-29', our_price_date: '2026-10-29' },
] });
const account = (): AirsPortfolioOverview => ({ dynamic_portefeuille: 'BOOK', name: 'Bustelberg Offensief',
  management_group: 'bustelberg', link_source: 'explicit', holdings: 24,
  saved_price_check: { checked_at: '2026-10-30T11:00:00Z', result: result() } });

let host: HTMLDivElement;
let root: ReturnType<typeof createRoot>;
beforeEach(() => {
  vi.clearAllMocks();
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  host = document.createElement('div'); document.body.append(host); root = createRoot(host);
  vi.mocked(apiFetch).mockImplementation(async (url) => new Response(JSON.stringify(
    String(url).includes('/portfolios/overview') ? [account()] : result())));
  vi.mocked(startJob).mockResolvedValue({ done: Promise.resolve({ status: 'done' }) } as Awaited<ReturnType<typeof startJob>>);
});
afterEach(async () => { await act(async () => root.unmount()); host.remove(); });

it('opens saved checks without launching AIRS, Yahoo or per-account validation requests', async () => {
  await act(async () => root.render(<AirsCheckPage />));
  expect(host.textContent).toContain('Bustelberg Offensief');
  expect(apiFetch).toHaveBeenCalledTimes(1);
  expect(vi.mocked(apiFetch).mock.calls[0][0]).toContain('/api/airs/portfolios/overview');
  expect(startJob).not.toHaveBeenCalled();
  const view = [...host.querySelectorAll('button')].find((button) => button.textContent === 'View prices')!;
  await act(async () => view.click());
  expect(host.textContent).toContain('ASML');
  expect(apiFetch).toHaveBeenCalledTimes(1);
  expect(startJob).not.toHaveBeenCalled();
});

it('runs one batch for the filtered portfolios and reloads saved checks afterwards', async () => {
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify([account(), {
    ...account(), dynamic_portefeuille: 'OTHER', name: 'Toppenberg Offensief', management_group: 'toppenberg',
  }])));
  await act(async () => root.render(<AirsCheckPage />));
  const select = host.querySelector('select')!;
  await act(async () => { select.value = 'bustelberg'; select.dispatchEvent(new Event('change', { bubbles: true })); });
  const force = [...host.querySelectorAll('button')].find((button) => button.textContent === 'Force price checks')!;
  await act(async () => force.click());
  expect(startJob).toHaveBeenCalledTimes(1);
  const call = vi.mocked(startJob).mock.calls[0];
  expect(call[0]).toContain('/api/airs/vermogen/price-checks/job');
  expect(JSON.parse(String(call[2]?.body))).toEqual({ accounts: ['BOOK'] });
  expect(vi.mocked(apiFetch).mock.calls.some(([url]) => String(url).includes('/accounts/BOOK/isins?stored_check=true'))).toBe(true);
});

it('reports a failed batch while keeping saved results available', async () => {
  vi.mocked(startJob).mockResolvedValue({ done: Promise.resolve({ status: 'failed' }) } as Awaited<ReturnType<typeof startJob>>);
  await act(async () => root.render(<AirsCheckPage />));
  const force = [...host.querySelectorAll('button')].find((button) => button.textContent === 'Force price checks')!;
  await act(async () => force.click());
  expect(host.querySelector('[role="alert"]')?.textContent).toContain('Some price checks failed');
  expect(host.textContent).toContain('Bustelberg Offensief');
  expect(force.disabled).toBe(false);
});

it('rechecks only the chosen instrument using its existing keyed price job', async () => {
  await act(async () => root.render(<AirsCheckPage />));
  const view = [...host.querySelectorAll('button')].find((button) => button.textContent === 'View prices')!;
  await act(async () => view.click());
  const recheck = host.querySelector<HTMLButtonElement>('button[title="Recheck"]')!;
  expect(recheck).not.toBeNull();
  await act(async () => recheck.click());
  expect(startJob).toHaveBeenCalledTimes(1);
  expect(vi.mocked(startJob).mock.calls[0][0]).toContain('/api/airs/accounts/BOOK/isins/NL0000334118/refresh-price/job');
});
