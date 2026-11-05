// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import Page from './page';
import { apiFetch } from '../../lib/apiFetch';
import { startJob, type JobToast } from '../../lib/stores/jobs';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('../../lib/stores/jobs', () => ({ startJob: vi.fn() }));
vi.mock('./LeonteqPanel', () => ({ default: () => <div>Saved Leonteq portfolios</div> }));
vi.mock('./PortfolioLinksPanel', () => ({ default: () => <div>Portfolio connections</div> }));

it('lists dynamic models separately without requesting fixed compositions', async () => {
  vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => [
    { id: 1, name: 'Dynamic model', portfolio_type: 'normaal' },
    { id: 2, name: 'Fixed model', portfolio_type: 'fixed (13.7)' },
    { id: 3, name: 'Multi model', portfolio_type: 'meervoudig' },
  ] } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<Page />));
    await act(async () => host.querySelectorAll<HTMLButtonElement>('[role=tab]')[2].click());
    expect(host.querySelector('table')?.textContent).toContain('Dynamic model');
    expect(host.querySelector('table')?.textContent).not.toContain('Fixed model');
    expect(host.querySelector('table')?.textContent).not.toContain('Multi model');
    expect(apiFetch).toHaveBeenCalledTimes(1);
  } finally { await act(async () => root.unmount()); host.remove(); vi.clearAllMocks(); }
});

it('loads the AIRS table on selection, preserves it across tabs, and refreshes only the list', async () => {
  const rows = [{ id: 1, name: 'BUS_WTS_Duurzaam', omschrijving: 'WTS Duurzaam FX',
    portfolio_type: 'fixed (13.7)', fixed_datum: '2024-02-28', scanned_at: '2026-11-05T10:00:00Z' }];
  vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => rows } as Response);
  let finish!: (job: JobToast) => void;
  vi.mocked(startJob).mockResolvedValue({ id: 'list-job', body: {}, done: new Promise((resolve) => { finish = resolve; }) });
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<Page />));
    const tabs = host.querySelectorAll<HTMLButtonElement>('[role=tab]');
    expect(tabs).toHaveLength(6);
    expect(Array.from(tabs, (tab) => tab.textContent)).toEqual(['Leonteq', 'AIRS Fixed', 'AIRS Dynamic', 'Connections', 'Rebalance', 'Returns']);
    expect(apiFetch).not.toHaveBeenCalled();
    await act(async () => tabs[1].click());
    expect(tabs[1].getAttribute('aria-selected')).toBe('true');
    expect(host.querySelector('table')?.textContent).toContain('WTS Duurzaam FX');
    expect(host.querySelector('table')?.textContent).toContain('fixed (13.7)');
    expect(host.querySelector('table')?.textContent).toContain('2024-02-28');
    expect(apiFetch).toHaveBeenCalledTimes(2);
    await act(async () => tabs[3].click());
    expect(host.querySelector<HTMLElement>('#source-panel-airs')!.hidden).toBe(true);
    await act(async () => tabs[1].click());
    expect(apiFetch).toHaveBeenCalledTimes(2);
    const refresh = Array.from(host.querySelectorAll('button')).find((button) => button.textContent === 'Refresh from AIRS')!;
    await act(async () => refresh.click());
    expect(startJob).toHaveBeenCalledWith(expect.stringContaining('/api/leonteq/airs/model-portfolios/refresh/job'), 'AIRS · Model portefeuilles');
    expect(host.querySelector('table')?.textContent).toContain('BUS_WTS_Duurzaam');
    await act(async () => { finish({ status: 'done' } as JobToast); });
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/airs/model-portfolios'), { noReadCache: true });
    vi.mocked(apiFetch).mockResolvedValue({ ok: true, json: async () => ({ portfolio_id: 1, portfolio: 'BUS_WTS_Duurzaam',
      datum: '2025-02-14', dates: ['2025-02-14'], cached_at: '2026-11-05T10:00:00Z', rows: [
        { Fonds: 'Liquiditeiten', Percentage: 2, ISINCode: null, valuta: null, Beleggingscategorie: null, Beleggingssector: null, regio: null, afmCategorie: null },
        { Fonds: 'Apple', Percentage: 4.95, ISINCode: 'US0378331005', valuta: 'USD', Beleggingscategorie: 'AAND', Beleggingssector: 'BU-Inf.Technol', regio: 'BU-USA', afmCategorie: '09AandOntw' },
      ],
    }) } as Response);
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="BUS_WTS_Duurzaam · View composition"]')!.click());
    expect(apiFetch).toHaveBeenLastCalledWith(expect.stringContaining('/api/leonteq/airs/model-portfolios/1/composition'), { cache: 'no-store', noReadCache: true });
    const composition = host.querySelector('table')!;
    expect(composition.querySelectorAll('thead th')).toHaveLength(8);
    expect(composition.textContent).toContain('Liquiditeiten');
    expect(composition.textContent).toContain('4.95%');
    expect(composition.textContent).toContain('US0378331005');
    expect(composition.textContent).toContain('09AandOntw');
    expect(host.textContent).toContain('2025-02-14');
    await act(async () => Array.from(host.querySelectorAll('button')).find((button) => button.textContent?.includes('All model portfolios'))!.click());
    const readCount = vi.mocked(apiFetch).mock.calls.length;
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="BUS_WTS_Duurzaam · View composition"]')!.click());
    expect(apiFetch).toHaveBeenCalledTimes(readCount);
    expect(host.querySelector('table')?.textContent).toContain('Apple');
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});

it('opens a preloaded composition without another request or loading screen', async () => {
  const model = { id: 2126, name: 'BUS_2.0_BOFF_FX', omschrijving: 'Bustelberg 2.0',
    portfolio_type: 'fixed (13.7)', fixed_datum: '2026-07-24', scanned_at: '2026-11-05T11:54:00Z' };
  vi.mocked(apiFetch).mockImplementation(async (url) => ({ ok: true, json: async () => url.endsWith('/compositions')
    ? { compositions: [{ portfolio_id: model.id, portfolio: model.name, datum: '2026-07-24', dates: ['2026-07-24'],
      cached_at: model.scanned_at, rows: [{ id: 18137, Fonds: 'Apple', Percentage: 4.95, ISINCode: 'US0378331005' }],
    }] } : [model],
  }) as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<Page />));
    await act(async () => host.querySelectorAll<HTMLButtonElement>('[role=tab]')[1].click());
    const calls = vi.mocked(apiFetch).mock.calls.length;
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="BUS_2.0_BOFF_FX · View composition"]')!.click());
    expect(apiFetch).toHaveBeenCalledTimes(calls);
    expect(host.querySelector('table')?.textContent).toContain('Apple');
    expect(host.textContent).not.toContain('Loading composition');
    const date = host.querySelector('select')!;
    await act(async () => { date.value = '2026-07-24'; date.dispatchEvent(new Event('change', { bubbles: true })); });
    expect(apiFetch).toHaveBeenCalledTimes(calls);
    expect(host.querySelector('table')?.textContent).toContain('Apple');
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});
