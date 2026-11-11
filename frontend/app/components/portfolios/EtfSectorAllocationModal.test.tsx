// @vitest-environment happy-dom
import React, { act, type ReactNode } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import EtfSectorAllocationModal from './EtfSectorAllocationModal';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));
vi.mock('./PanelDialog', () => ({ default: ({ children }: { children: ReactNode }) => <div>{children}</div> }));

it.each(['success', 'unavailable'] as const)('loads regions lazily and preserves the Sector view: %s', async mode => {
  vi.mocked(apiFetch).mockReset();
  const sector = { isin: 'IE000OEF25S1', name: 'Fund', source: 'justETF', source_url: 'https://www.justetf.com', as_of: '2026-09-28',
    sectors: [{ sector: 'Financials', weight_pct: 100, provider_sectors: ['Finance'], provider_weights: [{ sector: 'Finance', weight_pct: 100 }] }] };
  const region = { isin: sector.isin, name: 'Fund', source: 'iShares', source_url: 'https://www.ishares.com', as_of: '2026-11-09', regions: [
    { region: 'North America', weight_pct: 94.69, provider_weights: [{ country: 'United States', weight_pct: 50 }, { country: 'Canada', weight_pct: 44.69 }] },
    { region: 'Unclassified', weight_pct: 5.31, provider_weights: [{ country: 'Other', weight_pct: 5.31 }] }] };
  vi.mocked(apiFetch).mockResolvedValueOnce(mode === 'success' ? new Response(JSON.stringify(region))
    : new Response(JSON.stringify({ detail: 'Country weights are unavailable.' }), { status: 502 }));
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  const tab = (key: string) => host.querySelector<HTMLButtonElement>(`#etf-allocation-tab-${key}`)!;
  const panel = (key: string) => host.querySelector<HTMLElement>(`#etf-allocation-panel-${key}`)!;
  try {
    await act(async () => root.render(<EtfSectorAllocationModal isin={sector.isin} name="Fund" portfolioWeightPct={3.91} initialData={sector} onClose={() => {}} />));
    expect(apiFetch).not.toHaveBeenCalled();
    expect(panel('sector').textContent).toContain('Financials');
    await act(async () => tab('region').click());
    expect(panel('region').hidden).toBe(false);
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/region-allocation'), expect.objectContaining({ signal: expect.any(AbortSignal) }));
    if (mode === 'success') {
      expect(panel('region').textContent).toContain('Original country');
      expect(panel('region').textContent).toContain('Our region');
      expect(panel('region').textContent).toContain('United States');
      expect(panel('region').textContent).toContain('Canada');
      expect(panel('region').textContent).toContain('Other');
      expect(panel('region').textContent).toContain('Unclassified');
      expect(panel('region').textContent).toContain('94.69%');
      expect(panel('region').textContent).toContain('5.31%');
      expect(panel('region').textContent).toContain('0.21%');
      expect(host.textContent).toContain('9 Nov 2026');
    } else expect(panel('region').textContent).toContain('Country weights are unavailable');
    await act(async () => tab('sector').click());
    expect(panel('sector').textContent).toContain('Financials');
    expect(panel('sector').textContent).not.toContain('Country weights are unavailable');
    expect(host.textContent).toContain('28 Sept 2026');
    if (mode === 'success') {
      await act(async () => tab('region').click());
      expect(apiFetch).toHaveBeenCalledTimes(1);
      expect(panel('region').textContent).toContain('Unclassified');
    }
  } finally { await act(async () => root.unmount()); host.remove(); }
});
