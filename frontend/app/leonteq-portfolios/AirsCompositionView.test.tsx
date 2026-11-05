// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import AirsCompositionView, { type AirsComposition } from './AirsCompositionView';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/i18n', () => ({ useLang: () => ['en'] }));

it.each([true, false])('edits only the selected percentage and handles AIRS save success=%s', async (success) => {
  const initial: AirsComposition = { portfolio_id: 1, portfolio: 'Fixed model', datum: '2025-02-14', dates: ['2025-02-14'], rows: [
    { id: 18137, Fonds: 'Apple', ISINCode: 'US0378331005', Percentage: 4.95, valuta: 'USD', Beleggingscategorie: 'AAND', Beleggingssector: null, regio: null, afmCategorie: null },
    { id: 18138, Fonds: 'Cash', ISINCode: null, Percentage: 2, valuta: null, Beleggingscategorie: null, Beleggingssector: null, regio: null, afmCategorie: null },
  ] };
  const cache = new Map([['1:latest', initial], ['1:2024-01-01', initial]]);
  vi.mocked(apiFetch).mockResolvedValue(success
    ? { ok: true, json: async () => ({ ...initial, rows: [{ ...initial.rows[0], Percentage: 5.5 }, initial.rows[1]] }) } as Response
    : { ok: false, status: 409, json: async () => ({ detail: 'percentage_changed' }) } as Response);
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<AirsCompositionView id={1} name="Fixed model" cache={cache} onBack={() => {}} />));
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Edit percentage for Apple"]')!.click());
    const input = host.querySelector('input')!;
    await act(async () => {
      Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, '5,5');
      input.dispatchEvent(new Event('input', { bubbles: true }));
    });
    await act(async () => host.querySelector('form')!.dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/model-portfolios/1/positions/18137/percentage'), {
      method: 'PATCH', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ datum: '2025-02-14', expected_percentage: 4.95, percentage: 5.5 }),
    });
    expect(cache.has('1:latest')).toBe(false);
    expect(cache.has('1:2024-01-01')).toBe(false);
    if (success) {
      expect(host.textContent).toContain('5.5%');
      expect(host.textContent).toContain('saved and verified');
      expect(cache.get('1:2025-02-14')?.rows[1].Percentage).toBe(2);
    } else {
      expect(host.querySelector('[role=alert]')?.textContent).toContain('row changed in AIRS');
      expect(host.querySelector('input')).not.toBeNull();
    }
  } finally {
    await act(async () => root.unmount()); host.remove(); vi.clearAllMocks();
  }
});
