// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { RiskBundleContext } from './riskBundleContext';
import WeeklyReturnsPanel from './WeeklyReturnsPanel';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));

it('selects either weekly instrument from the Risk bundle without another request', async () => {
  const options = [
    { id: 'A', name: 'First instrument', isin: 'A', currency: 'EUR' },
    { id: 'B', name: 'Second instrument', isin: 'B', currency: 'EUR' },
  ];
  const periods = [{ week_end: '2026-01-02', target_date: '2026-01-02', as_of_date: '2026-01-02',
    from_date: '2025-12-31', partial: false, available: true }];
  const instruments = Object.fromEntries(options.map((option, index) => [option.id, {
    selected: { ...option, held: true, weeks: [{ status: 'priced', from_price: 100, price: 101 + index, return_pct: 1 + index }] },
    periods, missing_observations: 0,
  }]));
  const bundle = { error: null, views: { 'weekly-returns': {
    portefeuille: 'BOOK', start_date: '2026-01-01', through: '2026-01-02', options, instruments,
  } } };
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<RiskBundleContext.Provider value={bundle}>
      <WeeklyReturnsPanel portefeuille="BOOK" />
    </RiskBundleContext.Provider>));
    const inputs = host.querySelectorAll<HTMLInputElement>('[role="combobox"]');
    for (const [index, input] of Array.from(inputs).entries()) {
      await act(async () => input.focus());
      const choices = host.querySelectorAll<HTMLButtonElement>('[role="option"]');
      await act(async () => choices[index].click());
    }
    expect(host.textContent).toContain('+1.00%');
    expect(host.textContent).toContain('+2.00%');
    expect(apiFetch).not.toHaveBeenCalled();
  } finally {
    await act(async () => root.unmount());
    host.remove();
  }
});
