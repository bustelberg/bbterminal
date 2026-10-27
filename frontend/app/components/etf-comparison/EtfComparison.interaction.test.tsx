// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import EtfComparison from './EtfComparison';

vi.mock('./ComparisonRow', () => ({ default: ({ etf, benchmark, onRemove }: {
  etf: { ticker: string }; benchmark: string; onRemove: () => void;
}) => <div data-ticker={etf.ticker}>{etf.ticker} vs {benchmark}<button onClick={onRemove}>Remove {etf.ticker}</button></div> }));

it('adds custom Yahoo tickers, prevents duplicates, changes the benchmark and removes comparisons', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  const change = (input: HTMLInputElement, value: string) => {
    Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, 'value')!.set!.call(input, value);
    input.dispatchEvent(new Event('input', { bubbles: true }));
  };
  try {
    await act(async () => root.render(<EtfComparison />));
    const forms = host.querySelectorAll('form');
    const ticker = forms[1].querySelector('input')!;
    await act(async () => change(ticker, 'vwce.de'));
    await act(async () => forms[1].dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(host.querySelector('[data-ticker="VWCE.DE"]')?.textContent).toContain('VWCE.DE vs ACWI');
    await act(async () => change(ticker, 'VWCE.DE'));
    await act(async () => forms[1].dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(host.querySelectorAll('[data-ticker="VWCE.DE"]')).toHaveLength(1);
    expect(host.querySelector('[role="alert"]')?.textContent).toContain('already');
    await act(async () => change(forms[0].querySelector('input')!, 'SPY'));
    await act(async () => forms[0].dispatchEvent(new Event('submit', { bubbles: true, cancelable: true })));
    expect(host.querySelector('[data-ticker="VWCE.DE"]')?.textContent).toContain('vs SPY');
    await act(async () => (host.querySelector('[data-ticker="VWCE.DE"] button') as HTMLButtonElement).click());
    expect(host.querySelector('[data-ticker="VWCE.DE"]')).toBeNull();
  } finally {
    await act(async () => root.unmount());
  }
});
