// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { Chart } from './PortfolioAnalysisModal';

vi.mock('./PanelDialog', () => ({ default: ({ children }: { children: React.ReactNode }) => <div role="dialog">{children}</div> }));
vi.mock('../../../lib/i18n', () => ({ useLang: () => ['en'] }));

it.each(['North America', 'Europe', 'Unclassified', 'USD', 'EUR'])('opens the portfolio and benchmark holdings comparison for %s', async bucket => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); document.body.appendChild(host);
  const root = createRoot(host);
  try {
    await act(async () => root.render(<Chart axis={bucket === 'USD' || bucket === 'EUR' ? 'currency' : 'region'} benchmark="ACWI" rows={[{
      bucket, portfolio_pct: 3, benchmark_pct: 5, diff_pct: -2,
      holdings: [{ name: 'Apple', isin: 'AAPL', issuer_key: 'apple', in_both: true, weight_pct: 3 }],
      benchmark_holdings: [{ name: 'Apple Inc.', isin: 'AAPL', issuer_key: 'apple', in_both: true, weight_pct: 2 },
        { name: 'Benchmark-only company', isin: 'OTHER', weight_pct: 3 }],
    }]} />));
    const button = host.querySelector<HTMLButtonElement>(`button[aria-label="Show holdings in ${bucket === 'Unclassified' ? 'Other' : bucket}"]`)!;
    await act(async () => button.click());
    const dialog = host.querySelector('[role="dialog"]')!;
    expect(dialog.querySelector('h2')?.textContent).toBe(bucket === 'Unclassified' ? 'Other' : bucket);
    expect(dialog.textContent).toContain('Portfolio (1)');
    expect(dialog.textContent).toContain('ACWI (2)');
    expect(dialog.textContent).toContain('Benchmark-only company');
    expect(dialog.textContent).toContain('Held in both');
    expect(dialog.querySelectorAll('table')).toHaveLength(2);
    await act(async () => [...dialog.querySelectorAll('button')].find(button => button.textContent === 'Close')!.click());
    expect(host.querySelector('[role="dialog"]')).toBeNull();
  } finally { await act(async () => root.unmount()); host.remove(); }
});

it.each(['region', 'currency'] as const)('does not open stale %s holdings during a portfolio change', async axis => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); const root = createRoot(host);
  try {
    await act(async () => root.render(<Chart axis={axis} benchmark="ACWI" stale rows={[{
      bucket: 'North America', portfolio_pct: 3, benchmark_pct: 5, diff_pct: -2, holdings: [],
    }]} />));
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Show holdings in North America"]')!.click());
    expect(host.querySelector('[role="dialog"]')).toBeNull();
  } finally { await act(async () => root.unmount()); }
});


it('shows the actual source evidence for a small unclassified fund portion', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div'); const root = createRoot(host);
  try {
    await act(async () => root.render(<Chart axis="currency" benchmark="ACWI" rows={[{
      bucket: 'Unclassified', portfolio_pct: .003456, benchmark_pct: 0, diff_pct: .003456,
      holdings: [{ name: 'Utilities ETF', isin: 'FUND', weight_pct: .003456, classification_evidence: {
        reason: 'Provider reports other currencies', providerLabel: 'Other currencies', source: 'comdirect',
        sourceUrl: 'https://www.comdirect.de/inf/fonds/FUND', fundWeightPct: .27, portfolioWeightPct: .003456,
      } }],
    }]} />));
    await act(async () => host.querySelector<HTMLButtonElement>('button[aria-label="Show holdings in Other"]')!.click());
    const dialog = host.querySelector('[role="dialog"]')!;
    expect(dialog.textContent).toContain('Why Other?');
    expect(dialog.textContent).toContain('Provider reports other currencies');
    expect(dialog.textContent).toContain('0.2700% of the fund');
    expect(dialog.textContent).toContain('0.0035% of the portfolio');
    expect(dialog.textContent).toContain('<0.01%');
    expect(dialog.querySelector('a')?.href).toBe('https://www.comdirect.de/inf/fonds/FUND');
  } finally { await act(async () => root.unmount()); }
});
