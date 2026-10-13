import { describe, expect, it } from 'vitest';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { refreshPlanForTab } from './OwnerEarningsModal';

describe('refreshPlanForTab', () => {
  it('fetches every source Quick Valuation reads without spending on Deep Valuation key ratios', () => {
    expect(refreshPlanForTab('quickval')).toEqual({
      feeds: 'all', prices: true, keyRatios: false,
    });
  });

  it('keeps the specialised Deep Valuation key-ratio fetch on that tab', () => {
    expect(refreshPlanForTab('deepval')).toEqual({
      feeds: 'all', prices: true, keyRatios: true,
    });
  });

  it('does not fetch indicators or key ratios for Long Equity', () => {
    expect(refreshPlanForTab('longequity')).toEqual({
      feeds: 'statements_estimates', prices: true, keyRatios: false,
    });
  });

  it('names the active tab in the persistent refresh card', () => {
    const source = readFileSync(join(__dirname, 'OwnerEarningsModal.tsx'), 'utf8');
    expect(source).toContain('jobTitle={chrome.refreshingTab(chrome.tabs[tab])}');
    expect(source).toContain('{...refreshPlan}');
  });
});
