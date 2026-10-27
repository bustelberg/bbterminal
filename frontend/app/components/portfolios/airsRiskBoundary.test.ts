import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const panel = readFileSync(new URL('./AirsRiskPanel.tsx', import.meta.url), 'utf8');
const modal = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('AIRS risk / simulation separation', () => {
  it('opens actual risk first and mounts legacy calculations only inside the simulation section', () => {
    expect(panel).toContain("useState<View>('overview')");
    expect(panel).toContain("dynamic(() => import('./ActiveSharePanel')");
    expect(panel).toContain("view === 'simulation' ?");
    expect(panel).toContain('Current portfolio simulation');
    expect(panel).toContain('Actual losses');
    expect(panel).not.toContain('/portfolio/tracking-error');
    expect(modal).toContain('<AirsRiskPanel');
    expect(modal).toContain('airsHoldings={data.book_holdings ?? []} bands={data.bands}');
  });

  it('requests only actual account history and does not silently substitute a model', () => {
    expect(panel).toContain('/risk-history');
    expect(panel).not.toContain('/value-series');
    expect(panel).toContain('A simulation has not been substituted');
    expect(panel).toContain('No paired AIRS account');
    expect(panel).toContain('not a daily return series');
    expect(panel).toContain('No volatility estimate');
  });
});
