import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const panel = readFileSync(new URL('./ActiveSharePanel.tsx', import.meta.url), 'utf8');
const modal = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('Risk navigation', () => {
  it('opens the five visible risk measures directly without an AIRS overview wrapper', () => {
    expect(modal).toContain('<ActiveSharePanel benchmark=');
    expect(modal).toContain("dynamic(() => import('./ActiveSharePanel')");
    expect(modal).not.toContain('<AirsRiskPanel');
    expect(panel).toContain("(['active', 'corr', 'vol', 'dd', 'conc'] as const)");
    expect(panel).toContain("useState<'active' | 'te' | 'corr' | 'vol' | 'dd' | 'conc'>('active')");
    expect(panel).not.toContain('Current portfolio simulation');
    expect(panel).not.toContain('Detailed analysis');
  });
});
