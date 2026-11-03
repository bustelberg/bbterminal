import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const panel = readFileSync(new URL('./ActiveSharePanel.tsx', import.meta.url), 'utf8');
const modal = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('Risk navigation', () => {
  it('opens risk measures and weekly Returns directly without an AIRS overview wrapper', () => {
    expect(modal).toContain('<ActiveSharePanel benchmark=');
    expect(modal).toContain("dynamic(() => import('./ActiveSharePanel')");
    expect(modal).not.toContain('<AirsRiskPanel');
    expect(panel).toContain("(['active', 'corr', 'vol', 'dd', 'conc', 'returns'] as const)");
    expect(panel).toContain("useState<'active' | 'te' | 'corr' | 'vol' | 'dd' | 'conc' | 'returns'>('active')");
    expect(panel).toContain('<WeeklyReturnsPanel portefeuille={returnsAccount ?? portefeuille}');
    expect(modal).toContain('returnsAccount={reportAccount ?? data.book_portefeuille}');
    expect(panel).not.toContain('Current portfolio simulation');
    expect(panel).not.toContain('Detailed analysis');
  });
});
