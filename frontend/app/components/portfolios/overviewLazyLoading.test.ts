import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('Analyse opening-page request boundary', () => {
  it('automatically fills holdings after the overview paints, without a toggle button', () => {
    expect(source).toContain('const needsHoldingDetails = !reportOpen && (assetFilter == null || risk);');
    expect(source).toContain('!needsHoldingDetails || !detailsPending');
    expect(source).toContain('loadedFor !== viewKey');
    expect(source).toContain('const firstFrame = window.requestAnimationFrame');
    expect(source).toContain('secondFrame = window.requestAnimationFrame');
    expect(source).toContain('window.cancelAnimationFrame(secondFrame)');
    expect(source).not.toContain('holdingsOpen');
    expect(source).not.toContain('Hide holdings');
    expect(source).not.toContain('View holdings');
    expect(source).not.toContain('if (!isBasket && b.details_pending)');
    expect(source).toContain('data.details_pending ? (');
    expect(source).toContain('Loading holdings…');
    expect(source).toContain('motion-reduce:animate-none');
  });

  it('does not request ETF sector data unless the Stocks view is selected', () => {
    expect(source).toContain('assetFilter !== EQUITY_BUCKET || !includeEtfsInSector');
  });
});
