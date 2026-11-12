import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('Analyse opening-page request boundary', () => {
  it('loads complete opening accounting without an after-paint replacement request', () => {
    expect(source).toContain('&overview_only=${chartsOnly}');
    expect(source).not.toContain('const overviewOnly = !assetFilter && !data;');
    expect(source).not.toContain('const loadDetails =');
    expect(source).not.toContain('secondFrame = window.requestAnimationFrame');
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
