import { describe, expect, it } from 'vitest';
import { ANALYSE_COPY } from './analyseCopy';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { benchmarkProvenance } from './benchmarkSourceNote';

function leaves(node: unknown, path = '', out: Record<string, string> = {}): Record<string, string> {
  if (typeof node === 'string') out[path] = node;
  else if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node)) leaves(value, path ? `${path}.${key}` : key, out);
  }
  return out;
}

describe('Analyse primary-view copy', () => {
  it('keeps reconciliation warnings brief without calling rounded-zero differences missing positions', () => {
    expect(ANALYSE_COPY.en.reconciliation.missing('+5.80%', '+4.50pp'))
      .toBe('AIRS: +5.80%. Calculated: +4.50pp. The difference is unexplained, not a separate investment return.');
    expect(ANALYSE_COPY.nl.reconciliation.missing('+5.80%', '+4.50pp'))
      .toContain('Het verschil is onverklaard');
    expect(ANALYSE_COPY.en.reconciliation.mismatch('+4.03pp', '+4.03%', '-0.00pp')).toBe('Difference from AIRS: <0.01pp.');
    expect(ANALYSE_COPY.nl.reconciliation.mismatch('+4.03pp', '+4.03%', '+0.00pp')).toBe('Verschil met AIRS: <0.01pp.');
    expect(ANALYSE_COPY.en.reconciliation.mismatch('+4.03pp', '+4.33%', '-0.30pp')).toBe('Difference from AIRS: -0.30pp.');
    const source = readFileSync(join('app', 'components', 'portfolios', 'PortfolioAnalysisModal.tsx'), 'utf8');
    expect(source).not.toContain('copy.serverText(realised.residual_reason)');
  });

  const en = leaves(ANALYSE_COPY.en);
  const nl = leaves(ANALYSE_COPY.nl);

  it('has the same complete shape in both languages', () => {
    expect(Object.keys(nl).sort()).toEqual(Object.keys(en).sort());
    expect(Object.values(nl).filter((value) => !value.trim())).toEqual([]);
  });

  it('does not leave ordinary English labels in Dutch', () => {
    const sameByDesign = new Set([
      'actions.attribution', // User-chosen feature name: Performance in both languages.
      'chrome.benchmark', 'axes.sector', 'holdings.via', 'holdings.sector', 'holdings.momentum',
      'holdings.result', 'holdings.price', 'holdings.currency', 'holdings.rest',
      'holdings.direct', 'row.momentumNote',
      //  A UNIT, NOT A WORD. `YTD (€)` is the same caption in both languages — "YTD" is what a
      // Dutch wealth manager says out loud, and `(€)` is a symbol. Translating it would invent a
      // label nobody uses; the SENTENCES that explain the tile (`sleeve.ytdWhat/Note/How`) are
      // translated and are not on this list.
      'sleeve.ytdUnit',
      //  `Beta`, NOT `Bèta` (2026-09-07, on request). The accented form is the correct Dutch
      // spelling of the Greek letter's NAME, and it is not what this column is: `Beta` is the
      // finance term, spelled the same way in both languages and in every screener a reader will
      // compare this against. `copyParity`'s `FINANCE_EN` already lists it beside `Sharpe` and
      // `Alpha` for the same reason; this module keeps its own list, so it needs the entry too.
      'holdings.beta',
      'columnGroups.momentum.label',
      'columnGroups.beta.label',
    ]);
    expect(Object.keys(en).filter((key) => en[key] === nl[key] && !sameByDesign.has(key))).toEqual([]);
  });

  it('translates every asset class', () => {
    for (const name of ['Stocks', 'Bonds', 'Alternatives', 'Cash', 'Unclassified']) {
      expect(ANALYSE_COPY.nl.bucket(name)).not.toBe(name);
    }
  });

  it('translates the weighted benchmark scorecard caption', () => {
    expect(ANALYSE_COPY.en.score.weightedBenchmark).toBe('Weighted benchmark return (YTD) €');
    expect(ANALYSE_COPY.nl.score.weightedBenchmark).toBe('Gewogen benchmarkrendement (YTD) €');
  });

  it('keeps primary-view prose out of the component literals', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'PortfolioAnalysisModal.tsx'), 'utf8');
    const forbidden = [
      'Sold during the year — no longer a holding', 'No valued positions to show here.',
      'How the AIRS return is built', 'What this portfolio returned year to date',
      'A dash is not a zero', 'No sector — a fund', 'The book’s year',
      'positions, everything it held or sold', 'Fundamental — is',
      'This index is rebuilt from', 'Why the trading mattered for',
    ];
    expect(forbidden.filter((phrase) => source.includes(phrase))).toEqual([]);
  });

  it('keeps the return chart without an Explain returns dialog or hidden chart-click opener', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'BookReturnChart.tsx'), 'utf8');
    expect(source).not.toContain('Explain returns');
    expect(source).not.toContain('MonthlyPerformance');
    expect(source).not.toContain('monthly-yahoo-performance');
    expect(source).not.toContain('monthlyPerformanceCache');
    expect(source).not.toContain('role="dialog"');
    expect(source).not.toContain('Open monthly holding returns');
    expect(source).not.toContain('onClickCapture');
    expect(source).toContain('<AreaChart');
    expect(source).toContain('<ReturnTooltip');
    expect(source).toContain('/value-series');
  });

  it('localises benchmark provenance too', () => {
    const card = benchmarkProvenance({ source: 'etf', label: 'ACWI', ticker: 'ACWI' }, 'nl');
    expect(card.what).toContain('eigen EUR-rendement');
    expect(card.note).toContain('sinds jaarbegin');
    expect(card.how).toContain('slot');
  });

  it('explains that AIRS dividends are aligned to the remaining shares', () => {
    const note = ANALYSE_COPY.nl.row.airsReturnNote('BUS_Offensief_Dyn');
    expect(note).toContain('koers- en valutaresultaat');
    expect(note).toContain('nettodividend dat aan de resterende aandelen toekomt');
    expect(note).toContain('latere aankopen verhogen het nooit');
  });

  it('names both return measures plainly and explains the dated IRR procedure', () => {
    expect(ANALYSE_COPY.en.holdings.instrumentReturn).toBe('AIRS return');
    expect(ANALYSE_COPY.en.holdings.moneyWeighted).toBe('Money-weighted return');
    expect(ANALYSE_COPY.nl.holdings.instrumentReturn).toBe('AIRS-rendement');
    expect(ANALYSE_COPY.nl.holdings.moneyWeighted).toBe('Geldgewogen rendement');

    const how = ANALYSE_COPY.en.info.moneyHow('€100', '€1,000', '+10.00%');
    expect(how).toContain('Opening value and purchases are negative cash flows');
    expect(how).toContain('Sales, net income and the final valuation are positive cash flows');
    expect(how).toContain('discounted money invested equals discounted money received');
    expect(how).toContain('signed discounted cash flows sum to zero');
    expect(how).toContain('Result: +10.00% over the actual holding period (not annualised)');
    expect(how).toContain('typeset equation and every input used are shown below');
    expect(ANALYSE_COPY.en.info.moneyAggregateNote).toContain('Individual position rows use dated XIRR');
  });
});
