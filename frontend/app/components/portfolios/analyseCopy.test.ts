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
  const en = leaves(ANALYSE_COPY.en);
  const nl = leaves(ANALYSE_COPY.nl);

  it('has the same complete shape in both languages', () => {
    expect(Object.keys(nl).sort()).toEqual(Object.keys(en).sort());
    expect(Object.values(nl).filter((value) => !value.trim())).toEqual([]);
  });

  it('does not leave ordinary English labels in Dutch', () => {
    const sameByDesign = new Set([
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

  it('localises the monthly-return dialog controls too', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'BookReturnChart.tsx'), 'utf8');
    expect(source).toContain('useAnalyseCopy');
    expect(source).toContain('copy.actions.lookThrough');
    expect(source).toContain('copy.actions.close');
    expect(source).not.toContain('>Look through certificates<');
    expect(source).not.toContain('aria-label="Close"');
  });

  it('replaces the previous month with a loading state immediately', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'BookReturnChart.tsx'), 'utf8');
    expect(source).toContain('<MonthlyPerformance key={selectedMonth}');
    expect(source).toContain('role="status" aria-live="polite"');
    expect(source).toContain('animate-spin');
  });

  it('also clears the old rows when certificate look-through changes', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'BookReturnChart.tsx'), 'utf8');
    const toggle = source.slice(source.indexOf('checked={lookThrough}'), source.indexOf('className="h-3.5'));
    expect(toggle).toContain('setData(null)');
    expect(toggle).toContain('setLookThrough(event.target.checked)');
  });

  it('uses the shared Toppenberg-only certificate default in the monthly-return dialog', () => {
    const source = readFileSync(join('app', 'components', 'portfolios', 'BookReturnChart.tsx'), 'utf8');
    expect(source).toContain("import { defaultLookThroughCertificates } from './portfolioDefaults'");
    expect(source).toContain('() => defaultLookThroughCertificates(portfolioName)');
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
