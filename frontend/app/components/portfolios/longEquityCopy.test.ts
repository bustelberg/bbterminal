/**
 * The Long Equity card headings, in both languages.
 *
 * ⚠⚠ THE ENGLISH SIDE IS A REGRESSION TEST, NOT A TRANSLATION TEST. These sixteen strings were
 * LIFTED OUT of fourteen components' `<h4>`s into one table; every one of them was previously a
 * literal beside the markup that drew it. A typo made during that move renames a chart, and a
 * renamed chart is not something the reader can catch — there is no second copy on screen to
 * disagree with it. So the English is asserted verbatim.
 *
 * ⚠ AND `croic`/`roic` ARE ASSERTED AGAINST `MODES`, which is the real invariant rather than a
 * restatement of the constant. `CashReturnCard` draws its heading from here and its basis-switch
 * tooltip from `MODES[k].title`/`.what`/`.where` — two strings, side by side, naming the same
 * metric. If they drift, the card's own explanation describes a different ratio from its title.
 *
 * Pure — no DOM, no network.
 */
import { describe, expect, it } from 'vitest';

import { LANGS, type Lang } from '../../../lib/i18n';
import { MODES } from './cashReturnData';
import { CHART_KEYS, CHART_TITLES, chartTitle, type ChartKey } from './longEquityCopy';

/** Exactly what each `<h4>` rendered before the strings moved into `longEquityCopy`. */
const ENGLISH_BEFORE: Record<ChartKey, [string, string]> = {
  //                        [ sbc off,                    sbc on                     ]
  epsNri: ['EPS (excl. non-recurring)', 'EPS (excl. non-recurring)'],
  revenue: ['Revenue', 'Revenue'],
  fcfPs: ['FCF / share', 'FCF / share'],
  shares: ['Shares outstanding', 'Shares outstanding'],
  fcfMargin: ['FCF margin', 'FCF-SBC margin'],
  croic: ['Cash return on capital', 'Cash return on capital'],
  roic: ['Return on invested capital', 'Return on invested capital'],
  debtAssets: ['Debt / assets ex-GW', 'Debt / assets ex-GW'],
  interestBurden: ['Interest / op. profit', 'Interest / op. profit'],
  sbcOcf: ['SBC / OCF', 'SBC / OCF'],
  investedCapital: ['Invested capital', 'Invested capital'],
  capexMargin: ['Capex margin', 'Capex margin'],
  dividendYield: ['Dividend yield', 'Dividend yield'],
  fcfYield: ['FCF yield', 'FCF-SBC yield'],
  grossMargin: ['Gross margin', 'Gross margin'],
  // ⚠ U+2212 MINUS, not a hyphen — the character the card has always used.
  cashConversion: ['FCF / Net Income', '(FCF − SBC) / Net Income'],
};

describe('the English headings survived the move unchanged', () => {
  it.each(CHART_KEYS)('%s reads exactly as it did', (key: ChartKey) => {
    const [off, on] = ENGLISH_BEFORE[key];
    expect(chartTitle('en', key, false)).toBe(off);
    expect(chartTitle('en', key, true)).toBe(on);
  });

  it('the capital-return headings agree with the basis switch that explains them', () => {
    // ⚠ NOT A RESTATEMENT OF A CONSTANT — `MODES` feeds the tooltip on the same card's switch.
    expect(chartTitle('en', 'croic')).toBe(MODES.croic.title);
    expect(chartTitle('en', 'roic')).toBe(MODES.roic.title);
  });
});

describe('both languages are complete', () => {
  it.each(LANGS)('%s has a non-empty heading for every chart, both SBC states', (lang: Lang) => {
    for (const key of CHART_KEYS) {
      expect(chartTitle(lang, key, false).trim(), `${key}`).not.toBe('');
      expect(chartTitle(lang, key, true).trim(), `${key} (sbc)`).not.toBe('');
    }
  });

  it('the Dutch is actually Dutch, not the English pasted across', () => {
    const same = CHART_KEYS.filter((k) => chartTitle('en', k) === chartTitle('nl', k));
    expect(same, 'untranslated headings').toEqual([]);
  });

  it('only the three SBC-driven headings change with the checkbox', () => {
    // ⚠ IN BOTH LANGUAGES. A translation that folds the two states into one phrase would leave the
    // checkbox with no visible effect on that card — the state is legible from the heading, and
    // that is the only place it is stated per card.
    for (const lang of LANGS) {
      const moves = CHART_KEYS.filter((k) => chartTitle(lang, k, false) !== chartTitle(lang, k, true));
      expect([...moves].sort(), lang).toEqual(['cashConversion', 'fcfMargin', 'fcfYield']);
    }
  });
});

describe('the two translated surfaces agree with each other', () => {
  /**
   * ⚠⚠ THE `Tables` TAB SUMMARISES THESE CARDS, IN THE SAME MODAL. Its rows are named in
   * `tablesCopy`; these headings are named here. Two tables of the same metrics under two different
   * Dutch names is a summary of something else, and nothing on screen would say which name is the
   * real one. This pins the overlap rather than trusting two files to be edited together.
   */
  it('names the same metric the same way in Dutch as the Tables tab does', async () => {
    const { COPY } = await import('./tablesCopy');
    const nl = COPY.nl;
    // The rate row and the FCF/share growth card are the same line.
    expect(nl.chip.fcfCagr).toBe(chartTitle('nl', 'fcfPs'));
    // The ROIC row and the capital-return card's ROIC basis are the same ratio.
    expect(nl.chip.roic).toBe(chartTitle('nl', 'roic'));
    // The margin row and the margin card, uncorrected.
    expect(nl.chip.fcfMargin).toBe(chartTitle('nl', 'fcfMargin', false));
  });
});

describe('an unkeyed config keeps its English heading', () => {
  /** ⚠ `titleKey` IS OPTIONAL so a `MetricCfg` built outside Long Equity (`QuickValuationTab`) is
   *  not forced into a table it has no entry in — `MetricGrowthCard` falls back to `cfg.title`. */
  it('every Long Equity key exists in both tables', () => {
    for (const lang of LANGS) {
      expect(Object.keys(CHART_TITLES[lang]).sort()).toEqual([...CHART_KEYS].sort());
    }
  });
});
