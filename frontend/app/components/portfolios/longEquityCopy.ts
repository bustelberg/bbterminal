import type { Lang } from '../../../lib/i18n';

/**
 * The Long Equity cards' HEADINGS, in both languages.
 *
 * ⚠⚠ THE HEADING ONLY — not the stat tiles, legends, tooltips or footnotes on these cards. That is
 * the scope that was asked for, and it is a coherent one: a reader scanning fourteen cards in a
 * grid is reading the titles, and the titles are what say which metric they are looking at.
 *
 * ⚠⚠ AND IT IS A SEPARATE LOOKUP FROM `MetricCfg.title`, DELIBERATELY. The four growth cards carry
 * their title inside a config object that is ALSO used as a React `key` in `LongEquityTab`
 * (`key={revenue.title}`), as a series label, and as the fallback display name when a book has
 * none. Translating the config field itself would change the key with the language — remounting
 * every card and refetching its data on a switch that should only repaint text. So the config keeps
 * its English identity and gains a stable `titleKey`; only the rendered `<h4>` is translated.
 *
 * ⚠ EVERY ENTRY IS A FUNCTION OF `sbc`, INCLUDING THE ONES THAT IGNORE IT. Three headings genuinely
 * change with the SBC-correction checkbox (`FCF margin` → `FCF-SBC margin`), and a record mixing
 * bare strings with functions means every call site has to know which kind it is holding. One shape
 * costs a `()` and removes that question.
 *
 * ⚠ THE DUTCH IS LONGER THAN THE ENGLISH, sometimes by a lot — "SBC / OCF" against
 * "Aandelenbeloning / operationele kasstroom". These headings sit in a grid where the cards' stat
 * tiles are read as a row, so a heading that wraps to two lines pushes one card's tiles out of line
 * with its neighbours' (see the ⚠ on `CashReturnCard`'s `<h4>`). That is a real cost of spelling
 * the terms out, accepted because a reader who cannot tell which metric a card shows has a worse
 * problem than a ragged grid.
 */

export const CHART_KEYS = [
  'sharePrice', 'epsNri', 'revenue', 'fcfPs', 'shares',
  'fcfMargin', 'croic', 'roic', 'debtAssets', 'interestBurden', 'sbcOcf',
  'investedCapital', 'capexMargin', 'dividendYield', 'fcfYield', 'grossMargin', 'cashConversion',
] as const;
export type ChartKey = (typeof CHART_KEYS)[number];

/** `(sbcCorrection) => heading`. Most entries ignore the argument — see the ⚠ above. */
export type ChartTitles = Record<ChartKey, (sbc: boolean) => string>;

const en: ChartTitles = {
  sharePrice: () => 'Share price',
  epsNri: () => 'EPS (excl. non-recurring)',
  revenue: () => 'Revenue',
  fcfPs: () => 'FCF / share',
  shares: () => 'Shares outstanding',
  fcfMargin: (sbc) => `${sbc ? 'FCF-SBC' : 'FCF'} margin`,
  croic: () => 'Cash return on capital',
  roic: () => 'Return on invested capital',
  debtAssets: () => 'Debt / assets ex-GW',
  interestBurden: () => 'Interest / op. profit',
  sbcOcf: () => 'SBC / OCF',
  investedCapital: () => 'Invested capital',
  capexMargin: () => 'Capex margin',
  dividendYield: () => 'Dividend yield',
  fcfYield: (sbc) => `${sbc ? 'FCF-SBC' : 'FCF'} yield`,
  grossMargin: () => 'Gross margin',
  cashConversion: (sbc) => (sbc ? '(FCF − SBC) / Net Income' : 'FCF / Net Income'),
};

/**
 * ⚠ THE SAME DUTCH TERM FOR THE SAME THING AS THE `Tables` TAB USES. Both surfaces live in the same
 * modal, one summarising the other, so "vrije kasstroom / aandeel" and "rendement op geïnvesteerd
 * vermogen" have to be the identical phrase in both — a summary that renames the rows it summarises
 * is a summary of something else. See `tablesCopy`.
 *
 * ⚠ `SBC` SURVIVES AS A SHORT MARKER where spelling it out would double a heading's length
 * ("na aandelenbeloning"). It is the one abbreviation kept, and it is kept because the checkbox
 * that drives these three headings is itself labelled `SBC correction` in the tab's control row —
 * so the heading names the control the reader just clicked.
 */
const nl: ChartTitles = {
  sharePrice: () => 'Aandelenkoers',
  epsNri: () => 'Winst per aandeel (excl. bijzondere posten)',
  revenue: () => 'Omzet',
  fcfPs: () => 'Vrije kasstroom / aandeel',
  shares: () => 'Uitstaande aandelen',
  fcfMargin: (sbc) => (sbc ? 'Vrije kasstroom-marge na SBC' : 'Vrije kasstroom-marge'),
  croic: () => 'Kasrendement op kapitaal',
  roic: () => 'Rendement op geïnvesteerd vermogen',
  debtAssets: () => 'Schuld / activa excl. goodwill',
  interestBurden: () => 'Rente / bedrijfsresultaat',
  sbcOcf: () => 'Aandelenbeloning / operationele kasstroom',
  investedCapital: () => 'Geïnvesteerd vermogen',
  capexMargin: () => 'Investeringen / omzet',
  dividendYield: () => 'Dividendrendement',
  fcfYield: (sbc) => (sbc ? 'Vrije kasstroom-rendement na SBC' : 'Vrije kasstroom-rendement'),
  grossMargin: () => 'Brutomarge',
  cashConversion: (sbc) => (sbc
    ? '(Vrije kasstroom − SBC) / nettowinst'
    : 'Vrije kasstroom / nettowinst'),
};

export const CHART_TITLES: Record<Lang, ChartTitles> = { en, nl };

/** One card's heading. `sbc` matters to three of them; passing it always is the cheaper rule. */
export function chartTitle(lang: Lang, key: ChartKey, sbc = false): string {
  return CHART_TITLES[lang][key](sbc);
}
