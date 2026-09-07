'use client';

import { useLang, type Lang } from '../../../lib/i18n';
import { type Basis } from './quickValuation';

/**
 * The Quick Valuation tab's RENDERED chrome, in both languages — headings, captions, tile labels,
 * chart legends, the basis switch and the empty states.
 *
 * ⚠⚠ THE ⓘ CARDS ARE NOT IN SCOPE AND STAY ENGLISH, deliberately, and this is the same line
 * `longEquityCopy` drew for the Graphs cards: what a reader scanning the tab reads is the headings,
 * the tile labels and the legends, and those are what say which number they are looking at. The
 * `AspectCard` prose behind each ⓘ is a much larger body of measured, caveat-heavy text (it names
 * GuruFocus line items, explains why a yield is not a multiple, and carries the SBC caveat) and
 * translating it half-way would be worse than not starting.
 *
 * ⚠⚠ SO THE ⓘ PROSE KEEPS USING `BASIS[...]`'s ENGLISH LABELS AND THE UI USES THESE. That split is
 * the point, not an oversight: an English sentence with a Dutch metric name spliced into it —
 * "the cash the business threw off per share" under the heading `VKS per aandeel` — reads as a
 * rendering fault. Call sites hold `b` (English, for prose) and `bl` (translated, for what is
 * drawn), and the two are never mixed inside one string.
 *
 * ⚠ `BASIS[...].codes` AND `.estimateCodes` ARE DATA AND ARE NOT HERE. They are GuruFocus metric
 * codes; a translated code matches nothing and the chart silently empties.
 */

/** The display half of a `BASIS` entry — everything that is drawn rather than computed with. */
export type BasisLabels = {
  /** The switch's own label. */
  tab: string;
  /** The per-share series' name, as it appears in headings, legends and row labels. */
  perShare: string;
  /** Sentence-initial ("FCF yield") vs mid-sentence ("Current FCF yield") — two fields because
   *  `.toLowerCase()` does not survive an acronym. */
  yieldTitle: string;
  yieldInline: string;
  /** How the multiple is written: `P/E`, `P/FCF`. */
  multiple: string;
  /** What a negative year is called. A cash-burn year and a loss year are not the same event. */
  negativeYear: string;
};

export type QuickValuationCopy = {
  basis: Record<Basis, BasisLabels>;
  /** `Price vs FCF per share` — the primary chart. */
  priceVs: (perShare: string) => string;
  indexedAt: (year: string) => string;
  notPlottable: (n: string, negativeYear: string) => string;
  priceCagr: string;
  perShareCagr: (perShare: string) => string;
  currentSharePrice: string;
  priceTarget: string;
  priceTargetFy: (year: string) => string;
  estCagr: string;
  estCagrTo: (year: string) => string;
  noHistory: (perShare: string, name: string) => string;
  noPositiveBase: (perShare: string) => string;
  legendSharePrice: string;
  legendTrend: (r2: string, years: string) => string;
  yieldCaption: (perShare: string) => string;
  yieldLegend: (yieldTitle: string) => string;
  avg: string;
  latest: string;
  /** The forward-multiple card. */
  multipleForward: (multiple: string) => string;
  sinceMedian: (year: string) => string;
  vendorIndicator: string;
  median: string;
  forwardTile: (multiple: string) => string;
  asOf: string;
  loading: string;
  noForwardFcf: string;
  noForwardPublished: (multiple: string, year: string) => string;
  /**
   * The Price target card beside the chart.
   *
   * ⚠ ITS ROW LABELS ARE BUILT FROM THE BASIS, so `Current FCF per share` and `Current EPS` are one
   * template. Writing the seven rows out per basis would be fourteen strings that must agree with
   * the switch, which is the drift `BASIS` exists to prevent.
   */
  pt: {
    title: string;
    reset: string;
    resetAria: string;
    current: (perShare: string) => string;
    forecastCagr: (perShare: string) => string;
    forecast: (perShare: string) => string;
    currentYield: (yieldInline: string) => string;
    forecastYield: (yieldInline: string) => string;
    currentSharePrice: string;
    forecastSharePrice: string;
    estCagr: string;
    estCagrTo: (year: string) => string;
    /** The provenance badges on the share-price row. */
    fiscalBadge: string;
    fiscalBadgeTitle: string;
    staleBadge: (days: string) => string;
    staleBadgeTitle: (date: string, days: string) => string;
  };
};

const EN: QuickValuationCopy = {
  basis: {
    fcf: {
      tab: 'FCF',
      perShare: 'FCF per share',
      yieldTitle: 'FCF yield',
      yieldInline: 'FCF yield',
      multiple: 'P/FCF',
      negativeYear: 'cash-burn',
    },
    eps: {
      tab: 'EPS',
      perShare: 'EPS',
      yieldTitle: 'Earnings yield',
      yieldInline: 'earnings yield',
      multiple: 'P/E',
      negativeYear: 'loss',
    },
  },
  priceVs: (perShare) => `Price vs ${perShare}`,
  indexedAt: (year) => `indexed to 100 at FY${year} · log scale`,
  notPlottable: (n, negativeYear) =>
    `⚠ ${n} ${negativeYear} year${n === '1' ? '' : 's'} not plottable on a log axis`,
  priceCagr: 'Price CAGR',
  perShareCagr: (perShare) => `${perShare} CAGR`,
  currentSharePrice: 'Current share price',
  priceTarget: 'Price target',
  priceTargetFy: (year) => `Price target FY${year}`,
  estCagr: 'Est. CAGR',
  estCagrTo: (year) => `Est. CAGR to FY${year}`,
  noHistory: (perShare, name) => `No share price / ${perShare} history ingested for ${name}.`,
  noPositiveBase: (perShare) =>
    `No fiscal year has both a positive price and positive ${perShare}, so there is no base to index from.`,
  legendSharePrice: 'Share price',
  legendTrend: (r2, years) => `Trend (R² ${r2}), dotted = ${years}y projection`,
  yieldCaption: (perShare) => `${perShare} ÷ year-end price · average dashed`,
  yieldLegend: (yieldTitle) => `${yieldTitle} (avg dashed)`,
  avg: 'Avg',
  latest: 'Latest',
  multipleForward: (multiple) => `${multiple} — forward`,
  sinceMedian: (year) => `since ${year} · median dashed`,
  vendorIndicator: 'vendor indicator',
  median: 'Median',
  forwardTile: (multiple) => `Forward ${multiple}`,
  asOf: 'As of',
  loading: 'Loading…',
  noForwardFcf: 'GuruFocus has no historical forward-FCF series.',
  noForwardPublished: (multiple, year) =>
    `No forward ${multiple} published for this listing since ${year}.`,
  pt: {
    title: 'Price target',
    reset: 'reset',
    resetAria: 'Reset to the computed figure',
    current: (perShare) => `Current ${perShare}`,
    forecastCagr: (perShare) => `Forecast ${perShare} CAGR`,
    forecast: (perShare) => `Forecast ${perShare}`,
    currentYield: (yieldInline) => `Current ${yieldInline}`,
    forecastYield: (yieldInline) => `Forecast ${yieldInline}`,
    currentSharePrice: 'Current share price',
    forecastSharePrice: 'Forecast share price',
    estCagr: 'Est. CAGR',
    estCagrTo: (year) => `Est. CAGR to FY${year}`,
    fiscalBadge: '⚠ fiscal',
    fiscalBadgeTitle: 'No live close available — this is the fiscal year-end price.',
    staleBadge: (days) => `⚠ ${days}d old`,
    staleBadgeTitle: (date, days) => `Last close ${date} — ${days} days ago.`,
  },
};

/**
 * ⚠ TRANSLATED FROM THE ENGLISH ABOVE, never authored here — see the note on `Lang`.
 *
 * ⚠⚠ `FCF`, `EPS`, `P/FCF`, `P/E` AND `CAGR` STAY. They are what a Dutch wealth manager says out
 * loud — the same exception `managementCopy` records for "active share", "tracking error" and
 * "Sharpe". Spelling `FCF per share` out as `vrije kasstroom per aandeel` in a chart legend would
 * be both longer than the plot and a term nobody uses; `per aandeel` is the part that is ordinary
 * Dutch and is translated.
 */
const NL: QuickValuationCopy = {
  basis: {
    fcf: {
      tab: 'FCF',
      perShare: 'FCF per aandeel',
      yieldTitle: 'FCF-rendement',
      yieldInline: 'FCF-rendement',
      multiple: 'P/FCF',
      negativeYear: 'cash-burn',
    },
    eps: {
      tab: 'EPS',
      perShare: 'EPS',
      yieldTitle: 'Winstrendement',
      yieldInline: 'winstrendement',
      multiple: 'P/E',
      negativeYear: 'verlies',
    },
  },
  priceVs: (perShare) => `Koers vs ${perShare}`,
  indexedAt: (year) => `geïndexeerd op 100 in FY${year} · logaritmische schaal`,
  // ⚠ `jaar` → `jaren`, NOT `jaar` + `en`. The double vowel drops in the Dutch plural, so
  // suffixing produces "jaaren" — which is what a naive port of the English `+ 's'` gives, and
  // what the first cut of this shipped as. The two forms are written out.
  notPlottable: (n, negativeYear) =>
    `⚠ ${n} ${negativeYear}${n === '1' ? 'jaar' : 'jaren'} niet weer te geven op een logaritmische as`,
  priceCagr: 'Koers-CAGR',
  perShareCagr: (perShare) => `CAGR ${perShare}`,
  currentSharePrice: 'Huidige koers',
  priceTarget: 'Koersdoel',
  priceTargetFy: (year) => `Koersdoel FY${year}`,
  estCagr: 'Verwachte CAGR',
  estCagrTo: (year) => `Verwachte CAGR t/m FY${year}`,
  noHistory: (perShare, name) =>
    `Geen koers- of ${perShare}-historie ingelezen voor ${name}.`,
  noPositiveBase: (perShare) =>
    `Geen enkel boekjaar heeft zowel een positieve koers als een positieve ${perShare}, dus er is geen basis om op te indexeren.`,
  legendSharePrice: 'Koers',
  legendTrend: (r2, years) => `Trend (R² ${r2}), gestippeld = projectie van ${years} jaar`,
  yieldCaption: (perShare) => `${perShare} ÷ koers op jaareinde · gemiddelde gestippeld`,
  yieldLegend: (yieldTitle) => `${yieldTitle} (gemiddelde gestippeld)`,
  avg: 'Gem.',
  latest: 'Laatste',
  multipleForward: (multiple) => `${multiple} — forward`,
  sinceMedian: (year) => `vanaf ${year} · mediaan gestippeld`,
  vendorIndicator: 'indicator van de leverancier',
  median: 'Mediaan',
  forwardTile: (multiple) => `Forward ${multiple}`,
  asOf: 'Per',
  loading: 'Laden…',
  noForwardFcf: 'GuruFocus heeft geen historische forward-FCF-reeks.',
  noForwardPublished: (multiple, year) =>
    `Geen forward ${multiple} gepubliceerd voor deze notering sinds ${year}.`,
  pt: {
    title: 'Koersdoel',
    reset: 'herstellen',
    resetAria: 'Terug naar de berekende waarde',
    // ⚠ `Huidige` / `Verwachte` AGREE WITH THE NOUN, and the noun differs per basis: `FCF per
    // aandeel` is de-woord ("de FCF per aandeel"), `EPS` likewise. Both take the -e form, so one
    // template is correct for every row here — which is why these stay templates rather than
    // fourteen written-out strings.
    current: (perShare) => `Huidige ${perShare}`,
    forecastCagr: (perShare) => `Verwachte CAGR ${perShare}`,
    forecast: (perShare) => `Verwachte ${perShare}`,
    currentYield: (yieldInline) => `Huidig ${yieldInline}`,
    forecastYield: (yieldInline) => `Verwacht ${yieldInline}`,
    currentSharePrice: 'Huidige koers',
    forecastSharePrice: 'Verwachte koers',
    estCagr: 'Verwachte CAGR',
    estCagrTo: (year) => `Verwachte CAGR t/m FY${year}`,
    fiscalBadge: '⚠ fiscaal',
    fiscalBadgeTitle: 'Geen actuele slotkoers beschikbaar — dit is de koers op het einde van het '
      + 'boekjaar.',
    staleBadge: (days) => `⚠ ${days}d oud`,
    staleBadgeTitle: (date, days) => `Laatste slotkoers ${date} — ${days} dagen geleden.`,
  },
};

export const QUICK_VALUATION_COPY: Record<Lang, QuickValuationCopy> = { en: EN, nl: NL };

export function useQuickValuationCopy(): QuickValuationCopy {
  const [lang] = useLang();
  return QUICK_VALUATION_COPY[lang];
}
