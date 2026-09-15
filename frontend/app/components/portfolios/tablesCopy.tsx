import type { Lang } from '../../../lib/i18n';

/**
 * Every user-visible string in the `Tables` tab, in both languages.
 *
 * ⚠⚠ THE COPY IS A TYPE, NOT A LOOKUP WITH A FALL-BACK. `COPY` is `Record<Lang, TablesCopy>`, so a
 * string added to English and forgotten in Dutch fails `tsc` — it does not quietly render the
 * English one. A table that is 90% Dutch reads as a rendering fault rather than an unfinished
 * translation, and the reader cannot tell which of the two it is looking at.
 *
 * ⚠ INTERPOLATION IS A FUNCTION PER STRING, NEVER A TEMPLATE WITH `{placeholders}`. Dutch does not
 * share English word order — "the 5-year column" is "de 5-jaarskolom", and the expectation
 * labels have different word order — so a shared skeleton with holes punched in it forces English
 * grammar onto the translation. Each language owns its whole sentence, including where the number
 * goes.
 *
 * ⚠ WHAT IS **NOT** TRANSLATED, ON PURPOSE:
 *  - The dash tooltips (`Cagr.reason`, `WindowMean.reason`). They are produced by `lineCagr.ts` and
 *    `windowStats.ts` — pure modules shared with `CagrTable` and the growth cards, none of which
 *    are translated. Threading a language through them would either duplicate the modules or
 *    translate surfaces nobody asked for, and duplicated diagnostics drift.
 *  - The benchmark labels (AEX / SP500 / ACWI), the portfolio's own name, and server error text.
 *    Those are identifiers and vendor strings, not prose.
 */

/** The rows, declared once and language-free — the labels for these live in `COPY`. */
/**
 * The rows, in the order they are drawn.
 *
 * ⚠ GROUPED BY WHAT THEY ANSWER, not by when they were added: the RATES first (what grew, and how
 * fast), then the per-year RATIOS averaged over the window (how good the business is), then the one
 * FORWARD row last — an expectation is a different kind of claim from a measurement and reads oddly
 * in among them.
 *
 * ⚠⚠ THERE IS ONE RATE ROW PER **LEVEL** CHART ON THE LONG EQUITY TAB, AND THAT IS THE RULE. Share
 * price, EPS, Revenue, FCF/share, Invested capital and Shares outstanding are all currency-or-count
 * levels that COMPOUND, so "what did it grow at" is the summary of each — and a tab that draws six
 * such charts while summarising three leaves the reader to eyeball the other three off a log axis.
 * The RATIO charts (margins, ROIC, coverage, yields) do not compound and get a window MEAN instead;
 * annualising a percentage that oscillates around a level is not a rate of anything.
 *
 * ⚠ THE RATE ORDER MIRRORS THE CHART ORDER on the tab, so a reader moving between the two is not
 * re-finding rows: revenue → EPS → FCF/share → price → invested capital → shares.
 */
export const MEASURE_KEYS = [
  'revCagr', 'epsCagr', 'fcfCagr', 'priceCagr', 'invCapCagr', 'sharesCagr',
  'grossMargin', 'fcfMargin', 'roic', 'cashConv', 'intCover',
  'epsFwd',
] as const;
export type MeasureKey = (typeof MEASURE_KEYS)[number];

/**
 * The rows that are RATES — a compounded growth of a level — as opposed to a window mean.
 *
 * ⚠ DECLARED, NOT INFERRED FROM THE `Cagr` SUFFIX. `epsFwd` is a rate too and is deliberately NOT
 * in here: the forecast is a rate too but is not a historical level chart.
 */
export const RATE_KEYS = [
  'revCagr', 'epsCagr', 'fcfCagr', 'priceCagr', 'invCapCagr', 'sharesCagr',
] as const satisfies readonly MeasureKey[];

export type TablesCopy = {
  /** ⚠ The heading follows the window chips — see the ⚠ on it in `TablesTab`. */
  title: (windows: readonly number[]) => string;
  /** The year suffix used on column headings and the `3y` badge. */
  yearSuffix: string;
  rowsLabel: string;
  loading: string;
  noRows: string;
  colMeasure: string;
  colExcess: string;
  /** Tooltips on the window chips. */
  lastWindowLocked: string;
  showWindow: (w: number) => string;
  /** Tooltips on the row chips. */
  hideRow: (chip: string) => string;
  showRow: (chip: string) => string;
  /** Short names, for the filter chips — scanned, not read. */
  chip: Record<MeasureKey, string>;
  /** Full names, for the rows themselves. */
  rowLabel: Record<MeasureKey, string>;
  /**
   * The row's FORMULA IN SYMBOLS — the first line of its ⓘ, above a blank line and then the same
   * formula with this book's own numbers in it (`TablesTab::subFor` builds that second half).
   *
   * ⚠⚠ SYMBOLS FIRST, SUBSTITUTION SECOND, AND THE BLANK LINE BETWEEN THEM IS THE POINT — it is
   * the shape the Money-weighted column already uses, asked for here by name. A reader checking a
   * figure has two separate doubts ("what was computed?" and "does that arithmetic give this?") and
   * prose answers only the first. The substitution answers the second WITHOUT sending them
   * anywhere: the drill-down behind the row label carries every holding and every year, which is
   * the right place for a full audit and the wrong one for "is 84.8× the average of those ten".
   *
   * ⚠ NO EM DASH IN ANY OF THESE STRINGS. `AboutCard` promotes a leading fragment before ' — ' to
   * the card's bold title when it is under 48 characters and carries no sentence punctuation — and
   * half of a formula, bolded, with the rest starting mid-expression, is exactly the wrong split.
   * Commas and colons instead; the guard is real but it should not be the only thing holding.
   *
   * ⚠ `w` IS NOT DEFINED IN EVERY FORMULA, deliberately. It is the same weight on every row (the
   * holding's own share, or an index constituent's cap for that period), stated once in the
   * row-specific tooltip rather than nine times in the table body.
   */
  rowFormula: Record<MeasureKey, (sbc: boolean) => string>;
  /** The row's own explanation, on hover, BELOW the worked formula. `sbc` is the checkbox. */
  rowNote: Record<MeasureKey, (sbc: boolean) => string>;
  /** A rate cell's hover: the window it was actually measured over. */
  rateTip: (from: string, to: string, years: number) => string;
  /** A mean cell's hover. `of` is the window asked for; `n < of` means it is short. */
  meanTip: (n: number, from: string, to: string, of: number | null) => string;
  /** The row label's own hover: what clicking it opens. ⚠ IT PROMISES THE INPUTS, NOT "details" —
   *  a reader who doubts a figure is looking for the numbers it was divided from, and a vaguer word
   *  makes them guess whether it is worth a click. */
  showNumbers: string;
  /** ⚠ A COVERAGE THAT DOES NOT EXIST, which is a statement about the book rather than a gap: no
   *  interest was paid across the whole window, so there is nothing to cover. See
   *  `coverageFromBurden`. */
  noCoverage: (from: string, to: string) => string;
  /** The same absence in the Excess column, where naming the window twice would be noise. */
  noCoverageExcess: string;
};

const en: TablesCopy = {
  title: (w) => `Quality, ${w.length > 1 ? 'five and ten years' : `${w[0]} years`}`,
  yearSuffix: 'y',
  rowsLabel: 'Rows',
  loading: 'Loading…',
  noRows: 'No rows selected — turn one on above.',
  colMeasure: 'Measure',
  colExcess: 'Excess (pp)',
  lastWindowLocked:
    'At least one window has to stay on — with none there is nothing to show but the row labels.',
  showWindow: (w) => `Show the ${w}-year column for both sides and the excess.`,
  hideRow: (chip) => `Hide ${chip}`,
  showRow: (chip) => `Show ${chip}`,
  chip: {
    revCagr: 'Revenue CAGR',
    epsCagr: 'EPS CAGR',
    fcfCagr: 'FCF per share CAGR',
    priceCagr: 'Share price CAGR',
    invCapCagr: 'Invested capital CAGR',
    sharesCagr: 'Share count CAGR',
    grossMargin: 'Gross margin',
    fcfMargin: 'FCF margin',
    roic: 'ROIC',
    cashConv: 'Cash conversion',
    intCover: 'Interest coverage',
    epsFwd: 'EPS expected',
  },
  rowLabel: {
    revCagr: 'Revenue CAGR',
    epsCagr: 'EPS (excl. NRI) CAGR',
    fcfCagr: 'FCF per share CAGR',
    priceCagr: 'Share price CAGR',
    invCapCagr: 'Invested capital CAGR',
    sharesCagr: 'Shares outstanding CAGR',
    grossMargin: 'Gross margin (avg)',
    fcfMargin: 'FCF margin (avg)',
    roic: 'ROIC (avg)',
    cashConv: 'Cash conversion (avg)',
    intCover: 'Interest coverage (avg)',
    epsFwd: 'EPS (excl. NRI) expected, 3y',
  },
  rowFormula: {
    revCagr: () => `\\left(\\dfrac{\\text{revenue}_{\\text{end}}}{\\text{revenue}_{\\text{start}}}\\right)^{1/n} - 1`,
    epsCagr: () => `\\left(\\dfrac{\\text{EPS}_{\\text{end}}}{\\text{EPS}_{\\text{start}}}\\right)^{1/n} - 1`,
    fcfCagr: () => `\\left(\\dfrac{\\text{FCF/share}_{\\text{end}}}{\\text{FCF/share}_{\\text{start}}}\\right)^{1/n} - 1`,
    priceCagr: () => `\\left(\\dfrac{\\text{price}_{\\text{end}}}{\\text{price}_{\\text{start}}}\\right)^{1/n} - 1`,
    invCapCagr: () => `\\left(\\dfrac{\\text{capital}_{\\text{end}}}{\\text{capital}_{\\text{start}}}\\right)^{1/n} - 1`,
    sharesCagr: () => `\\left(\\dfrac{\\text{shares}_{\\text{end}}}{\\text{shares}_{\\text{start}}}\\right)^{1/n} - 1`,
    grossMargin: () => `\\text{mean}_{\\text{years}}\\left(\\dfrac{\\sum w \\cdot (\\text{gross profit} / \\text{revenue})}{\\sum w}\\right)`,
    fcfMargin: (sbc) => `\\text{mean}_{\\text{years}}\\left(\\dfrac{\\sum w \\cdot (\\text{FCF}${sbc ? ' - \\text{SBC}' : ''}) / \\text{revenue}}{\\sum w}\\right)`,
    roic: () => `\\text{mean}_{\\text{years}}\\left(\\dfrac{\\sum w \\cdot \\text{ROIC}}{\\sum w}\\right)`,
    cashConv: (sbc) => `\\text{mean}_{\\text{years}}\\left(\\dfrac{\\sum w \\cdot (\\text{FCF}${sbc ? ' - \\text{SBC}' : ''}) / \\text{net income}}{\\sum w}\\right)`,
    intCover: () => `\\dfrac{100}{\\text{mean}_{\\text{years}}\\left(\\dfrac{\\sum w \\cdot (\\text{interest} / \\text{operating profit})}{\\sum w}\\right)}`,
    epsFwd: () => `\\left(\\dfrac{\\text{EPS}_{\\text{consensus}}}{\\text{EPS}_{\\text{latest}}}\\right)^{1/n} - 1`,
  },
  rowNote: {
    revCagr: () => 'Weighted revenue line, point to point.',
    epsCagr: () => 'Weighted EPS line, point to point. History, not the expectation below.',
    fcfCagr: () => 'Weighted FCF-per-share line, point to point. The card fits a trend instead.',
    priceCagr: () => 'Weighted share-price line, point to point. Price only, in local currency.',
    invCapCagr: () => 'Weighted invested-capital line, point to point. The ROIC row’s denominator.',
    sharesCagr: () => 'Weighted share-count line, point to point. Negative is buybacks.',
    grossMargin: () => 'Gross profit over revenue. A bank has no gross profit line.',
    fcfMargin: (sbc) => `Free cash flow${sbc ? ' net of stock comp' : ''} over revenue.`,
    roic: () => 'GuruFocus’s own return on invested capital. Not touched by the SBC box.',
    cashConv: (sbc) =>
      `Free cash flow${sbc ? ' net of stock comp' : ''} over net income. 100% is break-even.`,
    intCover: () =>
      'Times over the book covers its interest. One over the weighted burden, so a debt-free '
      + 'name cannot run away with it.',
    epsFwd: () => 'Latest reported EPS to the 3-year consensus. An expectation, not a measurement.',
  },
  rateTip: (from, to, years) => `${from} → ${to}, ${years} years, compounded annually.`,
  meanTip: (n, from, to, of) =>
    `Mean of ${n} year${n === 1 ? '' : 's'} over ${from}–${to}`
    + `${of == null ? '' : `, of the ${of} asked for`}. Weighted per year by the same weights the `
    + 'chart on Graphs uses — this is that line, averaged.',
  showNumbers: 'Show the numbers behind this row — every holding, every year, and the figures each '
    + 'one was computed from',
  noCoverage: (from, to) =>
    `No interest was paid at all over ${from}–${to}, so there is no coverage to state — dividing `
    + 'by nothing has no answer. That is the best possible outcome, not a missing figure.',
  noCoverageExcess:
    'One side paid no interest at all over this window, so it has no coverage — and a difference '
    + 'against a figure that does not exist would be a number about nothing.',
};

const nl: TablesCopy = {
  title: (w) => `Kwaliteit, ${w.length > 1 ? 'vijf en tien jaar' : `${w[0]} jaar`}`,
  yearSuffix: 'j',
  rowsLabel: 'Rijen',
  loading: 'Laden…',
  noRows: 'Geen rijen geselecteerd — zet er hierboven één aan.',
  colMeasure: 'Maatstaf',
  colExcess: 'Verschil (pp)',
  lastWindowLocked:
    'Er moet minstens één periode aan blijven — anders resteren alleen de rijlabels.',
  showWindow: (w) => `Toon de ${w}-jaarskolom voor beide zijden en het verschil.`,
  hideRow: (chip) => `${chip} verbergen`,
  showRow: (chip) => `${chip} tonen`,
  // ⚠⚠ SPELLED OUT, NOT ABBREVIATED — AND WITH NO ACRONYM LEFT ANYWHERE, INCLUDING THE CHIPS. The
  // English acronyms do not survive the crossing: `FCF` and `EPS` are read on sight by an
  // English-speaking analyst, their Dutch contractions are not, and `WPA` for winst per aandeel
  // landed as unreadable despite being a real abbreviation.
  //
  // `ROIC` was kept one revision longer on the chip — a control is scanned rather than read, and
  // the row named the acronym once to tie the two together. That was still the English label
  // wearing a Dutch table, and it is gone. The chips are now longer than their English counterparts
  // and that is the correct trade: this language reads them, it does not decode them.
  chip: {
    revCagr: 'Omzet',
    epsCagr: 'Winst per aandeel',
    fcfCagr: 'Vrije kasstroom per aandeel',
    priceCagr: 'Aandelenkoers',
    invCapCagr: 'Geïnvesteerd vermogen',
    sharesCagr: 'Aantal aandelen',
    grossMargin: 'Brutomarge',
    fcfMargin: 'Vrije kasstroom-marge',
    roic: 'Rendement op geïnvesteerd vermogen',
    cashConv: 'Kasstroomconversie',
    intCover: 'Rentedekking',
    epsFwd: 'Winst per aandeel verwacht',
  },
  rowLabel: {
    revCagr: 'Omzet CAGR',
    epsCagr: 'Winst per aandeel (excl. eenmalig) CAGR',
    fcfCagr: 'Vrije kasstroom per aandeel CAGR',
    priceCagr: 'Aandelenkoers CAGR',
    invCapCagr: 'Geïnvesteerd vermogen CAGR',
    sharesCagr: 'Uitstaande aandelen CAGR',
    grossMargin: 'Brutomarge (gem.)',
    fcfMargin: 'Vrije kasstroom-marge (gem.)',
    roic: 'Rendement op geïnvesteerd vermogen (gem.)',
    cashConv: 'Kasstroomconversie (gem.)',
    intCover: 'Rentedekking (gem.)',
    epsFwd: 'Winst per aandeel (excl. bijzondere posten) verwacht, 3j',
  },
  rowFormula: {
    revCagr: () => `\\left(\\dfrac{\\text{omzet}_{\\text{eind}}}{\\text{omzet}_{\\text{begin}}}\\right)^{1/n} - 1`,
    epsCagr: () => `\\left(\\dfrac{\\text{WPA}_{\\text{eind}}}{\\text{WPA}_{\\text{begin}}}\\right)^{1/n} - 1`,
    fcfCagr: () => `\\left(\\dfrac{\\text{VKS/aandeel}_{\\text{eind}}}{\\text{VKS/aandeel}_{\\text{begin}}}\\right)^{1/n} - 1`,
    priceCagr: () => `\\left(\\dfrac{\\text{koers}_{\\text{eind}}}{\\text{koers}_{\\text{begin}}}\\right)^{1/n} - 1`,
    invCapCagr: () => `\\left(\\dfrac{\\text{vermogen}_{\\text{eind}}}{\\text{vermogen}_{\\text{begin}}}\\right)^{1/n} - 1`,
    sharesCagr: () => `\\left(\\dfrac{\\text{aandelen}_{\\text{eind}}}{\\text{aandelen}_{\\text{begin}}}\\right)^{1/n} - 1`,
    grossMargin: () => `\\text{gem.}_{\\text{jaren}}\\left(\\dfrac{\\sum w \\cdot (\\text{brutowinst} / \\text{omzet})}{\\sum w}\\right)`,
    fcfMargin: (sbc) => `\\text{gem.}_{\\text{jaren}}\\left(\\dfrac{\\sum w \\cdot (\\text{VKS}${sbc ? ' - \\text{SBC}' : ''}) / \\text{omzet}}{\\sum w}\\right)`,
    roic: () => `\\text{gem.}_{\\text{jaren}}\\left(\\dfrac{\\sum w \\cdot \\text{ROIC}}{\\sum w}\\right)`,
    cashConv: (sbc) => `\\text{gem.}_{\\text{jaren}}\\left(\\dfrac{\\sum w \\cdot (\\text{VKS}${sbc ? ' - \\text{SBC}' : ''}) / \\text{nettowinst}}{\\sum w}\\right)`,
    intCover: () => `\\dfrac{100}{\\text{gem.}_{\\text{jaren}}\\left(\\dfrac{\\sum w \\cdot (\\text{rente} / \\text{bedrijfsresultaat})}{\\sum w}\\right)}`,
    epsFwd: () => `\\left(\\dfrac{\\text{WPA}_{\\text{consensus}}}{\\text{WPA}_{\\text{laatste}}}\\right)^{1/n} - 1`,
  },
  rowNote: {
    revCagr: () => 'Gewogen omzetlijn, punt tot punt.',
    epsCagr: () => 'Gewogen WPA-lijn, punt tot punt. Historie, niet de verwachting hieronder.',
    fcfCagr: () =>
      'Gewogen lijn vrije kasstroom per aandeel, punt tot punt. De kaart fit een trend.',
    priceCagr: () => 'Gewogen koerslijn, punt tot punt. Alleen koers, in lokale valuta.',
    invCapCagr: () => 'Gewogen lijn geïnvesteerd vermogen, punt tot punt. De noemer van ROIC.',
    sharesCagr: () => 'Gewogen lijn aantal aandelen, punt tot punt. Negatief is inkoop.',
    grossMargin: () => 'Brutowinst gedeeld door omzet. Een bank heeft geen brutowinstregel.',
    fcfMargin: (sbc) => `Vrije kasstroom${sbc ? ' na aandelenbeloning' : ''} gedeeld door omzet.`,
    roic: () =>
      'Het rendement op geïnvesteerd vermogen van GuruFocus zelf. Los van het SBC-vinkje.',
    cashConv: (sbc) =>
      `Vrije kasstroom${sbc ? ' na aandelenbeloning' : ''} gedeeld door nettowinst. 100% is `
      + 'break-even.',
    intCover: () =>
      'Hoe vaak het boek zijn rente dekt. Eén gedeeld door de gewogen rentelast, zodat een '
      + 'schuldenvrije naam het niet overneemt.',
    epsFwd: () =>
      'Laatst gerapporteerde WPA naar de consensus over drie jaar. Een verwachting, geen meting.',
  },
  rateTip: (from, to, years) =>
    `${from} → ${to}, ${years} jaar, jaarlijks samengesteld.`,
  meanTip: (n, from, to, of) =>
    `Gemiddelde van ${n} jaar over ${from}–${to}`
    + `${of == null ? '' : `, van de ${of} gevraagde`}. Per jaar gewogen met dezelfde wegingen als `
    + 'de grafiek op Grafieken — dit is die lijn, gemiddeld.',
  showNumbers: 'Toon de cijfers achter deze regel — elke positie, elk jaar, en de getallen waaruit '
    + 'elk cijfer is berekend',
  noCoverage: (from, to) =>
    `Over ${from}–${to} is helemaal geen rente betaald, dus er is geen dekking om te tonen — delen `
    + 'door niets heeft geen uitkomst. Dat is de best mogelijke uitkomst, geen ontbrekend cijfer.',
  noCoverageExcess:
    'Eén kant heeft over deze periode helemaal geen rente betaald en heeft dus geen dekking — een '
    + 'verschil met een cijfer dat niet bestaat zou een getal over niets zijn.',
};

export const COPY: Record<Lang, TablesCopy> = { en, nl };
