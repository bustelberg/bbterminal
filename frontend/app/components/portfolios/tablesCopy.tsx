import type { ReactNode } from 'react';
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
 * footnote's clauses invert — so a shared skeleton with holes punched in it forces English grammar
 * onto the translation. Each language owns its whole sentence, including where the number goes.
 *
 * ⚠ AND THE FOOTNOTE RETURNS A NODE, for the same reason one step further. It emphasises a word
 * mid-sentence (`same` / `hetzelfde`), and that word does not sit in the same place in the two
 * languages. Splitting the prose into fragments around a fixed `<strong>` would pin both languages
 * to the English clause order.
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
 * in here: this list exists for the footnote clause about point-to-point vs fitted trend, which is
 * a statement about measuring HISTORY against the Long Equity growth cards. A forecast has no card
 * to disagree with.
 */
export const RATE_KEYS = [
  'revCagr', 'epsCagr', 'fcfCagr', 'priceCagr', 'invCapCagr', 'sharesCagr',
] as const satisfies readonly MeasureKey[];

export type TablesCopy = {
  /** ⚠ The heading follows the window chips — see the ⚠ on it in `TablesTab`. */
  title: (windows: readonly number[]) => string;
  /** The year suffix used on column headings, the `3y` badge AND in the footnote that names it.
   *  ⚠ ONE VALUE FOR ALL THREE: a footnote pointing at a `3y` marker beside a column reading `3j`
   *  is a note about a thing that is not on screen. */
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
   * footnote rather than nine times in nine tooltips.
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
  /** The "why they differ" link inside the footnote, and what it says. */
  whyDiffer: string;
  whyDifferLabel: string;
  footnote: (o: {
    windows: readonly number[]; showEps: boolean; showFcf: boolean; showPrice: boolean;
    /** Is a row on whose series is drawn from a FILTERED set of companies — FCF/share or EPS?
     *  ⚠ A member rule nobody can see is the whole hazard (see `_POSITIVE_ONLY_METRICS`), and a
     *  tooltip on one row is not where a reader comparing two columns will find it. */
    showFiltered: boolean;
    whyLink: ReactNode;
  }) => ReactNode;
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
  whyDiffer:
    'Point-to-point is (end/start)^(1/n) − 1: only the two endpoint years matter, so one weak year '
    + 'at either end swings it. The card\'s log-linear fit uses all of them and reports R² for how '
    + 'well they line up. Neither is wrong; a wide gap between them means the endpoints are '
    + 'unrepresentative.',
  whyDifferLabel: 'why they differ',
  footnote: ({ windows, showEps, showFcf, showPrice, showFiltered, whyLink }) => (
    <>
      Both sides are measured over the <strong>same</strong> window per row — the latest year they
      share — so the Excess column subtracts like from like. A dash means one side has nothing
      there; hover it.
      {/* ⚠ IN THE PROSE, NOT ONLY IN THE ROW'S TOOLTIP. Every other row on this table is a
          fundamental, where "no dividends" and "no FX" are not questions anyone thinks to ask. A
          price row invites both, and a reader who assumes either is reading a different number
          from the one on screen — a return they could check against a statement. */}
      {showPrice && <>
        {' '}The share-price row is <strong>price only</strong> — no dividends, on either side —
        and it chains each holding’s growth in its own currency, so no FX leg is in it. It is the
        market’s view of this basket, not the book’s EUR return.
      </>}
      {showEps && <>
        {' '}The last row is the <strong>only</strong> one the{' '}
        {windows.map((w) => `${w}y`).join('/')} heading{windows.length > 1 ? 's do' : ' does'} not
        apply to: the consensus thins fast (measured on ACWI, 2031e is carried by 166 of 1,761
        constituents against 2028e’s 1,310), so it is stated over three years and marked{' '}
        <code className="text-fg-subtle">3y</code> on the figure
        {windows.length > 1 && ', centred across both columns rather than sitting in either'}.
      </>}
      {/* ⚠ EVERY RATE ROW, NOT JUST FCF/SHARE. The clause used to name one row because there was
          one; with six of them, singling out FCF/share reads as "the others DO match the cards",
          which is the opposite of true. */}
      {showFcf && <>
        {' '}The rate rows are point-to-point and will not match the growth cards on the
        Graphs tab, which fit a trend through every year ({whyLink}).
      </>}
      {/* ⚠⚠ THE MEMBER RULE, IN THE PROSE. Every series on this table is the one the Long Equity
          chart of the same name draws — same holdings, same weighting, same coverage floor — and
          that includes a rule which DELETES COMPANIES. A filter nobody can see is the whole
          hazard: what is left looks exactly like an ordinary line. The cards print their own
          "n of m"; a table of rates has nowhere to put one per row, so it is said once, here. */}
      {showFiltered && <>
        {' '}The <strong>FCF per share</strong> and <strong>EPS</strong> rows are drawn from the
        same series as the charts on Graphs — and, like them, only from the companies whose
        figure is <strong>positive in every period</strong>, analyst estimates included. The rest
        are excluded outright, so read those two rows as how the survivors grew.
      </>}
    </>
  ),
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
    'Er moet minstens één venster aan blijven — anders resteren alleen de rijlabels.',
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
    + 'de grafiek op Graphs — dit is die lijn, gemiddeld.',
  showNumbers: 'Toon de cijfers achter deze regel — elke positie, elk jaar, en de getallen waaruit '
    + 'elk cijfer is berekend',
  noCoverage: (from, to) =>
    `Over ${from}–${to} is helemaal geen rente betaald, dus er is geen dekking om te tonen — delen `
    + 'door niets heeft geen uitkomst. Dat is de best mogelijke uitkomst, geen ontbrekend cijfer.',
  noCoverageExcess:
    'Eén kant heeft over deze periode helemaal geen rente betaald en heeft dus geen dekking — een '
    + 'verschil met een cijfer dat niet bestaat zou een getal over niets zijn.',
  whyDiffer:
    'Van eindpunt tot eindpunt is (eind/begin)^(1/n) − 1: alleen de twee eindjaren tellen, dus één '
    + 'zwak jaar aan een van beide kanten laat het uitslaan. De log-lineaire fit van de kaart '
    + 'gebruikt ze allemaal en rapporteert R² voor hoe goed ze op één lijn liggen. Geen van beide '
    + 'is fout; een groot verschil betekent dat de eindpunten niet representatief zijn.',
  whyDifferLabel: 'waarom ze verschillen',
  footnote: ({ windows, showEps, showFcf, showPrice, showFiltered, whyLink }) => (
    <>
      Beide zijden worden per rij over <strong>hetzelfde</strong> venster gemeten — het laatste jaar
      dat ze delen — zodat de kolom Verschil gelijk van gelijk aftrekt. Een streepje betekent dat
      één zijde daar niets heeft; beweeg erover.
      {showPrice && <>
        {' '}De koersrij is <strong>alleen koers</strong> — zonder dividend, aan geen van beide
        zijden — en ketent de groei van elke positie in haar eigen valuta, dus er zit geen
        valuta-effect in. Het is de blik van de markt op deze mand, niet het eurorendement van het
        boek.
      </>}
      {showEps && <>
        {' '}De laatste rij is de <strong>enige</strong> waarop de kop{' '}
        {windows.map((w) => `${w}j`).join('/')} niet van toepassing is: de consensus dunt snel uit
        (gemeten op ACWI wordt 2031e gedragen door 166 van de 1.761 bestanddelen, tegen 1.310 voor
        2028e), dus hij wordt over drie jaar gegeven en bij het getal gemarkeerd met{' '}
        <code className="text-fg-subtle">3j</code>
        {windows.length > 1 && ', gecentreerd over beide kolommen in plaats van in één ervan'}.
      </>}
      {showFcf && <>
        {' '}De groeirijen lopen van eindpunt tot eindpunt en zullen niet overeenkomen met de
        groeikaarten op het tabblad Graphs, die een trend door alle jaren leggen
        ({whyLink}).
      </>}
      {showFiltered && <>
        {' '}De rijen <strong>Vrije kasstroom per aandeel</strong> en <strong>Winst per
        aandeel</strong> gebruiken dezelfde reeks als de grafieken op Graphs — en net als daar alleen de bedrijven waarvan
        het cijfer in <strong>elke periode positief</strong> is, analistenramingen inbegrepen. De
        overige vallen er volledig uit; lees die twee rijen dus als de groei van de overblijvers.
      </>}
    </>
  ),
};

export const COPY: Record<Lang, TablesCopy> = { en, nl };
