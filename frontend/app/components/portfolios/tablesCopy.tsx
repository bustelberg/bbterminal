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
 * ⚠ GROUPED BY WHAT THEY ANSWER, not by when they were added: the three RATES first (what grew,
 * and how fast), then the four per-year RATIOS averaged over the window (how good the business is),
 * then the one FORWARD row last — an expectation is a different kind of claim from a measurement
 * and reads oddly in among them.
 */
export const MEASURE_KEYS = [
  'revCagr', 'fcfCagr', 'priceCagr',
  'grossMargin', 'fcfMargin', 'roic', 'cashConv', 'intCover',
  'epsFwd',
] as const;
export type MeasureKey = (typeof MEASURE_KEYS)[number];

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
    fcfCagr: 'FCF per share CAGR',
    priceCagr: 'Share price CAGR',
    grossMargin: 'Gross margin',
    fcfMargin: 'FCF margin',
    roic: 'ROIC',
    cashConv: 'Cash conversion',
    intCover: 'Interest coverage',
    epsFwd: 'EPS expected',
  },
  rowLabel: {
    revCagr: 'Revenue CAGR',
    fcfCagr: 'FCF per share CAGR',
    priceCagr: 'Share price CAGR',
    grossMargin: 'Gross margin (avg)',
    fcfMargin: 'FCF margin (avg)',
    roic: 'ROIC (avg)',
    cashConv: 'Cash conversion (avg)',
    intCover: 'Interest coverage (avg)',
    epsFwd: 'EPS (excl. NRI) expected, 3y',
  },
  rowFormula: {
    revCagr: () => '(revenue index at the end ÷ at the start) ^ (1 ÷ years) − 1',
    fcfCagr: () => '(FCF-per-share index at the end ÷ at the start) ^ (1 ÷ years) − 1',
    priceCagr: () => '(price index at the end ÷ at the start) ^ (1 ÷ years) − 1',
    grossMargin: () =>
      'per year: Σ(w × gross profit ÷ revenue) ÷ Σw, then the mean of those years',
    fcfMargin: (sbc) =>
      `per year: Σ(w × (FCF${sbc ? ' − SBC' : ''}) ÷ revenue) ÷ Σw, then the mean of those years`,
    roic: () => 'per year: Σ(w × ROIC) ÷ Σw, then the mean of those years',
    cashConv: (sbc) =>
      `per year: Σ(w × (FCF${sbc ? ' − SBC' : ''}) ÷ net income) ÷ Σw, then the mean of those `
      + 'years',
    // ⚠ THE BRACKETS ARE THE CLAIM: the inversion is OUTSIDE the mean. See `coverageFromBurden`.
    intCover: () =>
      '100 ÷ [ per year: Σ(w × interest ÷ operating profit) ÷ Σw, then the mean of those years ]',
    epsFwd: () => '(consensus EPS ÷ the latest reported EPS) ^ (1 ÷ years) − 1',
  },
  rowNote: {
    revCagr: () =>
      'Compound annual growth of the weighted REVENUE line, point to point — the same chaining, '
      + 'per-period cap weighting and coverage floor as the other rate rows. '
      + '⚠ A LEVEL, SO IT IS CHAINED FROM WEIGHTED GROWTH, never averaged from rebased revenues: '
      + 'the constituents report in different currencies, so their euros and dollars cannot be '
      + 'summed, but what each of them GREW by can be averaged.',
    grossMargin: () =>
      'Gross profit ÷ revenue, weighted across the holdings each year and averaged over the '
      + 'window. The cleanest read on pricing power. '
      + '⚠ A BANK HAS NO GROSS PROFIT LINE AT ALL — GuruFocus’s bank template reports net interest '
      + 'income instead — so a book with banks in it is averaged over the rest, and the coverage '
      + 'floor decides whether a year is drawn at all.',
    cashConv: (sbc) =>
      `Free cash flow ${sbc ? 'net of stock comp ' : ''}÷ net income, weighted per year and `
      + 'averaged over the window — whether the reported profit turns into money. '
      + '⚠ 100% IS BREAK-EVEN, NOT A CEILING: above it the business converts more cash than it '
      + 'books as profit, which is a compliment. ⚠ The numerator is whole-company cash while the '
      + 'denominator is the SHAREHOLDERS’ line, so a group with large minorities reads high. '
      + 'Follows the SBC checkbox.',
    intCover: () =>
      'Operating profit ÷ interest expense — how many times over the book covers its interest, '
      + 'averaged over the window. '
      + '⚠⚠ IT IS ONE OVER THE WEIGHTED INTEREST BURDEN, AND THAT IS THE CORRECT AGGREGATE rather '
      + 'than a shortcut: the burden (interest as a share of profit) is the additive quantity, '
      + 'exactly as an earnings yield is where a P/E is not, so its reciprocal is the weighted '
      + 'HARMONIC mean of the holdings’ coverages. Averaging coverage directly would let one '
      + 'debt-free name scoring in the thousands set the book’s figure. '
      + '⚠ A year the book pays no interest at all has no coverage to state — it is a dash, not '
      + '∞, and not a zero.',
    fcfCagr: () =>
      'Compound annual growth of the weighted FCF-per-share line, point to point. '
      + '⚠ The Long Equity growth card fits a log-linear TREND through every year instead '
      + '(that is what its R² is about), so the two will differ — most where one endpoint '
      + 'year is unrepresentative, which is when the gap is worth seeing.',
    priceCagr: () =>
      'Compound annual growth of the weighted SHARE-PRICE line, point to point — the same '
      + 'weighting, chaining and coverage floor as the rows around it, run over each holding’s '
      + 'fiscal-year-end share price. What the market did with the same basket. '
      + '⚠ PRICE ONLY: dividends are not in it, on either side, so a high-yielding book reads '
      + 'lower here than its total return. '
      + '⚠ AND IT CARRIES NO FX LEG. Each holding’s price is in its own currency and the line '
      + 'chains per-holding growth, so this is a local-currency price return — NOT the book’s EUR '
      + 'return, which is what the Analyse modal reports.',
    fcfMargin: (sbc) =>
      `Free cash flow ${sbc ? 'net of stock comp ' : ''}÷ revenue, averaged over `
      + 'the window. A ratio does not compound, so this is a mean and not a rate — it is '
      + 'the Long Equity margin chart, averaged. Follows the SBC checkbox.',
    roic: () =>
      'GuruFocus’s own published return on invested capital, weight-weighted per year '
      + 'and averaged over the window. ⚠ Unaffected by the SBC checkbox — there is no '
      + 'numerator of ours to adjust.',
    epsFwd: () =>
      'Compound annual growth from the latest REPORTED EPS excluding non-recurring items '
      + 'to the analyst consensus three years out. ⚠ NOT A MEASUREMENT — it is what '
      + 'analysts expect today, and only the constituents they cover are in it. The base '
      + 'is an actual on purpose: measuring 2026e → 2029e would be the consensus’s own '
      + 'internal slope, with no contact with anything that happened.',
  },
  rateTip: (from, to, years) => `${from} → ${to}, ${years} years, compounded annually.`,
  meanTip: (n, from, to, of) =>
    `Mean of ${n} year${n === 1 ? '' : 's'} over ${from}–${to}`
    + `${of == null ? '' : `, of the ${of} asked for`}. Weighted per year by the same weights the `
    + 'Long Equity chart uses — this is that line, averaged.',
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
  footnote: ({ windows, showEps, showFcf, showPrice, whyLink }) => (
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
      {showFcf && <>
        {' '}The CAGR row is point-to-point and will not match the FCF/share growth card, which
        fits a trend through every year ({whyLink}).
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
    fcfCagr: 'Vrije kasstroom per aandeel',
    priceCagr: 'Aandelenkoers',
    grossMargin: 'Brutomarge',
    fcfMargin: 'Vrije kasstroom-marge',
    roic: 'Rendement op geïnvesteerd vermogen',
    cashConv: 'Kasstroomconversie',
    intCover: 'Rentedekking',
    epsFwd: 'Winst per aandeel verwacht',
  },
  rowLabel: {
    revCagr: 'Omzet CAGR',
    fcfCagr: 'Vrije kasstroom per aandeel CAGR',
    priceCagr: 'Aandelenkoers CAGR',
    grossMargin: 'Brutomarge (gem.)',
    fcfMargin: 'Vrije kasstroom-marge (gem.)',
    roic: 'Rendement op geïnvesteerd vermogen (gem.)',
    cashConv: 'Kasstroomconversie (gem.)',
    intCover: 'Rentedekking (gem.)',
    epsFwd: 'Winst per aandeel (excl. bijzondere posten) verwacht, 3j',
  },
  rowFormula: {
    revCagr: () => '(omzetindex aan het eind ÷ aan het begin) ^ (1 ÷ jaren) − 1',
    fcfCagr: () =>
      '(index vrije kasstroom per aandeel aan het eind ÷ aan het begin) ^ (1 ÷ jaren) − 1',
    priceCagr: () => '(koersindex aan het eind ÷ aan het begin) ^ (1 ÷ jaren) − 1',
    grossMargin: () =>
      'per jaar: Σ(w × brutowinst ÷ omzet) ÷ Σw, daarna het gemiddelde van die jaren',
    fcfMargin: (sbc) =>
      `per jaar: Σ(w × (vrije kasstroom${sbc ? ' − SBC' : ''}) ÷ omzet) ÷ Σw, daarna het `
      + 'gemiddelde van die jaren',
    roic: () => 'per jaar: Σ(w × ROIC) ÷ Σw, daarna het gemiddelde van die jaren',
    cashConv: (sbc) =>
      `per jaar: Σ(w × (vrije kasstroom${sbc ? ' − SBC' : ''}) ÷ nettowinst) ÷ Σw, daarna het `
      + 'gemiddelde van die jaren',
    intCover: () =>
      '100 ÷ [ per jaar: Σ(w × rente ÷ bedrijfsresultaat) ÷ Σw, daarna het gemiddelde van die '
      + 'jaren ]',
    epsFwd: () =>
      '(verwachte winst per aandeel ÷ de laatst gerapporteerde) ^ (1 ÷ jaren) − 1',
  },
  rowNote: {
    revCagr: () =>
      'Samengestelde jaarlijkse groei van de gewogen OMZETLIJN, van punt tot punt — dezelfde '
      + 'ketening, weging per periode en dekkingsdrempel als de andere groeiregels. '
      + '⚠ EEN NIVEAU, DUS GEKETEND UIT GEWOGEN GROEI, nooit gemiddeld uit herbasiseerde omzetten: '
      + 'de deelnemingen rapporteren in verschillende valuta, dus hun euro’s en dollars kunnen niet '
      + 'worden opgeteld — waarmee ze GEGROEID zijn wel.',
    grossMargin: () =>
      'Brutowinst ÷ omzet, per jaar gewogen over de posities en gemiddeld over de periode. De '
      + 'zuiverste maatstaf voor prijszettingsmacht. '
      + '⚠ EEN BANK HEEFT GEEN BRUTOWINSTREGEL — GuruFocus rapporteert daar netto rentebaten — dus '
      + 'een boek met banken wordt over de rest gemiddeld.',
    cashConv: (sbc) =>
      `Vrije kasstroom ${sbc ? 'na aandelenbeloning ' : ''}÷ nettowinst, per jaar gewogen en `
      + 'gemiddeld over de periode — of de gerapporteerde winst ook geld wordt. '
      + '⚠ 100% IS HET BREEKPUNT, GEEN PLAFOND: daarboven zet de onderneming meer kasstroom om dan '
      + 'zij als winst boekt, wat een compliment is. Volgt het SBC-vinkje.',
    intCover: () =>
      'Bedrijfsresultaat ÷ rentelasten — hoe vaak het boek zijn rente dekt, gemiddeld over de '
      + 'periode. '
      + '⚠⚠ HET IS ÉÉN GEDEELD DOOR DE GEWOGEN RENTELAST, en dat is de juiste aggregatie: de '
      + 'rentelast (rente als aandeel van de winst) is de optelbare grootheid, net zoals een '
      + 'winstrendement dat is waar een koers-winstverhouding dat niet is. Rechtstreeks middelen '
      + 'zou één schuldenvrije naam met een dekking in de duizenden het cijfer laten bepalen. '
      + '⚠ Een jaar zonder rentelasten heeft geen dekking om te tonen — dat is een streepje, geen '
      + 'oneindig en geen nul.',
    fcfCagr: () =>
      'Samengestelde jaarlijkse groei van de gewogen lijn van de vrije kasstroom per aandeel, van '
      + 'eindpunt tot eindpunt. ⚠ De groeikaart in Long Equity legt in plaats daarvan een '
      + 'log-lineaire TREND '
      + 'door alle jaren (dáár gaat de R² over), dus de twee zullen verschillen — het meest '
      + 'wanneer één eindjaar niet representatief is, en juist dan is het verschil de moeite waard.',
    priceCagr: () =>
      'Samengestelde jaarlijkse groei van de gewogen KOERSLIJN, van eindpunt tot eindpunt — '
      + 'dezelfde weging, kettingberekening en dekkingsdrempel als de rijen eromheen, toegepast op '
      + 'de slotkoers per aandeel aan het einde van elk boekjaar. Wat de markt met dezelfde mand '
      + 'heeft gedaan. ⚠ ALLEEN KOERS: dividenden zitten er niet in, aan geen van beide zijden, '
      + 'dus een boek met veel dividend leest hier lager dan zijn totaalrendement. '
      + '⚠ EN ER ZIT GEEN VALUTA-EFFECT IN. Elke positie noteert in haar eigen valuta en de lijn '
      + 'ketent de groei per positie, dus dit is een koersrendement in lokale valuta — NIET het '
      + 'eurorendement van het boek, dat de Analyse-modal rapporteert.',
    fcfMargin: (sbc) =>
      `Vrije kasstroom ${sbc ? 'na aandelenbeloning ' : ''}÷ omzet, gemiddeld over het venster. `
      + 'Een verhouding groeit niet samengesteld, dus dit is een gemiddelde en geen groeivoet — '
      + 'het is de margegrafiek uit Long Equity, gemiddeld. Volgt het SBC-vinkje.',
    roic: () =>
      'Het door GuruFocus zelf gepubliceerde rendement op geïnvesteerd vermogen, per jaar gewogen '
      + 'en gemiddeld over het venster. ⚠ Niet beïnvloed door het SBC-vinkje — er is geen teller '
      + 'van onszelf om aan te passen.',
    epsFwd: () =>
      'Samengestelde jaarlijkse groei van de laatst GERAPPORTEERDE winst per aandeel exclusief '
      + 'bijzondere posten naar de analistenconsensus over drie jaar. ⚠ GEEN METING — dit is wat '
      + 'analisten vandaag verwachten, en alleen de bestanddelen die zij volgen zitten erin. De '
      + 'basis is bewust een realisatie: 2026e → 2029e meten zou de interne helling van de '
      + 'consensus zelf zijn, zonder enig contact met wat er werkelijk gebeurd is.',
  },
  rateTip: (from, to, years) =>
    `${from} → ${to}, ${years} jaar, jaarlijks samengesteld.`,
  meanTip: (n, from, to, of) =>
    `Gemiddelde van ${n} jaar over ${from}–${to}`
    + `${of == null ? '' : `, van de ${of} gevraagde`}. Per jaar gewogen met dezelfde wegingen als `
    + 'de Long Equity-grafiek — dit is die lijn, gemiddeld.',
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
  footnote: ({ windows, showEps, showFcf, showPrice, whyLink }) => (
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
        {' '}De CAGR-rij loopt van eindpunt tot eindpunt en zal niet overeenkomen met de
        groeikaart voor de vrije kasstroom per aandeel, die een trend door alle jaren legt
        ({whyLink}).
      </>}
    </>
  ),
};

export const COPY: Record<Lang, TablesCopy> = { en, nl };
