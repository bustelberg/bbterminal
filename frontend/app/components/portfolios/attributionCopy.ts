'use client';

import { useLang, type Lang } from '../../../lib/i18n';

const en = {
  lang: 'en',
  axis: { sector: 'sector', region: 'region', currency: 'currency', group: 'group', bySector: 'by Sector', byRegion: 'by Region', byCurrency: 'by Currency' },
  chrome: {
    ytd: 'Year-to-date', since: 'Since-inception', startYear: 'Start of year', inception: 'At inception', hide: 'Hide',
    title: (window: string, benchmark: string) => `${window} performance attribution compared to ${benchmark}`,
    loading: 'Computing attribution…', error: 'Attribution could not be computed.',
    residual: (value: string) => `⚠ The effects do not sum to the excess (residual ${value}). This is NOT a valid decomposition — do not read the rows below as one.`,
  },
  headers: { name: 'Name', weight: 'Weight', ret: 'Ret.', contribution: 'Contr.', yourWeight: 'Your weight', indexWeight: 'Index weight', yourReturn: 'Your return', indexReturn: 'Index return', allocation: 'Allocation', selection: 'Selection', interaction: 'Interact.', total: 'Total', totalExcess: 'Total (= the excess)' },
  names: {
    yourHoldings: 'Your holdings', inBoth: 'in both', attributableModel: 'of the attributable model', constituents: 'constituents', ofIndex: 'of the index',
    noneMine: (bucket: string) => `You hold nothing in ${bucket} — the whole effect is the decision not to own it, so Selection and Interaction are zero.`,
    noneIndex: (benchmark: string, bucket: string) => `${benchmark} holds nothing in ${bucket}, so there is no index return to judge your picks against; the whole effect is allocation.`,
    shared: (benchmark: string) => `Marked rows are held in both your portfolio and ${benchmark}; a share class counts as the same company.`,
    contributors: 'Biggest contributors', detractors: 'Biggest detractors', detractorsHint: 'what cost you the most',
    winners: (benchmark: string) => `${benchmark} winners you didn’t own`,
    winnersHint: 'matched by COMPANY, not ISIN — a share class is not a different business', weightReturnHint: 'weight × return, in EUR',
  },
  row: {
    show: (bucket: string, benchmark: string) => `Show the names behind ${bucket} — what you hold and what ${benchmark} holds`,
    hide: (bucket: string) => `Hide the names behind ${bucket}`,
  },
  prov: {
    weightIn: (owner: string) => `weight in ${owner}`,
    eachShare: (owner: string) => `Each holding's share of ${owner}.`,
    share: (name: string, owner: string) => `${name}'s share of ${owner}.`,
    /**
     * ⚠⚠ THE PROSE IS ONE SHORT SENTENCE AND THE MATHS IS TYPESET (2026-09-01, on request). These
     * cards used to state their formulas as prose with Unicode operators — `Σ(w × r) ÷ Σw over
     * your Technology holdings`, eighteen of them, which is what put `AttributionPanel` on
     * `tooltipStyle`'s ratchet. That is a row of glyphs resembling a formula: no real scripts, a
     * summation with no limits, nothing checkable against the cell. The expressions moved to
     * `attributionFormulas.ts` and reach the card through `worked` + `legend`; what is left here is
     * the sentence saying what the number MEANS.
     */
    weightHow: (basis: string, owner: string) => `${basis}, as a share of ${owner}.`,
    returnNote: 'EUR return over the window', eachReturn: 'What each holding returned, in EUR.',
    holdingReturn: (name: string) => `What ${name} returned, in EUR.`,
    returnHow: (basis: string) => `Weighted by holding, where each is ${basis}.`,
    contribution: 'contribution',
    eachContribution: (owner: string, held: boolean) => held
      ? `How much of ${owner}'s return each holding is responsible for — a big move in a tiny position contributes almost nothing.`
      : `What each holding was worth to ${owner}.`,
    holdingContribution: (name: string, owner: string, held: boolean) => held
      ? `How much of ${owner}'s return ${name} is responsible for.` : `What ${name} was worth to ${owner}.`,
    contributionNote: (owner: string, held: boolean) => held ? `share of ${owner}'s return` : `what it was worth to ${owner}`,
    /** ⚠ THE MATRIX HAS NO `owner` VARIABLE — that lives in the Names table, where the same
     *  three columns describe either the book or the index. Here the portfolio side is always
     *  the reader's, so the legend needs a word for it rather than a passed-in name. */
    yours: 'your book',
    contributionHow: 'The holding’s weight times what it returned.',
    bucketsHow: (w: string) => `Every ${w} held on either side, classified the same way.`,
    yourWeightHow: (basis: string) => `${basis}, renormalised over the attributable holdings.`,
    indexWeightHow: (benchmark: string) =>
      `${benchmark}’s cap weight at the start of the window.`,
    yourReturnHow: (basis: string) => `Weighted by holding, where each is ${basis}.`,
    indexReturnHow: (benchmark: string) => `Weighted by constituent, in EUR, over ${benchmark}.`,
    allocationHow: 'What the over- or underweight was worth, scored against the index total.',
    selectionHow: 'What the picks were worth, held at the index’s own weight.',
    interactionHow: 'The cross term — tilt and picks together.',
    totalHow: 'The three effects added. The column sums to the excess.',
    returnLegend: (owner: string) => [
      { sym: 'w_i', is: `each holding’s weight in ${owner}` },
      { sym: 'r_i', is: 'what it returned over the window, in EUR' },
    ],
    effectLegend: (benchmark: string) => [
      { sym: 'w_p', is: 'your weight in the bucket' },
      { sym: 'w_b', is: `${benchmark}’s weight in it` },
      { sym: 'r_p', is: 'what your holdings there returned' },
      { sym: 'r_b', is: `what ${benchmark}’s did` },
      { sym: 'r_B', is: `${benchmark}’s return as a whole` },
    ],
  },
} as const;

/**
 * Widen the literal types `as const` produced, so `nl` may hold any string of the same shape.
 *
 * ⚠⚠ IT WIDENS A FUNCTION'S RETURN, IT DOES NOT FORCE IT TO `string`. The first version mapped
 * every function to `(...args) => string`, which was true while every entry returned prose — and
 * became wrong the moment a `legend` builder returned an array of `{sym, is}` for the typeset
 * cards. Recursing through the return type keeps the original guarantee (a Dutch entry has the same
 * SHAPE as the English one) without asserting what that shape is.
 */
type Widen<T> = T extends (...args: infer A) => infer R ? (...args: A) => Widen<R>
  : T extends string ? string
  : T extends readonly (infer E)[] ? Widen<E>[]
  : { -readonly [K in keyof T]: Widen<T[K]> };
export type AttributionCopy = Widen<typeof en>;

const nl: AttributionCopy = {
  lang: 'nl',
  axis: { sector: 'sector', region: 'regio', currency: 'valuta', group: 'groep', bySector: 'per sector', byRegion: 'per regio', byCurrency: 'per valuta' },
  chrome: {
    ytd: 'Sinds jaarbegin', since: 'Sinds ingangsdatum', startYear: 'Begin van het jaar', inception: 'Bij ingangsdatum', hide: 'Verbergen',
    title: (window, benchmark) => `${window}: rendementsattributie ten opzichte van ${benchmark}`,
    loading: 'Attributie berekenen…', error: 'De attributie kon niet worden berekend.',
    residual: (value) => `⚠ De effecten tellen niet op tot het meerrendement (restverschil ${value}). Dit is GEEN geldige ontleding — lees de rijen hieronder niet als één geheel.`,
  },
  headers: { name: 'Naam', weight: 'Gewicht', ret: 'Rend.', contribution: 'Bijdr.', yourWeight: 'Uw gewicht', indexWeight: 'Indexgewicht', yourReturn: 'Uw rendement', indexReturn: 'Indexrendement', allocation: 'Allocatie', selection: 'Selectie', interaction: 'Interactie', total: 'Totaal', totalExcess: 'Totaal (= het meerrendement)' },
  names: {
    yourHoldings: 'Uw posities', inBoth: 'in beide', attributableModel: 'van het toewijsbare model', constituents: 'constituenten', ofIndex: 'van de index',
    noneMine: (bucket) => `U houdt niets aan in ${bucket} — het hele effect is de keuze om deze niet te bezitten; Selectie en Interactie zijn daarom nul.`,
    noneIndex: (benchmark, bucket) => `${benchmark} houdt niets aan in ${bucket}. Er is dus geen indexrendement om uw selectie tegen af te zetten; het hele effect is allocatie.`,
    shared: (benchmark) => `Gemarkeerde rijen worden zowel door uw portefeuille als door ${benchmark} aangehouden; een andere aandelenklasse telt als dezelfde onderneming.`,
    contributors: 'Grootste positieve bijdragen', detractors: 'Grootste negatieve bijdragen', detractorsHint: 'wat het meeste kostte',
    winners: (benchmark) => `Winnaars in ${benchmark} die u niet bezat`,
    winnersHint: 'gekoppeld op ONDERNEMING, niet op ISIN — een aandelenklasse is geen ander bedrijf', weightReturnHint: 'gewicht × rendement, in EUR',
  },
  row: {
    show: (bucket, benchmark) => `Toon de namen achter ${bucket} — wat u en ${benchmark} aanhouden`,
    hide: (bucket) => `Verberg de namen achter ${bucket}`,
  },
  prov: {
    weightIn: (owner) => `gewicht in ${owner}`,
    eachShare: (owner) => `Het aandeel van elke positie in ${owner}.`,
    share: (name, owner) => `Het aandeel van ${name} in ${owner}.`,
    weightHow: (basis, owner) => `${basis}, als aandeel van ${owner}.`,
    returnNote: 'EUR-rendement over de periode', eachReturn: 'Het EUR-rendement van elke positie.',
    holdingReturn: (name) => `Het EUR-rendement van ${name}.`,
    returnHow: (basis) => `Gewogen per positie, waarbij elke positie ${basis} is.`,
    contribution: 'bijdrage',
    eachContribution: (owner, held) => held
      ? `Welk deel van het rendement van ${owner} door elke positie wordt verklaard — een grote beweging in een kleine positie draagt vrijwel niets bij.`
      : `Wat elke positie voor ${owner} opleverde.`,
    holdingContribution: (name, owner, held) => held
      ? `Welk deel van het rendement van ${owner} door ${name} wordt verklaard.` : `Wat ${name} voor ${owner} opleverde.`,
    contributionNote: (owner, held) => held ? `aandeel in het rendement van ${owner}` : `wat dit voor ${owner} opleverde`,
    yours: 'uw boek',
    contributionHow: 'Het gewicht van de positie maal het rendement ervan.',
    bucketsHow: (w) => `Elke ${w} die aan een van beide kanten wordt gehouden, op dezelfde manier ingedeeld.`,
    yourWeightHow: (basis) => `${basis}, geherwogen over de toerekenbare posities.`,
    indexWeightHow: (benchmark) =>
      `Het marktkapitalisatiegewicht van ${benchmark} aan het begin van het venster.`,
    yourReturnHow: (basis) => `Gewogen per positie, waarbij elke positie ${basis} is.`,
    indexReturnHow: (benchmark) => `Gewogen per bestanddeel, in EUR, over ${benchmark}.`,
    allocationHow: 'Wat de over- of onderweging waard was, afgezet tegen het indextotaal.',
    selectionHow: 'Wat de selectie waard was, tegen het gewicht van de index zelf.',
    interactionHow: 'De kruisterm — weging en selectie samen.',
    totalHow: 'De drie effecten opgeteld. De kolom telt op tot het meerrendement.',
    returnLegend: (owner) => [
      { sym: 'w_i', is: `het gewicht van elke positie in ${owner}` },
      { sym: 'r_i', is: 'wat die positie over het venster rendeerde, in EUR' },
    ],
    effectLegend: (benchmark) => [
      { sym: 'w_p', is: 'uw gewicht in de groep' },
      { sym: 'w_b', is: `het gewicht van ${benchmark} daarin` },
      { sym: 'r_p', is: 'wat uw posities daar rendeerden' },
      { sym: 'r_b', is: `wat die van ${benchmark} deden` },
      { sym: 'r_B', is: `het rendement van ${benchmark} als geheel` },
    ],
  },
};

export const ATTRIBUTION_COPY: Record<Lang, AttributionCopy> = { en, nl };
export function useAttributionCopy(): AttributionCopy {
  const [lang] = useLang();
  return ATTRIBUTION_COPY[lang];
}
