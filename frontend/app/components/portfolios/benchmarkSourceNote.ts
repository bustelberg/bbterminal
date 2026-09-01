import type { SourceKey } from '../../../lib/provenance';
import type { Lang } from '../../../lib/i18n';

/**
 * What the headline benchmark figure actually is, as the ⓘ card's four fields.
 *
 * ⚠⚠ IT EXISTS BECAUSE THERE ARE NOW TWO BENCHMARK NUMBERS ONE CLICK APART. Since 2026-08-19 the
 * Scorecard's benchmark comes from the index ETF's own price series (`_benchmark_etf`), while the
 * Attribution panel still decomposes the constituent RECONSTRUCTION — because an ETF price has no
 * constituents in it to attribute. Measured on ACWI YTD, those disagree by ~2.8pp (+14.67% against
 * the rebuild's +11.83%: full market cap where MSCI float-adjusts, 1,678 of 1,998 members priced
 * with the missing weight redistributed, a static membership snapshot).
 *
 * A reader who opens the attribution and finds a different index return has to be able to learn
 * why from the screen. So the tile SAYS which one it is showing rather than leaving the difference
 * to be discovered.
 *
 * ⚠ THE SOURCE KEY IS PART OF THE ANSWER, NOT DECORATION. `benchmark` renders as "yfinance close
 * (benchmark constituents)" and `benchmark_etf` as "GuruFocus daily close (index ETF)". Getting
 * that wrong prints one vendor's name over the other's number, which is the single failure the
 * provenance badge exists to make impossible.
 *
 * ⚠ AND `how` NAMES THE CONVERSION, not just the currency. "In EUR" is ambiguous between
 * "converted at today's rate" (which strips the currency leg out of the return entirely) and
 * "each mark at its own date's rate" (what EUR-basis means everywhere in this app, and what the
 * server does). On ACWI YTD those differ by 1.5pp — the whole of the dollar's move.
 */
export type BenchmarkProvenance = {
  /** 'etf' — the index ETF's price series; anything else — the cap-weighted constituent rebuild. */
  source?: string | null;
  /** The ETF's ticker, when that is the source. */
  ticker?: string | null;
  /** The close the window actually OPENED on — not the 1 Jan anchor. */
  from?: string | null;
  /** The ETF's last bar. Null on the rebuild path, which has no single as-of. */
  asOf?: string | null;
  /** The index label the tile is measured against, e.g. 'ACWI'. */
  label: string;
  /** The four numbers the return is made of. ⚠ `fx*` is the ETF currency PER EUR (1.1750 USD/EUR),
   *  the direction the formula divides by. All absent on the rebuild path. */
  openPrice?: number | null;
  closePrice?: number | null;
  openFx?: number | null;
  closeFx?: number | null;
  /** The resulting EUR return, so the worked line can end where the tile does. */
  eurPct?: number | null;
};

export type BenchmarkProvenanceCard = {
  sourceKey: SourceKey;
  what: string;
  note: string;
  how: string;
};

/** Enough digits to reproduce the tile by hand, no more. Prices to 2dp (they are quoted that way),
 *  FX to 4dp (the rate moves in the 4th, and rounding it to 2 changes the answer). */
const px = (v: number) => v.toFixed(2);
const fx = (v: number) => v.toFixed(4);
const pc = (v: number) => `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;

/**
 * The rule, then the same rule with this window's own numbers under it.
 *
 * ⚠ THE BLANK LINE IS THE POINT, and it survives only because the card renders `how` with
 * `whitespace-pre-wrap`. In a plain span every run of whitespace collapses to one character and
 * the two lines would run together into one long sentence — which is exactly the thing a worked
 * example is supposed to break up.
 *
 * ⚠ IT DEGRADES TO THE RULE ALONE when any mark is missing, rather than printing `undefined ÷
 * undefined`. An older payload (before the marks were carried) and the rebuild path both land
 * here, and a formula card that renders "NaN" is worse than one that only states the method.
 */
function etfFormula(
  { ticker, label, from, asOf, openPrice, closePrice, openFx, closeFx, eurPct }: BenchmarkProvenance,
  lang: Lang,
): string {
  const sym = ticker ?? label;
  const rule = lang === 'nl'
    ? `(slot ÷ FX_slot) ÷ (begin ÷ FX_begin) − 1 — slot/begin zijn de USD-koersen van ${sym}; FX is USD per EUR op dezelfde dag`
    : `(close ÷ FX_close) ÷ (open ÷ FX_open) − 1  —  close/open are ${sym}'s USD prices, FX is USD per EUR on that same day`;
  const marks = [openPrice, closePrice, openFx, closeFx];
  if (marks.some((v) => typeof v !== 'number' || !Number.isFinite(v) || v === 0)) return rule;
  const worked = `(${px(closePrice as number)} ÷ ${fx(closeFx as number)}) `
    + `÷ (${px(openPrice as number)} ÷ ${fx(openFx as number)}) − 1`
    + (typeof eurPct === 'number' ? ` = ${pc(eurPct)}` : '');
  const dated = from && asOf ? `${from} → ${asOf}` : null;
  // ⚠ A BLANK LINE BETWEEN THEM, not a bullet or a dash: the second line IS the first line, said
  // again with numbers. Anything that reads as a new item invites it to be read as a new fact.
  return `${rule}\n\n${dated ? `${dated}\n` : ''}${worked}`;
}

export function benchmarkProvenance(p: BenchmarkProvenance, lang: Lang = 'en'): BenchmarkProvenanceCard {
  const { source, label } = p;
  if (source === 'etf') {
    return {
      sourceKey: 'benchmark_etf',
      what: lang === 'nl' ? `Het eigen EUR-rendement van ${label} over dezelfde periode — koersrendement, dus zonder uitkeringen.`
        : `What ${label} itself returned over the same window, in EUR — a price return, so distributions are not included.`,
      note: lang === 'nl' ? `rendement van ${label} sinds jaarbegin` : `${label}'s return, year to date`,
      how: etfFormula(p, lang),
    };
  }
  // ⚠ THE FALLBACK IS NAMED, NOT LEFT BLANK. The AEX has no reachable ETF and a window opening
  // before the fund existed cannot use one, so this path is reached on purpose — and its number
  // carries known biases the ETF's does not. Saying "rebuilt from constituents" is what lets a
  // reader ask why it differs from the figure they can look up. There is no worked line here
  // because there is no pair of numbers to work: there are 1,678 of them.
  return {
    sourceKey: 'benchmark',
    what: lang === 'nl' ? `Het eigen EUR-rendement van ${label} over dezelfde periode — koersrendement, dus zonder dividenden.`
      : `What ${label} itself returned over the same window, in EUR — a price return, so dividends are not included.`,
    note: lang === 'nl' ? `rendement van ${label} sinds jaarbegin` : `${label}'s return, year to date`,
    how: lang === 'nl'
      ? 'Σ(weging naar marktkapitalisatie aan het begin × EUR-rendement constituent) over de indexconstituenten, gewogen naar VOLLEDIGE marktkapitalisatie (de echte index corrigeert voor free float). Gebruikt wanneer geen index-ETF bereikbaar is of de periode vóór de start van de ETF begint.'
      : 'Σ(start-of-window cap weight × constituent EUR return) over the index’s constituents, cap-weighted on FULL market cap (the real index float-adjusts). Used where no index ETF is reachable (the AEX), or where the window opens before the ETF existed.',
  };
}
