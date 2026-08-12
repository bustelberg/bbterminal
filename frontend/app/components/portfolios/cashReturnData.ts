/** Shared types + helpers for the Cash-return-on-capital card and its drill-down. The ratio is
 *  derived on the client from three raw lines so the plotted number and the drill-down can't
 *  disagree. Mirrors {@link ./debtRatioData}. */

import { weightedByYear, type Weighted } from './marginData';
import { correctedFcf } from './sbcCorrection';

/** ⚠ `Weighted`, NOT a bare `weight_pct` — the server sends `market_cap_by_period` on an INDEX
 *  request and the invested-capital blend now needs it in the type, not merely at runtime (see
 *  `investedCapitalIndexByYear`: a period the row cannot be weighted in cannot be its base). */
export type CashReturnRow = Weighted & {
  isin: string; name: string; currency: string | null;
  ticker: string | null; exchange: string | null;
  status: 'ok' | 'unsubscribed' | 'no_data';
  fcf: Record<string, number | null>;
  noncurrent_liabilities: Record<string, number | null>;
  total_equity: Record<string, number | null>;   // Total Equity (incl. minority interest)
  /** Carried for the tab-level SBC correction; see `sbcCorrection`. */
  sbc: Record<string, number | null>;
  /** ⚠ ALREADY A PERCENTAGE — GuruFocus's own `Ratios__ROIC %`, not a line to divide. */
  roic: Record<string, number | null>;
};
export type CashReturnInputs = { years: string[]; rows: CashReturnRow[] };

export type CapitalMode = 'croic' | 'roic';

/**
 * The two ways to ask "what does this capital earn", and they are NOT variants of one number.
 *
 * ⚠ THEY DISAGREE, OFTEN BY A LOT, AND NEITHER IS WRONG. Measured on ASML: FCF per share went
 * 24.14 → 8.24 → 23.08 across 2021-2023 on capex and working-capital swings, while its reported
 * ROIC sat at 23.53 → 27.90 → 24.67. The cash measure is reading the capex cycle; the ROIC is
 * reading the business. Showing either alone answers a different question than the reader thinks.
 *
 * Four concrete differences, all of which push the same way for a capex-heavy, cash-rich compounder:
 *
 *   numerator   FCF is AFTER capex and AFTER interest paid. GuruFocus's ROIC is built on an
 *               operating profit after tax but BEFORE interest — capital-structure neutral by
 *               design, so leverage is not charged twice.
 *   capital     ours is every long-term liability (pensions, deferred tax, leases) plus all
 *               equity, with cash NOT subtracted and short-term debt NOT counted. A conventional
 *               invested capital nets off cash and includes short-term debt.
 *   timing      ours is the year-end balance; ROIC conventionally averages opening and closing.
 *   authorship  ours is derived here from three raw lines and can be audited in the drill-down.
 *               ROIC is GuruFocus's figure, read through — see `MODES.roic.derived`.
 */
export const MODES: Record<CapitalMode, {
  tab: string;
  title: string;
  /** Sentence-case, mid-sentence. */
  inline: string;
  /** Whether WE compute it — decides whether the drill-down can show workings. */
  derived: boolean;
  what: string;
  where: string;
  caveat: string;
}> = {
  croic: {
    tab: 'CROIC',
    title: 'Cash return on capital',
    inline: 'cash return on capital',
    derived: true,
    what: 'the free cash flow thrown off per unit of long-term capital employed',
    where: 'Derived here from three reported lines — Free Cash Flow ÷ (Total Long-Term Liabilities + Total Equity). The drill-down shows all three, so the number can be checked rather than believed.',
    caveat: '⚠ AFTER CAPEX AND AFTER INTEREST, so it swings with the investment cycle and charges leverage twice (interest depresses the numerator while the debt sits in the base). Blank where an issuer does not split current from non-current liabilities — a bank, or Berkshire — because the capital base is then undefined, NOT equity alone.',
  },
  roic: {
    tab: 'ROIC',
    title: 'Return on invested capital',
    inline: 'return on invested capital',
    derived: false,
    what: 'the after-tax operating profit earned per unit of invested capital',
    where: "GuruFocus's own `Ratios__ROIC %`, read through unchanged — 28 fiscal years for ASML.",
    caveat: "⚠ NOT OUR ARITHMETIC, AND DELIBERATELY SO. Deriving it would mean picking a NOPAT numerator (GuruFocus's EBIT and Operating Income are different lines — Mitsui 85,035 vs 56,602) and an invested-capital base, i.e. publishing a bespoke ratio under a name every reader already has a definition for. The cost is that the drill-down can show no workings: there is one number per company per year, not three lines to check it against.",
  },
};

/** One company's cash return on capital for a year (as a %), or null when it can't be computed:
 *  FCF ÷ invested capital, where invested capital = non-current liabilities + total equity. Both
 *  legs of the capital base must be present (a missing non-current-liabilities line means the
 *  issuer doesn't split current/non-current — Berkshire, banks — so the base is undefined, NOT
 *  equity alone), and the base must be positive. */
export function cashReturnOf(
  fcf: number | null | undefined,
  noncurrentLiabilities: number | null | undefined,
  totalEquity: number | null | undefined,
  sbc: number | null | undefined = null,
  correct = false,
) {
  if (noncurrentLiabilities == null || totalEquity == null) return null;
  const num = correctedFcf(fcf, sbc, correct);
  if (num == null) return null;
  const base = noncurrentLiabilities + totalEquity;
  if (!(base > 0)) return null;
  return num / base * 100;
}

/** The book's cash return on capital per year — a WEIGHT-weighted average of each company's ratio
 *  (each is a currency-free ratio, so averaging is currency-safe; summing mixed-currency amounts is
 *  not). For a single company this is just that company's ratio. */
export function cashReturnByYear(rows: CashReturnRow[], correct = false): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.total_equity),
    (r, y) => cashReturnOf(r.fcf[y], r.noncurrent_liabilities[y], r.total_equity[y],
      r.sbc?.[y], correct));
}

/**
 * One company's ROIC for a year — GuruFocus's figure, passed through.
 *
 * ⚠ NO ARITHMETIC HERE ON PURPOSE. The only thing this adds over reading the field is refusing a
 * non-finite value, so a malformed row cannot enter the weighted average as a NaN and blank the
 * whole book's line.
 */
export function roicOf(reported: number | null | undefined): number | null {
  return reported == null || !Number.isFinite(reported) ? null : reported;
}

/** The book's ROIC per year — weighted the SAME way as the cash return, because it is the same
 *  kind of quantity: a per-company percentage, averaged by weight, never summed. */
export function roicByYear(rows: CashReturnRow[]): Map<number, number> {
  return weightedByYear(rows, (r) => Object.keys(r.roic ?? {}), (r, y) => roicOf(r.roic?.[y]));
}

/** The series for whichever mode is switched on — one entry point, so the card, the tooltip and
 *  the drill-down cannot end up reading different modes. */
export function seriesByYear(
  rows: CashReturnRow[], mode: CapitalMode, correct = false,
): Map<number, number> {
  // ⚠ THE SBC CORRECTION CANNOT REACH ROIC, AND SILENTLY IGNORING IT WOULD BE A LIE. ROIC is
  // GuruFocus's own published percentage — there is no numerator of ours to adjust. So the flag is
  // simply not passed here, and the card states that the checkbox does not apply in this mode
  // rather than leaving a ticked box implying a correction that never happened.
  return mode === 'roic' ? roicByYear(rows) : cashReturnByYear(rows, correct);
}
