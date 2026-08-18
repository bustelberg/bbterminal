/**
 * Earnings Growth Model (EGM) — a 10-year annualised return and a fair value from three drivers:
 * earnings growth, dividend yield, and the change in the P/E multiple.
 *
 * Pure and dependency-free on purpose: no React, no fetching, no formatting. The panel renders what
 * this returns and never re-derives any of it.
 *
 *   compounder     = (1 + growthRate) · (1 + dividendYield)
 *   maxPE          = exitPE · (compounder / (1 + hurdleRate)) ^ years
 *   fairValue      = epsNextFY · maxPE
 *   upside         = fairValue / price − 1
 *   expectedReturn = compounder · (exitPE / forwardPE) ^ (1 / years) − 1
 *   peRatio        = maxPE / forwardPE
 *
 * Reading it: `maxPE` is the highest multiple you can pay TODAY and still clear the hurdle — when
 * growth plus dividends outrun the hurdle you are allowed to pay above `exitPE`. `expectedReturn`
 * is the annualised return at today's multiple, so a forward P/E above `exitPE` drags it below the
 * growth-plus-dividend base. `peRatio` above 1 means there is room to buy.
 *
 * ⚠ EVERY OUTPUT IS INDEPENDENTLY NULLABLE, AND NOTHING THROWS. A loss-making company has no
 * meaningful forward P/E, and the two outputs that divide by it must go `n/a` WITHOUT taking the
 * three that don't depend on it down with them — a panel that blanks entirely because one input is
 * missing hides the answers it still has.
 */

/** Measured facts about the company. */
export type EgmInputs = {
  price: number | null;
  forwardPE: number | null;
  epsNextFY: number | null;
};

/**
 * What the reader chooses.
 *
 * ⚠ THE DIVIDEND YIELD LIVES HERE, NOT IN `EgmInputs`. The measured yield is last period's
 * realised figure, and the model applies it as a CONSTANT for every one of the `years` — a claim
 * about the next decade, not an observation about the last one, and the same kind of claim as the
 * growth rate beside it. Keeping it on the measured side also meant a company GuruFocus has no
 * yield for silently became a non-payer, with no way to model an initiation or a cut. The panel
 * still seeds the field from the measured value, so the default answer is unchanged.
 */
export type EgmAssumptions = {
  growthRate: number;             // decimal — 0.10 is 10%
  dividendYield: number | null;   // decimal — 0.003 is 0.3%; null is treated as a non-payer
  exitPE: number;
  hurdleRate: number;             // decimal — 0.10 is 10%
  years: number;
};

/**
 * One driver of the expected return, as a rate AND as the thing it really is — a multiplier.
 *
 * ⚠⚠ THE TWO ARE NOT INTERCHANGEABLE AND THAT IS THE WHOLE REASON `factor` IS CARRIED. `rate` is
 * what a reader wants to see ("+10%/yr"); `factor` is what the model actually does with it. The
 * factors MULTIPLY to the answer exactly. The rates do not add to it — see `sumOfRates`.
 */
export type EgmLeg = {
  key: 'growth' | 'yield' | 'multiple';
  /** Annualised, decimal. 0.10 is +10%/yr. Identically `factor − 1`. */
  rate: number;
  /** What this driver multiplies one year's value by. */
  factor: number;
  /** The multiple leg only: the P/E it starts at and the one it is assumed to end at. ⚠ CARRIED
   *  RATHER THAN REBUILT BY THE PANEL, so a label can never name a different pair than the
   *  arithmetic used — the drag IS the ratio of these two, spread over the years. */
  from?: number;
  to?: number;
};

/**
 * The expected return, decomposed into the three drivers the panel's own subtitle promises.
 *
 * ⚠⚠ THE SUBTITLE SAYS "+" AND THE MODEL MULTIPLIES, WHICH IS WHY THIS EXISTS. A reader who takes
 * the heading literally and adds the three drivers gets a DIFFERENT NUMBER from the tile beside
 * them: measured at growth 10%, yield 0.3%, and a 30.5x → 20.0x rerating, the sum is +6.17% and
 * the answer is +5.77% — 0.40pp apart, both plausible, nothing on screen to say which is which.
 * So `sumOfRates` is computed and SHOWN rather than quietly avoided: the panel prints the addition
 * that does not work beside the product that does.
 *
 * ⚠ IT IS COMPUTED INSIDE `calculateEGM`, NOT BESIDE IT. The bridge and `expectedReturn` are the
 * same arithmetic read two ways, and the one thing this panel cannot survive is a breakdown that
 * does not tie to the total it is breaking down. `factor === expectedReturn + 1` by construction,
 * not by agreement.
 */
export type EgmBridge = {
  legs: EgmLeg[];
  /** The product of every leg's factor — identically `expectedReturn + 1`. */
  factor: number;
  /** `factor − 1` — identically `expectedReturn`. */
  rate: number;
  /** ⚠ THE NAIVE SUM OF THE LEG RATES, AND IT IS NOT `rate`. Carried so the panel can state the
   *  discrepancy rather than leave a reader to find it by adding the column up themselves. */
  sumOfRates: number;
};

export type EgmResult = {
  maxPE: number | null;
  fairValue: number | null;
  upside: number | null;          // decimal — −0.1606 is −16.06%
  expectedReturn: number | null;  // decimal, annualised
  peRatio: number | null;
  /** Null in exactly the cases `expectedReturn` is null — they are one computation. */
  bridge: EgmBridge | null;
  /**
   * Where the share price lands after `years` — today's price grown at the earnings rate and
   * rerated to the exit multiple.
   *
   * ⚠⚠ THE CAPITAL LEG ONLY, AND THAT IS NOT AN OVERSIGHT. `expectedReturn` is a TOTAL return: it
   * includes `years` of dividends, which are cash you were paid, not price you can sell at. A
   * "share price" that quietly had the dividend stream compounded into it would be a number no
   * screen will ever show you. So this is `price · (1+growth)^years · (exitPE ÷ forwardPE)`, and
   * on a non-payer it is exactly `price · (1 + expectedReturn)^years`; on a payer the two differ
   * by the dividend compounding, which is what `priceReturn` beside `totalReturn` exists to show.
   */
  impliedPrice: number | null;
  /** `impliedPrice ÷ price − 1` — the move in the price itself, over the whole window. */
  priceReturn: number | null;
  /**
   * `priceReturn` annualised — what the PRICE compounds at.
   *
   * ⚠⚠ NOT `expectedReturn`, ON A DIVIDEND PAYER. That one is the total return per year and this
   * one is the price leg per year; they differ by exactly the yield, and both are legitimately
   * called "the CAGR" depending on which row you are reading. This is the one that belongs beside
   * the two prices, because it is the only annual rate those two prices actually imply — quoting
   * the total there would put a figure next to a subtraction it cannot be derived from.
   */
  priceCagr: number | null;
  /** What `expectedReturn` compounds to over the whole window, dividends included. */
  totalReturn: number | null;
};

export const EGM_DEFAULTS: EgmAssumptions = {
  growthRate: 0.10,
  // ⚠ NOT A UNIVERSAL DEFAULT LIKE THE OTHERS — the panel overwrites this with the company's
  // measured yield unless the reader has typed one. Zero is the safe fallback: a yield nobody
  // measured and nobody chose should not add return.
  dividendYield: 0,
  exitPE: 20,
  hurdleRate: 0.10,
  years: 10,
};

/** Finite numbers only. `Infinity` and `NaN` are what a division by a zero input produces, and
 *  either one rendered as a figure is worse than a blank. */
const ok = (v: number | null | undefined): v is number => v != null && Number.isFinite(v);

export function calculateEGM(inputs: EgmInputs, a: EgmAssumptions): EgmResult {
  const empty: EgmResult = {
    maxPE: null, fairValue: null, upside: null, expectedReturn: null, peRatio: null, bridge: null,
    impliedPrice: null, priceReturn: null, priceCagr: null, totalReturn: null,
  };

  // A null dividend yield is a non-payer, which is a real 0 — not an unknown. (An absent PRICE or
  // EPS is genuinely unknown and stays null; those are different states, see the panel.)
  const divYield = ok(a.dividendYield) ? a.dividendYield : 0;
  if (!ok(a.growthRate) || !ok(a.exitPE) || !ok(a.hurdleRate) || !ok(a.years)) return empty;
  if (a.years <= 0 || a.exitPE <= 0) return empty;

  const compounder = (1 + a.growthRate) * (1 + divYield);
  const discount = 1 + a.hurdleRate;
  // A compounder or hurdle at or below zero has no real growth path. Raising a negative base to an
  // even number of years returns a POSITIVE figure, which would render as a perfectly ordinary
  // valuation built on arithmetic that stopped meaning anything.
  if (!(compounder > 0) || !(discount > 0)) return empty;

  const maxPERaw = a.exitPE * Math.pow(compounder / discount, a.years);
  const maxPE = ok(maxPERaw) ? maxPERaw : null;

  // fairValue does NOT depend on forwardPE — a loss-making company still has one.
  const fairValue = maxPE != null && ok(inputs.epsNextFY) && inputs.epsNextFY > 0
    ? inputs.epsNextFY * maxPE : null;
  const upside = fairValue != null && ok(inputs.price) && inputs.price > 0
    ? fairValue / inputs.price - 1 : null;

  // ⚠ The two that DO divide by forwardPE. A negative forward P/E is a loss, not a cheap stock:
  // (exitPE / −25) ^ 0.1 is the tenth root of a negative number — not a real number at all — and
  // JavaScript hands back NaN rather than raising. Both go n/a.
  const usablePE = ok(inputs.forwardPE) && inputs.forwardPE > 0;
  // ⚠ THE MULTIPLE'S ANNUAL FACTOR IS THE ONE PIECE OF ARITHMETIC SHARED BY THE TOTAL AND THE
  // BRIDGE. Computing it once is what makes `factor === expectedReturn + 1` true by construction
  // rather than by two expressions happening to agree — a breakdown that does not tie to the total
  // it breaks down is worse on this panel than no breakdown at all.
  const multFactor = usablePE
    ? Math.pow(a.exitPE / (inputs.forwardPE as number), 1 / a.years) : null;
  const expectedRaw = multFactor == null ? null : compounder * multFactor - 1;
  const expectedReturn = ok(expectedRaw) ? expectedRaw : null;
  const peRatio = usablePE && maxPE != null ? maxPE / (inputs.forwardPE as number) : null;

  const bridge: EgmBridge | null = expectedReturn == null || multFactor == null ? null : (() => {
    const legs: EgmLeg[] = [
      { key: 'growth', rate: a.growthRate, factor: 1 + a.growthRate },
      { key: 'yield', rate: divYield, factor: 1 + divYield },
      {
        key: 'multiple', rate: multFactor - 1, factor: multFactor,
        from: inputs.forwardPE as number, to: a.exitPE,
      },
    ];
    return {
      legs,
      factor: expectedReturn + 1,
      rate: expectedReturn,
      sumOfRates: legs.reduce((s, l) => s + l.rate, 0),
    };
  })();

  // ⚠ BUILT FROM `multFactor`, THE SAME TERM THE TOTAL USES — so the price leg and the return can
  // never be computed off two different reratings. `multFactor^years` IS `exitPE / forwardPE`; it
  // is written this way so the shared piece is visibly shared.
  const capitalFactor = multFactor == null
    ? null : Math.pow((1 + a.growthRate) * multFactor, a.years);
  const impliedRaw = capitalFactor != null && ok(inputs.price) && inputs.price > 0
    ? inputs.price * capitalFactor : null;
  const impliedPrice = ok(impliedRaw) ? impliedRaw : null;
  const priceReturn = capitalFactor != null && ok(capitalFactor) ? capitalFactor - 1 : null;
  // ⚠ THE PER-YEAR FORM OF THE SAME FACTOR, not a second derivation from `priceReturn` — one
  // `Math.pow` either way, and taken from the factor it cannot drift from the whole-period figure
  // sitting beside it on screen.
  const cagrRaw = multFactor == null ? null : (1 + a.growthRate) * multFactor - 1;
  const priceCagr = ok(cagrRaw) ? cagrRaw : null;
  const totalRaw = expectedReturn == null ? null : Math.pow(1 + expectedReturn, a.years) - 1;
  const totalReturn = ok(totalRaw) ? totalRaw : null;

  return {
    maxPE, fairValue, upside, expectedReturn, peRatio, bridge,
    impliedPrice, priceReturn, priceCagr, totalReturn,
  };
}
