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

export type EgmResult = {
  maxPE: number | null;
  fairValue: number | null;
  upside: number | null;          // decimal — −0.1606 is −16.06%
  expectedReturn: number | null;  // decimal, annualised
  peRatio: number | null;
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
    maxPE: null, fairValue: null, upside: null, expectedReturn: null, peRatio: null,
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
  const expectedRaw = usablePE
    ? compounder * Math.pow(a.exitPE / (inputs.forwardPE as number), 1 / a.years) - 1 : null;
  const expectedReturn = ok(expectedRaw) ? expectedRaw : null;
  const peRatio = usablePE && maxPE != null ? maxPE / (inputs.forwardPE as number) : null;

  return { maxPE, fairValue, upside, expectedReturn, peRatio };
}
