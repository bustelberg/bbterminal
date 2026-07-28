/**
 * Reverse DCF — a plain discounted cash flow run backwards.
 *
 * Forwards: grow free cash flow at `g` for `n` years, discount at `r`, add a Gordon terminal value.
 * Backwards: solve for the `g` at which that valuation equals today's market cap. The answer is
 * what the market must believe, which the reader then judges as plausible or not.
 *
 * Pure and dependency-free: no React, no fetching, no formatting.
 *
 * ⚠ IT IS SOLVED, NOT SEARCHED. The spreadsheet this ports from lays out a residual grid and hands
 * it to Solver, which leaves the answers stale the moment an input moves. Here it is bisected on
 * demand, so a figure on screen is always the one today's inputs imply.
 */

export type ReverseDcfInputs = {
  price: number | null;
  sharesOutstanding: number | null;   // millions, so market cap comes out in millions too
  fcf: number | null;                 // millions — the latest reported free cash flow, as filed
};

export type ReverseDcfAssumptions = {
  years: number;
  perpetuityGrowth: number;
  discountRates: number[];
  /** Worth overriding when the last reported year is unrepresentative. */
  fcfOverride?: number | null;
  /** Solve against a market cap other than today's — "what would justify €500bn?". */
  targetOverride?: number | null;
};

/**
 * ⚠ 3% IS A MACRO ASSUMPTION AND THERE IS NO COMPANY-SPECIFIC VERSION OF IT. Long-run nominal
 * growth for a going concern; no business grows faster than the economy for ever, so deriving this
 * one from the company's own history would encode the last decade as eternity. Stated as a
 * convention rather than dressed up as a measurement.
 */
export const PERPETUITY_GROWTH = 0.03;
/** A convention too — long enough for growth to matter, short enough to be arguable. */
export const FORECAST_YEARS = 10;
/** Used when the company publishes no usable WACC — see `defaultDiscountRate`. */
export const FALLBACK_DISCOUNT_RATE = 0.10;

export const REVERSE_DCF_DEFAULTS: ReverseDcfAssumptions = {
  years: FORECAST_YEARS,
  perpetuityGrowth: PERPETUITY_GROWTH,
  discountRates: [FALLBACK_DISCOUNT_RATE],
};

/**
 * The discount rate to start from: the company's OWN cost of capital where GuruFocus publishes one,
 * so a utility and a biotech do not get the same hurdle by default.
 *
 * ⚠ A WACC AT OR BELOW THE PERPETUITY GROWTH MAKES THE TERMINAL VALUE INFINITE, not large. The
 * Gordon leg divides by (r − gp), so a 2.5% WACC against 3% terminal growth is a negative
 * denominator and a negative valuation — the model returns null and the panel would read "no
 * solution" for a company whose only sin is a low cost of capital. Anything not comfortably above
 * the terminal rate falls back, as does an implausible reading at the top end.
 */
export function defaultDiscountRate(wacc: number | null | undefined,
  gp: number = PERPETUITY_GROWTH): number {
  if (wacc == null || !Number.isFinite(wacc)) return FALLBACK_DISCOUNT_RATE;
  if (!(wacc > gp + 0.01) || wacc > 0.40) return FALLBACK_DISCOUNT_RATE;
  return wacc;
}

const ok = (v: number | null | undefined): v is number => v != null && Number.isFinite(v);

/**
 * What the cash flows are worth: a growing annuity over the explicit years, plus a Gordon terminal
 * value on the final year's flow, discounted back.
 *
 * ⚠ THE ANNUITY DIVIDES BY (r − g) AND THE SOLVER WALKS g THROUGH r. At g = r the closed form is
 * 0/0; the limit is finite (every discounted term equals fcf/(1+r), so the sum is n·fcf/(1+r)) and
 * is used inside a small band around it. Without that the bisection hits ±Infinity mid-bracket and
 * the sign test decides on a non-number.
 *
 * Returns null when r ≤ perpetuity growth — a terminal value at or below the discount rate is
 * infinite, which is not a large number but an invalid one.
 */
export function modelValue(fcf: number, g: number, r: number, n: number, gp: number): number | null {
  if (!ok(fcf) || !ok(g) || !ok(r) || !ok(n) || !ok(gp)) return null;
  if (n <= 0) return null;
  if (!(r > gp)) return null;

  const explicitPV = Math.abs(r - g) < 1e-7
    ? fcf * n / (1 + r)
    : fcf * (1 - Math.pow((1 + g) / (1 + r), n)) / (r - g);
  const fcfTerminal = fcf * Math.pow(1 + g, n - 1);            // the flow in year n
  const terminalPV = (fcfTerminal * (1 + gp) / (r - gp)) / Math.pow(1 + r, n);

  const v = explicitPV + terminalPV;
  return Number.isFinite(v) ? v : null;
}

/**
 * ⚠ −99%/yr, NOT A "SANE" FLOOR. This was −50% on the same instinct that capped the top at 100%,
 * and it refused the same way: a company priced below what a halving cash flow is worth came back
 * "no solution" instead of a number. −100% is the true limit — cash flows shrink to nothing and
 * `(1+g)` hits zero — so the bracket stops just short of it.
 *
 * The model still cannot value a company below roughly one year of discounted free cash flow (at
 * g = −99% every later year is ~0, leaving fcf/(1+r)). A price under THAT has no solution because
 * of what a DCF is, not because of a bound anyone chose.
 */
const LOW_G = -0.99;
/**
 * ⚠ THE CEILING IS NOT THE DISCOUNT RATE, AND CAPPING IT THERE IS A REAL BUG. The rule "growth
 * cannot reach the discount rate" belongs to a PERPETUAL growing annuity. This one runs for `n`
 * years — a finite sum, which converges for any g. At g > r the closed form's numerator and
 * denominator both go negative and it stays correct; only g = r is singular (0/0), and that has
 * its own limit case. The terminal leg needs r > gp, not r > g.
 *
 * Bracketed at r instead, every richly-priced company came back "out of range": measured on a
 * €613,606M market cap against €11,160M of free cash flow, the model reported no solution at 10%
 * and claimed it could justify at most €250,738M — when the honest answer is that the price
 * implies roughly 24% annual growth. Refusing to answer read as a limitation of the company; it
 * was a limitation of the bracket.
 *
 * ⚠ AND THE TOP IS NOT A JUDGEMENT ABOUT WHAT IS PLAUSIBLE. It was 100%/yr on the reasoning that
 * past that the output stops being a valuation — but that reasoning belongs to the READER, not to
 * the solver, and it cost real answers: a company with ~3,600M of free cash flow behind a
 * multi-trillion market cap came back "no solution" when the honest reply is a number so large it
 * settles the question instantly. "You need 137%/yr" is more use than "no answer", and the panel's
 * red band already says what to make of it.
 *
 * 1000%/yr is now the bound — high enough never to bind on a real company, finite because
 * bisection needs a bracket.
 */
const HIGH_G = 10.0;
const TOL = 1e-8;
const MAX_ITER = 200;

/**
 * The growth rate at which the model value equals `target`, by bisection.
 *
 * Bisection rather than Newton on purpose: the value is monotonically increasing in g (more growth
 * is worth more, always), so a bracket that straddles the target converges without a derivative and
 * cannot be thrown off by the singularity at g = r.
 *
 * Returns null when no rate in the bracket reaches it — the honest answer for a price no plausible
 * growth explains, and never the bracket end dressed up as a solution.
 */
export function solveGrowth(fcf: number, target: number, r: number, n: number, gp: number): number | null {
  if (!ok(fcf) || !ok(target) || !(fcf > 0) || !(r > gp)) return null;

  let lo = LOW_G;
  let hi = HIGH_G;                // NOT r — see the note on HIGH_G
  if (!(hi > lo)) return null;

  const residual = (g: number) => {
    const v = modelValue(fcf, g, r, n, gp);
    return v == null ? null : v - target;
  };
  const rLo = residual(lo);
  const rHi = residual(hi);
  if (rLo == null || rHi == null) return null;
  // Same sign at both ends: the answer is outside the bracket. Reported as "out of range", not as
  // −50% or as 100%.
  if (rLo > 0 === rHi > 0) return null;

  for (let k = 0; k < MAX_ITER && hi - lo > TOL; k++) {
    const mid = (lo + hi) / 2;
    const rMid = residual(mid);
    if (rMid == null) return null;
    if (rMid < 0) lo = mid; else hi = mid;
  }
  return (lo + hi) / 2;
}

  export type ImpliedRow = { discountRate: number; impliedGrowth: number | null };

/** The implied growth at each discount rate asked for. */
export function impliedGrowth(i: ReverseDcfInputs, a: ReverseDcfAssumptions): ImpliedRow[] {
  const fcf = ok(a.fcfOverride) ? a.fcfOverride : i.fcf;
  const target = ok(a.targetOverride) ? a.targetOverride : marketCapOf(i);
  return a.discountRates.map((r) => ({
    discountRate: r,
    impliedGrowth: fcf == null || target == null
      ? null : solveGrowth(fcf, target, r, a.years, a.perpetuityGrowth),
  }));
}

export function marketCapOf(i: ReverseDcfInputs): number | null {
  if (!ok(i.price) || !ok(i.sharesOutstanding)) return null;
  if (!(i.price > 0) || !(i.sharesOutstanding > 0)) return null;
  const v = i.price * i.sharesOutstanding;
  return Number.isFinite(v) ? v : null;
}
