/**
 * Free cash flow normalised for stock compensation and growth capex — the Reverse DCF's base.
 *
 * Pure and dependency-free.
 *
 * ⚠⚠ THE TWO CORRECTIONS PULL IN OPPOSITE DIRECTIONS, AND THAT IS NOT AN INCONSISTENCY. They fix
 * two different faults in the same figure:
 *
 *   SBC is SUBTRACTED — a real cost that never leaves the cash flow statement. It is added back
 *   into operating cash flow as a non-cash charge, so reported FCF flatters every company that
 *   pays people in equity. The shares are issued; the dilution is borne by the holder.
 *
 *   GROWTH CAPEX is ADDED BACK — and it is added back precisely BECAUSE reported FCF already
 *   subtracted it. `Free Cash Flow` is operating cash flow minus TOTAL capex, so a company
 *   building its next decade is charged the whole bill against this year's cash. What sustains the
 *   current business is maintenance capex; the excess buys growth the DCF is separately being
 *   asked to solve for. Left in, the model is told the company earns less AND is asked what growth
 *   that lower figure must deliver — the same expansion counted twice, once as a cost and once as
 *   the thing to be explained.
 *
 * ⚠⚠ SUBTRACTING GROWTH CAPEX WOULD BE ARITHMETICALLY WRONG, not merely conservative. It is
 * already out of FCF; deducting it again charges the same euros twice. There is no reading of
 * "correct FCF for growth capex" that subtracts.
 *
 * ⚠ D&A IS THE PROXY FOR MAINTENANCE CAPEX, and it is a proxy rather than a measurement. Nobody
 * publishes maintenance capex; depreciation is what the company itself says its existing assets
 * consume. It is imperfect in both directions — inflation makes replacement dearer than historic
 * cost, and a company that has just written down a plant depreciates little — which is why the
 * excess is only ever taken as growth when it is POSITIVE. A company spending BELOW depreciation
 * is under-investing, and treating that shortfall as a windfall (a negative "growth capex", added
 * back as a deduction) would reward exactly the behaviour that hollows a business out.
 */

/** The inputs, in the vendor's own signs — see `growthCapex` for why the sign matters. */
export type FcfParts = {
  /** `Cashflow Statement__Free Cash Flow`, as filed. Millions. */
  fcf: number | null;
  /** `Stock Based Compensation`. Filed POSITIVE (a non-cash add-back inside operating cash flow). */
  sbc: number | null;
  /** `Capital Expenditure`. Filed NEGATIVE — it is an outflow. Verified on ASML: FCF = OCF + capex
   *  to the decimal across three years, so the sign is the vendor's and not ours to guess. */
  capex: number | null;
  /** `Cash Flow Depreciation, Depletion and Amortization`. Filed POSITIVE. */
  dep: number | null;
};

export type NormalisedFcf = {
  /** The figure to value: `fcf − sbc + growthCapex`, or null when FCF itself is missing. */
  used: number | null;
  /** As filed, for the panel to show beside the corrected one. */
  reported: number | null;
  /** Subtracted. Null when not reported — see `applied`. */
  sbc: number | null;
  /** Added back. Null when either leg is missing; 0 when capex is at or below depreciation. */
  growthCapex: number | null;
  /** Which corrections actually ran. ⚠ A correction that could not be computed is NOT a zero. */
  applied: { sbc: boolean; growthCapex: boolean };
};

const ok = (v: number | null | undefined): v is number => v != null && Number.isFinite(v);

/**
 * The growth half of capex: spend above what the existing assets consume.
 *
 * ⚠ `Math.abs(capex)` — the vendor files it negative and a reader typing an override will type it
 * positive. Taking the magnitude means both agree, and it removes the one sign error in this file
 * that would silently invert the correction: with a negative capex, `capex − dep` is always
 * negative, always clamps to 0, and the add-back quietly never happens on any company.
 */
export function growthCapex(capex: number | null | undefined,
  dep: number | null | undefined): number | null {
  if (!ok(capex) || !ok(dep)) return null;
  return Math.max(Math.abs(capex) - dep, 0);
}

/**
 * ⚠ EACH CORRECTION APPLIES ONLY IF ITS INPUTS EXIST, AND SAYS SO. A missing SBC line is not a
 * company that pays no stock comp, and a missing depreciation line is not a company with no
 * maintenance capex. Treating either absence as a zero would publish a correction that did not
 * happen, under a heading that says it did — the reader has no way to tell a normalised figure
 * from an un-normalised one once both are just a number in a box.
 */
export function normalisedFcf(parts: FcfParts): NormalisedFcf {
  const { fcf, sbc, capex, dep } = parts;
  const growth = growthCapex(capex, dep);
  const applied = { sbc: ok(sbc), growthCapex: growth != null };
  if (!ok(fcf)) {
    return { used: null, reported: null, sbc: ok(sbc) ? sbc : null, growthCapex: growth, applied };
  }
  let used = fcf;
  if (applied.sbc) used -= sbc as number;
  if (applied.growthCapex) used += growth as number;
  return { used, reported: fcf, sbc: ok(sbc) ? sbc : null, growthCapex: growth, applied };
}
