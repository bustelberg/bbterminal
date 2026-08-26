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
 * The FORWARD base: next year's consensus free cash flow, derived.
 *
 * ⚠⚠ DERIVED BECAUSE IT CANNOT BE READ. GuruFocus's Excel add-in has `Estimated Free Cash Flow for
 * Next FY1 End (M)`; the REST endpoint we ingest has no FCF estimate at all — only operating cash
 * flow (see `egmInputs.OCF_EST_CODE`). `FCF = OCF − capex` is the definition of the line, not an
 * approximation of it, so the only estimated quantity here is the OCF; the capex is last year's
 * filing, which is the same compromise the spreadsheet makes (its capex leg is trailing too).
 *
 * ⚠⚠ AND THE APPROXIMATION LARGELY CANCELS, WHICH IS THE REASON THIS IS SOUND RATHER THAN MERELY
 * CONVENIENT. Feed this to `normalisedFcf` and, for any company spending at or above depreciation:
 *
 *     (OCF_est − capex) − sbc + (capex − dep)  =  OCF_est − dep − sbc
 *
 * The capex cancels ENTIRELY. What the model values is estimated operating cash flow, less
 * maintenance capex proxied by depreciation, less stock compensation — and the trailing capex
 * figure never touches the answer. It survives only in the clamped case (capex below depreciation),
 * where an under-investing company is charged its actual spend, which is the intended behaviour.
 *
 * ⚠ `Math.abs(capex)` for the same reason `growthCapex` takes it: the vendor files capex negative
 * and a typed override arrives positive. Adding a negative capex here would ADD the spend to the
 * estimate — a company's capex counted as cash generated, on the one figure the whole panel solves
 * against.
 *
 * ⚠ NULL WHEN EITHER LEG IS MISSING, never a partial answer. `OCF_est` alone is not free cash flow
 * for any company that owns anything, and a base silently missing its capex leg would read as a
 * business with no capital needs at all.
 */
export function forwardFcf(ocfEstimate: number | null | undefined,
  capex: number | null | undefined): number | null {
  if (!ok(ocfEstimate) || !ok(capex)) return null;
  return ocfEstimate - Math.abs(capex);
}

/**
 * THE FORWARD BASE **AND THE CAPEX/D&A PAIR THAT MUST GO WITH IT**.
 *
 * ⚠⚠ ONE RULE, AND IT IS THE WHOLE FUNCTION: THE ADD-BACK USES THE SAME CAPEX THE BASE NETTED.
 * The corrections only cancel — `(OCF − C) + (C − D) = OCF − D` — while both halves are the same
 * `C`. Mix them and the total is out by exactly `C_forward − C_trailing`, which on Meta FY2026 is
 * **39,593**. Measured, all four combinations, in millions:
 *
 *     vendor base 5,412 + TRAILING add-back   46,872   ← split basis, ~10.4k short
 *     vendor base 5,412 + FORWARD  add-back   57,250   ✓
 *     derived base 45,005 + trailing add-back 86,465   ← consistent, but on the trailing D&A
 *     OCF_est − D&A_est − SBC                 57,250   ✓ the same answer, by algebra
 *
 * ⚠⚠ THIS IS THE DEFECT IN THE SPREADSHEET THIS PANEL PORTS. `=@GURUF(…"Estimated Free Cash Flow
 * for Next FY1")` nets a FORWARD capex, and the `MAX(−capex − D&A, 0)` beside it reads the TRAILING
 * lines — so the sheet lands ~39.6bn low on a company whose capex is inflecting, and exactly right
 * on one whose capex is flat. That is the worst kind of wrong: invisible on most names.
 *
 * ⚠ SO THE VENDOR'S FIGURE IS PREFERRED ONLY WHEN THE CORRECTION CAN FOLLOW IT. Forward capex is
 * `OCF_est − FCF_est` and forward D&A is `EBITDA_est − EBIT_est` — all four from the same
 * consensus. Without EBITDA/EBIT there is no forward D&A, so taking the vendor base would force
 * the split; the derivation is used instead, where the trailing capex cancels and the only
 * trailing input reaching the answer is depreciation.
 *
 * ⚠ UNLESS `normalise` IS OFF, where there is no add-back to be inconsistent with. Then the
 * vendor's forecast is simply the better number and is taken whenever it exists.
 *
 * ⚠ `EBITDA − EBIT = D&A` IS INFERRED, NOT PUBLISHED. It assumes the vendor builds EBITDA that
 * way; the series behaves like D&A (monotone, widening with the capex programme, 51,944 for Meta
 * FY2026 against 22,729 trailing) but it is a derivation and is named as one.
 */
export function forwardLegs(o: {
  ocfEstimate: number | null; fcfEstimate: number | null;
  ebitdaEstimate: number | null; ebitEstimate: number | null;
  capex: number | null; dep: number | null; normalise: boolean;
}): { fcf: number | null; capex: number | null; dep: number | null; vendor: boolean } {
  const pair = ok(o.ocfEstimate) && ok(o.fcfEstimate)
    && ok(o.ebitdaEstimate) && ok(o.ebitEstimate)
    ? { capex: o.ocfEstimate - o.fcfEstimate, dep: o.ebitdaEstimate - o.ebitEstimate }
    : null;
  const vendor = ok(o.fcfEstimate) && (!o.normalise || pair != null);
  if (!vendor) {
    return { fcf: forwardFcf(o.ocfEstimate, o.capex), capex: o.capex, dep: o.dep, vendor: false };
  }
  // ⚠ WITH `normalise` OFF AND NO PAIR, the trailing legs ride along unused — the panel still
  // renders them, and they are the honest figures for the rows they label.
  return {
    fcf: o.fcfEstimate,
    capex: pair ? pair.capex : o.capex,
    dep: pair ? pair.dep : o.dep,
    vendor: true,
  };
}

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
