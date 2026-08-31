/**
 * The Reverse DCF's FCF normalisation.
 *
 * ⚠⚠ THE TWO CORRECTIONS HAVE OPPOSITE SIGNS AND THAT IS THE WHOLE THING TO GET RIGHT. SBC comes
 * off (a real cost that never leaves the cash flow statement); growth capex goes back ON, because
 * reported `Free Cash Flow` is operating cash flow minus TOTAL capex and has already taken it out.
 * Subtracting it a second time would charge the same euros twice — not a conservative choice, an
 * arithmetic error — so the sign is pinned here rather than left to a reviewer's eye.
 *
 * ⚠ CAPEX IS FILED NEGATIVE. Verified on ASML: FCF = OCF + capex to the decimal across three
 * years. A `capex − dep` written without the magnitude is always negative, always clamps to zero,
 * and the add-back then silently never happens on any company in the book — the failure mode this
 * file exists to make impossible.
 *
 * Pure — no DOM, no network.
 */
import { describe, expect, it } from 'vitest';

import { forwardFcf, forwardLegs, growthCapex, normalisedFcf } from './normalisedFcf';

/** ASML's latest filed year, in millions — the row the sign convention was verified against. */
const ASML = { fcf: 11027.3, sbc: 202.3, capex: -1631.2, dep: 1025.9 };

describe('growth capex', () => {
  it('is capex above depreciation, on the magnitude of a negatively-filed capex', () => {
    expect(growthCapex(-1631.2, 1025.9)).toBeCloseTo(605.3, 6);
  });

  it('reads a positively-typed override the same way', () => {
    // ⚠ A reader overriding capex types it POSITIVE. Both spellings must mean one outflow.
    expect(growthCapex(1631.2, 1025.9)).toBeCloseTo(growthCapex(-1631.2, 1025.9)!, 6);
  });

  it('floors at zero when capex is below depreciation', () => {
    // ⚠ NOT A NEGATIVE ADD-BACK. A company spending under depreciation is under-investing;
    // treating the shortfall as a windfall would reward exactly what hollows a business out.
    expect(growthCapex(-500, 900)).toBe(0);
  });

  it('refuses rather than guessing when either leg is missing', () => {
    expect(growthCapex(null, 900)).toBeNull();
    expect(growthCapex(-500, null)).toBeNull();
    expect(growthCapex(NaN, 900)).toBeNull();
  });
});

describe('normalised FCF', () => {
  it('subtracts stock comp and adds growth capex back', () => {
    const n = normalisedFcf(ASML);
    expect(n.growthCapex).toBeCloseTo(605.3, 6);
    expect(n.used).toBeCloseTo(11027.3 - 202.3 + 605.3, 6);
    expect(n.used!).toBeGreaterThan(n.reported!);
  });

  it('never subtracts growth capex — the sign that would double-charge it', () => {
    const n = normalisedFcf(ASML);
    const doubleCharged = ASML.fcf - ASML.sbc - 605.3;
    expect(n.used).not.toBeCloseTo(doubleCharged, 6);
  });

  it('leaves the reported figure untouched beside the corrected one', () => {
    // ⚠ The panel shows both. Folding the correction into `reported` would make an adjusted
    // number indistinguishable from the vendor's.
    expect(normalisedFcf(ASML).reported).toBe(ASML.fcf);
  });

  it('an unreported correction does not run, and says it did not', () => {
    // ⚠⚠ ABSENT IS NOT ZERO. A company with no SBC line is not a company that pays none, and a
    // card that prints "− 0 stock comp" claims a correction nobody could make.
    const noSbc = normalisedFcf({ ...ASML, sbc: null });
    expect(noSbc.applied.sbc).toBe(false);
    expect(noSbc.used).toBeCloseTo(ASML.fcf + 605.3, 6);

    const noDep = normalisedFcf({ ...ASML, dep: null });
    expect(noDep.applied.growthCapex).toBe(false);
    expect(noDep.growthCapex).toBeNull();
    expect(noDep.used).toBeCloseTo(ASML.fcf - ASML.sbc, 6);
  });

  it('with neither leg reported it returns the filed figure unchanged', () => {
    const n = normalisedFcf({ fcf: 500, sbc: null, capex: null, dep: null });
    expect(n.used).toBe(500);
    expect(n.applied).toEqual({ sbc: false, growthCapex: false });
  });

  it('no FCF means no answer, even when the corrections are computable', () => {
    // A correction to nothing is not a valuation input; the panel must report the input missing.
    const n = normalisedFcf({ ...ASML, fcf: null });
    expect(n.used).toBeNull();
    expect(n.reported).toBeNull();
    expect(n.growthCapex).toBeCloseTo(605.3, 6);
  });

  it('a heavy build-out reclassifies most of capex as growth — the case to be careful with', () => {
    /**
     * ⚠⚠ MEASURED ON MICROSOFT'S LATEST FILED YEAR: FCF 66,987, SBC 12,405, capex 90,000-odd
     * against depreciation of ~13,000, so the add-back is 77,414 and normalised FCF is 131,996 —
     * very nearly DOUBLE. That is what the definition asks for, and it is also where the
     * depreciation-as-maintenance-capex proxy is weakest: an asset base being built out for the
     * first time depreciates far less than it costs to build. The normalised figure is defensible
     * for a mature business and aggressive for a hyperscaler mid-build, which is why the panel
     * makes this a toggle and shows the working rather than silently valuing the corrected number.
     */
    const msft = normalisedFcf({ fcf: 66987, sbc: 12405, capex: -90414, dep: 13000 });
    expect(msft.growthCapex).toBe(77414);
    expect(msft.used! / msft.reported!).toBeGreaterThan(1.9);
  });
});

/**
 * ⚠⚠ THE FORWARD BASE, DERIVED — the fallback for a company whose consensus free cash flow we do
 * not hold. GuruFocus does publish one (`keyratios` → `Fundamental`, undocumented) but the fetch is
 * on demand, so most companies arrive without it and this is what they get.
 *
 * The identity below is what makes the substitution sound rather than merely convenient.
 */
describe('forward FCF', () => {
  it('is the consensus operating cash flow less capex', () => {
    expect(forwardFcf(17000, -1631.2)).toBeCloseTo(15368.8, 6);
  });

  it('⚠ reads a positively-typed capex the same way', () => {
    // Without the magnitude a negatively-filed capex would be ADDED — a company's capital
    // spending counted as cash generated, on the one figure the whole panel solves against.
    expect(forwardFcf(17000, 1631.2)).toBeCloseTo(forwardFcf(17000, -1631.2)!, 6);
  });

  it('refuses rather than guessing when either leg is missing', () => {
    // ⚠ OCF alone is not free cash flow for any company that owns anything: a base missing its
    // capex leg reads as a business with no capital needs at all.
    expect(forwardFcf(null, -1631.2)).toBeNull();
    expect(forwardFcf(17000, null)).toBeNull();
    expect(forwardFcf(NaN, -1631.2)).toBeNull();
  });

  it('⚠⚠ the trailing capex CANCELS out of the figure the model values', () => {
    // (OCF − capex) − sbc + (capex − dep) = OCF − dep − sbc.
    //
    // This is why a trailing capex leg on a forward base is sound: it leaves the answer entirely.
    // The only forecast quantity is the operating cash flow, and the only trailing one that
    // survives is depreciation — which is a maintenance-capex proxy, i.e. deliberately the slow
    // -moving leg.
    const ocfEst = 17000;
    const n = normalisedFcf({ ...ASML, fcf: forwardFcf(ocfEst, ASML.capex) });
    expect(n.used).toBeCloseTo(ocfEst - ASML.dep - ASML.sbc, 6);
  });

  it('⚠ and it does NOT cancel below depreciation, which is the intended behaviour', () => {
    // An under-investing company is charged its ACTUAL spend: the add-back clamps to zero, so the
    // capex leg stays in. Treating the shortfall as a windfall would reward hollowing a business
    // out — the same rule `growthCapex` floors for.
    const parts = { fcf: forwardFcf(17000, -500), sbc: 202.3, capex: -500, dep: 1025.9 };
    const n = normalisedFcf(parts);
    expect(n.growthCapex).toBe(0);
    expect(n.used).toBeCloseTo(17000 - 500 - 202.3, 6);
    expect(n.used).not.toBeCloseTo(17000 - 1025.9 - 202.3, 6);
  });

  it('⚠ a missing consensus leaves NO forward base — it does not fall back to the filing', () => {
    // The fallback is the panel's, and it is a visible one: the Base control names which of the
    // two is in use and shows the other beside it. A silent fallback here would put a filed figure
    // under a label reading "next fiscal year".
    expect(forwardFcf(null, ASML.capex)).toBeNull();
    expect(normalisedFcf({ ...ASML, fcf: forwardFcf(null, ASML.capex) }).used).toBeNull();
  });
});

/**
 * ⚠⚠ THE ADD-BACK MUST USE THE SAME CAPEX THE BASE NETTED — the one rule `forwardLegs` exists for,
 * and the defect in the spreadsheet this panel ports.
 *
 * `=@GURUF(…"Estimated Free Cash Flow for Next FY1")` nets a FORWARD capex; the `MAX(−capex − D&A,
 * 0)` beside it reads the TRAILING lines. On a company whose capex is flat the two agree and the
 * sheet is right; on one mid-buildout they do not, and it is short by exactly the step-up.
 *
 * Meta FY2026, real figures throughout.
 */
describe('forward legs', () => {
  const META = {
    ocfEstimate: 134330.10, fcfEstimate: 5412.45,
    ebitdaEstimate: 140802, ebitEstimate: 88858,
    capex: -89325, dep: 22729,        // trailing twelve months
  };
  const SBC = 25136;
  const valued = (l: ReturnType<typeof forwardLegs>) =>
    normalisedFcf({ fcf: l.fcf, sbc: SBC, capex: l.capex, dep: l.dep }).used;

  it('takes the vendor base WITH the forward pair, and the correction still cancels', () => {
    const l = forwardLegs({ ...META, normalise: true });
    expect(l.vendor).toBe(true);
    expect(l.fcf).toBeCloseTo(5412.45, 6);
    expect(l.capex).toBeCloseTo(128917.65, 6);          // OCF_est − FCF_est
    expect(l.dep).toBeCloseTo(51944, 6);                // EBITDA_est − EBIT_est
    // …and the whole thing lands on OCF_est − D&A_est − SBC, by algebra.
    expect(valued(l)).toBeCloseTo(META.ocfEstimate - 51944 - SBC, 4);
    expect(valued(l)).toBeCloseTo(57250.10, 2);
  });

  it('⚠⚠ the vendor base with a TRAILING add-back is short, by an amount nobody would guess', () => {
    // 46,872 against 57,250 — the split basis, stated as the number it produces.
    //
    // ⚠ THE SHORTFALL IS THE CAPEX STEP-UP **NET OF** THE D&A STEP-UP, because the mixed version
    // takes BOTH legs trailing:
    //     correct − split = (C_fwd − C_ttm) − (D_fwd − D_ttm)
    //                     = (128,917.65 − 89,325) − (51,944 − 22,729) = 10,377.65
    // Not the 39.6bn capex gap on its own — which is exactly why an eyeball reconciliation against
    // GuruFocus does not find it: the number is wrong by neither of the two differences on screen.
    const split = normalisedFcf({ fcf: META.fcfEstimate, sbc: SBC,
      capex: META.capex, dep: META.dep }).used;
    expect(split).toBeCloseTo(46872.45, 2);
    expect(57250.10 - split!).toBeCloseTo((128917.65 - 89325) - (51944 - 22729), 1);
    expect(57250.10 - split!).toBeCloseTo(10377.65, 2);
  });

  it('⚠ refuses the vendor base when the correction cannot follow it', () => {
    // No EBITDA/EBIT ⇒ no forward D&A ⇒ taking the vendor figure would FORCE the split above. The
    // derivation is used instead, where the trailing capex cancels.
    const l = forwardLegs({ ...META, ebitdaEstimate: null, ebitEstimate: null, normalise: true });
    expect(l.vendor).toBe(false);
    expect(l.fcf).toBeCloseTo(45005.10, 6);              // OCF_est − |capex_ttm|
    expect(l.capex).toBe(META.capex);
    expect(valued(l)).toBeCloseTo(META.ocfEstimate - META.dep - SBC, 4);
  });

  it('⚠ but takes it with Normalise OFF, where there is no add-back to be inconsistent with', () => {
    const l = forwardLegs({ ...META, ebitdaEstimate: null, ebitEstimate: null, normalise: false });
    expect(l.vendor).toBe(true);
    expect(l.fcf).toBeCloseTo(5412.45, 6);
  });

  it('falls back to the derivation when there is no consensus free cash flow at all', () => {
    const l = forwardLegs({ ...META, fcfEstimate: null, normalise: true });
    expect(l.vendor).toBe(false);
    expect(l.fcf).toBeCloseTo(45005.10, 6);
  });

  it('has no forward base at all when neither route has its inputs', () => {
    expect(forwardLegs({
      ocfEstimate: null, fcfEstimate: null, ebitdaEstimate: null, ebitEstimate: null,
      capex: null, dep: null, normalise: true,
    })).toEqual({ fcf: null, capex: null, dep: null, vendor: false });
  });
});
