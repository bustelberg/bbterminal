/**
 * THE ONE-SIGMA ACTIVE-RETURN BAND — what a tracking error actually says about a year.
 *
 *     centre = ā · f          band = centre ± TE
 *
 * ⚠⚠ THE BAND IS CENTRED ON ā, NOT ON ZERO, AND THAT IS THE WHOLE REASON THIS MODULE EXISTS.
 * "TE 12.41%" is read, near-universally, as "we can land 12.41pp either side of the benchmark".
 * That is the OTHER definition of tracking error — √(Σaₜ²/T)·√f, the one that does NOT subtract ā —
 * and `_tracking_error.py` deliberately does subtract it (its own ⚠⚠). The spread we report is
 * therefore around the sleeve's own average active return: a book that has beaten its index by 3pp
 * a year has a typical year at +3 ± 12.41, i.e. −9.41 to +15.41, which is not symmetric about the
 * index at all. The two readings coincide only when ā = 0 — the one case no real book is in.
 *
 * ⚠ THE CENTRE ANNUALISES BY ×f AND THE SPREAD BY ×√f. Not an inconsistency waiting to be tidied:
 * a mean accumulates with time, a standard deviation with its square root. Applying one factor to
 * both is the usual way this arithmetic goes wrong, and at f = 52 it is wrong by 7.21×.
 *
 * ⚠⚠ THE CENTRE IS THE ARITHMETIC MEAN, WHICH IS *NOT* THE `Active return (ann.)` TILE BESIDE IT.
 * That tile is geometric — `prod(1+aₜ)^(f/T) − 1` — and geometric ≈ arithmetic − σ²/2, so at a TE
 * of 12.41% the two sit ~0.77pp apart. Both are correct; they answer different questions, and a
 * ±σ band is only coherent around the arithmetic one. So the gap is NAMED in the tooltip rather
 * than papered over by centring the band on whichever number is already on screen — which would
 * have been the tidy-looking and statistically wrong choice, and invisible once shipped.
 *
 * ⚠ ONE SIGMA, AND ACTIVE RETURNS ARE FATTER-TAILED THAN NORMAL. "About two years in three" is the
 * honest gloss; a ±2σ band sold as "95%" is not, which is why this returns one band and not two.
 *
 * ⚠ IT REFUSES RATHER THAN GUESSES — the rule every builder in `workedFormula` follows. A missing
 * or unusable operand returns null and the caller falls back to the formula alone.
 */

export type ActiveBand = {
  /** ā · f — the annualised ARITHMETIC mean active return, in pp/yr. The band's centre. */
  centre: number;
  /** The annualised tracking error it was widened by, in pp. Carried so the caller cannot pair
   *  a band with a different TE than the one that built it. */
  te: number;
  lo: number;
  hi: number;
};

const ok = (v: number | null | undefined): v is number => v != null && Number.isFinite(v);

/**
 * `{centre, te, lo, hi}` for one cadence's figures, or null if any operand is unusable.
 *
 * All three inputs come off the `TrackingError` payload exactly as the backend computed them —
 * `mean_active_per_period_pct` (arithmetic, per period, already ×100), `periods_per_year` and
 * `tracking_error_pct` (already annualised, already ×100). ⚠ NOTHING HERE RE-ANNUALISES THE TE:
 * `annualized_stats` did it once, and a second √f applied here would be a silent 7.21×.
 */
export function oneSigmaBand(
  meanActivePerPeriodPct: number | null | undefined,
  periodsPerYear: number | null | undefined,
  trackingErrorPct: number | null | undefined,
): ActiveBand | null {
  if (!ok(meanActivePerPeriodPct) || !ok(periodsPerYear) || !ok(trackingErrorPct)) return null;
  // ⚠ A NON-POSITIVE f IS NOT A CADENCE, and a zero-or-negative TE has no band — the same refusal
  // the information-ratio tile makes for the same reason: there is no risk to lay either side of.
  if (periodsPerYear <= 0 || trackingErrorPct <= 0) return null;
  const centre = meanActivePerPeriodPct * periodsPerYear;
  return {
    centre,
    te: trackingErrorPct,
    lo: centre - trackingErrorPct,
    hi: centre + trackingErrorPct,
  };
}
