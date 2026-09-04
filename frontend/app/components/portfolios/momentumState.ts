/**
 * The seven-state relative-momentum chip: label, tone and the sentence that explains it.
 *
 * ⚠⚠ THIS IS A RANK, NOT A RETURN, AND THE TWO ROUTINELY DISAGREE IN SIGN. A holding up 8% in a
 * universe whose median is up 27% is genuinely WEAK relative to what else could have been owned,
 * and renders `--`. Shown alone that reads as "this fell", which is false — so the cell keeps the
 * raw percentage beside the chip and the tooltip names both. Never render `stateLabel` without a
 * number next to it or a percentile behind it.
 *
 * ⚠⚠ THE SEVEN STATES HAVE FIXED POPULATIONS BY CONSTRUCTION. The cut points are percentiles
 * (10/25/40/60/75/90), so exactly 10% of the reference universe is `+++` on any given day and
 * exactly 10% is `---`. This indicator therefore CANNOT say "everything is strong" — it does not
 * measure that. A reader who thinks it does will misread every bull and bear market, which is why
 * the tooltip says "of N" rather than leaving the population implied.
 *
 * ⚠ THE CUT POINTS ARE NOT REPEATED HERE. The state arrives from the server already bucketed
 * (`momentum/relative.py::_CUTS`); a second copy of those numbers in the client is how two screens
 * come to disagree about whether a holding is `+` or `++`.
 */

/** -3 (weakest) .. +3 (strongest). */
export type MomentumState = -3 | -2 | -1 | 0 | 1 | 2 | 3;

export const MOMENTUM_STATE_LABELS: Record<MomentumState, string> = {
  [-3]: '−−−',
  [-2]: '−−',
  [-1]: '−',
  0: '•',
  1: '+',
  2: '++',
  3: '+++',
};

/**
 * ⚠ TOKENS, NEVER HEX OR A RAW TAILWIND COLOUR — the whole app re-skins from `globals.css`.
 * ⚠ The neutral state is deliberately `fg-subtle` and not a colour: `•` means "middle of the
 * pack", and giving it a tone would make an absence of signal look like one.
 */
export const MOMENTUM_STATE_TONES: Record<MomentumState, string> = {
  [-3]: 'text-neg-400',
  [-2]: 'text-neg-400',
  [-1]: 'text-neg-300',
  0: 'text-fg-subtle',
  1: 'text-pos-300',
  2: 'text-pos-400',
  3: 'text-pos-400',
};

export function isMomentumState(v: number | null | undefined): v is MomentumState {
  return v != null && Number.isInteger(v) && v >= -3 && v <= 3;
}

export function stateLabel(v: number | null | undefined): string | null {
  return isMomentumState(v) ? MOMENTUM_STATE_LABELS[v] : null;
}

export function stateTone(v: number | null | undefined): string {
  return isMomentumState(v) ? MOMENTUM_STATE_TONES[v] : 'text-fg-subtle';
}

/**
 * "82nd" — the ordinal a reader expects for a percentile.
 *
 * ⚠ 0-1 IN, ORDINAL OUT. The server sends a fraction; printing `0.82nd` or `82.0th` both look like
 * a bug. Rounded to a whole percentile because the chip is a bucket anyway — a tenth of a
 * percentile is precision this measure does not have.
 *
 * ⚠ The 11th/12th/13th exception is not decoration: without it a 111th percentile would read
 * "111st". It cannot arise from a fraction ≤ 1, but the helper is the kind of thing that gets
 * reused, and the rule costs one clause.
 *
 * ⚠⚠ DUTCH IS NOT ENGLISH WITH A DIFFERENT SUFFIX TABLE — it takes a single `e` ("82e"), and
 * emitting "82nd" inside an otherwise-Dutch sentence is the kind of half-translation that reads as
 * a bug in the number rather than in the copy. The language is a parameter because this string is
 * built in one place and consumed by both `analyseCopy` locales.
 */
export function ordinalPercentile(
  pct: number | null | undefined, lang: 'en' | 'nl' = 'en',
): string | null {
  if (pct == null || !Number.isFinite(pct)) return null;
  const n = Math.max(1, Math.min(100, Math.round(pct * 100)));
  if (lang === 'nl') return `${n}e`;
  const rem100 = n % 100;
  const suffix = rem100 >= 11 && rem100 <= 13
    ? 'th'
    : { 1: 'st', 2: 'nd', 3: 'rd' }[n % 10] ?? 'th';
  return `${n}${suffix}`;
}
