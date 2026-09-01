/**
 * THE ATTRIBUTION PANEL'S ARITHMETIC, TYPESET — the Brinson decomposition, one expression each.
 *
 * ⚠⚠ IN A PURE MODULE, NOT IN THE JSX, BECAUSE A LaTeX STRING IS TESTABLE AND A TOOLTIP IS NOT.
 * These eighteen cards used to write their formulas as PROSE with Unicode operators —
 * `Σ(wᵢ × rᵢ) ÷ Σwᵢ over your Technology holdings` — which reads as a formula and is not one: no
 * real scripts, a summation with no limits, and nothing a reader can check against the row. That
 * is the shape `tooltipStyle.test.ts` forbids, and this panel had no alternative until
 * `Provenance` learned `worked` + `legend`.
 *
 * ⚠⚠ AN UNESCAPED `%` STARTS A LaTeX COMMENT and silently truncates the line at the first figure,
 * painting a shorter formula that looks finished — the app renders with `throwOnError: false`, so
 * nothing reports it. Every figure in this panel is a percentage or a percentage POINT, so this is
 * the module where that hazard is densest: every operand goes through `pctTex`.
 *
 * ⚠ THE OPERANDS ARRIVE PRE-FORMATTED, SIGNS AND ALL. The panel already renders `12.3%` and
 * `+0.45pp` through its own formatters; a `toFixed` here would be a second rounding convention and
 * the worked line would stop matching the cell it explains.
 */
import { texEscape, withWorked } from './workedFormula';

/**
 * A formatted figure, safe for maths mode.
 *
 * ⚠ `%` AND `pp` BOTH HANDLED. `12.3%` becomes `12.3\%`; `+0.45pp` becomes `+0.45\,\text{pp}`,
 * because `pp` in maths mode is the product of two italic variables p and p.
 */
export function pctTex(v: string | null | undefined): string {
  if (v == null || v === '') return '';
  const t = String(v).trim();
  if (t.endsWith('pp')) return `${texEscape(t.slice(0, -2))}\\,\\text{pp}`;
  if (t.endsWith('%')) return `${texEscape(t.slice(0, -1))}\\%`;
  return texEscape(t);
}

/** Every builder returns `''` when an operand is missing — `withWorked` then shows the rule alone. */
const has = (...xs: (string | null | undefined)[]) => xs.every((x) => x != null && x !== '');

/** `w = Σ wᵢ ÷ Σ w` — a bucket's share of the attributable book, renormalised to 100%. */
export const WEIGHT_TEX = String.raw`w_{\text{bucket}} = \dfrac{\sum_{i \in \text{bucket}} w_i}`
  + String.raw`{\sum_{i} w_i}`;

export function workedWeight(result: string | null | undefined): string {
  return withWorked(WEIGHT_TEX, has(result) ? String.raw`= ${pctTex(result)}` : '');
}

/** `r = Σ wᵢrᵢ ÷ Σ wᵢ` — a bucket's return, weighted by the holdings in it. */
export const RETURN_TEX = String.raw`r_{\text{bucket}} = \dfrac{\sum_i w_i\,r_i}{\sum_i w_i}`;

export function workedReturn(result: string | null | undefined): string {
  return withWorked(RETURN_TEX, has(result) ? String.raw`= ${pctTex(result)}` : '');
}

/** `contribution = w · r` — one holding's share of the book times what it returned. */
export const CONTRIBUTION_TEX = String.raw`c_i = w_i \times r_i`;

export function workedContribution(w: string | null | undefined,
  r: string | null | undefined, result: string | null | undefined): string {
  return withWorked(CONTRIBUTION_TEX,
    has(w, r, result) ? String.raw`${pctTex(w)} \times ${pctTex(r)} = ${pctTex(result)}` : '');
}

/**
 * `A = (wₚ − w_b)(r_b − r_B)` — the allocation effect.
 *
 * ⚠ SCORED AGAINST THE INDEX **TOTAL**, not against zero: over-weighting a bucket that rose by
 * less than the index as a whole counts against you. That subtraction is the whole reason this
 * expression has three returns in it and is the one readers query.
 */
export const ALLOCATION_TEX = String.raw`A = \left(w_p - w_b\right)\left(r_b - r_B\right)`;

export function workedAllocation(wp: string | null | undefined, wb: string | null | undefined,
  rb: string | null | undefined, rB: string | null | undefined,
  result: string | null | undefined): string {
  return withWorked(ALLOCATION_TEX, has(wp, wb, rb, rB, result)
    ? String.raw`\left(${pctTex(wp)} - ${pctTex(wb)}\right)`
      + String.raw`\left(${pctTex(rb)} - ${pctTex(rB)}\right) = ${pctTex(result)}`
    : '');
}

/** `S = w_b(rₚ − r_b)` — the selection effect, held at the index's own weight. */
export const SELECTION_TEX = String.raw`S = w_b\left(r_p - r_b\right)`;

export function workedSelection(wb: string | null | undefined, rp: string | null | undefined,
  rb: string | null | undefined, result: string | null | undefined): string {
  return withWorked(SELECTION_TEX, has(wb, rp, rb, result)
    ? String.raw`${pctTex(wb)}\left(${pctTex(rp)} - ${pctTex(rb)}\right) = ${pctTex(result)}`
    : '');
}

/** `I = (wₚ − w_b)(rₚ − r_b)` — the cross term. */
export const INTERACTION_TEX = String.raw`I = \left(w_p - w_b\right)\left(r_p - r_b\right)`;

export function workedInteraction(wp: string | null | undefined, wb: string | null | undefined,
  rp: string | null | undefined, rb: string | null | undefined,
  result: string | null | undefined): string {
  return withWorked(INTERACTION_TEX, has(wp, wb, rp, rb, result)
    ? String.raw`\left(${pctTex(wp)} - ${pctTex(wb)}\right)`
      + String.raw`\left(${pctTex(rp)} - ${pctTex(rb)}\right) = ${pctTex(result)}`
    : '');
}

/** `T = A + S + I` — a bucket's whole share of the excess. */
export const TOTAL_TEX = String.raw`T = A + S + I`;

export function workedTotal(a: string | null | undefined, sel: string | null | undefined,
  i: string | null | undefined, result: string | null | undefined): string {
  return withWorked(TOTAL_TEX, has(a, sel, i, result)
    ? String.raw`${pctTex(a)} + ${pctTex(sel)} + ${pctTex(i)} = ${pctTex(result)}`
    : '');
}
