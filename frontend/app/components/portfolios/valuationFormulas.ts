/**
 * THE DEEP VALUATION TAB'S WORKED EXPRESSIONS — the EGM's three and the Reverse DCF's five.
 *
 * ⚠⚠ THEY LIVE HERE RATHER THAN IN THE JSX BECAUSE A LaTeX STRING IS TESTABLE AND A TOOLTIP IS
 * NOT. The failure these guard against is invisible on screen: an unescaped `%` starts a LaTeX
 * COMMENT, so `= +5.8%` truncates the line at the first figure and paints a shorter formula that
 * looks finished (see `workedFormula`'s own ⚠⚠, and `workedFormula.latex.test.ts`, which renders
 * in strict mode so the same input throws instead). Written inline, each of these would be a
 * string nothing could render until a person opened that one ⓘ on a company where every operand
 * happened to be present.
 *
 * ⚠ THE OPERANDS COME OFF THE RESULT, NOT OFF THE INPUTS — the rule `workedCagr` and `workedBand`
 * already follow. The EGM's legs carry the growth, yield and the two multiples the model actually
 * used, so a worked line built from `assumptions` and `src` would be a second route to the same
 * pair, free to drift from the arithmetic it claims to prove.
 *
 * ⚠ THE RESULT IS PASSED IN, ALWAYS — it is the tile's own value, never recomputed here. A second
 * implementation whose only job is to agree with the first is the thing that stops agreeing.
 *
 * ⚠⚠ A MONEY RESULT ARRIVES AS A NUMBER AND IS PRINTED WITH `subNum`, NOT WITH THE PANEL'S OWN
 * FORMATTER. `mn()` and `money()` prefix a CURRENCY CODE (`USD 15,369M`), and in maths mode
 * `USD` is not a word — it is the product of three variables, set in italics. The one formatted
 * string that IS safe is a percentage: it carries no letters, and `texEscape` turns its `%` into
 * `\%` before KaTeX can read it as a comment. Units live in the legend and on the row's own label,
 * where they are prose.
 *
 * ⚠ EVERY BUILDER RETURNS '' WHEN AN OPERAND IS MISSING, and `AspectCard` collapses an empty
 * `worked` rather than leaving a gap. A worked example is worth having only while every number in
 * it is real.
 */
import { type EgmBridge, type EgmLeg } from './egm';
import { subNum, texEscape, withWorked } from './workedFormula';

const ok = (v: number | null | undefined): v is number => v != null && Number.isFinite(v);

/**
 * The bridge's own operands.
 *
 * ⚠ THE PANEL'S `yieldUsed` IS `number | null` AND THE MODEL READS A NULL AS 0, so the two are the
 * same number only by a convention neither side states. The leg is what was multiplied.
 */
export function bridgeParts(b: EgmBridge) {
  const leg = (k: EgmLeg['key']) => b.legs.find((l) => l.key === k);
  return {
    g: leg('growth')?.rate ?? 0,
    y: leg('yield')?.rate ?? 0,
    from: leg('multiple')?.from ?? null,
    to: leg('multiple')?.to ?? null,
  };
}

/**
 * `(1+g)(1+y)(PE_exit ÷ PE_fwd)^(1/n) − 1` — the EGM's expected annual return.
 *
 * ⚠ FOUR DECIMALS ON THE TWO RATES, not the two `subDigits` would give a number below 1. A yield
 * of 0.30% is `0.003`, and at three decimals it rounds to `0.003` — which is fine — while a yield
 * of 0.0025 becomes `0.003` too, so two different companies print the same operand beside two
 * different answers. The rates here are decimals of a percent and need the extra digit.
 */
export function workedEgmReturn(bridge: EgmBridge, years: number, result: string): string {
  const p = bridgeParts(bridge);
  if (!ok(p.from) || !ok(p.to) || !(p.from > 0)) return '';
  return withWorked(
    String.raw`(1+g)(1+y)\left(\dfrac{PE_{\text{exit}}}{PE_{\text{fwd}}}\right)^{1/n} - 1`,
    String.raw`(1+${subNum(p.g, 4)})(1+${subNum(p.y, 4)})`
    + String.raw`\left(\dfrac{${subNum(p.to, 2)}}{${subNum(p.from, 2)}}\right)^{1/${years}} - 1`
    + ` = ${texEscape(result)}`);
}

/**
 * `P₀ (1+g)^n · (PE_exit ÷ PE_fwd)` — the price at the end of the window.
 *
 * ⚠ THE RERATING IS APPLIED ONCE, NOT PER YEAR, and the expression has to show that: the same
 * ratio appears under a `1/n` exponent in the return above, and a reader moving between the two
 * ⓘs is entitled to see why the same two multiples give a per-year figure there and a whole-period
 * one here.
 *
 * ⚠ NO DIVIDEND TERM. This is the capital leg alone — see `EgmResult.impliedPrice`.
 */
export function workedImpliedPrice(price: number | null | undefined, bridge: EgmBridge,
  years: number, result: number | null | undefined): string {
  const p = bridgeParts(bridge);
  if (!ok(price) || !ok(result) || !ok(p.from) || !ok(p.to) || !(p.from > 0)) return '';
  return withWorked(
    String.raw`P_0\,(1+g)^{n}\,\dfrac{PE_{\text{exit}}}{PE_{\text{fwd}}}`,
    `${subNum(price, 2)}`
    + String.raw`\,(1+${subNum(p.g, 4)})^{${years}}`
    + String.raw`\,\dfrac{${subNum(p.to, 2)}}{${subNum(p.from, 2)}}`
    + ` = ${subNum(result, 2)}`);
}

/** `Pₙ ÷ P₀ − 1` — the whole-period price move. */
export function workedPriceMove(implied: number | null | undefined,
  price: number | null | undefined, result: string): string {
  if (!ok(implied) || !ok(price) || price === 0) return '';
  return withWorked(
    String.raw`\dfrac{P_n}{P_0} - 1`,
    String.raw`\dfrac{${subNum(implied, 2)}}{${subNum(price, 2)}} - 1 = ${texEscape(result)}`);
}

/**
 * `OCF_est − |C|` — next year's free cash flow, derived.
 *
 * ⚠ THE BARS ARE NOT DECORATION. The vendor files capex NEGATIVE; without the magnitude this
 * expression ADDS the company's capital spending to its cash flow, and the result is a bigger,
 * entirely plausible number. Same reason `growthCapex` takes `Math.abs`.
 */
export function workedForwardFcf(ocfEstimate: number | null | undefined,
  capex: number | null | undefined, result: number | null | undefined): string {
  if (!ok(ocfEstimate) || !ok(capex) || !ok(result)) return '';
  return withWorked(
    String.raw`OCF_{\text{est}} - \left|C\right|`,
    `${subNum(ocfEstimate, 2)} - ${subNum(Math.abs(capex), 2)} = ${subNum(result, 2)}`);
}

/** `max(|C| − D, 0)` — the growth half of capital spending. */
export function workedGrowthCapex(capex: number | null | undefined,
  dep: number | null | undefined, result: number | null | undefined): string {
  if (!ok(capex) || !ok(dep) || !ok(result)) return '';
  return withWorked(
    String.raw`\max\!\left(\left|C\right| - D,\; 0\right)`,
    String.raw`\max\!\left(${subNum(Math.abs(capex), 2)} - ${subNum(dep, 2)},\; 0\right)`
    + ` = ${subNum(result, 2)}`);
}

/**
 * `F − S + G` — the base and whichever corrections actually ran.
 *
 * ⚠⚠ THE SYMBOLIC HALF IS BUILT FROM WHAT RAN, NOT FROM THE FULL FORM. Printing `F − S + G` over a
 * company with no stock-compensation line states an arithmetic that did not happen — the same "an
 * absent line is not a zero" rule the rows keep, one level up in the notation. A reader checking
 * the subtraction would find one term short and no explanation for it.
 */
export function workedCashFlowValued(base: number | null | undefined,
  sbc: number | null | undefined, growth: number | null | undefined,
  total: number | null | undefined): string {
  if (!ok(base) || !ok(total)) return '';
  const hasSbc = ok(sbc);
  const hasGrowth = ok(growth);
  // ⚠ NO CORRECTION RAN ⇒ NO WORKED LINE. `F = F` is not an arithmetic anybody needs checking, and
  // a typeset restatement of the row above invites the reader to look for the step it is missing.
  if (!hasSbc && !hasGrowth) return '';
  return withWorked(
    `F${hasSbc ? ' - S' : ''}${hasGrowth ? ' + G' : ''}`,
    `${subNum(base, 2)}`
    + (hasSbc ? ` - ${subNum(sbc, 2)}` : '')
    + (hasGrowth ? ` + ${subNum(growth, 2)}` : '')
    + ` = ${subNum(total, 2)}`);
}

/**
 * `P₀ × N` — the market cap solved against.
 *
 * ⚠ `N` IS IN MILLIONS, which is why the product is too and why nothing in this panel scales
 * anything. Stated in the legend rather than left to the box's `(m)` suffix.
 */
export function workedMarketCap(price: number | null | undefined,
  shares: number | null | undefined, total: number | null | undefined): string {
  if (!ok(price) || !ok(shares) || !ok(total)) return '';
  return withWorked(
    String.raw`P_0 \times N`,
    String.raw`${subNum(price, 2)} \times ${subNum(shares, 0)} = ${subNum(total, 0)}`);
}

/**
 * The reverse DCF itself, as an EQUALITY with one unknown.
 *
 * ⚠⚠ IT IS NOT WRITTEN AS `PV = …`, AND THAT IS THE WHOLE POINT OF THE PANEL. A present value on
 * the left of an equals sign reads as a valuation this app computed; what is actually happening is
 * that every term but `g` is known and `g` is what makes the two sides meet. The `⟹ g = …` tail
 * says which way the arrow runs.
 *
 * ⚠ THE PERPETUITY LEG IS SHOWN. It is usually most of the value, and a formula that stopped at
 * the explicit years would understate what the reader is being asked to believe.
 *
 * ⚠⚠ THE EXPONENT IS `t-1`, NOT `t`, AND THE FIRST VERSION OF THIS LINE GOT IT WRONG. `modelValue`
 * pays the BASE cash flow in year 1 and starts growing in year 2 — its closed form is
 * `F·(1 − x^n)/(r − g)` with `x = (1+g)/(1+r)`, which expands to `Σ F(1+g)^{t-1}/(1+r)^t`, and its
 * `g = r` limit case (`F·n/(1+r)`, every term equal) only comes out right on that reading. The
 * terminal leg agrees: it grows the base by `(1+g)^{n-1}` and calls it "the flow in year n".
 *
 * Written with `t` the formula overstates the explicit leg by one whole year of growth — a
 * tooltip that cannot be reconciled with the number beside it, in the one place whose entire job
 * is to let a reader redo the arithmetic. Caught 2026-08-26 by reading `modelValue` to answer a
 * question about it, not by anything that runs.
 */
export function workedImpliedGrowth(o: {
  fcf: number | null | undefined; rate: number; perpetuityGrowth: number; years: number;
  target: number | null | undefined; growth: number | null | undefined;
}): string {
  if (!ok(o.fcf) || !(o.fcf > 0) || !ok(o.target) || !ok(o.growth)) return '';
  const pct = (v: number) => texEscape(`${(v * 100).toFixed(1)}%`);
  return withWorked(
    String.raw`\sum_{t=1}^{n}\dfrac{F\,(1+g)^{t-1}}{(1+r)^{t}}`
    + String.raw` + \dfrac{F\,(1+g)^{n-1}(1+g_\infty)}{(r-g_\infty)(1+r)^{n}} = M`,
    String.raw`F = ${subNum(o.fcf, 2)},\;`
    + String.raw`r = ${pct(o.rate)},\;`
    + String.raw`g_\infty = ${pct(o.perpetuityGrowth)},\;`
    + String.raw`n = ${o.years},\;`
    + String.raw`M = ${subNum(o.target, 0)}`
    + String.raw`\;\Rightarrow\; g = ${pct(o.growth)}`);
}
