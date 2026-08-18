'use client';

import { useMemo, useState } from 'react';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import {
  defaultDiscountRate, FORECAST_YEARS, impliedGrowth, marketCapOf,
  PERPETUITY_GROWTH,
} from './reverseDcf';
import { type ReverseDcfSource } from './egmInputs';
import { normalisedFcf } from './normalisedFcf';
import ReverseDcfInputsModal from './ReverseDcfInputsModal';
import { type MetricRow } from './quickValuation';

/**
 * Reverse DCF — a plain DCF run backwards. Grow free cash flow at `g` for `n` years, discount it,
 * add a terminal value; solve for the `g` at which that equals the market cap.
 *
 * Every assumption is on screen with a default, and every default that CAN come from the company's
 * own financials does — the discount rate is its published WACC. The two that cannot are marked as
 * conventions rather than dressed up as measurements: no company has its own perpetuity growth
 * rate, and none has its own forecast horizon.
 *
 * ⚠ BLANK MEANS "USE THE DEFAULT" in every field. Seeding the inputs with the computed values would
 * freeze them the moment the payload updates, and there would be no way to tell a number the reader
 * chose from one that merely arrived.
 */

/** 7% … 20%, the sweep shown under the answer. */
const SWEEP = Array.from({ length: 14 }, (_, i) => (7 + i) / 100);

/** GuruFocus's 3–5 year consensus rates, as PERCENTS. See the backend `_growth_estimates`. */
export type GrowthEstimates = {
  eps_3_5y: number | null;
  eps_nri_3_5y: number | null;
  ocf_ps_3_5y: number | null;
  revenue_3_5y: number | null;
};

/**
 * ⚠ THE DEFAULT IS THE VALUE, NOT A PLACEHOLDER. `value` is the override when one has been typed
 * and the computed default otherwise, so the box always shows the number actually in use — a
 * greyed placeholder reads as an empty field, and an empty field beside a valuation invites the
 * question of what it was computed from.
 *
 * Clearing the box hands control back to the default; `null` (never typed) and `''` (cleared) both
 * fall through to it.
 */
function Field({ label, value, onChange, suffix, info }: {
  label: string; value: string; onChange: (v: string) => void;
  suffix?: string; info?: React.ReactNode;
}) {
  return (
    <label className="flex items-center gap-2 py-1">
      <span className="min-w-0 flex-1 truncate text-[12px] text-fg-muted">{label}</span>
      {/* ⚠ ONE WIDTH FOR EVERY BOX, whatever it holds. They were `w-32` / `w-36` / `w-20`, sized to
          their content — five inputs at three widths, so the column they form was a staircase.
          A market cap in millions is the widest thing here and it overflows INSIDE the box rather
          than widening it, which is what keeps the rows aligned and the panel from resizing as the
          reader types. */}
      <input type="number" value={value} onChange={(e) => onChange(e.target.value)}
        className="w-24 shrink-0 rounded border border-neutral-700 bg-page px-1.5 py-0.5 text-right font-mono text-[12px] text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
      <span className="w-2 shrink-0 text-[11px] text-fg-muted">{suffix}</span>
      {/* ⚠⚠ THE ⓘ IS A TRAILING SLOT, NOT A SUFFIX ON THE LABEL — the same rule and the same width
          as the EGM panel above. Beside the label its x landed wherever that label happened to end
          (`Discount rate ⓘ` against `Target market cap (USD m) ⓘ` is most of the column apart), so
          five explanations read as scattered punctuation. Last slot, fixed width: every ⓘ on BOTH
          panels lands on one vertical line, because the boxes are equal-width grid cells and the
          two panels are the same card. */}
      <span className="flex w-9 shrink-0 items-center">{info}</span>
    </label>
  );
}

export default function ReverseDcfPanel({ src, currency, metrics, name, isin, growthEst }: {
  src: ReverseDcfSource; currency?: string | null;
  metrics: MetricRow[]; name?: string | null; isin: string;
  /** Analysts' 3–5y consensus — context beside the implied rate, never an input to it. */
  growthEst?: GrowthEstimates | null;
}) {
  // ⚠ `null` MEANS "NEVER TYPED", which is not the same as `''` (cleared). The defaults are not
  // known when this component first renders — the payload has not loaded — so they cannot be
  // seeded into state; instead the input DISPLAYS the default until an override exists, which also
  // means the box keeps tracking the data if the payload updates underneath it.
  const [fcfStr, setFcfStr] = useState<string | null>(null);
  const [targetStr, setTargetStr] = useState<string | null>(null);
  const [rateStr, setRateStr] = useState<string | null>(null);
  const [perpStr, setPerpStr] = useState<string | null>(null);
  const [yearsStr, setYearsStr] = useState<string | null>(null);
  const [showRaw, setShowRaw] = useState(false);
  /**
   * ⚠ DEFAULT ON, LIKE THE LONG EQUITY TAB'S SBC BOX AND FOR THE SAME REASON: the uncorrected
   * figure is the flattering one on the SBC leg, and the reported one is the misleading one on the
   * capex leg (it charges a decade of expansion against a single year). A reader who never touches
   * this control should get the better number, not the raw one.
   *
   * ⚠ IT IS A MODE, NOT AN INPUT, and it still counts as `dirty` when switched OFF so `Reset`
   * genuinely restores the panel's defaults rather than most of them.
   */
  const [normalise, setNormalise] = useState(true);

  // The defaults, derived here so the boxes and the model read the same numbers.
  const norm = useMemo(
    () => normalisedFcf({ fcf: src.fcf, sbc: src.sbc, capex: src.capex, dep: src.dep }),
    [src]);
  /** ⚠ FALLS BACK TO THE REPORTED FIGURE WHEN NOTHING COULD BE CORRECTED — `normalisedFcf` returns
   *  `used === reported` in that case, so this is belt and braces on a null FCF only. */
  const defFcf = normalise ? (norm.used ?? src.fcf) : src.fcf;
  const defTarget = marketCapOf(src);
  const defPerp = PERPETUITY_GROWTH;
  const defRate = defaultDiscountRate(src.wacc, defPerp);
  const defYears = FORECAST_YEARS;

  const num = (s: string | null) => {
    if (s == null || s.trim() === '') return null;
    const v = parseFloat(s);
    return Number.isFinite(v) ? v : null;
  };
  const fcf = num(fcfStr) ?? defFcf;
  const target = num(targetStr) ?? defTarget;
  const perpetuityGrowth = (num(perpStr) ?? defPerp * 100) / 100;
  const rate = (num(rateStr) ?? defRate * 100) / 100;
  const years = num(yearsStr) ?? defYears;

  // What each box shows: the override if one was typed, else the default as a plain number.
  const show = (s: string | null, def: number | null, dp = 0) =>
    (s != null ? s : def == null ? '' : def.toFixed(dp));

  const dirty = [fcfStr, targetStr, rateStr, perpStr, yearsStr].some((s) => s != null)
    || !normalise;
  const reset = () => {
    setFcfStr(null); setTargetStr(null); setRateStr(null); setPerpStr(null); setYearsStr(null);
    setNormalise(true);
  };

  const growth = useMemo(() => impliedGrowth(src, {
    years, perpetuityGrowth, discountRates: [rate], fcfOverride: fcf, targetOverride: target,
  })[0]?.impliedGrowth ?? null, [src, years, perpetuityGrowth, rate, fcf, target]);

  // The sweep. Same solver, same overrides — so a cell and the headline cannot disagree about the
  // rate they share.
  const sweep = useMemo(() => impliedGrowth(src, {
    years, perpetuityGrowth, discountRates: SWEEP, fcfOverride: fcf, targetOverride: target,
  }), [src, years, perpetuityGrowth, fcf, target]);

  const missing: string[] = [];
  if (target == null) {
    if (src.price == null) missing.push('price');
    if (src.sharesOutstanding == null) missing.push('share count');
  }
  if (fcf == null) missing.push('free cash flow');

  const mn = (v: number | null) => (v == null ? 'n/a'
    : `${currency ? `${currency} ` : ''}${Math.round(v).toLocaleString('en-US')}M`);
  /**
   * The same figure in the unit a reader actually thinks in.
   *
   * ⚠⚠ THE BOXES HOLD MILLIONS AND NOTHING SAID SO EXCEPT `(m)` IN A LABEL. `312400` in an input
   * is unreadable at a glance and is off by a factor of a thousand from what anyone means when
   * they say "market cap" — so every ⓘ restates its default BOTH ways, and the one that is easy to
   * misread is the one the box holds. GuruFocus files shares and free cash flow in millions, so
   * price × shares comes out in millions too (`reverseDcf.ts`); this is a display, never a value
   * anything computes from.
   */
  const scaled = (v: number | null) => {
    if (v == null) return 'n/a';
    const c = currency ? `${currency} ` : '';
    const a = Math.abs(v);
    if (a >= 1_000_000) return `${c}${(v / 1_000_000).toFixed(2)}tn`;
    if (a >= 1_000) return `${c}${(v / 1_000).toFixed(2)}bn`;
    return `${c}${v.toFixed(0)}M`;
  };

  // ⚠ The comparison the whole panel is for: the model says the price implies X, and these are
  // what anyone actually forecasts. Rendered only when at least one arrived — an empty pair of
  // columns headed "analysts" reads as "they expect nothing".
  const eps35 = growthEst?.eps_3_5y ?? null;
  const ocf35 = growthEst?.ocf_ps_3_5y ?? null;
  // ⚠ THE MEAN OF THE TWO THAT ARE THERE, AND NOTHING WHEN ONE IS MISSING. Averaging a present
  // figure with an absent one silently returns the present one under a label that claims it is a
  // blend of both — the reader would have no way to tell a two-source average from a one-source
  // one. A single number is shown in its own column already.
  const avg35 = eps35 != null && ocf35 != null ? (eps35 + ocf35) / 2 : null;
  const analysts: [string, number | null, string][] = [
    ['EPS 3-5y', eps35,
      'Analysts’ 3–5 year EPS growth consensus (GuruFocus “Future 3-5Y EPS Growth Rate Estimate”). A forecast, not a solve — and over 3–5 years, not the 10 the model compounds.'],
    ['OCF/sh 3-5y', ocf35,
      'Analysts’ 3–5 year operating-cash-flow-per-share growth consensus. ⚠ OCF, not free cash flow — it runs ahead of FCF by whatever capex the company spends, and the model compounds FCF.'],
    ['Avg', avg35,
      'The plain mean of the two consensus rates to the left. ⚠ Blank unless BOTH are present — an average of one number is that number, and labelling it an average would hide which.'],
  ];
  const hasAnalysts = analysts.some(([, v]) => v != null);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">Reverse DCF</h4>
        <span className="text-[12px] text-fg-faint">
          what the price implies, not what the company is worth
        </span>
      </div>

      {/**
        * ⚠⚠ INPUT LEFT, OUTPUT RIGHT — the same pair of rectangles the EGM panel above uses, and
        * deliberately the same shape: these two answer one question from opposite ends ("what are
        * these assumptions worth" / "what does the price already assume"), so a reader moving
        * between them should not have to re-learn where anything is.
        *
        * ⚠ THE HEIGHTS MATCH FOR FREE — grid children stretch — and `justify-center` keeps each
        * body optically centred in whichever box is the shorter.
        */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-3 items-stretch">

        {/* ── INPUT ────────────────────────────────────────────────────────────────────────── */}
        <div className="flex flex-col rounded-lg border border-neutral-800/40 bg-inset p-3">
          <div className="flex items-center gap-2">
            <span className="text-[11px] uppercase tracking-wide text-fg-faint">Input</span>
            {/* ⚠ RENDERED ALWAYS, HIDDEN WITH `invisible` — mounting it on the first keystroke
                makes the header row taller, which grows this box, which grows the OUTPUT box
                beside it through the grid's stretch. The whole panel would jump because someone
                typed a digit. `visibility: hidden` reserves the geometry and still takes it out of
                the tab order and the accessibility tree, which `disabled` would not. */}
            {/* ⚠ `ml-auto` MOVED HERE FROM `Reset`, so the group stays right-aligned as one. The
                checkbox is always mounted — it is not conditional on anything — so it cannot make
                the header row grow or shrink under the reader. */}
            <label className="ml-auto flex items-center gap-1.5 text-[11px] text-fg-soft cursor-pointer whitespace-nowrap"
              title={'Value free cash flow net of stock compensation and before growth capex.\n\n'
                + 'SBC is subtracted: it is a real cost that never leaves the cash flow statement.\n'
                + 'Growth capex (capex above depreciation) is ADDED BACK: reported FCF already '
                + 'subtracted it, and it buys the very growth this model is solving for.'}>
              <input type="checkbox" checked={normalise}
                onChange={(e) => setNormalise(e.target.checked)}
                className="accent-accent-600 w-3.5 h-3.5" />
              Normalise
            </label>
            <button type="button" onClick={reset} aria-hidden={!dirty} tabIndex={dirty ? 0 : -1}
              title="Put every input back to its default"
              className={`rounded border border-neutral-700 px-1.5 py-0.5 text-[11px] text-fg-soft hover:bg-overlay/5 ${
                dirty ? '' : 'invisible'}`}>
              Reset
            </button>
          </div>

          <div className="flex flex-col divide-y divide-neutral-800/30">
            <Field label={`Free cash flow${currency ? ` (${currency}m)` : ' (m)'}`}
              value={show(fcfStr, defFcf)} onChange={setFcfStr}
              info={<InfoTip content={<AspectCard
                what={normalise
                  ? 'Free cash flow, net of stock compensation and before growth capex.'
                  : 'Latest reported free cash flow.'}
                where="GuruFocus — cashflow statement, as filed."
                when="Most recent fiscal year."
                how={(normalise && norm.reported != null
                  // ⚠ THE WORKING, LINE BY LINE, AND ONLY THE LINES THAT ACTUALLY RAN. A card
                  // reading "− SBC" on a company with no SBC line would claim a correction that
                  // did not happen; `applied` is what distinguishes "no stock comp" from "no stock
                  // comp REPORTED", and only one of those is a fact about the company.
                  ? `${mn(norm.reported)} reported`
                    + (norm.applied.sbc ? `\n− ${mn(norm.sbc as number)} stock comp` : '')
                    + (norm.applied.growthCapex
                      ? `\n+ ${mn(norm.growthCapex as number)} growth capex`
                        + ` (capex ${mn(Math.abs(src.capex as number))} − depreciation`
                        + ` ${mn(src.dep as number)}, floored at 0)`
                      : '')
                    + `\n= ${mn(norm.used as number)}`
                    + (!norm.applied.sbc || !norm.applied.growthCapex
                      ? `\n\n⚠ ${[!norm.applied.sbc ? 'Stock compensation' : null,
                        !norm.applied.growthCapex ? 'Capex or depreciation' : null]
                        .filter(Boolean).join(' and ')} not reported, so that correction did `
                        + 'not run — an absent line is not a zero.'
                      : '')
                    + '\n\nSBC is subtracted (a real cost that never leaves the cash flow '
                    + 'statement). Growth capex is ADDED BACK because reported FCF already '
                    + 'subtracted it — it buys the growth this model solves for, so leaving it in '
                    + 'charges the expansion once as a cost and again as the thing to explain.'
                  : 'Raw data, no formula.')
                  + ` The box holds MILLIONS${defFcf != null ? ` — ${mn(defFcf)} is ${scaled(defFcf)}` : ''}.`} />} />} />
            <Field label={`Target market cap${currency ? ` (${currency}m)` : ' (m)'}`}
              value={show(targetStr, defTarget)} onChange={setTargetStr}
              info={<InfoTip content={<AspectCard
                what="The valuation solved against."
                where="Computed from the price and the share count."
                when="Latest close, latest reported share count."
                how={`Price × diluted shares${src.price != null && src.sharesOutstanding != null
                  ? ` = ${currency ? `${currency} ` : ''}${src.price.toFixed(2)} × ${src.sharesOutstanding.toLocaleString('en-US', { maximumFractionDigits: 0 })}M` : ''}. In MILLIONS, like the cash flow above.`} />} />} />
            <Field label="Discount rate" value={show(rateStr, defRate * 100, 1)} onChange={setRateStr}
              suffix="%"
              info={<InfoTip content={<AspectCard
                what="Rate the cash flows are discounted at."
                where={src.wacc != null && defRate === src.wacc
                  ? 'GuruFocus — this company\'s own WACC.'
                  : src.wacc != null
                    ? `House default. Its WACC of ${(src.wacc * 100).toFixed(1)}% is too close to the perpetuity growth to use.`
                    : 'House default — no WACC is reported for this company.'}
                when="Latest fiscal year."
                how="Percent per year. Raw data; must exceed the perpetuity growth." />} />} />
            <Field label="Perpetuity growth" value={show(perpStr, defPerp * 100, 1)} onChange={setPerpStr}
              suffix="%"
              info={<InfoTip content={<AspectCard
                what="Growth after the forecast years."
                where="House convention — no company version of it exists."
                when={`Year ${defYears + 1} onwards, for ever.`}
                how="Percent per year. Raw input, no formula." />} />} />
            <Field label="Forecast years" value={show(yearsStr, defYears)} onChange={setYearsStr}
              info={<InfoTip content={<AspectCard
                what="Length of the explicit growth phase."
                where="House convention — no company has its own horizon."
                when={`Years 1 to ${years}.`}
                how="A count of years. Raw input, no formula." />} />} />
          </div>

          <button type="button" onClick={() => setShowRaw(true)}
            title="Show every company figure this reads, with its source"
            className="mt-1.5 self-start text-[11px] text-fg-faint underline decoration-dotted underline-offset-2 hover:text-fg-strong">
            raw data ↗
          </button>
        </div>

        {/* ── OUTPUT ───────────────────────────────────────────────────────────────────────── */}
        <div className="flex flex-col rounded-lg border border-neutral-800/40 bg-inset p-3">
          <span className="text-[11px] uppercase tracking-wide text-fg-faint">Output</span>

          <div className="flex flex-1 flex-col justify-center">
            {/**
              * ⚠⚠ THE ANSWER IS A TABLE ROW, NOT A SENTENCE. It was a paragraph — "That market cap
              * implies 24.3% annual FCF growth for 10 years" — with a 2xl number inside it, which
              * reflowed on every keystroke and could not line up with anything. The refusals below
              * were four more paragraphs of DIFFERENT lengths, so the panel changed height
              * depending on which input was missing.
              *
              * ⚠ THE ROWS ARE CONSTANT AND ONLY THE VALUE GOES `n/a` — the same rule the EGM panel
              * follows. The reason a solve failed is in the ⓘ, which is present either way, so the
              * SHAPE of the panel stops being a signal and the numbers can be one.
              */}
            <table className="w-full table-fixed text-[12px]">
              {/* The last column is the ⓘ slot — see `Field`. Empty on every row with no
                  explanation, which is what holds the ones that have to a single line. */}
              <colgroup>
                <col />
                <col className="w-[5rem]" />
                <col className="w-9" />
              </colgroup>
              <tbody>
                <tr>
                  <td className="truncate py-0.5 font-medium text-fg-strong">Implied FCF growth</td>

                  <td className="py-0.5 pl-2 text-right font-mono tabular-nums font-semibold text-fg-strong">
                    {/* ⚠ UNCOLOURED ON PURPOSE. Whether 50% is absurd or reasonable is the reader's
                        call, and a threshold picked out of the air was making it for them. */}
                    {fcf != null && fcf > 0 && growth != null
                      ? `${(growth * 100).toFixed(1)}%` : 'n/a'}
                  </td>
                  <td className="py-0.5">
                    <InfoTip content={<AspectCard
                      what="FCF growth the market cap already assumes."
                      where="Computed — solved, not forecast."
                      when={`Years 1 to ${years}, then the perpetuity growth.`}
                      how={fcf != null && fcf > 0 && growth != null
                        ? 'The rate at which the discounted cash flows equal the target market cap. Not a valuation — what you would have to believe.'
                        : fcf != null && fcf <= 0
                          ? `Free cash flow of ${mn(fcf)} is at or below zero, so no growth rate works. A fact about the company, not an error.`
                          : missing.length > 0
                            ? `Not enough inputs — no ${missing.join(', ')} ingested.`
                            : !(rate > perpetuityGrowth)
                              ? 'The discount rate must exceed the perpetuity growth — the terminal value divides by the gap.'
                              : 'No rate between −99% and 1000% a year reconciles that market cap with this cash flow.'} />} />
                  </td>
                </tr>

                {/**
                  * ⚠⚠ THE COMPARISON THE WHOLE PANEL IS FOR, MOVED OUT OF THE SWEEP. These were
                  * three extra columns bolted onto the discount-rate table and fenced off with
                  * borders, because they are NOT a solve at anything — they are what people
                  * actually forecast. The fencing existed to stop them reading as "the implied
                  * rate at some other discount rate"; as rows under the answer they cannot, and
                  * the sweep goes back to being one thing.
                  *
                  * ⚠ ONLY WHEN AT LEAST ONE ARRIVED. Three rows of `—` headed "analysts" reads as
                  * "they expect nothing".
                  */}
                {hasAnalysts && analysts.map(([label, value, tip], i) => (
                  <tr key={label} className={i === 0 ? 'border-t border-neutral-800/40' : ''}>
                    <td className={`truncate py-0.5 text-fg-muted ${i === 0 ? 'pt-1.5' : ''}`}
                      title={tip}>
                      {i === 0 && <span className="text-fg-faint">analysts </span>}{label}
                    </td>
                    <td className={`py-0.5 pl-2 text-right font-mono tabular-nums ${
                      i === analysts.length - 1 ? 'text-fg-strong' : 'text-fg-soft'
                    } ${i === 0 ? 'pt-1.5' : ''}`}>
                      {value == null ? '—' : `${value.toFixed(1)}%`}
                    </td>
                    <td />
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {/* The sensitivity, which is the honest context for a single figure: the answer moves ~1.5pp
          per point of discount rate, so a reader who disagrees with the rate can read their own
          off the row instead of taking this one. Click a column to make it the headline. */}
      {fcf != null && fcf > 0 && target != null && (
        <div className="space-y-1">
          {/* ⚠ FULL WIDTH, UNDER BOTH BOXES, AND NOT INSIDE THE OUTPUT ONE. Eleven discount
              rates do not fit in half a card without a horizontal scrollbar, and a sensitivity
              grid is a different kind of object from a conclusion: it is the honest context for a
              single figure (the answer moves ~1.5pp per point of discount rate), not part of the
              input → output story the two rectangles tell. Clicking a column still sets the rate,
              which is why it stays on the page at all. */}
          <div className="text-[11px] uppercase tracking-wide text-fg-faint">
            Implied growth by discount rate
          </div>
          <div className="overflow-x-auto rounded-lg border border-neutral-800/40 max-w-full">
            <table className="text-xs">
              <tbody>
                <tr className="border-b border-neutral-800/40 bg-page">
                  <th className="px-2 py-1 text-left font-medium text-fg-faint text-[11px] uppercase tracking-wide sticky left-0 bg-page">
                    Discount
                  </th>
                  {sweep.map((s) => (
                    <td key={s.discountRate}
                      className={`px-2 py-1 text-right font-mono whitespace-nowrap cursor-pointer hover:bg-overlay/5 ${
                        Math.abs(s.discountRate - rate) < 0.0005
                          ? 'text-fg-strong font-semibold' : 'text-fg-muted'}`}
                      onClick={() => setRateStr((s.discountRate * 100).toFixed(1))}
                      title="Use this rate">
                      {(s.discountRate * 100).toFixed(0)}%
                    </td>
                  ))}
                  {/* ⚠⚠ THE ANALYST COLUMNS ARE GONE FROM HERE (2026-08-18) — they are ROWS in
                      the Output box now. They were never part of this sweep: a forecast, not a
                      solve at some other discount rate, which is why they needed two border rules
                      and a caption to stop them being read as one. Under the answer they compare
                      with, they need neither, and this table goes back to being one thing. */}
                </tr>
                <tr>
                  <th className="px-2 py-1 text-left font-medium text-fg-faint text-[11px] uppercase tracking-wide sticky left-0 bg-card">
                    Growth
                  </th>
                  {sweep.map((s) => (
                    <td key={s.discountRate}
                      className={`px-2 py-1 text-right font-mono whitespace-nowrap cursor-pointer hover:bg-overlay/5 ${
                        Math.abs(s.discountRate - rate) < 0.0005
                          ? 'text-fg-strong font-semibold' : 'text-fg-soft'}`}
                      onClick={() => setRateStr((s.discountRate * 100).toFixed(1))}>
                      {/* A rate at or below the perpetuity growth has no valid terminal value, so
                          the cell is empty rather than showing a negative one. */}
                      {s.impliedGrowth == null ? '—' : `${(s.impliedGrowth * 100).toFixed(1)}%`}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      )}

      {showRaw && (
        <ReverseDcfInputsModal metrics={metrics} currency={currency}
          name={name} isin={isin} fcf={fcf} target={target} discountRate={rate}
          years={years} perpetuityGrowth={perpetuityGrowth}
          onClose={() => setShowRaw(false)} />
      )}
    </div>
  );
}
