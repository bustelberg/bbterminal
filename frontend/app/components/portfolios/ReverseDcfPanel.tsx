'use client';

import { useMemo, useState } from 'react';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import {
  defaultDiscountRate, FALLBACK_DISCOUNT_RATE, FORECAST_YEARS, impliedGrowth, marketCapOf,
  PERPETUITY_GROWTH,
} from './reverseDcf';
import { type ReverseDcfSource } from './egmInputs';
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
function Field({ label, value, onChange, suffix, info, width = 'w-32' }: {
  label: string; value: string; onChange: (v: string) => void;
  suffix?: string; info?: React.ReactNode; width?: string;
}) {
  return (
    <label className="flex flex-col gap-1 min-w-0">
      <span className="flex items-center gap-1 text-[11px] uppercase tracking-wide text-fg-muted">
        {label}{info}
      </span>
      <span className="flex items-center gap-1">
        <input type="number" value={value} onChange={(e) => onChange(e.target.value)}
          className={`${width} bg-page border border-neutral-700 rounded-lg px-2 py-1 text-sm font-mono text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30`} />
        {suffix && <span className="text-xs text-fg-muted">{suffix}</span>}
      </span>
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

  // The defaults, derived here so the boxes and the model read the same numbers.
  const defFcf = src.fcf;
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

  const dirty = [fcfStr, targetStr, rateStr, perpStr, yearsStr].some((s) => s != null);
  const reset = () => {
    setFcfStr(null); setTargetStr(null); setRateStr(null); setPerpStr(null); setYearsStr(null);
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
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-4 min-w-0">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">Reverse DCF</h4>
        <span className="text-[11px] text-fg-faint">
          what the price implies, not what the company is worth
        </span>
        <span className="ml-auto flex items-center gap-1.5">
          {dirty && (
            <button type="button" onClick={reset}
              className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
              Reset to defaults
            </button>
          )}
          <button type="button" onClick={() => setShowRaw(true)}
            title="Show every company figure this reads, with its source"
            className="text-[11px] px-2 py-0.5 rounded-lg border border-neutral-700 text-fg-soft hover:bg-overlay/5">
            Raw data ↗
          </button>
        </span>
      </div>

      <div className="flex flex-wrap gap-x-6 gap-y-3 items-start">
        <Field label={`Free cash flow${currency ? ` (${currency}m)` : ' (m)'}`}
          value={show(fcfStr, defFcf)} onChange={setFcfStr}
          info={<InfoTip content={<AspectCard
            what="The cash flow the model compounds."
            where="The latest reported Free Cash Flow, straight off the cashflow statement — nothing adjusted, nothing forecast."
            when="The most recent fiscal year."
            how="Override it when the last reported year is unrepresentative — a one-off settlement, a capex spike." />} />} />
        <Field label={`Target market cap${currency ? ` (${currency}m)` : ' (m)'}`}
          value={show(targetStr, defTarget)} onChange={setTargetStr} width="w-36"
          info={<InfoTip content={<AspectCard
            what="The valuation the growth rate is solved against."
            where="Today's share price × diluted average shares outstanding."
            when="Latest close, latest reported share count."
            how="Change it to ask a different question — what growth would justify a price 30% lower?" />} />} />
        <Field label="Discount rate" value={show(rateStr, defRate * 100, 1)} onChange={setRateStr}
          suffix="%" width="w-20"
          info={<InfoTip content={<AspectCard
            what="The rate the projected cash flows are discounted at."
            // ⚠ Named for its source. A reader seeing 8.2% needs to know whether that is this
            // company's cost of capital or a house number — it changes what a 24% answer means.
            where={src.wacc != null && defRate === src.wacc
              ? `This company's own WACC, ${(src.wacc * 100).toFixed(1)}%, as published by GuruFocus.`
              : src.wacc != null
                ? `Its WACC reads ${(src.wacc * 100).toFixed(1)}%, too close to the perpetuity growth to use — the terminal value would be negative — so the ${(FALLBACK_DISCOUNT_RATE * 100).toFixed(0)}% house default stands in.`
                : `No WACC is reported for this company, so the ${(FALLBACK_DISCOUNT_RATE * 100).toFixed(0)}% house default stands in.`}
            when="The latest fiscal year."
            how="Must exceed the perpetuity growth: the terminal value divides by the gap, so equal rates give an infinite value rather than a large one." />} />} />
        <Field label="Perpetuity growth" value={show(perpStr, defPerp * 100, 1)} onChange={setPerpStr}
          suffix="%" width="w-20"
          info={<InfoTip content={<AspectCard
            what="The rate the cash flows grow at for ever after the forecast period."
            where="A convention, not a measurement — there is no company-specific version of it."
            when="From year 11 onwards."
            how="Long-run nominal economic growth. No business outgrows the economy for ever, so deriving this from the company's own history would encode its last decade as eternity." />} />} />
        <Field label="Forecast years" value={show(yearsStr, defYears)} onChange={setYearsStr}
          width="w-20"
          info={<InfoTip content={<AspectCard
            what="How long the explicit growth phase runs before the terminal value takes over."
            where="A convention — no company has its own forecast horizon."
            when="Years 1 to n."
            how="Long enough for growth to matter, short enough to be arguable. Shorten it and more of the valuation sits in the terminal value." />} />} />
      </div>

      {fcf != null && fcf > 0 && growth != null ? (
        <p className="text-sm text-fg-soft break-words whitespace-normal max-w-[80ch]">
          That market cap implies{' '}
          {/* Uncoloured on purpose: whether 50% is absurd or reasonable is the reader's call, and
              a threshold picked out of the air was making it for them. */}
          <span className="font-mono text-2xl font-semibold text-fg-strong">
            {`${(growth * 100).toFixed(1)}%`}
          </span>{' '}
          annual FCF growth for {years} years.
        </p>
      ) : fcf != null && fcf <= 0 ? (
        // Not an error — a fact about the company. No growth rate makes a positive valuation work
        // off a non-positive cash flow.
        <p className="text-sm text-warn-300 break-words whitespace-normal max-w-[80ch]">
          Free cash flow of {mn(fcf)} is at or below zero, so there is no growth rate that makes any
          market cap work. Type one above to model a recovery.
        </p>
      ) : missing.length > 0 ? (
        <p className="text-sm text-warn-300 break-words whitespace-normal max-w-[80ch]">
          Not enough inputs to solve — no {missing.join(', ')} ingested for this company.
        </p>
      ) : !(rate > perpetuityGrowth) ? (
        // ⚠ The terminal value divides by (r − gp). Equal or inverted is not a big number, it is a
        // negative one — and without this the panel would just say "no solution".
        <p className="text-sm text-warn-300 break-words whitespace-normal max-w-[80ch]">
          The discount rate must exceed the perpetuity growth — otherwise the terminal value is
          infinite rather than large.
        </p>
      ) : (
        // With the bracket at the maths' own limits (−99% to 1000%/yr) this is now all but
        // unreachable — a price below one year of discounted cash flow, which no decline rate can
        // reach. No explanatory paragraph: there is no bound left to explain.
        <p className="text-sm text-warn-300 break-words whitespace-normal max-w-[80ch]">
          No growth rate reconciles that market cap with this cash flow.
        </p>
      )}

      {/* The sensitivity, which is the honest context for a single figure: the answer moves ~1.5pp
          per point of discount rate, so a reader who disagrees with the rate can read their own
          off the row instead of taking this one. Click a column to make it the headline. */}
      {fcf != null && fcf > 0 && target != null && (
        <div className="space-y-1">
          <div className="text-[11px] uppercase tracking-wide text-fg-muted">
            Implied growth by discount rate
          </div>
          <div className="overflow-x-auto rounded-lg border border-neutral-800/40 max-w-full">
            <table className="text-xs">
              <tbody>
                <tr className="border-b border-neutral-800/40 bg-page">
                  <th className="px-2 py-1 text-left font-medium text-fg-faint text-[10px] uppercase tracking-wide sticky left-0 bg-page">
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
                  {/* ⚠ CONTEXT, NOT PART OF THE SWEEP — a forecast, not a solve, so it is fenced
                      off by a border and labelled with its own horizon. Sitting it in the same row
                      unmarked would read as "the implied rate at some other discount rate". */}
                  {hasAnalysts && analysts.map(([label], i) => (
                    <td key={label}
                      className={`px-2 py-1 text-right font-mono whitespace-nowrap text-fg-muted ${
                        // One fence between the solve and the forecasts, one before the average —
                        // a border on every column would read as three unrelated things.
                        i === 0 || i === analysts.length - 1 ? 'border-l border-neutral-800/40' : ''}`}>
                      {label}
                    </td>
                  ))}
                </tr>
                <tr>
                  <th className="px-2 py-1 text-left font-medium text-fg-faint text-[10px] uppercase tracking-wide sticky left-0 bg-card">
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
                  {hasAnalysts && analysts.map(([label, value, tip], i) => (
                    <td key={label}
                      className={`px-2 py-1 text-right font-mono whitespace-nowrap ${
                        i === analysts.length - 1 ? 'text-fg-strong font-semibold' : 'text-fg-soft'
                      } ${i === 0 || i === analysts.length - 1 ? 'border-l border-neutral-800/40' : ''}`}
                      title={tip}>
                      {value == null ? '—' : `${value.toFixed(1)}%`}
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
