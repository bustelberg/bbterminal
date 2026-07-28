'use client';

import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { type PriceTarget } from './quickValuation';

/**
 * A price target from a demanded FCF yield, and the return it implies.
 *
 *     forecast price = forecast FCF/share ÷ forecast yield
 *     CAGR           = (forecast price / today's price) ^ (1/years) − 1
 *
 * Both forecasts start from what the chart above already knows — the trend's value two years out,
 * and the company's own average FCF yield over the decade — and both are editable, because the
 * whole point is to try your own.
 *
 * ⚠ IT IS FCF YIELD, NOT FCF-SBC YIELD. The series behind it is GuruFocus's `Free Cash Flow per
 * Share` as reported, with no stock-compensation deduction; the FCF-SBC cards elsewhere in the app
 * subtract SBC explicitly and are a different, lower number. Labelling this one "FCF-SBC" would
 * claim an adjustment that was never made.
 */

/**
 * ⚠ THE THREE HELPERS BELOW ARE AT MODULE SCOPE, NOT INSIDE THE COMPONENT. A component created
 * during render is a new type on every render, so React unmounts and remounts it — which drops the
 * caret out of the input after the first keystroke. The lint rule that catches this is protecting a
 * real bug.
 *
 * A dotted leader between label and value, the way a contents page or an invoice runs them — with
 * seven rows of ragged-right labels against right-aligned numbers, the eye otherwise has to guess
 * which value belongs to which line.
 *
 * The filler is a flex-1 span with a dotted bottom border, `aria-hidden` because it is decoration:
 * a screen reader announcing a row of dots between every label and figure is worse than silence.
 */
const fmtCagr = (v: number | null) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);

function Leader() {
  return (
    <span aria-hidden
      className="flex-1 min-w-[1rem] border-b border-dotted border-neutral-700/70 translate-y-[-0.2rem]" />
  );
}

function Row({ label, info, children }: {
  label: string; info?: React.ReactNode; children: React.ReactNode;
}) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="flex items-center gap-1 text-[11px] text-fg-muted shrink-0">
        {label}{info}
      </span>
      <Leader />
      <span className="font-mono text-xs text-fg-soft whitespace-nowrap shrink-0">{children}</span>
    </div>
  );
}

function Input({ value, onChange, suffix }: {
  value: string; onChange: (v: string) => void; suffix?: string;
}) {
  return (
    <span className="flex items-center gap-1 justify-end">
      <input type="number" value={value} onChange={(e) => onChange(e.target.value)}
        className="w-20 bg-page border border-neutral-700 rounded px-1.5 py-0.5 text-xs font-mono text-fg-strong text-right focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30" />
      {suffix && <span className="text-[10px] text-fg-muted">{suffix}</span>}
    </span>
  );
}

export default function PriceTargetCalculator({
  target, years, currency, className = '',
  fcfStr, onFcf, defaultForecastFcfPs,
  yieldStr, onYield, defaultForecastYield, onReset,
}: {
  /** ⚠ COMPUTED BY THE PARENT, because the chart draws the price line out to the same target. One
   *  computation, two readers — see the priceTarget() helper. */
  target: PriceTarget;
  years: number;
  currency?: string | null;
  /** Grid placement from the parent — the component owns its look, the layout owns its slot. */
  className?: string;
  /** null = never typed, so the box shows the default and keeps tracking it. */
  fcfStr: string | null;
  onFcf: (v: string) => void;
  defaultForecastFcfPs: number | null;
  yieldStr: string | null;
  onYield: (v: string) => void;
  defaultForecastYield: number | null;
  onReset: () => void;
}) {
  const dirty = fcfStr != null || yieldStr != null;
  const show = (s: string | null, def: number | null, dp: number) =>
    (s != null ? s : def == null ? '' : def.toFixed(dp));

  const n2 = (v: number | null) => (v == null ? '—' : v.toFixed(2));
  const n1 = (v: number | null) => (v == null ? '—' : v.toFixed(1));
  const ccy = currency ? currency + ' ' : '';

  return (
    <div className={`rounded-xl border border-neutral-800/40 bg-card p-4 space-y-1.5 min-w-0 ${className}`}>
      <div className="flex items-center gap-2 pb-1">
        <h4 className="text-base font-semibold text-fg-strong">Price target</h4>
        {dirty && (
          <button type="button" onClick={onReset}
            className="ml-auto text-[10px] text-accent-400 hover:underline">reset</button>
        )}
      </div>

      <Row label="Current FCF / share"
        info={<InfoTip content={<AspectCard
          what="The latest reported free cash flow per share."
          where="GuruFocus `Free Cash Flow per Share`, as filed — the same series the chart to the left plots."
          when="The most recent fiscal year, so up to a year old."
          how="⚠ Not SBC-adjusted. The FCF-SBC cards on the Long Equity tab subtract stock compensation and are a lower number; this one is free cash flow as reported." />} />}>
        {ccy}{n2(target.currentFcfPs)}
      </Row>
      <Row label="Forecast FCF / share"
        info={<InfoTip content={<AspectCard
          what="What the fitted trend says free cash flow per share will be."
          where="The dotted projection on the chart to the left, converted from the index back into currency."
          when={`${years} years past the last reported one.`}
          how="⚠ An extrapolation, not a forecast anyone made — it continues the exponential through the last decade. Type your own over it." />} />}>
        <Input value={show(fcfStr, defaultForecastFcfPs, 2)} onChange={onFcf} />
      </Row>
      <Row label="Current FCF yield"
        info={<InfoTip content={<AspectCard
          what="What the shares yield in free cash flow at today's price."
          where="Current FCF per share ÷ current share price — the two rows above and below."
          when="The latest fiscal year."
          how="The starting point the forecast yield is judged against: the gap between the two IS the rerating this calculator assumes." />} />}>
        {n1(target.currentYield)}%
      </Row>
      <Row label="Forecast FCF yield"
        info={<InfoTip content={<AspectCard
          what="The yield you expect the market to price the shares at."
          where="Defaults to this company's OWN average FCF yield over the charted decade — the dashed line on the yield chart."
          when="At the end of the forecast window."
          how="⚠ THE ASSUMPTION THAT DRIVES EVERYTHING BELOW. The forecast price is simply the forecast cash flow divided by this, so a percentage point here moves the target more than any other input." />} />}>
        <Input value={show(yieldStr, defaultForecastYield, 1)} onChange={onYield} suffix="%" />
      </Row>
      <Row label="Current share price"
        info={<InfoTip content={<AspectCard
          what="The price the return is measured from."
          where="GuruFocus `Month End Stock Price` — the close at the last fiscal year end."
          when="⚠ NOT TODAY'S QUOTE. It can be up to a year old, and the CAGR below is measured from it."
          how="Both series come from the same fiscal rows, which is what makes the yield above internally consistent — at the cost of being stale against a live price." />} />}>
        {ccy}{n2(target.currentPrice)}
      </Row>
      <Row label="Forecast share price"
        info={<InfoTip content={<AspectCard
          what="What the shares are worth if the forecast cash flow is priced at the forecast yield."
          where="Forecast FCF per share ÷ forecast FCF yield."
          when={`${years} years out.`}
          how="Blank when either input is non-positive: a zero yield divides to infinity, and a cash-burning forecast has no price at a positive one." />} />}>
        {ccy}{n2(target.forecastPrice)}
      </Row>

      <div className="flex items-baseline gap-2 pt-1.5 border-t border-neutral-800/40">
        <span className="flex items-center gap-1 text-[11px] text-fg-muted shrink-0">
          Est. {years}-year CAGR
          <InfoTip content={<AspectCard
            what="The annualised return from today's price to the forecast one."
            where="(forecast price ÷ current price) ^ (1/years) − 1."
            when={`Over ${years} years.`}
            how="⚠ Price only — no dividends, and no return on the cash the business throws off in the meantime. It answers what the multiple and the cash flow do to the share price, not what you would earn holding it." />} />
        </span>
        <Leader />
        {/* The one figure here that is a conclusion rather than an input, so it carries the sign's
            colour — a target below today's price is the finding, not a formatting accident. */}
        <span className={`font-mono text-base font-semibold shrink-0 ${
          target.cagr == null ? 'text-fg-muted' : target.cagr >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
          {fmtCagr(target.cagr)}
        </span>
      </div>
    </div>
  );
}
