'use client';

import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
import { paddedDomain } from './marginData';
import { medianOf, type BASIS } from './quickValuation';
import { align, type Point } from './multiplesSeries';

/**
 * THE MULTIPLE THROUGH TIME — a decade of it, at the resolution the price moves.
 *
 * The fiscal-year chart answers "what did it trade at each year end"; twelve dots cannot show a
 * de-rating that happened over four months. This is the same question at weekly resolution, and
 * it is the only place the FORWARD multiple has real history.
 *
 * TWO LINES ON THE EPS BASIS, ONE ON FCF, AND THE DIFFERENCE IS NOT COSMETIC:
 *
 *   Forward P/E   GuruFocus's own published indicator — back to 2015, weekly. Not computed here.
 *   Trailing      price ÷ the figure last REPORTED at that date.
 *
 * ⚠ THERE IS NO FORWARD P/FCF, HERE OR ANYWHERE. Nobody forecasts capex, so no vendor publishes a
 * free-cash-flow consensus — so the FCF basis shows the trailing line alone and says why. Drawing
 * a modelled one would be the extrapolation that was deliberately removed from the sibling chart.
 *
 * ⚠ AND THE TRAILING P/FCF IS GENUINELY JAGGED — that is the data, not the chart. Measured on
 * ASML: 21.8x (2022) -> 116.4x (2024) -> 28.9x (2025), because FCF per share went 24.14 -> 8.24 ->
 * 23.08 on capex and working-capital swings. Smoothing it would hide the very volatility that
 * explains why the forward line cannot exist.
 */

/** A multiple this far above the median is a collapsed denominator, not a valuation. Still drawn;
 *  just not allowed to flatten the other decade of points into a line. */
const OUTLIER_MULT = 5;

/**
 * ⚠ THE TWO SERIES ARE BLUE AND AMBER, AND THAT PAIR WAS MEASURED, NOT CHOSEN.
 *
 * They shipped as `accent` and `accentStrong` — two steps of the SAME blue — which the palette
 * validator fails outright:
 *
 *     #3b82c9 ↔ #2c6bb0   ΔE 7.3 normal   (floor 15)   FAIL
 *     #3b82c9 ↔ #c0891a   ΔE 27.2 normal, 24.3 protan, 22.4 tritan   PASS
 *
 * Note the failing pair is below the NORMAL-vision floor: this was not merely a colourblind
 * problem, full-colour readers could not separate them either — which is exactly how it was
 * reported. Blue+violet (`compare`) fails too (ΔE 4.0 deutan), the same finding CLAUDE.md already
 * records for the app's default A/B pair. Re-run before changing these:
 *   node scripts/validate_palette.js "#3b82c9,#c0891a" --mode light
 *
 * ⚠ TRAILING KEEPS THE PRIMARY BLUE WHETHER OR NOT A FORWARD LINE EXISTS. Colour follows the
 * entity, never its rank — the FCF basis has no forward line at all, and a lone amber line there
 * would mean the same series changed colour because a different one disappeared.
 */
const TRAILING_COLOR = chartTheme.accent;      // series A — always present
const FORWARD_COLOR = chartTheme.warn;         // series B — EPS basis only
/** The median is a REFERENCE, not a third series: recessive grey, never a categorical hue. */
const MEDIAN_COLOR = chartTheme.axisTick;

export default function MultipleHistoryChart({
  basis: b, forward, trailing, currency, fromYear, height = 320, className = '',
}: {
  basis: (typeof BASIS)[keyof typeof BASIS];
  /** The vendor's published forward multiple. Empty on the FCF basis, by nature. */
  forward: Point[];
  /** price ÷ last reported figure. */
  trailing: Point[];
  currency?: string | null;
  fromYear: number;
  height?: number;
  className?: string;
}) {
  const hasForward = forward.length > 0;
  const tVals = trailing.map((p) => p.value);
  const median = medianOf(tVals);
  const latestFwd = forward.at(-1)?.value ?? null;
  const latestTrail = trailing.at(-1)?.value ?? null;

  /**
   * ⚠ ALIGNED, NOT MERGED BY TIMESTAMP. The vendor's forward indicator and our trailing series are
   * sampled independently and share almost no dates, so a naive merge yields rows holding one
   * value and a null for the other, alternating — and `connectNulls={false}` then joins nothing,
   * drawing both series as isolated dots. `align` carries each series across the other's
   * timestamps while still leaving a genuine gap null, so a real hole still breaks the line.
   */
  const data = align({ trail: trailing, fwd: forward });

  // Scale over the multiples that describe a valuation; an outlier still plots and overflows.
  const scaleSet = median == null ? tVals
    : [...tVals, ...forward.map((p) => p.value)].filter((v) => v <= median * OUTLIER_MULT);
  const clipped = tVals.filter((v) => median != null && v > median * OUTLIER_MULT).length;

  const years: number[] = [];
  if (data.length) {
    const y0 = new Date(Number(data[0].t)).getUTCFullYear();
    const y1 = new Date(Number(data[data.length - 1].t)).getUTCFullYear();
    for (let y = y0; y <= y1; y++) years.push(Date.UTC(y, 0, 1));
  }
  const x = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}×`);

  return (
    <div className={`rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0 ${className}`}>
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">
          {hasForward ? `${b.multiple} — forward & trailing` : `${b.multiple} — trailing`}
        </h4>
        <span className="text-[11px] text-fg-faint">since {fromYear} · median dashed</span>
        <span className="text-[11px] text-fg-muted"
          title={hasForward
            ? "The forward line is GuruFocus's own published forward-P/E indicator, not our arithmetic. Dividing the close by it recovers the CURRENT fiscal year's consensus EPS — so early in a year it looks ~12 months ahead, and by December it prices earnings nearly banked."
            : 'No analyst publishes a free-cash-flow forecast — capex is not forecast — so there is no forward multiple to draw on this basis, at any date.'}>
          {hasForward ? 'forward = vendor indicator' : 'no forward — nobody forecasts capex'}
        </span>
      </div>

      <div className="flex flex-wrap gap-2">
        {hasForward && (
          <Stat label={`Forward ${b.multiple}`} value={x(latestFwd)} color={FORWARD_COLOR}
            info={<InfoTip content={<AspectCard
              what="What the market pays today for the fiscal year now in progress."
              where="GuruFocus `forward_pe_ratio`, published as a time series — read, not computed."
              when={`Weekly since ${fromYear}; latest point ${forward.at(-1) ? new Date(forward.at(-1)!.t).toISOString().slice(0, 10) : '—'}.`}
              how="⚠ Its denominator is the CURRENT fiscal year's consensus, not a rolling twelve months — verified by backing the EPS out of price ÷ this ratio (argenx: 23.20 implied vs 23.23 published, against 27.93 for an NTM blend)." />} />} />
        )}
        <Stat label={`Trailing ${b.multiple}`} value={x(latestTrail)} color={TRAILING_COLOR}
          info={<InfoTip content={<AspectCard
            what={`Price over the ${b.perShare} last REPORTED at each date.`}
            where={`${b.source} Priced off GuruFocus daily closes.`}
            when="Every fiscal figure is held back until it was plausibly public — see the reporting-lag note."
            how={`⚠ NO LOOK-AHEAD. A fiscal row is stamped with the period END, but the accounts are published weeks later; using them on the year-end date makes the whole series look cheaper and cleverer than anything anyone could have traded.${
              b.multiple === 'P/FCF' ? ' ⚠ Expect it to be jagged — ASML went 21.8x → 116.4x → 28.9x in three years on real capex swings.' : ''}`} />} />} />
        <Stat label="Median" value={x(median)} color={MEDIAN_COLOR}
          info={<InfoTip content={<AspectCard
            what={`The middle trailing ${b.multiple} over the window.`}
            where="The trailing line above, not the forward one — a median of two different measures would be neither."
            when={`${tVals.length} weekly observations since ${fromYear}.`}
            how="⚠ MEDIAN, NOT MEAN. One collapsed-denominator year prints a 300× that no reader would call typical and drags a mean by tens of turns." />} />} />
      </div>

      <div>
        {data.length < 2 ? (
          <p className="text-[11px] text-fg-faint py-16 text-center">
            No priced history on this basis — the multiple needs both a close and a reported
            {' '}{b.perShare} at the same date.
          </p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={height}>
              <ComposedChart data={data} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                {/* Time, not fiscal years: the whole point of this chart is what happened BETWEEN
                    the reporting dates. Ticked at 1 January so the labels stay years. */}
                <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
                  ticks={years} interval="preserveStartEnd"
                  tickFormatter={(t: number) => String(new Date(t).getUTCFullYear())}
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                <YAxis domain={paddedDomain(scaleSet)} allowDataOverflow width={52}
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }}
                  tickFormatter={(v: number) => `${v.toFixed(0)}×`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
                  labelStyle={{ color: chartTheme.axisLabel }}
                  labelFormatter={(t) => new Date(Number(t)).toISOString().slice(0, 10)}
                  formatter={(v, n) => [typeof v === 'number' ? `${v.toFixed(1)}×` : '—',
                    n === 'fwd' ? `Forward ${b.multiple}` : `Trailing ${b.multiple}`]} />
                {median != null && (
                  <ReferenceLine y={median} stroke={MEDIAN_COLOR} strokeDasharray="5 3"
                    strokeOpacity={0.55} />
                )}
                {/* ⚠ `connectNulls={false}` ON BOTH. A stretch with no reported figure, or a loss
                    year with no multiple, is a HOLE — joining across it draws a smooth valuation
                    through a period that had none. */}
                <Line dataKey="trail" name="trail" type="monotone" stroke={TRAILING_COLOR}
                  strokeWidth={2} dot={false} connectNulls={false} />
                {hasForward && (
                  <Line dataKey="fwd" name="fwd" type="monotone" stroke={FORWARD_COLOR}
                    strokeWidth={2} dot={false} connectNulls={false} />
                )}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 inline-block rounded" style={{ background: TRAILING_COLOR }} />
                Trailing {b.multiple}{currency ? ` (${currency})` : ''}
              </span>
              {hasForward && (
                <span className="flex items-center gap-1.5">
                  <span className="w-3 h-0.5 inline-block rounded" style={{ background: FORWARD_COLOR }} />
                  Forward {b.multiple} — GuruFocus
                </span>
              )}
              {clipped > 0 && (
                <span className="text-warn-300"
                  title={`Above ${OUTLIER_MULT}x the median. Still drawn, only excluded from the axis range — one collapsed-FCF year would otherwise flatten the whole decade.`}>
                  ⚠ {clipped} point{clipped > 1 ? 's' : ''} off the top of the axis
                </span>
              )}
              <span className="text-fg-muted"
                title="Every fiscal figure is held back until it was plausibly published, so no point on the trailing line uses a number the market did not yet have. Without it the series looks cheaper and cleverer than anything anyone could have traded.">
                reporting lag applied
              </span>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
