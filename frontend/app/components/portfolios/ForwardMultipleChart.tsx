'use client';

import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
import { paddedDomain } from './marginData';
import { medianOf, multipleOf, type BASIS, type YearPoint } from './quickValuation';

/**
 * THE FORWARD MULTIPLE — `P/E` on the EPS basis, `P/FCF` on the FCF one, switched by the same
 * control as the two charts above it.
 *
 * The question the yield chart cannot answer: the yield chart is historical on both sides, so it
 * says what the shares HAVE yielded and nothing about what you are paying now for what the company
 * is expected to earn. This chart carries the multiple forward.
 *
 * ⚠ IT IS THE RECIPROCAL OF THE YIELD CHART, DELIBERATELY. The historical line here is exactly
 * `100 / yield`, and duplicating it is the point: a multiple and a yield are one fact, people
 * reason about valuation in multiples, and a forward point has to land somewhere a reader can
 * compare it to — "18×" alone means nothing.
 *
 * ⚠ TWO PRICES ON ONE LINE, AND THE HANDOVER IS WHERE THE MEANING CHANGES:
 *
 *     history   each fiscal YEAR-END price ÷ that year's own figure   (what it traded at)
 *     forward   TODAY'S price ÷ each forecast year's figure           (what you would pay now)
 *
 * Mixing them silently would be indefensible; kept apart and labelled, the join IS the reading —
 * today's multiple against the decade the company traded through. The two series are separate,
 * the forward one is dotted like every other projection on this tab, and they meet at "today".
 *
 * ⚠ NOTHING ON THIS CHART IS PREDICTED BY US. The forward half is the published analyst consensus
 * or it is absent — on EPS it exists, on FCF it does not (nobody forecasts capex), and where it
 * does not the chart shows the measured multiple and today's, and says why there is no more. The
 * first version filled that gap with the tab's own fitted trend: it drew at the same weight, with
 * the same decimals, on the same axis as a consensus would, and no badge makes a house
 * extrapolation read differently from what the market actually expects. A multiple is a fact about
 * a price and a filing; if we do not have the filing, there is no multiple to draw.
 */

/** A multiple this far above the median is a near-zero-earnings year, not a valuation — see the
 *  `medianOf` note. Kept on the chart (it is real) but the axis is not allowed to be ruined by it,
 *  so `paddedDomain` runs over the CLIPPED set and the outlier simply overflows. */
const OUTLIER_MULT = 4;

export default function ForwardMultipleChart({
  points, basis: b, currentPrice, priceLive, nowX, forward, currency, yearTicks,
  height = 320, className = '',
}: {
  /** The fiscal history — handed down, never re-derived, so this chart cannot disagree with the
   *  two above it about what the company earned. */
  points: YearPoint[];
  basis: (typeof BASIS)[keyof typeof BASIS];
  /** Today's price (or the fiscal fallback), already in the reporting currency. */
  currentPrice: number | null;
  priceLive: boolean;
  /** Where "today" sits on the fiscal-year axis. Null when there is no priced year to anchor to. */
  nowX: number | null;
  /** The forecast per-share figures, oldest first — consensus or trend, per basis. */
  forward: { year: number; value: number }[];
  currency?: string | null;
  /** The integer fiscal years the sibling chart ticks at, so the two line up column for column. */
  yearTicks: number[];
  /** Shared with the sibling charts — three plot areas of different heights in one 2×2 grid read
   *  as three unrelated panels. */
  height?: number;
  className?: string;
}) {
  // ⚠ NO `useMemo` ANYWHERE IN THIS COMPONENT. The React Compiler could not preserve a manual memo
  // over `data` below and responded by skipping optimisation of the WHOLE component — worse than
  // the memo was worth. Left plain, the compiler memoizes all of it itself. (Same finding, same
  // resolution, as `chartData` in QuickValuationTab.)
  const hist = points.map((p) => ({ year: p.year, mult: multipleOf(p.price, p.value) }));
  const histValues = hist.map((h) => h.mult).filter((v): v is number => v != null);
  const median = medianOf(histValues);
  /** Years the multiple simply does not exist for — a loss or a cash burn. NOT a data gap. */
  const noMultiple = points.filter((p) => p.value != null && p.value <= 0).length;

  const latestPs = [...points].reverse().find((p) => p.value != null)?.value ?? null;
  const trailing = multipleOf(currentPrice, latestPs);
  const forwardMults = forward.map((f) => ({ year: f.year, mult: multipleOf(currentPrice, f.value) }));
  const nextForward = forwardMults.find((f) => f.mult != null)?.mult ?? null;

  /**
   * One row per x, with the two series in their own keys.
   *
   * The forward line starts at TODAY'S trailing multiple so it visibly departs from the history
   * rather than floating unattached — that first point is the same price over the last REPORTED
   * figure, i.e. the pivot the forecast swings from.
   */
  const data = (() => {
    const rows = new Map<number, { year: number; hist: number | null; fwd: number | null }>();
    const put = (year: number, key: 'hist' | 'fwd', v: number | null) => {
      const row = rows.get(year) ?? { year, hist: null, fwd: null };
      row[key] = v;
      rows.set(year, row);
    };
    for (const h of hist) put(h.year, 'hist', h.mult);
    if (nowX != null && trailing != null) put(nowX, 'fwd', trailing);
    for (const f of forwardMults) put(f.year, 'fwd', f.mult);
    // Sorted, because a numeric axis draws a line in DATA order, not x order.
    return [...rows.values()].sort((a, b) => a.year - b.year);
  })();

  // The axis is scaled over the multiples that describe a valuation, not the ones that describe a
  // collapsing denominator — the outlier still plots, it just overflows rather than flattening
  // every other point into a line.
  const scaleSet = median == null ? [...histValues]
    : [...histValues, ...forwardMults.map((f) => f.mult)]
      .filter((v): v is number => v != null && v <= median * OUTLIER_MULT);
  const clipped = histValues.filter((v) => median != null && v > median * OUTLIER_MULT).length;

  const x = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}×`);
  const ccy = currency ? `${currency} ` : '';
  /** ⚠ PER COMPANY, NOT PER BASIS. FCF never has one; EPS has one only where analysts cover the
   *  name, and an uncovered company on the EPS basis lands here too. */
  const hasForward = forwardMults.some((f) => f.mult != null);

  return (
    <div className={`rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0 ${className}`}>
      <div className="flex items-baseline gap-2 flex-wrap">
        {/* The word "Forward" is earned, not decorative: without a consensus there is no forward
            half, and titling the panel for one would promise a line that is not on it. */}
        <h4 className="text-base font-semibold text-fg-strong">
          {hasForward ? `Forward ${b.multiple}` : b.multiple}
        </h4>
        <span className="text-[11px] text-fg-faint">
          each year-end price ÷ that year&apos;s own {b.perShare} · median dashed
          {hasForward ? ` · forward = today's price ÷ consensus ${b.perShare}` : ''}
        </span>
        {/* Every point on this chart is measured, and the badge says which kind. An absent
            forecast is an ANSWER — nobody forecasts capex — not a gap we should be filling. */}
        <span className="text-[11px] text-fg-muted" title={b.forwardSource}>
          {hasForward ? 'forward = analyst consensus'
            : b.estimateCodes ? 'no analyst estimates for this company'
              : 'no forward — nobody forecasts capex'}
        </span>
        {noMultiple > 0 && (
          <span className="text-[11px] text-warn-300"
            title={`A ${b.negativeYear} year has no multiple: price ÷ a negative figure is negative, and a −20× sorts below every cheap year as though it were the bargain of the decade. Those years are dropped rather than drawn.`}>
            ⚠ {noMultiple} {b.negativeYear} year{noMultiple > 1 ? 's' : ''} have no multiple
          </span>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        {/* ⚠ RENDERED EVEN WHEN THERE IS NO FORWARD, as `n/a`. Dropping the tile would make the
            absence invisible and the FCF panel look like a chart that simply has fewer stats; the
            reader has to be able to see that a forward multiple was asked for and does not exist. */}
        <Stat label={`Forward ${b.multiple}`} value={hasForward ? x(nextForward) : 'n/a'}
          color={chartTheme.accentStrong}
          info={<InfoTip content={<AspectCard
            what={hasForward
              ? `What you pay today for the next forecast year's ${b.perShare}.`
              : `There is none — and that is an answer, not a missing number.`}
            where={b.forwardSource}
            when={hasForward
              ? `FY${forwardMults.find((f) => f.mult != null)?.year ?? '—'}, on the ${priceLive ? 'live' : 'fiscal year-end'} price.`
              : 'Nothing on this chart is projected; every point is a filed figure at a real price.'}
            how={hasForward
              ? `The headline number: ${b.multiple} below the median is the market paying less for this business than it usually has — provided the consensus holds, which is the whole risk.`
              : `Switch to EPS for a forward multiple: analysts publish an earnings consensus, and none of them publishes a free-cash-flow one.`} />} />} />
        <Stat label={`Trailing ${b.multiple}`} value={x(trailing)} color={chartTheme.accent}
          info={<InfoTip content={<AspectCard
            what={`Today's price over the LAST REPORTED ${b.perShare}.`}
            where={`${b.source} Priced at ${priceLive ? "today's close" : 'the last fiscal year-end close'}.`}
            when="The most recent fiscal year, so the denominator can be up to a year old."
            how="The pivot the forward line swings from — the gap between this and the forward multiple is exactly the growth the forecast assumes." />} />} />
        <Stat label="Median" value={x(median)} color={chartTheme.warn}
          info={<InfoTip content={<AspectCard
            what={`The middle ${b.multiple} of the ${histValues.length} year${histValues.length === 1 ? '' : 's'} plotted — the dashed line.`}
            where="Each fiscal year's own year-end price ÷ that year's own figure. Historical on both sides, unlike the two stats beside it."
            when={`${histValues.length} of the ${points.length} fiscal years on the chart${noMultiple ? `; ${noMultiple} ${b.negativeYear} year(s) have no multiple` : ''}.`}
            how="⚠ MEDIAN, NOT MEAN. One year of near-zero earnings prints a 300× that no reader would call typical and drags a mean by tens of turns; the median ignores it." />} />} />
      </div>

      <div>
        {histValues.length === 0 && forwardMults.every((f) => f.mult == null) ? (
          <p className="text-[11px] text-fg-faint py-16 text-center">
            No positive {b.perShare} year and no usable forecast, so there is no multiple to plot.
          </p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={height}>
              <ComposedChart data={data} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                {/* Numeric and ticked on the same integer years as the chart above — today sits at
                    its true fractional position, which is where the two series hand over. */}
                <XAxis dataKey="year" type="number" domain={['dataMin', 'dataMax']}
                  ticks={yearTicks} allowDecimals={false} interval="preserveStartEnd"
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                <YAxis domain={paddedDomain(scaleSet)} allowDataOverflow
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} width={52}
                  tickFormatter={(v: number) => `${v.toFixed(0)}×`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
                  labelStyle={{ color: chartTheme.axisLabel }}
                  labelFormatter={(v) => (typeof v === 'number' && !Number.isInteger(v)
                    ? 'Today' : `FY${v}`)}
                  formatter={(v, n) => [typeof v === 'number' ? `${v.toFixed(1)}×` : '—',
                    n === 'hist' ? `${b.multiple} at year end` : `${b.multiple} on today's price`]} />
                {/* ⚠ NO PROJECTION PAST TODAY WITHOUT A CONSENSUS. `nowX` is the newest x on the
                    chart in that case, so the plot simply stops where the data does. */}
                {median != null && (
                  <ReferenceLine y={median} stroke={chartTheme.warn} strokeDasharray="5 3"
                    strokeOpacity={0.6} />
                )}
                <Line dataKey="hist" name="hist" type="monotone" stroke={chartTheme.accent}
                  strokeWidth={2} dot={{ r: 2.5 }} connectNulls={false} />
                {/* ⚠ `connectNulls={false}` ON THE HISTORY. A loss year is a HOLE in this series,
                    and joining across it would draw a smooth multiple through a year that had
                    none — the one place the reader most needs to see a break. The forward line
                    does connect, because its gap is only the stretch between today and the first
                    forecast year, where nothing is being hidden. */}
                <Line dataKey="fwd" name="fwd" type="linear" stroke={chartTheme.accentStrong}
                  strokeWidth={1.5} strokeDasharray="2 4" strokeOpacity={0.75}
                  dot={{ r: 2.5, strokeWidth: 0, fill: chartTheme.accentStrong }} connectNulls />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />
                {b.multiple} at each year end
              </span>
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 inline-block rounded"
                  style={{ background: chartTheme.accentStrong, opacity: 0.75 }} />
                on today&apos;s price{ccy ? ` (${ccy.trim()})` : ''}
                {hasForward ? ', dotted = consensus years' : ' — trailing, the single point'}
              </span>
              {clipped > 0 && (
                <span className="text-warn-300"
                  title={`Above ${OUTLIER_MULT}× the median. The point is still drawn; it is only excluded from the axis range, so one collapsed-earnings year cannot flatten the other nine into a straight line.`}>
                  ⚠ {clipped} year{clipped > 1 ? 's' : ''} off the top of the axis
                </span>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
