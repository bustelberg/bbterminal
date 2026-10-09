'use client';

import { useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { tiltedAxis } from '../../../lib/chartAxis';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
import { paddedDomain } from './marginData';
import { medianOf, type BASIS, type Basis } from './quickValuation';
import { type Point } from './multiplesSeries';
import MultipleHistoryModal from './MultipleHistoryModal';
import { useQuickValuationCopy } from './quickValuationCopy';
import { workedRatio } from './workedFormula';

/**
 * The multiple through time — a decade of it, at the resolution the price moves.
 *
 * The fiscal-year chart answers "what did it trade at each year end"; twelve dots cannot show a
 * de-rating that happened over four months. This is the same question at weekly resolution, and
 * it is the only place the FORWARD multiple has real history.
 *
 *  Forward only since 2026-08-21, ON REQUEST. The trailing line — price ÷ the figure last
 * REPORTED at that date, on both bases — was removed. What is left is one line: GuruFocus's own
 * published forward-P/E indicator, back to 2015, weekly, read straight through and computed from
 * nothing here.
 *
 *  So the FCF basis draws nothing, and says so rather than looking broken. Nobody forecasts
 * capex, so no vendor publishes a free-cash-flow consensus and there is no forward P/FCF to read —
 * anywhere, at any date. A forward FCF line is planned; until it exists this panel is honest about
 * being empty instead of falling back to the measure that was just removed.
 *
 *  The median moved with the line. It used to be the median of the TRAILING series, and the tile
 * said so explicitly ("a median of two different measures would be neither"). With one series left
 * it is that series' median — the same reasoning, applied to what is now on screen.
 *
 *  What the removed line was for, recorded so the decision can be revisited rather than
 * rediscovered: it was the only measure available on the FCF basis, and it carried its own inputs
 * (close, per-share) into the drill-down, which the vendor indicator cannot — a published number
 * has nothing to decompose. It was also genuinely jagged on FCF (ASML 21.8x -> 116.4x -> 28.9x in
 * three years, on real capex swings), which is what made a forward line desirable there.
 */

/**
 *  The two series are blue and amber, and that pair was measured, not chosen.
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
 *  Trailing keeps the primary blue whether or not a forward line exists. Colour follows the
 * entity, never its rank — the FCF basis has no forward line at all, and a lone amber line there
 * would mean the same series changed colour because a different one disappeared.
 */
/**  THE FORWARD LINE KEEPS AMBER, NOT THE PRIMARY BLUE IT COULD NOW CLAIM. Colour follows the
 *  ENTITY, never its rank — the same rule the two-line version stated in the other direction. A
 *  reader who knows this chart knows the forward line as amber; promoting it to blue because the
 *  blue series left would mean the same series changed colour because a different one disappeared. */
const OCF_COLOR = chartTheme.accent;
const EPS_COLOR = chartTheme.warn;
/** The median is a REFERENCE, not a third series: recessive grey, never a categorical hue. */
const MEDIAN_COLOR = chartTheme.axisTick;

export default function MultipleHistoryChart({
  basis: b, basisKey, forward, comparisonForward, currency, fromYear, name, isin, height = 320, className = '',
  estimateRetrievedAt, priceSource,
  onRefresh, onCancel, canRefresh = false, refreshing = false, cancelling = false,
}: {
  basis: (typeof BASIS)[keyof typeof BASIS];
  /** Which basis is switched on, as a KEY — the translated labels are looked up by it. */
  basisKey: Basis;
  /** The vendor's published forward multiple — the only series here. Empty on the FCF basis, by
   *  nature: no vendor publishes a free-cash-flow consensus. */
  forward: Point[];
  /** The other earnings-power measure, plotted on the same multiple axis for comparison. */
  comparisonForward: Point[];
  currency?: string | null;
  fromYear: number;
  /** When the GuruFocus estimate/estimate-history payload was most recently loaded. */
  estimateRetrievedAt?: string | null;
  priceSource?: {
    symbol?: string | null; nativeCurrency: string; currency: string;
    from: string | null; through: string | null;
  } | null;
  name?: string | null;
  isin: string;
  height?: number;
  className?: string;
  /**
   * Go and get the vendor's series again.
   *
   *  This series is the one thing on the tab that goes stale without saying so. It is read from
   * GuruFocus, not computed here, and the vendor publishes it with a multi-week lag — measured on
   * argenx, the newest observation was 24 July while the file was read on 17 August. Everything
   * else on this card is derived from it, so a stale line silently ages the median, the tile and
   * the drill-down together and nothing looks wrong.
   *
   *  The card does not fetch — the tab owns the metrics and the chart is a function of them, so a
   * fetch here would be a second loader for one payload. It gets a callback and three flags.
   */
  onRefresh?: () => void;
  onCancel?: () => void;
  /** False when no GuruFocus company backs this ISIN — the button is then disabled, not absent. */
  canRefresh?: boolean;
  refreshing?: boolean;
  cancelling?: boolean;
}) {
  //  Translated labels for what is drawn, English `b` for the ⓘ prose — the same split as the
  // tab that owns this card. See `quickValuationCopy`.
  //  The key comes in as a prop, not from `b.tab`. Deriving it as `b.tab === 'EPS' ? …` would key
  // a lookup off a LABEL, which is the pattern this folder keeps paying for — and it would be
  // silently wrong the day that label is translated or renamed.
  const t = useQuickValuationCopy();
  const bl = t.basis[basisKey];
  const comparisonBasisKey: Basis = basisKey === 'ocf' ? 'eps' : 'ocf';
  const comparisonBl = t.basis[comparisonBasisKey];
  const primaryColor = basisKey === 'ocf' ? OCF_COLOR : EPS_COLOR;
  const comparisonColor = comparisonBasisKey === 'ocf' ? OCF_COLOR : EPS_COLOR;
  // Click-to-inspect, the same affordance the two charts beside it carry.
  const [showData, setShowData] = useState(false);
  const hasForward = forward.length > 0;
  // Both lines are now calculated daily from price ÷ next-fiscal-year consensus. The former
  // weekly GuruFocus forward-P/E indicator is not used here because it cannot overlay P/OCF.
  const vendorSeries = false;
  const consensusLabel = basisKey === 'eps' ? 'EPS' : 'OCF/share';
  const fVals = forward.map((p) => p.value);
  const median = medianOf(fVals);
  const latestFwd = forward.at(-1)?.value ?? null;
  const latestPoint = forward.at(-1);
  const estimateRetrievedDate = estimateRetrievedAt
    ? new Date(estimateRetrievedAt).toISOString().slice(0, 10) : null;
  const estimateTarget = latestPoint?.forecastTargetDate ?? null;
  /**
   * The date of the newest observation, for the As-of tile.
   *
   *  The vendor's date, not ours. It is when GuruFocus last published a point, which is the only
   * date that answers "is this current" — the moment we happened to read it says nothing about
   * whether there was anything newer to read.
   *
   *  Iso here and `onDate` IN THE TOAST, DELIBERATELY. A `Stat` value is 18px mono inside 8rem, so
   * `2026-07-24` fits at ten characters and `24 July 2026` truncates to a date that reads as a
   * different one. The refresh's toast is a sentence with room, and gets the human form.
   */
  const asOf = forward.at(-1)
    ? new Date(forward.at(-1)!.t).toISOString().slice(0, 10) : null;
  const priceDate = asOf;
  const yahooWhere = priceSource
    ? `yfinance asset_price daily closes${priceSource.symbol ? ` (${priceSource.symbol})` : ''}, ${priceSource.nativeCurrency} listing currency converted on each date to ${priceSource.currency}.`
    : 'yfinance asset_price daily closes are unavailable for this ISIN.';
  const yahooWhen = priceSource
    ? `Yahoo close history spans ${priceSource.from ?? '—'} to ${priceSource.through ?? '—'}; this price applies on ${priceDate ?? '—'}.`
    : 'No yfinance daily close series was returned.';

  /**
   *  NO `align` ANY MORE, AND THAT IS THE ONE SIMPLIFICATION THE REMOVAL ACTUALLY BUYS. It
   * existed because the vendor's forward indicator and our trailing series were sampled
   * independently and shared almost no dates — a naive merge gave rows holding one value and a
   * null for the other, alternating, which `connectNulls={false}` then drew as isolated dots. One
   * series has nothing to be aligned against.
   */
  const dataByTime = new Map<number, {
    t: number; fwd: number | null; comparison: number | null;
    price?: number; estimate?: number; forecastTargetDate?: string;
  }>();
  for (const point of forward) {
    dataByTime.set(point.t, {
      t: point.t, fwd: point.value, comparison: dataByTime.get(point.t)?.comparison ?? null,
      price: point.price, estimate: point.estimate, forecastTargetDate: point.forecastTargetDate,
    });
  }
  for (const point of comparisonForward) {
    const previous = dataByTime.get(point.t);
    dataByTime.set(point.t, previous
      ? { ...previous, comparison: point.value }
      : { t: point.t, fwd: null, comparison: point.value });
  }
  const data = [...dataByTime.values()].sort((a, b) => a.t - b.t);

  // The domain must cover every plotted multiple. A high P/OCF is information about the annual
  // denominator, not a reason to draw a point beyond the chart boundary.
  const scaleSet = [...fVals, ...comparisonForward.map((point) => point.value)];

  const years: number[] = [];
  if (data.length) {
    const start = Number(data[0].t);
    const end = Number(data[data.length - 1].t);
    const y0 = new Date(start).getUTCFullYear();
    const y1 = new Date(end).getUTCFullYear();
    // Keep the chart clipped to its actual observations. If its first 1 January falls before the
    // first price (as in NVIDIA's February-starting FY2026 window), tick the first observation as
    // that year instead of extending the plot with an empty January margin.
    years.push(start);
    for (let y = y0 + 1; y <= y1; y++) {
      const jan1 = Date.UTC(y, 0, 1);
      if (jan1 <= end) years.push(jan1);
    }
  }
  const x = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}×`);

  return (
    <div className={`rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0 ${className}`}>
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-base font-semibold text-fg-strong">{t.forwardMultipleComparison}</h4>
        <span className="text-xs text-fg-faint">{t.sinceMedian(String(fromYear))}</span>
        {/*  ONLY ON THE BASIS THAT HAS A VENDOR LINE. The FCF basis used to print its own chip
            here explaining why there is no forward series; the empty state below already says it,
            and saying it twice in one card — once in the header, once across the middle of it —
            was the redundancy, not the sentence. Removed on request. */}
        {hasForward && vendorSeries && (
          <span className="text-xs text-fg-muted"
            title="GuruFocus's own published forward-P/E indicator, not our arithmetic. Dividing the close by it recovers the CURRENT fiscal year's consensus EPS — so early in a year it looks ~12 months ahead, and by December it prices earnings nearly banked.">
            {t.vendorIndicator}
          </span>
        )}
        {/*  ONE CONTROL, THREE STATES, AND IT TURNS INTO THE CANCEL — the same shape and the same
            three WORDS as the share-price Refresh on the Deep Valuation tab. The reader pressed it HERE,
            so this is where stopping it belongs; sending them to the toast in the corner to undo
            something they started on this card is a Cancel that does nothing.
              Refresh  idle     Cancel  running, press to abort     Cancelling…  unwinding
             RENDERED ONLY ON A BASIS THAT HAS A VENDOR LINE. On the FCF basis there is no forward
            series anywhere, at any date, so a refresh button would promise a fetch that cannot
            exist — see the note beside `hasForward`.
             DISABLED, NOT ABSENT, WITH NO COMPANY: a control that vanishes takes its space with
            it, and the header reflows on a state the reader cannot see the cause of. */}
        {hasForward && onRefresh && (
          <button type="button"
            onClick={() => (refreshing ? onCancel?.() : onRefresh())}
            disabled={cancelling || !canRefresh}
            aria-label={refreshing ? 'Cancel the re-read' : 'Ask GuruFocus for this series again'}
            title={cancelling ? 'Cancelling…'
              : refreshing ? 'Re-reading — press to cancel'
                : !canRefresh ? 'No GuruFocus company for this ISIN, so there is nothing to re-read'
                  : 'Ask GuruFocus for this series again'}
            className={`ml-auto inline-block text-xs leading-none ${
              cancelling ? 'cursor-wait text-fg-faint'
                : refreshing ? 'text-warn-400 hover:text-neg-400'
                  : !canRefresh ? 'cursor-default text-fg-faint/40'
                    : 'text-fg-faint hover:text-accent-400'}`}>
            {cancelling ? 'Cancelling…' : refreshing ? 'Cancel' : 'Refresh'}
          </button>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        {hasForward && (
          <Stat label={t.forwardTile(bl.multiple)} value={x(latestFwd)} color={primaryColor}
            info={<InfoTip content={<AspectCard
              what={vendorSeries
                ? `Forward ${b.multiple} for the current fiscal year.`
                : 'Latest forward P/OCF from the current daily close and next fiscal-year OCF/share consensus.'}
              where={vendorSeries ? 'GuruFocus forward P/E series.' : `GuruFocus annual ${consensusLabel} consensus; ${yahooWhere}`}
              when={vendorSeries ? `Weekly since ${fromYear}.`
                : `Price applies on ${priceDate ?? '—'}; consensus applies to the fiscal year ending ${estimateTarget ?? '—'}${estimateRetrievedDate ? ` and was retrieved ${estimateRetrievedDate}` : ''}.`}
              how={vendorSeries ? 'Uses consensus EPS for the current fiscal year.' : `Divide each daily price by the next fiscal year’s ${consensusLabel} consensus.`}
              worked={vendorSeries ? undefined : workedRatio(
                latestPoint?.price, latestPoint?.estimate, x(latestFwd),
                ` ${currency ?? ''}`, ` ${currency ?? ''}/share`)} />} />} />
        )}
        {/*  THE VENDOR'S OWN PUBLICATION DATE, WHICH NOTHING ON THIS CARD USED TO SHOW. Every
            figure here descends from a series read from GuruFocus with a multi-week lag, and the
            card said only "since 2015" — the window, never the edge. A reader could not tell a
            line current to yesterday from one that stopped five weeks ago, and the Refresh beside it had
            no number to move. */}
        {hasForward && (
          <Stat label={t.asOf} value={asOf ?? '—'}
            info={<InfoTip content={<AspectCard
              what={vendorSeries ? 'Latest publication date.' : 'As-of date of the market-price numerator.'}
              where={vendorSeries ? 'Newest point in this series.' : yahooWhere}
              when={vendorSeries ? `Weekly since ${fromYear}.`
                : `The price is ${priceDate ?? '—'}; its denominator is the consensus OCF/share for fiscal year ending ${estimateTarget ?? '—'}${estimateRetrievedDate ? `, retrieved ${estimateRetrievedDate}` : ''}.`}
              how={vendorSeries ? 'GuruFocus may publish this series with a delay.' : 'The date is the daily price date, not the forecast fiscal-year end.'} />} />} />
        )}
        <Stat label={t.median} value={x(median)} color={MEDIAN_COLOR}
          info={<InfoTip content={<AspectCard
            what={`Median forward ${b.multiple}.`}
            where={vendorSeries ? 'The forward series shown above.' : `Every daily price divided by the next-fiscal-year ${consensusLabel} consensus observation shown above.`}
            when={`${fVals.length} ${vendorSeries ? 'weekly' : 'daily'} observations since ${fromYear}${!vendorSeries && estimateRetrievedDate ? `; estimate history retrieved ${estimateRetrievedDate}` : ''}.`}
            how={vendorSeries ? 'The median is less affected by extreme values than the average.' : `Sort all daily forward ${b.multiple} values and take the middle one; the annual consensus is shifted back one fiscal year before each daily division.`} />} />} />
      </div>

      <div>
        {data.length < 2 ? (
          <p className="text-xs text-fg-faint py-16 text-center px-6">
            {/*  THE TWO EMPTINESSES ARE DIFFERENT AND ONLY ONE IS EVER FIXABLE. On the FCF basis
                there is no vendor forward series to read at all — a fact about the market, not
                about this company. On EPS it means GuruFocus publishes no forward P/E for this
                listing, which a re-ingest might. */}
            {b.multiple === 'P/OCF'
              ? t.noForwardOcf
              : t.noForwardPublished(bl.multiple, String(fromYear))}
          </p>
        ) : (
          <>
            <ResponsiveContainer width="100%" height={height}>
              <ComposedChart data={data} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowData(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                {/* Time, not fiscal years: the whole point of this chart is what happened BETWEEN
                    the reporting dates. Ticked at 1 January so the labels stay years. */}
                <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
                  ticks={years} interval="preserveStartEnd"
                  tickFormatter={(t: number) => String(new Date(t).getUTCFullYear())}
                  {...tiltedAxis()} />
                <YAxis domain={paddedDomain(scaleSet)} width={52}
                  tick={{ fontSize: 12, fill: chartTheme.axisTick }}
                  tickFormatter={(v: number) => `${v.toFixed(0)}×`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
                  labelStyle={{ color: chartTheme.axisLabel }}
                  labelFormatter={(t) => new Date(Number(t)).toISOString().slice(0, 10)}
                  formatter={(v, label) => [typeof v === 'number' ? `${v.toFixed(1)}×` : '—', label]} />
                {median != null && (
                  <ReferenceLine y={median} stroke={MEDIAN_COLOR} strokeDasharray="5 3"
                    strokeOpacity={0.55} />
                )}
                {/*  `connectNulls={false}` STILL. A stretch the vendor published nothing for is a
                    HOLE — joining across it draws a smooth valuation through a period that had
                    none, which is as wrong with one line as it was with two. */}
                <Line dataKey="fwd" name={`Forward ${bl.multiple}`} type="monotone" stroke={primaryColor}
                  strokeWidth={2} dot={false} connectNulls />
                <Line dataKey="comparison" name={`Forward ${comparisonBl.multiple}`} type="monotone"
                  stroke={comparisonColor} strokeWidth={2} dot={false} connectNulls />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 inline-block rounded" style={{ background: primaryColor }} />
                {vendorSeries ? `Forward ${b.multiple} — GuruFocus` : `Forward ${b.multiple} — daily price / next FY ${consensusLabel} consensus`}{currency ? ` (${currency})` : ''}
              </span>
              {comparisonForward.length > 0 && (
                <span className="flex items-center gap-1.5">
                  <span className="w-3 h-0.5 inline-block rounded" style={{ background: comparisonColor }} />
                  {comparisonBasisKey === 'eps'
                    ? `Forward ${comparisonBl.multiple} — GuruFocus`
                    : `Forward ${comparisonBl.multiple} — daily price / next FY OCF consensus`}{currency ? ` (${currency})` : ''}
                </span>
              )}
              {/*  THE "reporting lag applied" NOTE WENT WITH THE TRAILING LINE, deliberately. It
                  was about holding a fiscal figure back until it was plausibly public — a property
                  of a multiple WE computed from reported accounts. This line is read from the
                  vendor, so the note would be reassurance about arithmetic that no longer happens
                  here, which is worse than silence. */}
            </div>
          </>
        )}
      </div>

      {showData && (
        //  HANDED `data` — the exact rows plotted above. Nothing is recomputed, so the table
        // cannot disagree with the line that opened it. Same rule as `QuickValuationInputsModal`.
        <MultipleHistoryModal rows={data} basis={b} median={median}
          currency={currency} name={name} isin={isin}
          fromYear={fromYear} estimateRetrievedAt={estimateRetrievedAt} priceSource={priceSource}
          onClose={() => setShowData(false)} />
      )}
    </div>
  );
}
