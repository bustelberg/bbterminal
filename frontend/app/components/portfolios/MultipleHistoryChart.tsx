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
import { medianOf, type BASIS } from './quickValuation';
import { type Point } from './multiplesSeries';
import MultipleHistoryModal from './MultipleHistoryModal';

/**
 * THE MULTIPLE THROUGH TIME — a decade of it, at the resolution the price moves.
 *
 * The fiscal-year chart answers "what did it trade at each year end"; twelve dots cannot show a
 * de-rating that happened over four months. This is the same question at weekly resolution, and
 * it is the only place the FORWARD multiple has real history.
 *
 * ⚠⚠ FORWARD ONLY SINCE 2026-08-21, ON REQUEST. The trailing line — price ÷ the figure last
 * REPORTED at that date, on both bases — was removed. What is left is one line: GuruFocus's own
 * published forward-P/E indicator, back to 2015, weekly, read straight through and computed from
 * nothing here.
 *
 * ⚠ SO THE FCF BASIS DRAWS NOTHING, AND SAYS SO RATHER THAN LOOKING BROKEN. Nobody forecasts
 * capex, so no vendor publishes a free-cash-flow consensus and there is no forward P/FCF to read —
 * anywhere, at any date. A forward FCF line is planned; until it exists this panel is honest about
 * being empty instead of falling back to the measure that was just removed.
 *
 * ⚠ THE MEDIAN MOVED WITH THE LINE. It used to be the median of the TRAILING series, and the tile
 * said so explicitly ("a median of two different measures would be neither"). With one series left
 * it is that series' median — the same reasoning, applied to what is now on screen.
 *
 * ⚠ WHAT THE REMOVED LINE WAS FOR, RECORDED so the decision can be revisited rather than
 * rediscovered: it was the only measure available on the FCF basis, and it carried its own inputs
 * (close, per-share) into the drill-down, which the vendor indicator cannot — a published number
 * has nothing to decompose. It was also genuinely jagged on FCF (ASML 21.8x -> 116.4x -> 28.9x in
 * three years, on real capex swings), which is what made a forward line desirable there.
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
/** ⚠ THE FORWARD LINE KEEPS AMBER, NOT THE PRIMARY BLUE IT COULD NOW CLAIM. Colour follows the
 *  ENTITY, never its rank — the same rule the two-line version stated in the other direction. A
 *  reader who knows this chart knows the forward line as amber; promoting it to blue because the
 *  blue series left would mean the same series changed colour because a different one disappeared. */
const FORWARD_COLOR = chartTheme.warn;
/** The median is a REFERENCE, not a third series: recessive grey, never a categorical hue. */
const MEDIAN_COLOR = chartTheme.axisTick;

export default function MultipleHistoryChart({
  basis: b, forward, currency, fromYear, name, isin, height = 320, className = '',
  onRefresh, onCancel, canRefresh = false, refreshing = false, cancelling = false,
}: {
  basis: (typeof BASIS)[keyof typeof BASIS];
  /** The vendor's published forward multiple — the only series here. Empty on the FCF basis, by
   *  nature: no vendor publishes a free-cash-flow consensus. */
  forward: Point[];
  currency?: string | null;
  fromYear: number;
  name?: string | null;
  isin: string;
  height?: number;
  className?: string;
  /**
   * Go and get the vendor's series again.
   *
   * ⚠⚠ THIS SERIES IS THE ONE THING ON THE TAB THAT GOES STALE WITHOUT SAYING SO. It is read from
   * GuruFocus, not computed here, and the vendor publishes it with a multi-week lag — measured on
   * argenx, the newest observation was 24 July while the file was read on 17 August. Everything
   * else on this card is derived from it, so a stale line silently ages the median, the tile and
   * the drill-down together and nothing looks wrong.
   *
   * ⚠ THE CARD DOES NOT FETCH — the tab owns the metrics and the chart is a function of them, so a
   * fetch here would be a second loader for one payload. It gets a callback and three flags.
   */
  onRefresh?: () => void;
  onCancel?: () => void;
  /** False when no GuruFocus company backs this ISIN — the button is then disabled, not absent. */
  canRefresh?: boolean;
  refreshing?: boolean;
  cancelling?: boolean;
}) {
  // Click-to-inspect, the same affordance the two charts beside it carry.
  const [showData, setShowData] = useState(false);
  const hasForward = forward.length > 0;
  const fVals = forward.map((p) => p.value);
  const median = medianOf(fVals);
  const latestFwd = forward.at(-1)?.value ?? null;
  /**
   * The date of the newest observation, for the As-of tile.
   *
   * ⚠ THE VENDOR'S DATE, NOT OURS. It is when GuruFocus last published a point, which is the only
   * date that answers "is this current" — the moment we happened to read it says nothing about
   * whether there was anything newer to read.
   *
   * ⚠ ISO HERE AND `onDate` IN THE TOAST, DELIBERATELY. A `Stat` value is 18px mono inside 8rem, so
   * `2026-07-24` fits at ten characters and `24 July 2026` truncates to a date that reads as a
   * different one. The refresh's toast is a sentence with room, and gets the human form.
   */
  const asOf = forward.at(-1)
    ? new Date(forward.at(-1)!.t).toISOString().slice(0, 10) : null;

  /**
   * ⚠ NO `align` ANY MORE, AND THAT IS THE ONE SIMPLIFICATION THE REMOVAL ACTUALLY BUYS. It
   * existed because the vendor's forward indicator and our trailing series were sampled
   * independently and shared almost no dates — a naive merge gave rows holding one value and a
   * null for the other, alternating, which `connectNulls={false}` then drew as isolated dots. One
   * series has nothing to be aligned against.
   */
  const data = forward.map((p) => ({ t: p.t, fwd: p.value }));

  // Scale over the multiples that describe a valuation; an outlier still plots and overflows.
  const scaleSet = median == null ? fVals
    : fVals.filter((v) => v <= median * OUTLIER_MULT);
  const clipped = fVals.filter((v) => median != null && v > median * OUTLIER_MULT).length;

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
        <h4 className="text-base font-semibold text-fg-strong">{b.multiple} — forward</h4>
        <span className="text-[12px] text-fg-faint">since {fromYear} · median dashed</span>
        <span className="text-[12px] text-fg-muted"
          title={hasForward
            ? "GuruFocus's own published forward-P/E indicator, not our arithmetic. Dividing the close by it recovers the CURRENT fiscal year's consensus EPS — so early in a year it looks ~12 months ahead, and by December it prices earnings nearly banked."
            : 'No analyst publishes a free-cash-flow forecast — capex is not forecast — so there is no forward multiple to draw on this basis, at any date. A forward FCF line is planned.'}>
          {hasForward ? 'vendor indicator' : 'no forward FCF yet — nobody forecasts capex'}
        </span>
        {/* ⚠⚠ ONE CONTROL, THREE STATES, AND IT TURNS INTO THE CANCEL — the same shape and the same
            three glyphs as the share-price ↻ on the Deep Valuation tab. The reader pressed it HERE,
            so this is where stopping it belongs; sending them to the toast in the corner to undo
            something they started on this card is a Cancel that does nothing.
              ↻  idle        ✕  running, press to abort        ⋯  unwinding after the press
            ⚠ RENDERED ONLY ON A BASIS THAT HAS A VENDOR LINE. On the FCF basis there is no forward
            series anywhere, at any date, so a refresh button would promise a fetch that cannot
            exist — see the note beside `hasForward`.
            ⚠ DISABLED, NOT ABSENT, WITH NO COMPANY: a control that vanishes takes its space with
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
            className={`ml-auto inline-block w-4 text-center text-[12px] leading-none ${
              cancelling ? 'cursor-wait text-fg-faint'
                : refreshing ? 'text-warn-400 hover:text-neg-400'
                  : !canRefresh ? 'cursor-default text-fg-faint/40'
                    : 'text-fg-faint hover:text-accent-400'}`}>
            {cancelling ? '⋯' : refreshing ? '✕' : '↻'}
          </button>
        )}
      </div>

      <div className="flex flex-wrap gap-2">
        {hasForward && (
          <Stat label={`Forward ${b.multiple}`} value={x(latestFwd)} color={FORWARD_COLOR}
            info={<InfoTip content={<AspectCard
              what="What the market pays today for the fiscal year now in progress."
              where="GuruFocus `forward_pe_ratio`, published as a time series — read, not computed."
              when={`Weekly since ${fromYear}. The As-of tile beside this one dates the newest point.`}
              how="⚠ Its denominator is the CURRENT fiscal year's consensus, not a rolling twelve months — verified by backing the EPS out of price ÷ this ratio (argenx: 23.20 implied vs 23.23 published, against 27.93 for an NTM blend)." />} />} />
        )}
        {/* ⚠⚠ THE VENDOR'S OWN PUBLICATION DATE, WHICH NOTHING ON THIS CARD USED TO SHOW. Every
            figure here descends from a series read from GuruFocus with a multi-week lag, and the
            card said only "since 2015" — the window, never the edge. A reader could not tell a
            line current to yesterday from one that stopped five weeks ago, and the ↻ beside it had
            no number to move. */}
        {hasForward && (
          <Stat label="As of" value={asOf ?? '—'}
            info={<InfoTip content={<AspectCard
              what="When GuruFocus last published a point in this series."
              where="The newest observation on the line, not the moment we read it."
              when={`Weekly since ${fromYear}.`}
              how="The vendor publishes with a lag of some weeks, so this can sit behind today with nothing wrong. The ↻ above asks for anything newer." />} />} />
        )}
        <Stat label="Median" value={x(median)} color={MEDIAN_COLOR}
          info={<InfoTip content={<AspectCard
            what={`The middle forward ${b.multiple} over the window — what this has typically cost.`}
            where="The forward line above. ⚠ It was the median of a TRAILING series until that line was removed; a median of a line nobody can see is worse than none."
            when={`${fVals.length} weekly observations since ${fromYear}.`}
            how="⚠ MEDIAN, NOT MEAN. One collapsed-denominator year prints a 300× that no reader would call typical and drags a mean by tens of turns." />} />} />
      </div>

      <div>
        {data.length < 2 ? (
          <p className="text-[12px] text-fg-faint py-16 text-center px-6">
            {/* ⚠ THE TWO EMPTINESSES ARE DIFFERENT AND ONLY ONE IS EVER FIXABLE. On the FCF basis
                there is no vendor forward series to read at all — a fact about the market, not
                about this company. On EPS it means GuruFocus publishes no forward P/E for this
                listing, which a re-ingest might. */}
            {b.multiple === 'P/FCF'
              ? 'No forward P/FCF exists — nobody forecasts capex, so no vendor publishes a '
                + 'free-cash-flow consensus at any date. A forward FCF line is planned; the '
                + 'trailing measure that used to fill this panel was removed deliberately.'
              : `No forward ${b.multiple} published for this listing since ${fromYear}.`}
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
                <YAxis domain={paddedDomain(scaleSet)} allowDataOverflow width={52}
                  tick={{ fontSize: 12, fill: chartTheme.axisTick }}
                  tickFormatter={(v: number) => `${v.toFixed(0)}×`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
                  labelStyle={{ color: chartTheme.axisLabel }}
                  labelFormatter={(t) => new Date(Number(t)).toISOString().slice(0, 10)}
                  formatter={(v) => [typeof v === 'number' ? `${v.toFixed(1)}×` : '—',
                    `Forward ${b.multiple}`]} />
                {median != null && (
                  <ReferenceLine y={median} stroke={MEDIAN_COLOR} strokeDasharray="5 3"
                    strokeOpacity={0.55} />
                )}
                {/* ⚠ `connectNulls={false}` STILL. A stretch the vendor published nothing for is a
                    HOLE — joining across it draws a smooth valuation through a period that had
                    none, which is as wrong with one line as it was with two. */}
                <Line dataKey="fwd" name="fwd" type="monotone" stroke={FORWARD_COLOR}
                  strokeWidth={2} dot={false} connectNulls={false} />
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5">
                <span className="w-3 h-0.5 inline-block rounded" style={{ background: FORWARD_COLOR }} />
                Forward {b.multiple} — GuruFocus{currency ? ` (${currency})` : ''}
              </span>
              {clipped > 0 && (
                <span className="text-warn-300"
                  title={`Above ${OUTLIER_MULT}x the median. Still drawn, only excluded from the axis range — one collapsed-FCF year would otherwise flatten the whole decade.`}>
                  ⚠ {clipped} point{clipped > 1 ? 's' : ''} off the top of the axis
                </span>
              )}
              {/* ⚠ THE "reporting lag applied" NOTE WENT WITH THE TRAILING LINE, deliberately. It
                  was about holding a fiscal figure back until it was plausibly public — a property
                  of a multiple WE computed from reported accounts. This line is read from the
                  vendor, so the note would be reassurance about arithmetic that no longer happens
                  here, which is worse than silence. */}
            </div>
          </>
        )}
      </div>

      {showData && (
        // ⚠ HANDED `data` — the exact rows plotted above. Nothing is recomputed, so the table
        // cannot disagree with the line that opened it. Same rule as `QuickValuationInputsModal`.
        // ⚠ NO INPUTS COLUMNS ANY MORE, and that is a property of the data rather than lost detail:
        // the trailing multiple was OUR division and carried its two operands; a vendor's published
        // indicator has nothing to decompose.
        <MultipleHistoryModal rows={data} basis={b} median={median}
          currency={currency} name={name} isin={isin}
          fromYear={fromYear} onClose={() => setShowData(false)} />
      )}
    </div>
  );
}
