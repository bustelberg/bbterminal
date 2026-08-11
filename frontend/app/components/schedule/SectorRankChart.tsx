'use client';

import {
  Line, LineChart, ReferenceArea, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { chartTheme } from '../../../lib/chartTheme';
import { colorForSector } from '../../../lib/sectorColors';
import { inkForBackground, sectorCode } from '../../../lib/sectorCodes';
import { maxRank, sectorRankSeries, type RankDay } from './sectorRankSeries';

/**
 * Each sector's RANK over the window — one small panel per sector.
 *
 * ⚠ SMALL MULTIPLES, NOT ELEVEN LINES ON ONE AXIS, AND THAT IS A MEASURED DECISION RATHER THAN A
 * STYLE ONE. A bump chart is the obvious form for rank-over-time, and it needs one distinguishable
 * colour per series. This palette does not have eleven: `dataviz/scripts/validate_palette.js` over
 * all pairs on the light surface FAILS the NORMAL-vision floor for Services vs Energy (dE 3.5,
 * floor 15) and the CVD floor for Industrials vs Technology (dE 0.9 deutan). Overlaying them would
 * produce the one thing worse than a spaghetti chart: a spaghetti chart whose strands cannot be
 * told apart even by a reader with full colour vision. One series per panel means colour carries
 * nothing load-bearing — the panel heading names the sector.
 *
 * ⚠ ONE SHARED Y-DOMAIN ACROSS PANELS. Per-panel autoscaling would draw a sector that swung from
 * 1st to 11th and one that wobbled between 3rd and 4th with identical amplitude — the classic way
 * small multiples lie. `maxRank` is computed over every panel and applied to all of them.
 *
 * ⚠ Y IS INVERTED: rank 1 at the TOP. A rank axis running 1-at-the-bottom reads as "up is better"
 * while showing the opposite.
 */
export default function SectorRankChart({ days, topN, windowLabel }: {
  days: RankDay[];
  /** The strategy's `top_n_sectors` — the pick cutoff, drawn on every panel. */
  topN?: number | null;
  windowLabel: string;
}) {
  const series = sectorRankSeries(days);
  if (!series.length) {
    return (
      <p className="text-[12px] text-fg-faint">
        No sector ranks in this window — the days here were calculated before sector scores were
        stored. Recalculate them to fill the chart in.
      </p>
    );
  }
  const worst = maxRank(series);
  const cutoff = topN && topN > 0 && topN < worst ? topN : null;
  const anyDropped = series.some((s) => s.droppedRuns.length > 0);

  return (
    <div className="space-y-2">
      <div className="flex items-baseline gap-2 flex-wrap">
        <h4 className="text-xs font-medium text-fg-strong">Sector rank · {windowLabel}</h4>
        <span className="text-[11px] text-fg-faint">
          rank 1 at the top · same scale on every panel
        </span>
        {cutoff && (
          <span className="text-[11px] text-fg-muted flex items-center gap-1.5">
            <span className="inline-block w-4 border-t border-dashed shrink-0"
              style={{ borderColor: chartTheme.axisTick }} />
            ceiling — a sector is bought while it sits on or above rank {cutoff}
          </span>
        )}
        {anyDropped && (
          <span className="text-[11px] text-fg-muted flex items-center gap-1.5"
            title="The day was ranked, but no company in this sector cleared the strategy's minimum price score, so the whole sector fell out of the pool. Not missing data — a fact about the sector that day.">
            <span className="inline-block w-4 h-2.5 rounded-sm shrink-0"
              style={{ backgroundColor: chartTheme.axisTick, opacity: 0.16 }} />
            shaded — no company cleared the price floor, so the sector was not ranked
          </span>
        )}
      </div>
      <div className="grid gap-2 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4">
        {series.map((s) => {
          const color = colorForSector(s.sector);
          return (
            <div key={s.sector} className="rounded-lg border border-neutral-800/40 bg-card px-2 pt-1.5 pb-0.5">
              <div className="flex items-center gap-1.5 mb-1">
                <span className="inline-flex items-center justify-center rounded-sm font-mono font-semibold shrink-0"
                  style={{
                    backgroundColor: color, color: inkForBackground(color),
                    width: '1.35rem', height: '0.95rem', fontSize: '0.5rem',
                  }}>
                  {sectorCode(s.sector)}
                </span>
                <span className="text-[12px] text-fg-soft truncate" title={s.sector}>{s.sector}</span>
                {/* The current rank, in ink — a value never wears the series colour. */}
                <span className="ml-auto text-[12px] font-mono text-fg-muted shrink-0"
                  title={`Ranked on ${s.ranked} of ${s.points.length} days in this window`}>
                  {s.latest == null ? '—' : `#${s.latest}`}
                </span>
              </div>
              <ResponsiveContainer width="100%" height={92}>
                <LineChart data={s.points} margin={{ top: 2, right: 6, bottom: 0, left: -22 }}>
                  <XAxis dataKey="date" tick={{ fontSize: 10, fill: chartTheme.axisTick }}
                    interval="preserveStartEnd" minTickGap={40}
                    tickFormatter={(d: string) => d.slice(5)} />
                  {/* Reversed so 1 sits at the top; `allowDecimals={false}` because there is no
                      rank 2.5. */}
                  <YAxis reversed domain={[1, worst]} allowDecimals={false} width={28}
                    tick={{ fontSize: 10, fill: chartTheme.axisTick }} />
                  <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
                    labelStyle={{ color: chartTheme.axisLabel }}
                    formatter={(v) => [typeof v === 'number' ? `#${v}` : '—', 'Rank']} />
                  {/* ⚠ THE CEILING — the rank a sector has to stay AT OR ABOVE to be bought.
                      It shipped on `chartTheme.zeroLine` (#d7dce2), which at a 92px panel is
                      invisible: the single most important line on the chart read as a grid
                      artefact. It is now ink-toned and labelled ON each panel rather than
                      explained once in a header nobody re-reads while scanning eleven charts.
                      Still recessive — a reference is not a series, so it never takes a series
                      colour. */}
                  {cutoff && (
                    <>
                      {/* The "bought" zone, so membership reads without tracing the line. Above
                          the ceiling on this inverted axis = better rank = picked. */}
                      <ReferenceArea y1={1} y2={cutoff} fill={chartTheme.accent} fillOpacity={0.07}
                        ifOverflow="extendDomain" />
                      <ReferenceLine y={cutoff} stroke={chartTheme.axisTick} strokeWidth={1}
                        strokeDasharray="3 2"
                        label={{
                          value: `top ${cutoff}`, position: 'insideTopRight',
                          fontSize: 9, fill: chartTheme.axisTick,
                        }} />
                    </>
                  )}
                  {/* ⚠ A DROP-OUT IS SHADED, A DATA HOLE IS NOT — the two look identical as a bare
                      break in the line, and that ambiguity got reported as a bug twice. Shaded =
                      the day WAS ranked and this sector was not in the pool: with a
                      `min_price_score` floor and no backfill, a name below it is dropped outright,
                      so a sector whose every company falls under the floor disappears. Measured:
                      Consumer Cyclical, rank 3 on 11 June, gone on the 12th and 15th, back at rank
                      8 on the 16th, on a floor of 30. An unshaded break is the other thing — no
                      ranking stored for that day at all. */}
                  {s.droppedRuns.map((r) => (
                    <ReferenceArea key={r.from} x1={r.from} x2={r.to}
                      fill={chartTheme.axisTick} fillOpacity={0.16} ifOverflow="extendDomain" />
                  ))}
                  {/* ⚠ `connectNulls={false}`: a day with no rank is a BREAK in the line.
                      Joining across it would draw a confident path through a period with none. */}
                  <Line dataKey="rank" type="monotone" stroke={color} strokeWidth={1.75}
                    dot={false} connectNulls={false} isAnimationActive={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>
          );
        })}
      </div>
    </div>
  );
}
