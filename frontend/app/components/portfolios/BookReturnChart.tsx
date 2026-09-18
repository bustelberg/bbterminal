'use client';

/**
 * The book's cumulative return through the year — 0% on 1 January, and what happened since.
 *
 *  It is AIRS's own `cumulatief_rendement`, READ AND NEVER DERIVED FROM THE VALUE SERIES. That
 * is the whole reason this chart can exist: AIRS's figure is FLOW-AWARE. AzTopSelectie goes from 0
 * to EUR 1,000,000 on 2026-06-30 because it was FUNDED that day, and this line stays at 0.00%
 * straight through it — measured. A curve computed from two of our own values would draw that
 * funding as a 100% gain in one session, and no ratio of two values can tell the two apart.
 *
 *  And it is the same column the scorecard beside it reads (`_airs_accounts._year_perf`), so the
 * chart's last point and the `Return` chip are one number by construction rather than by
 * coincidence. Deriving a second answer here would put two YTD figures in one row of one screen —
 * the failure the Analyse modal's benchmark tile already pays for once (see `benchmarkSourceNote`).
 *
 *  The zero is an anchor, not an observation. `cumulatief_rendement` restarts every January, so
 * the curve's origin is the opening of the first period AIRS published — pinned at exactly 0.0% by
 * the server, which reports its date as `return_from`. It is the one point on this line nobody
 * measured, and it is what makes every other point readable.
 *
 *  The hover is deliberately only date + RETURN. Value and holding-count diagnostics belong in
 * the detailed tables; adding them here turns a quick time-series read into a miniature ledger.
 *
 *  The header is the return and nothing else (2026-09-01, on request): no value chip, no span
 * line. Both facts survive where a reader looks for them — the window on the x axis and in the ⓘ's
 * `when`, the value and the holding count in the tooltip — and this row is 24rem wide beside three
 * Scorecard chips, so every span in it is spent against the figure it is there to show.
 *
 *  It is not the drawdown panel's series and must not be read against it. That one rebuilds a
 * daily curve from the holdings as they stand today — look-ahead and survivorship included, as it
 * says — over years. This is what the book actually returned, on the dates AIRS has valued it.
 * Different objects, which is why this sits at the top of the modal rather than beside it.
 *
 *  Its own request. The Analyse modal is ONE payload with no partial paint, so its wall clock is
 * the reader's wait; a chart nobody has scrolled to yet does not belong in it. This fetches itself,
 * exactly as the Risk panels do.
 *
 *  And fetching itself is why it needs `refreshSeq` (2026-09-03, reported: the chip read +3.44%
 * and the chart +3.05%). The two ARE one column of one table — that part was never wrong — but the
 * modal's Refresh re-runs the AIRS scrape, which writes a NEW `airs_performance` row, and only the
 * modal's own payload was re-read afterwards. This effect depended on `portfolioId` alone, so the
 * chart kept the response it fetched when the modal opened: the chip moved to the row the scrape
 * had just written and the chart still held the one before it. "By construction" is a claim about
 * the COLUMN, and it says nothing about WHEN each side last read it.
 *
 *  It shares the scorecard's row, right of the excess tile, so its height is set against three
 * chips rather than against what a chart would like: 104px of plot, one line of chrome above and
 * one below. The caller fixes the WIDTH for the same reason — see the note at its call site.
 */
import { useEffect, useState } from 'react';
import {
  Area, AreaChart, CartesianGrid, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import { v } from '../../../lib/dynamicValue';
import InfoTip from '../InfoTip';
import { traceError } from '../../../lib/debugTrace';
import type { BookValueSeries } from '../../../lib/types/api';

/** `2026-08-26` → a UTC timestamp.  UTC, not local: a date-only string parsed as local time
 *  shifts by an hour twice a year, which is enough to move a point across a tick. */
const ts = (d: string) => Date.parse(`${d}T00:00:00Z`);

/** True only for the literal last calendar day of a month — not the last observation we happen
 * to have in it. The return graph is a month-end report with one live point, not a sparse daily
 * history. */
const isCalendarMonthEnd = (date: string) => {
  const [year, month, day] = date.split('-').map(Number);
  return Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)
    && day === new Date(Date.UTC(year, month, 0)).getUTCDate();
};

/** `26 Aug`, from a timestamp. */
const tick = (t: number) => new Date(t).toLocaleDateString('en-GB', {
  day: 'numeric', month: 'short', timeZone: 'UTC',
});

/**  ALWAYS SIGNED. On a curve pinned at zero the sign is the whole reading, and `2.4%` beside a
 *  line below the baseline is a contradiction the reader has to resolve by squinting. */
const pct = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

function ReturnTooltip({ active, label, payload }: {
  active?: boolean;
  label?: unknown;
  payload?: { value?: unknown }[];
}) {
  if (!active || typeof label !== 'number') return null;
  const value = payload?.find((p) => typeof p.value === 'number')?.value;
  return (
    <div style={{ ...chartTheme.tooltipCard.contentStyle, padding: '7px 10px' }}>
      <p style={{ color: chartTheme.axisLabel, margin: 0 }}>
        {new Date(label).toLocaleDateString('en-GB', {
          day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
        })}
      </p>
      <p style={{ margin: '3px 0 0' }}>Return: {pct(typeof value === 'number' ? value : null)}</p>
    </div>
  );
}

/** A zero-crossing is added only to let the red and green paths meet. It is not an AIRS valuation,
 * so it must never be rendered as a date dot that looks selectable or measured. */
function ReturnDot({ cx, cy, fill, payload }: {
  cx?: number; cy?: number; fill?: string;
  payload?: { interpolated?: boolean };
}) {
  if (payload?.interpolated || cx == null || cy == null) return null;
  return <circle cx={cx} cy={cy} r={1.5} fill={fill} stroke={fill} />;
}

export default function BookReturnChart(
  {
    portfolioId,
    /**
     * Bumped by the modal's caller when a refresh finishes — the SAME counter the modal's own
     * payload effect takes.
     *
     *  A real dependency, not defensive padding. See the note at the top of this file: without it
     * this chart is the one surface in the block that never re-reads what the scrape just wrote,
     * and it disagrees with the chip above it by exactly one AIRS row.
     */
    refreshSeq = 0,
  }: { portfolioId: number; refreshSeq?: number },
) {
  const [data, setData] = useState<BookValueSeries | null>(null);
  const [err, setErr] = useState<string | null>(null);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${portfolioId}/value-series`);
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as BookValueSeries);
      } catch (e) {
        //  The full diagnostic goes to the console, one short line to the reader.
        traceError('analyse', 'value series', e);
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [portfolioId, refreshSeq]);

  // Do not trust storage order for a charting rule. The server normally returns periods in date
  // order, but the latest observation must mean the latest DATE, never merely the final array
  // element (which could otherwise put a backfilled mid-month row back on the graph).
  const all = [...(data?.returns ?? [])].sort((a, b) => a.date.localeCompare(b.date));
  if (err) return <p className="text-[12px] text-neg-300">{err}</p>;
  if (!data) return <p className="text-[12px] text-fg-subtle">Loading the book’s return…</p>;
  //  The anchor alone is not a line. The server refuses to emit a lone pinned zero for exactly
  // that reason, so an empty series here means AIRS has published no return for this book yet —
  // or there is no book at all, which `reason` says.
  if (all.length < 2) {
    return (
      <p className="text-[12px] text-fg-faint">
        {data.reason ?? 'No return published for this book yet.'}
      </p>
    );
  }

  const last = all[all.length - 1];
  // Complete calendar month-ends, plus the single newest observation for the live month. This is
  // intentionally a complete display rule: no inception anchor, scrape-date or cash-flow marker
  // may add a visual point between month-ends.
  const points = all.filter((p) => isCalendarMonthEnd(p.date));
  if (points.at(-1)?.date !== last.date) points.push(last);
  const first = points[0] ?? last;
  const rows = points.map((p) => ({ ...p, t: ts(p.date) }));
  const up = (data.return_pct ?? 0) >= 0;
  /** Two series let Recharts colour each part of the return path by its own sign. A crossing gets
   * an interpolated 0% point in BOTH series: without it, the green/red segments stop at their last
   * sampled points and leave a visible gap exactly where the chart changes meaning. */
  const colouredRows = rows.reduce<Array<(typeof rows)[number] & {
    positive: number | null; negative: number | null; interpolated: boolean;
  }>>(
    (out, row, i) => {
      const previous = rows[i - 1];
      if (previous && ((previous.cum_pct < 0 && row.cum_pct > 0)
        || (previous.cum_pct > 0 && row.cum_pct < 0))) {
        const share = -previous.cum_pct / (row.cum_pct - previous.cum_pct);
        out.push({ ...row, t: previous.t + (row.t - previous.t) * share,
          cum_pct: 0, positive: 0, negative: 0, interpolated: true });
      }
      out.push({ ...row,
        positive: row.cum_pct >= 0 ? row.cum_pct : null,
        negative: row.cum_pct <= 0 ? row.cum_pct : null, interpolated: false });
      return out;
    }, []);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-3">
      <div className="flex items-baseline gap-2 flex-wrap mb-1">
        <span className="text-[11px] uppercase tracking-wider text-fg-faint">Return YTD</span>
        <span className={`font-mono ${up ? 'text-pos-400' : 'text-neg-400'}`}>
          {pct(data.return_pct)}
        </span>
        {/*  NO VALUE CHIP AND NO SPAN LINE — removed on request (2026-09-01), and NEITHER FACT
            LEFT THE COMPONENT. The window is the ⓘ's `when` and it is written along the x axis;
            the book's value and that date's holding count are on every hover. What the header
            carries now is the one figure this chart exists to state, and a chip the reader has to
            step over to reach it is a cost with no reader. */}
        {/*  NO `how` AND NO `worked` (2026-09-03, on request: "it's simply copied straight from
            AIRS, no calculation needed"). This figure is READ — `cumulatief_rendement`, one column,
            one row — so a chained-product formula under it described an arithmetic nobody here
            performs, and the house rule already said so: no worked line over raw data. What is
            left is what / where / when, which is the whole of the Active Share shape when there is
            no maths to typeset.
             AND EVERY LIVE FIGURE IS BADGED (`v()`). The dates were interpolated bare, so a
            reader scanning the card could not tell this book's window from the sentence around it
            — which is the one question badging exists to answer, and the only reason the card
            carries them. The return itself is not repeated here: it is set in the header above. */}
        <InfoTip className="ml-auto" content={<AspectCard
          what="What the book has returned so far this year, from 0% at the start of it."
          where={`AIRS's own Rendementen sheet — the same figure as the Return tile beside this, `
            + `over ${v(all.length - 1)} published points.`}
          when={`${v(data.return_from ?? first.date)} to ${v(last.date)}.`} />} />
      </div>
      <ResponsiveContainer width="100%" height={104}>
        <AreaChart data={colouredRows} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          {/*  A TIME AXIS, NOT A CATEGORY ONE, AND THE DIFFERENCE IS THE WHOLE SHAPE OF THE
              LINE. The points are irregular — AIRS publishes a month-end for each closed month and
              then a row per day the scrape ran — so on a categorical axis a five-month stretch of
              month-ends occupies as much of the chart as a fortnight of daily points, and a
              six-week gap is drawn the same width as an overnight one. Plotted against real time,
              distance means elapsed time everywhere. */}
          <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
            tickFormatter={tick} minTickGap={28}
            tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
          {/*  ZERO IS ALWAYS IN THE DOMAIN, unlike the value chart this replaced. There the
              baseline was meaningless and an auto domain was the only way to see the shape; here
              zero is the reading — a curve floating entirely above or below a baseline that is off
              the plot says nothing about whether the book is up. */}
          <YAxis domain={[(min: number) => Math.min(0, min), (max: number) => Math.max(0, max)]}
            width={44} tick={{ fontSize: 11, fill: chartTheme.axisTick }}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
          <Tooltip content={<ReturnTooltip />} />
          {/*  THE BASELINE IS DRAWN, not just included in the domain. "Start at 0%" is the whole
              claim of this chart, and a gridline the reader has to identify is not the same as a
              rule they can see the line cross. */}
          <ReferenceLine y={0} stroke={chartTheme.axisTick} strokeOpacity={0.55} />
          <Area dataKey="positive" type="monotone" stroke={chartTheme.pos} strokeWidth={2}
            fill={chartTheme.pos} fillOpacity={0.08}
            dot={<ReturnDot fill={chartTheme.pos} />} />
          <Area dataKey="negative" type="monotone" stroke={chartTheme.neg} strokeWidth={2}
            fill={chartTheme.neg} fillOpacity={0.08}
            dot={<ReturnDot fill={chartTheme.neg} />} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
