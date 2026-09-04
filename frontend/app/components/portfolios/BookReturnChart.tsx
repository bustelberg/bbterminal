'use client';

/**
 * THE BOOK'S CUMULATIVE RETURN THROUGH THE YEAR — 0% on 1 January, and what happened since.
 *
 * ⚠⚠ IT IS AIRS'S OWN `cumulatief_rendement`, READ AND NEVER DERIVED FROM THE VALUE SERIES. That
 * is the whole reason this chart can exist: AIRS's figure is FLOW-AWARE. AzTopSelectie goes from 0
 * to EUR 1,000,000 on 2026-06-30 because it was FUNDED that day, and this line stays at 0.00%
 * straight through it — measured. A curve computed from two of our own values would draw that
 * funding as a 100% gain in one session, and no ratio of two values can tell the two apart.
 *
 * ⚠⚠ AND IT IS THE SAME COLUMN THE SCORECARD BESIDE IT READS (`_airs_accounts._year_perf`), so the
 * chart's last point and the `Return` chip are one number by construction rather than by
 * coincidence. Deriving a second answer here would put two YTD figures in one row of one screen —
 * the failure the Analyse modal's benchmark tile already pays for once (see `benchmarkSourceNote`).
 *
 * ⚠ THE ZERO IS AN ANCHOR, NOT AN OBSERVATION. `cumulatief_rendement` restarts every January, so
 * the curve's origin is the opening of the first period AIRS published — pinned at exactly 0.0% by
 * the server, which reports its date as `return_from`. It is the one point on this line nobody
 * measured, and it is what makes every other point readable.
 *
 * ⚠ IT WAS A VALUE CHART AND THE VALUE HAS NOT GONE — it is on every hover, with that date's
 * holding count. What changed is which of the two quantities the LINE draws: what the book earned
 * rather than what it was worth, because a book's worth moves with the money paid into it and its
 * return does not.
 *
 * ⚠ THE HEADER IS THE RETURN AND NOTHING ELSE (2026-09-01, on request): no value chip, no span
 * line. Both facts survive where a reader looks for them — the window on the x axis and in the ⓘ's
 * `when`, the value and the holding count in the tooltip — and this row is 24rem wide beside three
 * Scorecard chips, so every span in it is spent against the figure it is there to show.
 *
 * ⚠ IT IS NOT THE DRAWDOWN PANEL'S SERIES AND MUST NOT BE READ AGAINST IT. That one rebuilds a
 * daily curve from the holdings as they stand today — look-ahead and survivorship included, as it
 * says — over years. This is what the book actually returned, on the dates AIRS has valued it.
 * Different objects, which is why this sits at the top of the modal rather than beside it.
 *
 * ⚠ ITS OWN REQUEST. The Analyse modal is ONE payload with no partial paint, so its wall clock is
 * the reader's wait; a chart nobody has scrolled to yet does not belong in it. This fetches itself,
 * exactly as the Risk panels do.
 *
 * ⚠⚠ AND FETCHING ITSELF IS WHY IT NEEDS `refreshSeq` (2026-09-03, reported: the chip read +3.44%
 * and the chart +3.05%). The two ARE one column of one table — that part was never wrong — but the
 * modal's Refresh re-runs the AIRS scrape, which writes a NEW `airs_performance` row, and only the
 * modal's own payload was re-read afterwards. This effect depended on `portfolioId` alone, so the
 * chart kept the response it fetched when the modal opened: the chip moved to the row the scrape
 * had just written and the chart still held the one before it. "By construction" is a claim about
 * the COLUMN, and it says nothing about WHEN each side last read it.
 *
 * ⚠ IT SHARES THE SCORECARD'S ROW, RIGHT OF THE EXCESS TILE, so its height is set against three
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
import { lastPerMonth } from './monthEnds';
import type { BookValueSeries } from '../../../lib/types/api';

/** `2026-08-26` → a UTC timestamp. ⚠ UTC, not local: a date-only string parsed as local time
 *  shifts by an hour twice a year, which is enough to move a point across a tick. */
const ts = (d: string) => Date.parse(`${d}T00:00:00Z`);

/** `26 Aug`, from a timestamp. */
const tick = (t: number) => new Date(t).toLocaleDateString('en-GB', {
  day: 'numeric', month: 'short', timeZone: 'UTC',
});

const eur = (v: number | null | undefined) =>
  (v == null ? '—' : `€${Math.round(v).toLocaleString('en-US')}`);

/** ⚠ ALWAYS SIGNED. On a curve pinned at zero the sign is the whole reading, and `2.4%` beside a
 *  line below the baseline is a contradiction the reader has to resolve by squinting. */
const pct = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

export default function BookReturnChart(
  {
    portfolioId,
    /**
     * Bumped by the modal's caller when a refresh finishes — the SAME counter the modal's own
     * payload effect takes.
     *
     * ⚠ A REAL DEPENDENCY, not defensive padding. See the note at the top of this file: without it
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
        // ⚠ THE FULL DIAGNOSTIC GOES TO THE CONSOLE, one short line to the reader.
        traceError('analyse', 'value series', e);
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [portfolioId, refreshSeq]);

  const all = data?.returns ?? [];
  if (err) return <p className="text-[12px] text-neg-300">{err}</p>;
  if (!data) return <p className="text-[12px] text-fg-subtle">Loading the book’s return…</p>;
  // ⚠ THE ANCHOR ALONE IS NOT A LINE. The server refuses to emit a lone pinned zero for exactly
  // that reason, so an empty series here means AIRS has published no return for this book yet —
  // or there is no book at all, which `reason` says.
  if (all.length < 2) {
    return (
      <p className="text-[12px] text-fg-faint">
        {data.reason ?? 'No return published for this book yet.'}
      </p>
    );
  }

  const anchor = all[0];
  /**
   * ⚠⚠ ONE POINT A MONTH, ALWAYS — see `monthEnds`. AIRS publishes a month-end row for every month
   * of the year and then a row per scrape date once we started scraping, so the recent half of this
   * line is forty-odd points against the earlier half's six, and undrawn that way the chart reads
   * as two different things joined in the middle. A month-end series is the same shape at a
   * resolution the whole span can be drawn at.
   *
   * ⚠⚠ A SINGLE MOST-RECENT POINT PER MONTH, AND THE CURRENT MONTH IS NOT A SPECIAL CASE
   * (2026-09-03, on request, after one was tried and removed the same day). Ending on the newest
   * row is what makes this line agree with the `Return` chip above it — the chip reads that row.
   * A rule that also asked "is this point TODAY's?" dropped the current month on any morning AIRS
   * had not published yet, leaving the line at last month's close while the chip and this chart's
   * own header both stated yesterday's figure: a resolution inventing a disagreement to hide one.
   *
   * ⚠⚠ SO A LAST POINT THAT LOOKS A DAY BEHIND IS A STALE READ, NOT A RESOLUTION — see the
   * `refreshSeq` note at the top of this file, which is what the reported +3.44% against +3.05%
   * actually was.
   *
   * ⚠⚠ THE "All points" TOGGLE THAT USED TO SIT IN THE HEADER CAME OFF, 2026-09-02 ON REQUEST.
   * The dense view is therefore gone, not hidden: `dense` state, both button labels and both
   * tooltips went with it. The day-to-day movement it showed is real and is no longer reachable
   * from this chart — every point is still on the hover of the month that contains it, and the
   * Risk panel's own drawdown view is where intra-month movement is the subject.
   *
   * ⚠ THE ANCHOR SURVIVES THE THINNING, ALWAYS, AND THAT MATTERS MORE NOW THAT THERE IS NO ESCAPE
   * HATCH. The thinning keeps the LAST point in each month, and the pinned 0% sits on the FIRST
   * of its own month — so thinning the whole series drops it and the line starts at January's
   * −1.94% with no baseline on the chart. It is held out and re-attached; everything after it
   * thins normally.
   */
  const points = [anchor, ...lastPerMonth(all.slice(1))];

  // ⚠ THE HEADLINE AND THE ⓘ'S WINDOW COME FROM `all`, NEVER FROM THE THINNED VIEW. The figure the
  // header reports is the newest AIRS published, which is usually NOT a month end — reading it off
  // the thinned series would restate both the number and the period it covers.
  const last = all[all.length - 1];
  const rows = points.map((p) => ({ ...p, t: ts(p.date) }));
  const up = (data.return_pct ?? 0) >= 0;
  // ⚠ THE LINE WEARS THE SIGN OF THE HEADLINE FIGURE beside it, which is the same convention the
  // Scorecard's own Return chip follows. One book, one verdict, whichever of the two you read.
  const ink = up ? chartTheme.pos : chartTheme.neg;
  // ⚠ ONLY THE FLOWS INSIDE THE PLOTTED SPAN. `airs_performance` reaches back further than the
  // year in hand, and a marker on a date the axis does not carry lands at the wrong x.
  const marks = (data.flows ?? []).filter((f) => f.date >= anchor.date && f.date <= last.date);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-3">
      <div className="flex items-baseline gap-2 flex-wrap mb-1">
        <span className="text-[11px] uppercase tracking-wider text-fg-faint">Return YTD</span>
        <span className={`font-mono ${up ? 'text-pos-400' : 'text-neg-400'}`}>
          {pct(data.return_pct)}
        </span>
        {/* ⚠ NO VALUE CHIP AND NO SPAN LINE — removed on request (2026-09-01), and NEITHER FACT
            LEFT THE COMPONENT. The window is the ⓘ's `when` and it is written along the x axis;
            the book's value and that date's holding count are on every hover. What the header
            carries now is the one figure this chart exists to state, and a chip the reader has to
            step over to reach it is a cost with no reader. */}
        {/* ⚠⚠ NO `how` AND NO `worked` (2026-09-03, on request: "it's simply copied straight from
            AIRS, no calculation needed"). This figure is READ — `cumulatief_rendement`, one column,
            one row — so a chained-product formula under it described an arithmetic nobody here
            performs, and the house rule already said so: no worked line over raw data. What is
            left is what / where / when, which is the whole of the Active Share shape when there is
            no maths to typeset.
            ⚠⚠ AND EVERY LIVE FIGURE IS BADGED (`v()`). The dates were interpolated bare, so a
            reader scanning the card could not tell this book's window from the sentence around it
            — which is the one question badging exists to answer, and the only reason the card
            carries them. The return itself is not repeated here: it is set in the header above. */}
        <InfoTip className="ml-auto" content={<AspectCard
          what="What the book has returned so far this year, from 0% at the start of it."
          where={`AIRS's own Rendementen sheet — the same figure as the Return tile beside this, `
            + `over ${v(all.length - 1)} published points.`}
          when={`${v(data.return_from ?? anchor.date)} to ${v(last.date)}.`} />} />
      </div>
      <ResponsiveContainer width="100%" height={104}>
        <AreaChart data={rows} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          {/* ⚠⚠ A TIME AXIS, NOT A CATEGORY ONE, AND THE DIFFERENCE IS THE WHOLE SHAPE OF THE
              LINE. The points are irregular — AIRS publishes a month-end for each closed month and
              then a row per day the scrape ran — so on a categorical axis a five-month stretch of
              month-ends occupies as much of the chart as a fortnight of daily points, and a
              six-week gap is drawn the same width as an overnight one. Plotted against real time,
              distance means elapsed time everywhere. */}
          <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
            tickFormatter={tick} minTickGap={28}
            tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
          {/* ⚠⚠ ZERO IS ALWAYS IN THE DOMAIN, unlike the value chart this replaced. There the
              baseline was meaningless and an auto domain was the only way to see the shape; here
              zero is the reading — a curve floating entirely above or below a baseline that is off
              the plot says nothing about whether the book is up. */}
          <YAxis domain={[(min: number) => Math.min(0, min), (max: number) => Math.max(0, max)]}
            width={44} tick={{ fontSize: 11, fill: chartTheme.axisTick }}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
          <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
            labelStyle={{ color: chartTheme.axisLabel }}
            labelFormatter={(t) => (typeof t === 'number'
              ? new Date(t).toLocaleDateString('en-GB',
                { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })
              : String(t))}
            formatter={(v, _n, p) => {
              const row = p?.payload as
                { date?: string; value_eur?: number | null; holdings?: number | null } | undefined;
              const shown = pct(typeof v === 'number' ? v : null);
              // ⚠ THE ANCHOR SAYS WHAT IT IS. A 0.00% reading on a date nobody valued the book
              // reads as a flat day; it is the origin the rest of the line is measured from.
              if (row?.date === anchor.date) return [`${shown} · start of the year`, 'Return'];
              // ⚠ THE VALUE ONLY WHERE WE HOLD ONE. AIRS publishes returns for dates we have no
              // snapshot for, and `€—` beside a real percentage reads as a hole in our data
              // rather than as a month we simply were not scraping yet.
              return [row?.value_eur == null ? shown
                : `${shown} · ${eur(row.value_eur)} · ${row.holdings ?? '—'} holdings`, 'Return'];
            }} />
          {/* ⚠ THE BASELINE IS DRAWN, not just included in the domain. "Start at 0%" is the whole
              claim of this chart, and a gridline the reader has to identify is not the same as a
              rule they can see the line cross. */}
          <ReferenceLine y={0} stroke={chartTheme.axisTick} strokeOpacity={0.55} />
          {/* ⚠ THE FLOWS ARE STILL MARKED, AND THEY NO LONGER EXPLAIN A STEP. On the value chart
              they were the difference between a funding and a gain; here the line is already
              flow-aware, so what they say is "the book got bigger on this date" — which changes
              what a later percentage is a percentage OF. Marked, never narrated: the sentence the
              value chart needed above the plot would now be describing something the line does. */}
          {marks.map((f) => (
            <ReferenceLine key={f.date} x={ts(f.date)} stroke={chartTheme.warn}
              strokeDasharray="4 3" strokeOpacity={0.8}
              label={{ value: (f.deposits_eur ?? 0) >= (f.withdrawals_eur ?? 0) ? 'in' : 'out',
                position: 'insideTopLeft', fontSize: 10, fill: chartTheme.warn }} />
          ))}
          <Area dataKey="cum_pct" type="monotone" stroke={ink} strokeWidth={2}
            fill={ink} fillOpacity={0.08} dot={{ r: 1.5 }} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
