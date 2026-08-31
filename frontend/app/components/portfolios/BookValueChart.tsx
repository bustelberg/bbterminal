'use client';

/**
 * THE BOOK'S VALUE THROUGH TIME — the recorded series, not a reconstruction.
 *
 * ⚠⚠ IT IS SUMMED FROM OUR OWN SNAPSHOTS (`airs_holding`), which is the whole point of it: every
 * scrape stores one row per holding with its EUR value, so the book's value on that date is their
 * sum. Measured on AzTopSelectie_DYN, it reproduces AIRS's own `eindvermogen` to the euro on 21 of
 * 24 dates, and holds two dates AIRS's own sheet has no row for at all.
 *
 * ⚠ THE ⓘ CARD IS FOUR SHORT FIELDS AND A TYPESET SUM — no ⚠ blocks, on request. Everything the
 * removed prose said is either visible (the flow markers, the dated axis) or is in this file, which
 * is where a reader who needs the reasoning is better served than a tooltip could serve them.
 *
 * ⚠⚠ VALUE IS NOT RETURN, AND ON THIS DATA THAT IS NOT A QUIBBLE. AzTopSelectie goes from 0 to
 * EUR 1,000,000 on 2026-06-30 because it was FUNDED that day. Drawn without its flows the line says
 * the book made 100% in a session — so deposits and withdrawals are marked, the axis is money
 * rather than an index, and the sentence above the chart says so in words. The book's RETURN is the
 * Scorecard directly above this.
 *
 * ⚠⚠ AND BEFORE OUR FIRST SNAPSHOT IT IS AIRS'S OWN MONTH-END VALUE. Our snapshots
 * begin 2026-06-23 at the earliest and on two books 2026-07-30 — which is what a reader sees as
 * "why does this start in August?" while AIRS's stored sheet has held month-ends since 2026-01-31
 * (AITopSelectie: 1,044,066 in January against 1,353,619 today). Refusing six months of history we
 * already store, to keep the series pure, answers a question nobody asked — but the two are
 * different measurements of one quantity, and that is said in the tooltip, in the ⓘ card and in the
 * payload's per-point `source` — NOT in the line, which is a single solid stroke, and no longer in
 * a sentence under it either. It was drawn as two (a second colour, then a dash) and both read as a second SERIES being
 * compared, which is a claim about the book that nothing here is making.
 *
 * ⚠ IT IS NOT THE DRAWDOWN PANEL'S SERIES AND MUST NOT BE READ AGAINST IT. That one rebuilds a
 * daily curve from the holdings as they stand today — look-ahead and survivorship included, as it
 * says — over years. This is what the book was actually worth, on the days we have, since
 * 2026-06-23. Different objects, which is why this sits at the top of the modal rather than beside
 * it.
 *
 * ⚠ ITS OWN REQUEST. The Analyse modal is ONE payload with no partial paint, so its wall clock is
 * the reader's wait; a chart nobody has scrolled to yet does not belong in it. This fetches itself,
 * exactly as the Risk panels do.
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
import InfoTip from '../InfoTip';
import { traceError } from '../../../lib/debugTrace';
import { lastPerMonth } from './monthEnds';
import { texEscape, withWorked } from './workedFormula';
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

/**
 * THE SUM, TYPESET — the ⓘ's formula half.
 *
 * ⚠ WORDS INSIDE `\text{}`, NOT SYMBOLS. `V_d = Σᵢ v_{i,d}` needs a legend to say what V and v
 * are; spelled out it needs none, and this card is meant to be read at a glance rather than
 * decoded.
 */
const SUM_TEX = String.raw`\text{book value}(d) = \sum_{\text{holdings}} \text{value}_i(d)`;

/**
 * The same expression with this book's newest point in it.
 *
 * ⚠⚠ THE THOUSANDS SEPARATORS ARE `{,}`. A bare comma in maths mode is PUNCTUATION and KaTeX sets
 * a space after it, so `1,353,619` renders as three numbers in a list.
 *
 * ⚠ THE HOLDING COUNT ONLY WHERE THERE IS ONE. An AIRS month-end is a total with no positions
 * behind it (`holdings: null`), and `(0 holdings)` beside a seven-figure sum would be a claim
 * that the book is empty.
 *
 * ⚠ '' WHEN THERE IS NOTHING TO SUBSTITUTE, which `withWorked` collapses to the formula alone —
 * the rule every worked line in this app follows.
 */
function workedTotal(p: { date: string; value_eur: number; holdings?: number | null }): string {
  if (p.value_eur == null) return '';
  const amount = Math.round(p.value_eur).toLocaleString('en-US').replace(/,/g, '{,}');
  const n = p.holdings;
  return String.raw`\text{book value}(\text{${texEscape(p.date)}}) = \text{EUR}\,${amount}`
    + (n ? String.raw` \quad (${n}\text{ holdings})` : '');
}

export default function BookValueChart({ portfolioId }: { portfolioId: number }) {
  const [data, setData] = useState<BookValueSeries | null>(null);
  const [err, setErr] = useState<string | null>(null);
  /**
   * ⚠⚠ ONE POINT A MONTH BY DEFAULT — see `monthEnds`. The scrape runs most working days, so the
   * recent half of this line is forty-odd points against the earlier half's six, and the chart
   * reads as two different things joined in the middle. A month-end series is the same shape at a
   * resolution the whole span can be drawn at.
   *
   * ⚠ EVERY SNAPSHOT IS STILL ONE CLICK AWAY, and it has to be: the thinning is a choice about
   * legibility, and the day-to-day movement it hides is real. Nothing is averaged either way —
   * both views plot observations on their own dates.
   */
  const [dense, setDense] = useState(false);

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
  }, [portfolioId]);

  const all = data?.points ?? [];
  const points = dense ? all : lastPerMonth(all);
  if (err) return <p className="text-[12px] text-neg-300">{err}</p>;
  if (!data) return <p className="text-[12px] text-fg-subtle">Loading the book’s value…</p>;
  // ⚠ TWO POINTS IS THE FLOOR, NOT ONE. A single snapshot is a dot, and a line drawn through it
  // states a shape the data does not have. An unpaired model has no book at all — `reason` says so.
  // ⚠ ON `all`, NOT ON THE THINNED VIEW. Two snapshots inside one month thin to a single point,
  // and refusing to draw then would hide a book we do have a series for behind "only one snapshot".
  if (all.length < 2) {
    return (
      <p className="text-[12px] text-fg-faint">
        {data.reason ?? (all.length
          ? 'Only one snapshot so far — the line starts once there are two.'
          : 'No stored snapshots for this book yet.')}
      </p>
    );
  }

  // ⚠ THE SPAN COMES FROM `all`, NEVER FROM THE THINNED VIEW. Toggling a display resolution must
  // not move a reported figure — the header names the period we HOLD, and the headline value is the
  // newest observation, whichever set of points is on screen.
  const first = all[0];
  const last = all[all.length - 1];
  /**
   * ⚠ THE ROWS EXIST ONLY TO CARRY A TIMESTAMP — the axis is real time, and `date` is a string.
   *
   * ⚠⚠ IT WAS TWO SERIES AND IS NOW ONE. The stretch before our first snapshot came from AIRS's own
   * month-ends and was drawn dashed to say so; on request, twice, that distinction has come off the
   * line. It is one quantity — what the book was worth — and two ways of having measured it is a
   * fact about provenance rather than about the book. It survives where it belongs: in the tooltip
   * (`AIRS month-end` against `21 holdings`), in the ⓘ card's `where`, and in the payload's
   * per-point `source`. What it no longer does is fragment the shape the chart exists to show, or
   * spend two lines of chrome under a 104px plot saying so.
   */
  const rows = points.map((p) => ({ ...p, t: ts(p.date) }));
  // ⚠ ONLY THE FLOWS INSIDE THE PLOTTED SPAN. `airs_performance` reaches back further than our
  // snapshots do, and a marker on a date the axis does not carry lands at the wrong x.
  const marks = (data.flows ?? []).filter((f) => f.date >= first.date && f.date <= last.date);
  const netFlow = marks.reduce((s, f) => s + (f.deposits_eur ?? 0) - (f.withdrawals_eur ?? 0), 0);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-3">
      <div className="flex items-baseline gap-2 flex-wrap mb-1">
        <span className="text-[11px] uppercase tracking-wider text-fg-faint">Book value</span>
        <span className="font-mono text-fg-strong">{eur(last.value_eur)}</span>
        <span className="text-[11px] text-fg-faint">
          {tick(ts(first.date))} – {tick(ts(last.date))} · {all.length} snapshots
        </span>
        {/* ⚠ THE CONTROL NAMES WHAT IT WILL SHOW, not what is showing — a button labelled with the
            current state reads as a status and gets clicked to "confirm" it. */}
        <button type="button" onClick={() => setDense((v) => !v)}
          title={dense
            ? 'Show one point per month — the last we hold in each'
            : 'Show every snapshot we hold, most working days'}
          className="text-[11px] leading-none px-1.5 py-1 rounded border border-neutral-800/40
                     text-fg-subtle hover:text-accent-300 hover:bg-overlay/5 cursor-pointer">
          {dense ? 'Monthly' : 'All points'}
        </button>
        <InfoTip className="ml-auto" content={<AspectCard
          what="What this book has actually been worth, on every date we hold a snapshot for."
          where={data.own_from && data.own_from !== first.date
            ? `Our own stored holdings from ${data.own_from}; AIRS's month-end value before that.`
            : 'Our own stored holdings — one row per position per scrape.'}
          when={`${first.date} to ${last.date}, the dates we hold.`}
          how="Value, not return: a deposit or a withdrawal moves it too, and those dates are marked."
          worked={withWorked(SUM_TEX, workedTotal(last))} />} />
      </div>
      {/* ⚠ THE FLOWS ARE STATED IN WORDS AS WELL AS MARKED. A dashed line is noticed by somebody
          already studying the chart; a reader who takes the headline figure and moves on needs the
          sentence — and on a funded book the sentence is most of the explanation. */}
      {netFlow !== 0 && (
        <p className="text-[11px] text-warn-300 mb-1">
          {`⚠ ${eur(Math.abs(netFlow))} ${netFlow > 0 ? 'paid in' : 'taken out'} over this span `}
          {`(${marks.length} ${marks.length === 1 ? 'date' : 'dates'}, marked) — the line moves `}
          {'with the money as well as with the markets.'}
        </p>
      )}
      <ResponsiveContainer width="100%" height={104}>
        <AreaChart data={rows} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          {/* ⚠⚠ A TIME AXIS, NOT A CATEGORY ONE, AND THE DIFFERENCE IS THE WHOLE SHAPE OF THE
              LINE. The points are irregular — a snapshot exists only for a day the scrape ran AND
              AIRS had valued the book — so on a categorical axis a six-week gap and an overnight
              one are drawn the same width, and the earlier AIRS month-ends (one a month) occupy as
              much of the chart as a fortnight of daily points. Plotted against real time, distance
              means elapsed time everywhere and the gaps are visible as gaps. */}
          <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
            tickFormatter={tick} minTickGap={28}
            tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
          {/* ⚠ NOT ANCHORED AT ZERO. A book moves a few percent over ten weeks, and a zero-based
              axis renders that as a flat line — the shape is the one thing this chart is for. The
              tooltip carries the absolute figure, so nothing is hidden by the choice. */}
          <YAxis domain={['auto', 'auto']} width={64}
            tick={{ fontSize: 11, fill: chartTheme.axisTick }}
            tickFormatter={(v: number) => `${Math.round(v / 1000).toLocaleString('en-US')}k`} />
          <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle}
            labelStyle={{ color: chartTheme.axisLabel }}
            labelFormatter={(t) => (typeof t === 'number'
              ? new Date(t).toLocaleDateString('en-GB',
                { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' })
              : String(t))}
            formatter={(v, _n, p) => {
              const row = p?.payload as { holdings?: number | null; source?: string } | undefined;
              // ⚠ THE COUNT ONLY WHERE THERE IS ONE. An AIRS point is a total with no positions
              // behind it, and "— holdings" beside it reads as a hole in our data rather than as
              // a different source.
              return [row?.source === 'airs'
                ? `${eur(typeof v === 'number' ? v : null)} · AIRS month-end`
                : `${eur(typeof v === 'number' ? v : null)} · ${row?.holdings ?? '—'} holdings`,
              'Value'];
            }} />
          {marks.map((f) => (
            <ReferenceLine key={f.date} x={ts(f.date)} stroke={chartTheme.warn}
              strokeDasharray="4 3" strokeOpacity={0.8}
              label={{ value: (f.deposits_eur ?? 0) >= (f.withdrawals_eur ?? 0) ? 'in' : 'out',
                position: 'insideTopLeft', fontSize: 10, fill: chartTheme.warn }} />
          ))}
          {/* ⚠ ONE LINE, ONE COLOUR, NO DASH — see the note on `rows`. Where the figures came
              from is said in words and in the tooltip; it is not a second series. */}
          <Area dataKey="value_eur" type="monotone" stroke={chartTheme.accent} strokeWidth={2}
            fill={chartTheme.accent} fillOpacity={0.08} dot={{ r: 1.5 }} />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
