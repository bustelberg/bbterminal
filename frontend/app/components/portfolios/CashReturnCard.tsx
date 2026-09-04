'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { pairedSpan, RatioStats } from './CardStats';
import { workedMean } from './workedFormula';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import CashReturnInputsModal from './CashReturnInputsModal';
import { MODES, seriesByYear, type CapitalMode, type CashReturnInputs } from './cashReturnData';
import { paddedDomain , xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';
import CardHeading from './CardHeading';

/**
 * Cash-return-on-capital card: Free Cash Flow ÷ invested capital (non-current liabilities + total
 * equity) per fiscal year, on a LINEAR % axis (a ratio, not a compounding series — no log /
 * exponential trend). Click through to the three base lines per company.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`cashReturnByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./DebtRatioCard}.
 */

export default function CashReturnCard({ holdingsTarget, holdingsName, sbcCorrection = true, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** Tab-level toggle — see `sbcCorrection`. ⚠ Has NO effect in ROIC mode. */
  sbcCorrection?: boolean;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  const [data, setData] = useState<CashReturnInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);
  /** ⚠ ONE PAYLOAD, TWO MODES — the switch does NOT refetch. Both series come from the same
   *  response, so flipping cannot land you on a different vintage of the same company's accounts. */
  const [mode, setMode] = useState<CapitalMode>('croic');
  const M = MODES[mode];

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/cash-return-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as CashReturnInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const ratioByYr = useMemo(
    () => seriesByYear(data?.rows ?? [], mode, sbcCorrection), [data, mode, sbcCorrection]);

  const [benchData, benchErr] = useBenchInputs<CashReturnInputs>('cash-return-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? seriesByYear(benchData.rows, mode, sbcCorrection) : null), [benchData, mode, sbcCorrection]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(ratioByYr, benchByYr, 'ratio'), [ratioByYr, benchByYr]);

  const own = holdingsName ?? M.title;
  /**
   * The book's figures and the benchmark's, over the ONE window both lines cover — see
   * `CardStats`/`sharedSpan`. ⚠ COMPUTED ONCE: `own.avg` is BOTH the tile and the dashed average
   * line on the chart below, so the card cannot plot a mean it does not print.
   */
  const stats = useMemo(() => pairedSpan(ratioByYr, benchByYr), [ratioByYr, benchByYr]);
  const avg = stats.own.avg;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      {/* ⚠ EXACTLY ONE LINE TALL, LIKE EVERY SIBLING CARD'S BARE <h4>. These cards sit in a grid
          and the eye reads their stat tiles as a row; a header that wraps to two lines pushes this
          card's tiles down and breaks that alignment for a decoration. Hence `flex-nowrap`, no
          subtitle, and a switch shorter than the heading's own line box — the provenance line that
          used to live here is in the tiles' info card (`M.where`) instead.
          Rendered even on the empty state: a bank has a published ROIC and no usable capital base,
          so "no figures" on one mode must not look like "no figures for this company". */}
      <div className="flex items-center justify-between gap-2 flex-nowrap">
        {/* `min-w-0` is what lets `truncate` actually shrink — a flex item defaults to
            min-width:auto and would push the switch off the card instead of ellipsising. */}
        <CardHeading chartKey={mode} className="truncate min-w-0" />
        <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden shrink-0"
          role="group" aria-label="Capital-return basis">
          {(Object.keys(MODES) as CapitalMode[]).map((k) => (
            <button key={k} type="button" onClick={() => setMode(k)} aria-pressed={mode === k}
              title={`${MODES[k].title} — ${MODES[k].what}. ${MODES[k].where}`}
              className={`px-2.5 py-0.5 text-[12px] font-medium transition-colors ${
                mode === k ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
              {MODES[k].tab}
            </button>
          ))}
        </div>
      </div>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : ratioByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">
          {M.derived
            ? 'No FCF / capital figures ingested to compute a ratio.'
            : 'GuruFocus reports no ROIC for these holdings.'}
        </p>
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what={`Average ${M.inline} over the years shown — ${M.what}.`}
              where={M.where}
              when="The years on the chart. Weight-averaged across holdings — a per-company percentage, averaged, never summed (mixed currencies cannot be added)."
              worked={workedMean(stats.own.values)}
              how={M.caveat} />} />} />

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: M.derived ? 'pointer' : 'default' }}
                onClick={() => { if (M.derived) setShowInputs(true); }}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis(xToPeriod)} />
                <YAxis domain={paddedDomain(withBench(ratioByYr.values(), benchByYr))} tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, n === 'bench' ? (benchTarget?.label ?? 'Benchmark') : own]} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="ratio" name="ratio" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              {/* ⚠ ONE LEGEND ROW, LIKE THE SIBLINGS. The drill-down note is a `title`, not a
                  second line — the same alignment argument as the header. It still has to be said
                  somewhere: a published ratio has no three lines to check it against, so the chart
                  is not clickable in ROIC mode and a modal must not imply workings it cannot show. */}
              <LegendItem color={chartTheme.accent} label={own}
                title={M.derived
                  ? 'Derived here — click the chart for the three underlying lines per company.'
                  : "GuruFocus's own figure, read through. There are no underlying lines to drill into."} />
              {avg != null && <LegendItem color={chartTheme.accent} stroke="dashed"
                label={`${own} average`} />}
              {benchByYr && <LegendItem color={chartTheme.pos} label={benchTarget?.label} />}
              {note && (
                <span className="text-fg-faint" title="An overlay that simply does not appear is indistinguishable from an index that matches this book exactly. Full detail is in the console.">
                  {note}
                </span>
              )}
            </div>
          </div>
        </>
      )}

      {showInputs && (
        <CashReturnInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
