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
import { useLang } from '../../../lib/i18n';
import { chartTitle } from './longEquityCopy';
import { pairedSpan, RatioStats } from './CardStats';
import { withWorked, workedMean } from './workedFormula';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import CapexMarginInputsModal from './CapexMarginInputsModal';
import { capexMarginByYear, type CapexMarginInputs } from './capexMarginData';
import { paddedDomain , xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * Capex-margin card: |Capex| ÷ Revenue per fiscal year, on a LINEAR % axis (a ratio, not a
 * compounding series — no log / exponential trend). Capital intensity: the share of sales
 * reinvested in capex. Lower = more capital-light. Click through to the two base lines per company.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`capexMarginByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./SbcOcfCard}.
 */

/** ⚠ `String.raw`, or every backslash in the expressions below is eaten before KaTeX
 *  sees it. */
const R = String.raw;

export default function CapexMarginCard({ holdingsTarget, holdingsName, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  // ⚠ READ FROM THE STORE, NOT DRILLED THROUGH `LongEquityTab` AS A PROP. Fourteen sibling
  // cards would mean fourteen chances to forget one, and a card left on English would look
  // like a missing translation rather than a missing prop. `useLang` is an external store
  // (see `lib/i18n.ts`), so every card reads the one value directly.
  const [lang] = useLang();
  const [data, setData] = useState<CapexMarginInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/capex-margin-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as CapexMarginInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const marginByYr = useMemo(() => capexMarginByYear(data?.rows ?? []), [data]);

  const [benchData, benchErr] = useBenchInputs<CapexMarginInputs>('capex-margin-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? capexMarginByYear(benchData.rows) : null), [benchData]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(marginByYr, benchByYr, 'margin'), [marginByYr, benchByYr]);

  const own = holdingsName ?? 'Capex margin';
  /**
   * The book's figures and the benchmark's, over the ONE window both lines cover — see
   * `CardStats`/`sharedSpan`. ⚠ COMPUTED ONCE: `own.avg` is BOTH the tile and the dashed average
   * line on the chart below, so the card cannot plot a mean it does not print.
   */
  const stats = useMemo(() => pairedSpan(marginByYr, benchByYr), [marginByYr, benchByYr]);
  const avg = stats.own.avg;
  const pct = (v: number | null) => (v == null ? '—' : `${v.toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">{chartTitle(lang, 'capexMargin')}</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : marginByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No capex / revenue figures ingested to compute a margin.</p>
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what="Average capex margin (capital intensity) over the years shown."
              where="Computed here, per year, then weight-averaged across holdings."
              when="The years on the chart."
              worked={withWorked(
                R`\text{capex margin} = \dfrac{|\text{capex}|}{\text{revenue}}`,
                workedMean(stats.own.values))}
              how="The share of each sales-euro reinvested in property, plant & intangibles. Lower = more capital-light (asset-heavy businesses read high)." />} />} />

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis {...periodAxis(xToPeriod)} />
                <YAxis domain={paddedDomain(withBench(marginByYr.values(), benchByYr))} tick={{ fontSize: 12, fill: chartTheme.axisTick }} width={48}
                  tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }} itemSorter={benchmarkFirst}
                  formatter={(v, n) => [`${typeof v === 'number' ? v.toFixed(1) : '—'}%`, n === 'bench' ? (benchTarget?.label ?? 'Benchmark') : own]} />
                <ReferenceLine y={0} stroke={chartTheme.zeroLine} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="margin" name="margin" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <LegendItem color={chartTheme.accent} label={own} />
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
        <CapexMarginInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
