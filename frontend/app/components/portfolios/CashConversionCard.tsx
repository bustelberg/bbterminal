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
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import CashConversionInputsModal from './CashConversionInputsModal';
import { cashConversionByYear, type CashConversionInputs } from './cashConversionData';
import { paddedDomain , xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * Cash-conversion card: Free Cash Flow ÷ Net Income per fiscal year, on a LINEAR % axis. Whether
 * the reported profit turns into money — profit you cannot bank is an opinion about revenue
 * recognition. Click through to the two base lines per company.
 *
 * ⚠ 100% IS NOT THE CEILING. Depreciation ahead of capex converts more cash than the accounts book
 * as profit, so a durable reading above 100% is a compliment (ASML 2025: 11,027.3 / 9,609.4 =
 * 114.8%; Apple 88.2%). The reference line sits at 100 because that is the break-even, not the max.
 *
 * ⚠ A LOSS HAS NO CONVERSION. Net income ≤ 0 is a hole, never a negative percentage: a loss-maker
 * with positive cash flow would read as burning cash, and two companies could show −80% for
 * opposite reasons. A negative FCF against POSITIVE earnings IS kept — that is the finding.
 *
 * ⚠ SCOPE-MISMATCHED BY CONSTRUCTION, deliberately: FCF is whole-company cash while Net Income is
 * the SHAREHOLDERS' line, so a group with large minorities reads high (Mitsui: 34,378 vs 46,910).
 * The alternative would mismatch EPS and every other card on this tab — see `_METRIC_CODES`.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`cashConversionByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./SbcOcfCard}.
 */

export default function CashConversionCard({ holdingsTarget, holdingsName, sbcCorrection = true, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** Tab-level toggle — see `sbcCorrection`. */
  sbcCorrection?: boolean;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  // ⚠ READ FROM THE STORE, NOT DRILLED THROUGH `LongEquityTab` AS A PROP — see the same
  // note on the sibling cards. `useLang` is an external store (`lib/i18n.ts`).
  const [lang] = useLang();
  const [data, setData] = useState<CashConversionInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/cash-conversion-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as CashConversionInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const marginByYr = useMemo(
    () => cashConversionByYear(data?.rows ?? [], sbcCorrection), [data, sbcCorrection]);

  const [benchData, benchErr] = useBenchInputs<CashConversionInputs>('cash-conversion-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? cashConversionByYear(benchData.rows, sbcCorrection) : null), [benchData, sbcCorrection]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(marginByYr, benchByYr, 'margin'), [marginByYr, benchByYr]);

  const own = holdingsName ?? 'FCF / Net Income';
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
      <h4 className="text-base font-semibold text-fg-strong">
        {chartTitle(lang, 'cashConversion', sbcCorrection)}
      </h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : marginByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No FCF / net-income figures ingested to compute a conversion.</p>
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what="Average FCF ÷ Net Income over the years shown — how much of the reported profit turned into cash."
              where="Computed here — Free Cash Flow ÷ Net Income per year, weight-averaged across holdings. ⚠ FCF is whole-company cash while Net Income is the SHAREHOLDERS' line, so a group with large minorities reads high."
              when="The years on the chart."
              how="⚠ 100% IS BREAK-EVEN, NOT A CEILING — above it the business converts more cash than it books as profit (depreciation ahead of capex), which is a compliment. Persistently below it means the earnings are not turning into money. A LOSS has no conversion at all, so that year is a hole rather than a negative percentage." />} />} />

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
                {/* ⚠ 100 IS THE MEANINGFUL LINE ON THIS CHART, NOT 0. Crossing it is the event —
                    profit converting to cash or not — whereas 0 only matters in the rare year FCF
                    goes negative. Drawn in recessive grey: it is a reference, not a series. */}
                <ReferenceLine y={100} stroke={chartTheme.axisTick} strokeDasharray="2 4" strokeOpacity={0.5} />
                {avg != null && <ReferenceLine y={avg} stroke={chartTheme.accent} strokeDasharray="5 3" strokeOpacity={0.6} />}
                <Line dataKey="margin" name="margin" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <LegendItem color={chartTheme.accent} label={own} />
              {avg != null && <LegendItem color={chartTheme.accent} stroke="dashed"
                label={`${own} average`} />}
              {/* ⚠ ITS OWN ENTRY, because it is its own line — and the one a reader is most
                  likely to misread: 100% is where profit converts fully into cash, so CROSSING it
                  is the event this chart exists to show. Packed into the series entry as the words
                  "100% dotted", beside a solid blue swatch, it named a mark that appears nowhere. */}
              <LegendItem color={chartTheme.axisTick} stroke="dotted" label="100% — full conversion"
                title="Profit converting fully into cash. Above this line is better, not an error." />
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
        <CashConversionInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
