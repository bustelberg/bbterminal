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
import { workedMean } from './workedFormula';
import { LegendItem } from './ChartLegend';
import { type Target } from './HoldingsRevenueModal';
import GrossMarginInputsModal from './GrossMarginInputsModal';
import { grossMarginByYear, type GrossMarginInputs } from './grossMarginData';
import { paddedDomain , xToPeriod } from './marginData';
import { periodAxis } from '../../../lib/chartAxis';
import { benchNote, benchmarkFirst, mergeSeries, useBenchInputs, withBench, type BenchTarget } from './benchSeries';

/**
 * Gross-margin card: Gross Profit ÷ Revenue per fiscal year, on a LINEAR % axis (a ratio, not a
 * compounding series — no log / exponential trend). What is left of each sale after the direct
 * cost of making it — the cleanest read on pricing power. Click through to the two base lines.
 *
 * ⚠ A BANK HAS NO GROSS MARGIN, AND THE CARD MUST SHOW A HOLE RATHER THAN A ZERO. GuruFocus's 'B'
 * industry template has no cost of goods sold, so the line is absent (JPMorgan) — the concept does
 * not apply. A 0 would draw a company selling at cost. Where too much of a book is like that the
 * year is dropped entirely (`MIN_YEAR_COVERAGE_PCT`), rather than publishing a "gross margin" that
 * silently describes only the half that has one.
 *
 * ⚠ DERIVED, THOUGH GURUFOCUS ALSO PUBLISHES `Ratios__Gross Margin %`. It reproduces their figure
 * exactly (ASML 2025: 17,258/32,667.3 = 52.83% vs published 52.83; Apple 46.91 vs 46.905) and
 * leaves two lines the drill-down can show — a published ratio has no workings to check.
 *
 * ⚠ THE RATIO IS DERIVED HERE from the raw lines (`grossMarginByYear`), so the line, the tiles and
 * the drill-down are one computation. Aggregation is a weight-weighted average of per-company
 * ratios — currency-safe, unlike summing mixed-currency amounts. Mirrors {@link ./SbcOcfCard}.
 */

export default function GrossMarginCard({ holdingsTarget, holdingsName, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null;
  /** The index drawn beside the book — same endpoint, same helper. See `benchSeries`. */
  benchTarget?: BenchTarget | null;
}) {
  // ⚠ READ FROM THE STORE, NOT DRILLED THROUGH `LongEquityTab` AS A PROP. Fourteen sibling
  // cards would mean fourteen chances to forget one, and a card left on English would look
  // like a missing translation rather than a missing prop. `useLang` is an external store
  // (see `lib/i18n.ts`), so every card reads the one value directly.
  const [lang] = useLang();
  const [data, setData] = useState<GrossMarginInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      try {
        const r = await apiFetch(`${API_URL}/api/earnings/gross-margin-inputs`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(holdingsTarget),
        });
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as GrossMarginInputs);
      } catch (e) {
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [holdingsTarget]);

  const marginByYr = useMemo(() => grossMarginByYear(data?.rows ?? []), [data]);

  const [benchData, benchErr] = useBenchInputs<GrossMarginInputs>('gross-margin-inputs', benchTarget);
  const benchByYr = useMemo(
    () => (benchData ? grossMarginByYear(benchData.rows) : null), [benchData]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(
    () => mergeSeries(marginByYr, benchByYr, 'margin'), [marginByYr, benchByYr]);

  const own = holdingsName ?? 'Gross margin';
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
      <h4 className="text-base font-semibold text-fg-strong">{chartTitle(lang, 'grossMargin')}</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : marginByYr.size === 0 ? (
        <p className="text-[12px] text-fg-faint py-16 text-center">No gross-profit / revenue figures ingested to compute a margin.</p>
      ) : (
        <>
          <RatioStats stats={stats} benchLabel={benchTarget?.label} fmt={pct}
            avgInfo={<InfoTip content={<AspectCard
              what="Average gross margin over the years shown — what is left of each sale after the direct cost of making it."
              where="Computed here — Gross Profit ÷ Revenue per year, weight-averaged across holdings. Reproduces GuruFocus's own `Gross Margin %` exactly, but leaves the two lines visible in the drill-down."
              when="The years on the chart."
              worked={workedMean(stats.own.values)}
              // ⚠ THIS LINE USED TO BE THE CAPEX CARD'S, PASTED IN — "the share of each sales-euro
              // reinvested in property, plant & intangibles. Lower = more capital-light". That is
              // capital intensity, and it reads the metric BACKWARDS: on gross margin, higher is
              // better. It survived because the sentence is fluent and plausible, which is exactly
              // how a wrong tooltip survives.
              how="Higher = more pricing power, or a mix tilted towards software and services; a commoditised or resale-heavy business reads low. ⚠ A BANK HAS NO GROSS PROFIT LINE AT ALL — GuruFocus's bank template reports net interest income instead — so a book with banks in it is averaged over the rest." />} />} />

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
        <GrossMarginInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.label ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
