'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  CartesianGrid, ComposedChart, Line, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { logLinearFit } from '../../../lib/trendFit';
import { AspectCard } from '../../../lib/tipCard';
import InfoTip from '../InfoTip';
import { Stat } from './MetricGrowthCard';
import { type Target } from './HoldingsRevenueModal';
import InvestedCapitalInputsModal from './InvestedCapitalInputsModal';
import { investedCapitalIndexByYear, investedCapitalSeries } from './investedCapitalData';
import { paddedLogDomain , xToPeriod } from './marginData';
import { benchNote, rebaseSeries, useBenchInputs, type BenchTarget } from './benchSeries';
import { type CashReturnInputs } from './cashReturnData';

/**
 * Invested-capital card: non-current liabilities + total equity per fiscal year — the SAME base
 * the Cash-return card divides FCF by, computed from the two raw lines `cash-return-inputs` already
 * returns. A currency LEVEL on a LOG axis with an exponential-trend R²/CAGR, exactly like Revenue.
 * A single company shows its absolute figure; a portfolio shows a blended GROWTH INDEX (currency
 * levels can't be summed). Click through to the two components + their sum per company.
 */

export default function InvestedCapitalCard({ holdingsTarget, holdingsName, isAgg, benchTarget }: {
  holdingsTarget: Target; holdingsName?: string | null; isAgg: boolean;
  /**
   * The index drawn beside the book — same endpoint, same helper. See `benchSeries`.
   *
   * This used to read "the only card that cannot always draw it": for a portfolio the chart was a
   * growth INDEX an index could sit beside honestly, while for a SINGLE COMPANY it was a currency
   * LEVEL, and an index at 100 next to EUR millions is two scales on one axis — the dual-axis
   * mistake with the second axis hidden.
   *
   * ⚠ THAT NO LONGER APPLIES, because BOTH lines are now indexed to 100 on their shared anchor
   * (see `rebaseSeries` below) and the actual amounts moved to the hover. The two are on one
   * honest axis in either mode, so the benchmark draws for a single company too.
   */
  benchTarget?: BenchTarget | null;
}) {
  const [data, setData] = useState<CashReturnInputs | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [showInputs, setShowInputs] = useState(false);

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

  const { points, currency, isIndex } = useMemo(() => {
    const rows = data?.rows ?? [];
    if (isAgg) {
      const m = investedCapitalIndexByYear(rows);
      return { points: [...m].map(([year, value]) => ({ year, value })).sort((a, b) => a.year - b.year), currency: null as string | null, isIndex: true };
    }
    const r = rows.find((x) => investedCapitalSeries(x).size > 0) ?? rows[0];
    const m = r ? investedCapitalSeries(r) : new Map<number, number>();
    return { points: [...m].map(([year, value]) => ({ year, value })).sort((a, b) => a.year - b.year), currency: r?.currency ?? null, isIndex: false };
  }, [data, isAgg]);

  const [benchData, benchErr] = useBenchInputs<CashReturnInputs>('cash-return-inputs', benchTarget);
  /** The index's own blended series, left RAW — `rebaseSeries` below indexes both lines together.
   *  It used to be pre-scaled onto ours via `rebaseOnto`; doing both would transform it twice. */
  const benchByYr = useMemo(
    () => (benchData ? investedCapitalIndexByYear(benchData.rows) : null), [benchData]);

  const fit = useMemo(() => logLinearFit(points), [points]);

  /** ⚠⚠ INDEXED AXIS, ACTUAL HOVER — the same rule as the three growth cards, and for the same
   *  reason: invested capital is a LEVEL, so a company's EUR base and a blended index cannot share
   *  a raw axis. Both are rebased to 100 on the first year they share with positive values; the
   *  real EUR amount rides along for the tooltip, because "this company deploys EUR X of capital"
   *  is the fact that makes CROIC's denominator interpretable. Refuses (→ absolute) rather than
   *  anchoring on a zero or negative base. */
  const ownByYr = useMemo(
    () => new Map(points.map((p) => [p.year, p.value as number | null])), [points]);
  const indexed = useMemo(() => rebaseSeries(ownByYr, benchByYr), [ownByYr, benchByYr]);

  const note = benchNote(benchTarget, benchData, benchErr, benchByYr)
    ?? (benchByYr && !indexed
      ? 'No year in common with a positive value — showing absolute, not indexed'
      : null);

  const chartData = useMemo(() => {
    const trendByYear = new Map(fit.trend.map((t) => [t.year, t.value]));
    const plotOwn = indexed?.own ?? ownByYr;
    const plotBench = indexed ? indexed.bench : benchByYr;
    // The trend is fitted on the RAW series, so it takes the same multiplier as its own line.
    const trendScale = indexed ? 100 / (ownByYr.get(indexed.anchor) as number) : 1;
    const years = new Set<number>(points.map((p) => p.year));
    if (plotBench) for (const y of plotBench.keys()) years.add(y);
    return [...years].sort((a, b) => a - b).map((year) => {
      const v = plotOwn.get(year) ?? null;
      const b = plotBench ? plotBench.get(year) ?? null : null;
      const t = trendByYear.get(year);
      return {
        year,
        value: v != null && v > 0 ? v : null,
        trend: t != null ? t * trendScale : null,
        bench: b != null && b > 0 ? b : null,
        // Tooltip only, and ONLY for a single company: in `isAgg` mode `points` is already a
        // blended index (there is no portfolio capital base), so its "raw" is just another index.
        // The benchmark is always a blend, so it never has one either.
        rawValue: isIndex ? null : (ownByYr.get(year) ?? null),
      };
    });
  }, [points, fit, indexed, ownByYr, benchByYr, isIndex]);
  const logDomain = useMemo(() =>
    paddedLogDomain(chartData.flatMap((d) => [d.value, d.trend, d.bench]).filter((v): v is number => v != null)),
  [chartData]);

  /** ⚠ AN INDEX IS A BARE NUMBER — see the same guard in `MetricGrowthCard`. `fmt` scales to
   *  M/B/T, so left to it an index of 100 renders as "100M" and reads as an amount. */
  const fmtIndex = (v: number | null | undefined, dp = 0) => (v == null ? '—' : v.toFixed(dp));

  const fmt = (v: number | null | undefined) => {
    if (v == null) return '—';
    if (isIndex) return v.toFixed(1);            // blended growth index (no currency)
    const a = Math.abs(v);
    if (a >= 1e6) return `${(v / 1e6).toFixed(2)}T`;
    if (a >= 1e3) return `${(v / 1e3).toFixed(1)}B`;
    return `${v.toFixed(0)}M`;
  };
  const ccy = !isIndex && currency ? `${currency} ` : '';
  const cagr = (v: number | null) => (v == null ? '—' : `${v >= 0 ? '+' : ''}${(v * 100).toFixed(1)}%`);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-4 space-y-3 min-w-0">
      <h4 className="text-base font-semibold text-fg-strong">Invested capital</h4>

      {data == null && !err ? (
        <p className="text-xs text-fg-subtle py-16 text-center">Loading…</p>
      ) : err ? (
        <p className="text-xs text-neg-300 py-16 text-center">{err}</p>
      ) : points.length === 0 ? (
        <p className="text-[11px] text-fg-faint py-16 text-center">No invested-capital figures ingested for this {isAgg ? 'portfolio' : 'company'}.</p>
      ) : (
        <>
          <div className="flex flex-wrap gap-2">
            <Stat label="R²" value={fit.r2 == null ? '—' : fit.r2.toFixed(2)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what="How tightly invested capital hugs a constant-growth line (0–1)."
                where="Computed here — a log-linear regression on the points below."
                when={`Over the ${fit.n} year(s) shown.`}
                how="Invested capital = non-current liabilities + total equity — the long-term capital funding the business, and the base the Cash-return card divides FCF by." />} />} />
            <Stat label="CAGR" value={cagr(fit.cagr)} color={chartTheme.accent}
              info={<InfoTip content={<AspectCard
                what="The compound annual growth rate of the fitted trend."
                where="Computed here from the same fit." when={`Over the ${fit.n} year(s) shown.`}
                how="e^(slope) − 1 of the log-linear regression. Rising = the business is soaking up more capital over time." />} />} />
          </div>

          <div>
            <ResponsiveContainer width="100%" height={320}>
              <ComposedChart data={chartData} margin={{ top: 5, right: 12, bottom: 5, left: 4 }}
                style={{ cursor: 'pointer' }} onClick={() => setShowInputs(true)}>
                <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
                <XAxis dataKey="year" tickFormatter={xToPeriod} tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
                <YAxis scale="log" domain={logDomain ?? ['dataMin', 'dataMax']} allowDataOverflow
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }}
                  tickFormatter={(v: number) => (indexed ? fmtIndex(v) : fmt(v))} width={60} />
                {/* ⚠ THE HOVER READS THE ACTUAL AMOUNT, NOT THE AXIS — see `MetricGrowthCard`. The
                    plotted number is an index; the EUR figure rides on the row and is what gets
                    shown, with the index in parentheses so the point stays identifiable. A blend
                    has no actual amount (there is no portfolio capital base), so it shows the
                    index alone. */}
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v, name, item) => {
                    const row = item?.payload as { rawValue?: number | null } | undefined;
                    const plotted = typeof v === 'number' ? v : null;
                    const label = name === 'trend' ? 'Trend'
                      : name === 'bench' ? (benchTarget?.universe ?? 'Benchmark') : 'Invested capital';
                    if (!indexed) return [`${ccy}${fmt(plotted)}`, label];
                    // Indexed axis: only our own line, and only for a single company, has an
                    // actual amount behind it. Trend, benchmark and blend do not.
                    const raw = name === 'value' ? row?.rawValue ?? null : null;
                    if (raw == null) return [fmtIndex(plotted, 1), label];
                    return [`${ccy}${fmt(raw)}  (index ${fmtIndex(plotted, 1)})`, label];
                  }} />
                <Line dataKey="value" name="value" type="monotone" stroke={chartTheme.accent} strokeWidth={2} dot={{ r: 2.5 }} connectNulls />
                <Line dataKey="trend" name="trend" type="monotone" stroke={chartTheme.warn} strokeWidth={2} strokeDasharray="5 3" dot={false} connectNulls />
                {/* ⚠ THE BENCHMARK IS GREEN ON ALL FOURTEEN CHARTS — see `MetricGrowthCard` for the
                    measured separations, including why green beside this card's amber trend line
                    needs the trend to stay dashed. */}
                {benchByYr && <Line dataKey="bench" name="bench" type="monotone" stroke={chartTheme.pos} strokeWidth={2} dot={{ r: 2 }} connectNulls />}
              </ComposedChart>
            </ResponsiveContainer>
            <div className="flex justify-center flex-wrap gap-x-4 gap-y-1 text-xs mt-1">
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.accent }} />Invested capital{isIndex ? ' (index)' : ''}</span>
              <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.warn }} />Trend (R² {fit.r2 == null ? '—' : fit.r2.toFixed(2)})</span>
              {benchByYr && (
                <span className="flex items-center gap-1.5"
                  title={indexed
                    ? `Both lines are indexed to 100 at ${indexed.anchor}, the first year they share. Only the growth is being compared — hover any point for the actual amount.`
                    : 'Absolute amounts: the two series share no year with a positive value, so there is no honest base to index them on.'}>
                  <span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.pos }} />
                  {benchTarget?.universe}
                </span>
              )}
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
        <InvestedCapitalInputsModal target={holdingsTarget} portfolioName={holdingsName}
          benchTarget={benchTarget} benchLabel={benchTarget?.universe ?? null}
          onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
