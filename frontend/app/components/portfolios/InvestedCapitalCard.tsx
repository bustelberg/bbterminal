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
import { benchNote, rebaseOnto, useBenchInputs, type BenchTarget } from './benchSeries';
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
   * ⚠ THE ONLY CARD THAT CANNOT ALWAYS DRAW IT. For a portfolio this chart is a rebased growth
   * INDEX (100 at the first year) and an index of the S&P sits on the same axis honestly. For a
   * SINGLE COMPANY it is a currency LEVEL — invested capital in EUR millions — and an index at
   * 100 beside it is two scales on one axis, i.e. the dual-axis mistake with the second axis
   * hidden. So the benchmark is drawn only in index mode, and its absence says so rather than
   * looking like a data gap.
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
  /** The index's own blended series, REBASED onto ours — see `rebaseOnto` for why it must be.
   *  Null when the two share no period; there is then nothing to anchor a scale on. */
  const benchByYr = useMemo(() => {
    if (!benchData) return null;
    const own = new Map(points.map((p) => [p.year, p.value as number | null]));
    return rebaseOnto(investedCapitalIndexByYear(benchData.rows), own);
  }, [benchData, points]);

  const fit = useMemo(() => logLinearFit(points), [points]);
  const note = benchNote(benchTarget, benchData, benchErr, benchByYr);

  const chartData = useMemo(() => {
    const trendByYear = new Map(fit.trend.map((t) => [t.year, t.value]));
    const years = new Set<number>(points.map((p) => p.year));
    if (benchByYr) for (const y of benchByYr.keys()) years.add(y);
    const byYear = new Map(points.map((p) => [p.year, p.value]));
    return [...years].sort((a, b) => a - b).map((year) => ({
      year,
      value: (byYear.get(year) ?? 0) > 0 ? byYear.get(year) as number : null,
      trend: trendByYear.get(year) ?? null,
      bench: benchByYr ? benchByYr.get(year) ?? null : null,
    }));
  }, [points, fit, benchByYr]);
  const logDomain = useMemo(() =>
    paddedLogDomain(chartData.flatMap((d) => [d.value, d.trend, d.bench]).filter((v): v is number => v != null)),
  [chartData]);

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
                  tick={{ fontSize: 11, fill: chartTheme.axisTick }} tickFormatter={(v: number) => fmt(v)} width={60} />
                <Tooltip contentStyle={chartTheme.tooltipCard.contentStyle} labelStyle={{ color: chartTheme.axisLabel }}
                  formatter={(v, name) => [`${ccy}${fmt(typeof v === 'number' ? v : null)}`,
                    name === 'trend' ? 'Trend'
                      : name === 'bench' ? `${benchTarget?.universe ?? 'Benchmark'} (rebased)` : 'Invested capital']} />
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
                  title="The index's blended invested capital, scaled to meet this line at the first period both cover. On a log axis that is a vertical shift — the growth rate, which is the comparison, is untouched.">
                  <span className="w-3 h-0.5 inline-block rounded" style={{ background: chartTheme.pos }} />
                  {benchTarget?.universe} (rebased)
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
        <InvestedCapitalInputsModal target={holdingsTarget} portfolioName={holdingsName} onClose={() => setShowInputs(false)} />
      )}
    </div>
  );
}
