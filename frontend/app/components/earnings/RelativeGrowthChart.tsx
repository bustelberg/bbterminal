'use client';

import { useMemo } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';
import InfoTip from '../InfoTip';
import { MC, type MetricRow } from './types';
import { annualSeries, computeCAGR, fmtPct, timeSeries, tooltipStyle } from './utils';

/** Log-scale chart comparing share price growth to Owner Earnings (EPS +
 * Dividends), both indexed to 100 at the first overlapping year. Three
 * series: price (indigo), OE actual (emerald), OE estimate (rose). Shows
 * the cumulative CAGRs above the chart for quick "is the stock getting
 * pricier or cheaper relative to fundamentals" reads. */
export default function RelativeGrowthChart({ metrics }: { metrics: MetricRow[] }) {
  const data = useMemo(() => {
    // Daily close prices for a smooth price line, fall back to annual
    const dailyPrice = timeSeries(metrics, 'close_price');
    const annualPrice = annualSeries(metrics, MC.PRICE);
    const priceSeries = dailyPrice.length > 0 ? dailyPrice : annualPrice;

    const epsActual = annualSeries(metrics, MC.EPS_WO_NRI);
    const divActual = annualSeries(metrics, MC.DIV_PS);
    const epsEst = annualSeries(metrics, MC.EPS_EST);
    const divEst = annualSeries(metrics, MC.DIV_EST);

    // Build OE actual = EPS_WO_NRI + DIV_PS
    const divMap: Record<string, number> = {};
    for (const d of divActual) divMap[d.date.slice(0, 4)] = d.value;

    const oeActual = epsActual.map((e) => {
      const yr = e.date.slice(0, 4);
      const div = divMap[yr] ?? 0;
      return { date: e.date, value: e.value + div };
    });

    // Build OE estimate = EPS_EST + DIV_EST
    const divEstMap: Record<string, number> = {};
    for (const d of divEst) divEstMap[d.date.slice(0, 4)] = d.value;

    const oeEst = epsEst.map((e) => {
      const yr = e.date.slice(0, 4);
      const div = divEstMap[yr] ?? 0;
      return { date: e.date, value: e.value + div };
    });

    if (priceSeries.length === 0 || oeActual.length === 0) return { chartData: [], cagrs: {} };

    // Find the earliest date where both price and OE actual exist and are positive.
    const firstOE = oeActual.find((o) => o.value > 0);
    if (!firstOE) return { chartData: [], cagrs: {} };

    const firstPrice = priceSeries.find((p) => p.date >= firstOE.date && p.value > 0);
    if (!firstPrice) return { chartData: [], cagrs: {} };

    const startDate = firstPrice.date;
    const priceBase = firstPrice.value;
    const oeBase = firstOE.value;

    // Cap chart end date: last price date + 2 years so estimates don't stretch x-axis
    const lastPriceDate = priceSeries[priceSeries.length - 1].date;
    const endCutoff = `${parseInt(lastPriceDate.slice(0, 4)) + 2}-12-31`;

    // Build lookup maps
    const oeActMap: Record<string, number> = {};
    for (const o of oeActual) oeActMap[o.date] = o.value;
    const oeEstMap: Record<string, number> = {};
    for (const o of oeEst) if (o.date <= endCutoff) oeEstMap[o.date] = o.value;
    const priceMap: Record<string, number> = {};
    for (const p of priceSeries) priceMap[p.date] = p.value;

    // Find the last actual OE date to bridge the gap to estimates
    const lastActualDate = [...oeActual].filter((o) => o.date >= startDate && o.value > 0).pop()?.date;
    const lastActualIndexed = lastActualDate && oeActMap[lastActualDate] > 0
      ? (oeActMap[lastActualDate] / oeBase) * 100
      : undefined;

    // Collect all dates from all series
    const allDates = new Set<string>();
    for (const p of priceSeries) if (p.date >= startDate) allDates.add(p.date);
    for (const o of oeActual) if (o.date >= startDate && o.date <= endCutoff) allDates.add(o.date);
    for (const d of Object.keys(oeEstMap)) if (d >= startDate) allDates.add(d);
    const sortedDates = [...allDates].sort();

    const chartData = sortedDates.map((d) => {
      const oeEstVal = oeEstMap[d] != null && oeEstMap[d] > 0 ? (oeEstMap[d] / oeBase) * 100 : undefined;
      return {
        date: d,
        ts: new Date(d).getTime(),
        price: priceMap[d] != null ? (priceMap[d] / priceBase) * 100 : undefined,
        oe_actual: oeActMap[d] != null && oeActMap[d] > 0 ? (oeActMap[d] / oeBase) * 100 : undefined,
        // Bridge: at the last actual date, also set oe_est so the red line starts there
        oe_est: d === lastActualDate ? (oeEstVal ?? lastActualIndexed) : oeEstVal,
      };
    });

    // CAGRs — use annual price for CAGR to match OE intervals
    const annualPriceForCagr = annualSeries(metrics, MC.PRICE);
    const priceFiltered = annualPriceForCagr.filter((p) => p.date >= startDate);
    const oeActFiltered = oeActual.filter((o) => o.date >= startDate && o.value > 0);
    const oeEstFiltered = oeEst.filter((o) => o.date >= startDate && o.value > 0);

    return {
      chartData,
      cagrs: {
        price: computeCAGR(priceFiltered),
        oe_act: computeCAGR(oeActFiltered),
        oe_est: computeCAGR(oeEstFiltered),
      },
    };
  }, [metrics]);

  if (data.chartData.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">Not enough data for Relative Growth chart. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1 flex-wrap">Price vs OE, indexed to 100 <InfoTip text="Compares share price growth to Owner Earnings (EPS + Dividends) growth on a log scale. If price grows faster than OE, the stock is getting more expensive (multiple expansion). If OE outpaces price, it's getting cheaper." /></div>
      <div className="flex flex-wrap gap-x-5 gap-y-1 mb-2">
        <div className="flex items-center gap-1.5">
          <div className="text-indigo-400 text-xs">Price</div>
          <div className="text-indigo-400 font-mono text-sm font-semibold">{fmtPct(data.cagrs.price ?? null)}</div>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="text-emerald-400 text-xs">OE Act</div>
          <div className="text-emerald-400 font-mono text-sm font-semibold">{fmtPct(data.cagrs.oe_act ?? null)}</div>
        </div>
        <div className="flex items-center gap-1.5">
          <div className="text-rose-400 text-xs">OE Est</div>
          <div className="text-rose-400 font-mono text-sm font-semibold">{fmtPct(data.cagrs.oe_est ?? null)}</div>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data.chartData} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis
            dataKey="ts"
            type="number"
            scale="time"
            domain={['dataMin', 'dataMax']}
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => new Date(v).getFullYear().toString()}
          />
          <YAxis
            scale="log"
            domain={['auto', 'auto']}
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => v.toFixed(0)}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: '#9ca3af' }}
            labelFormatter={(v) => new Date(Number(v)).toISOString().slice(0, 10)}
            formatter={(v) => [Number(v).toFixed(1), '']}
          />
          <Line type="monotone" dataKey="price" name="Price" stroke="#6366f1" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="oe_actual" name="OE Actual" stroke="#34d399" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="oe_est" name="OE Estimate" stroke="#f87171" strokeWidth={2} dot={false} connectNulls />
        </LineChart>
      </ResponsiveContainer>
      <div className="flex justify-center gap-5 text-xs mt-1">
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-indigo-400 inline-block rounded" />Price</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-emerald-400 inline-block rounded" />OE Actual</span>
        <span className="flex items-center gap-1.5"><span className="w-3 h-0.5 bg-rose-400 inline-block rounded" />OE Estimate</span>
      </div>
    </>
  );
}
