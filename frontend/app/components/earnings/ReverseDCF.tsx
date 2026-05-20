'use client';

import { useCallback, useEffect, useState } from 'react';
import InfoTip from '../InfoTip';
import { MC, type MetricRow } from './types';
import { fmtNum, fmtPct, latestValue } from './utils';

/** Compute intrinsic value given FCF/sh, growth, discount rate, terminal growth, years. */
function dcfValue(fcf: number, growth: number, discount: number, termGrowth: number, years: number): number {
  let total = 0;
  let projected = fcf;
  for (let t = 1; t <= years; t++) {
    projected *= (1 + growth);
    total += projected / Math.pow(1 + discount, t);
  }
  // Terminal value (Gordon growth)
  const terminalFCF = projected * (1 + termGrowth);
  const terminalValue = terminalFCF / (discount - termGrowth);
  total += terminalValue / Math.pow(1 + discount, years);
  return total;
}

/** Binary search for implied growth rate that matches current price. */
function solveImpliedGrowth(fcf: number, price: number, discount: number, termGrowth: number, years: number): number | null {
  if (fcf <= 0 || price <= 0 || discount <= termGrowth) return null;
  let lo = -0.5, hi = 1.0;
  for (let i = 0; i < 100; i++) {
    const mid = (lo + hi) / 2;
    const val = dcfValue(fcf, mid, discount, termGrowth, years);
    if (val < price) lo = mid;
    else hi = mid;
  }
  return (lo + hi) / 2;
}

/** "What growth rate is the market pricing in?" calculator. Solves the
 * standard DCF (10-year explicit projection + Gordon-growth terminal) in
 * reverse: given current price, FCF/sh, WACC, terminal growth, and years,
 * binary-search for the FCF growth rate that makes the model agree with
 * the price. Shown alongside historic FCF growth as a sanity check —
 * green when implied ≤ historic (the market isn't asking for more than
 * the company has historically delivered), red otherwise. */
export default function ReverseDCF({ metrics }: { metrics: MetricRow[] }) {
  const priceRaw = latestValue(metrics, 'close_price') ?? latestValue(metrics, MC.PRICE);
  const fcfRaw = latestValue(metrics, MC.FCF_PS);
  const waccRaw = latestValue(metrics, MC.WACC);
  const netCashRaw = latestValue(metrics, MC.NET_CASH_PS);
  const historicFcfGrowth = latestValue(metrics, MC.FCF_GROWTH_5Y);

  // Context metrics
  const roic = latestValue(metrics, MC.ROIC);
  const gfIntrinsic = latestValue(metrics, MC.GF_INTRINSIC);
  const buybackRatio = latestValue(metrics, MC.BUYBACK_RATIO);
  const divYield = latestValue(metrics, MC.DIV_YIELD);
  const piotroski = latestValue(metrics, MC.PIOTROSKI);
  const altmanZ = latestValue(metrics, MC.ALTMAN_Z);
  const ebitda5y = latestValue(metrics, MC.EBITDA_5Y_GROWTH);
  const yoyRevGrowth = latestValue(metrics, MC.YOY_REV_GROWTH);

  const [price, setPrice] = useState<string>('');
  const [fcf, setFcf] = useState<string>('');
  const [netCash, setNetCash] = useState<string>('');
  const [discount, setDiscount] = useState<string>('10');
  const [termGrowth, setTermGrowth] = useState<string>('2');
  const [years, setYears] = useState<string>('10');
  const [initialized, setInitialized] = useState(false);

  const resetDefaults = useCallback(() => {
    setPrice(priceRaw ? priceRaw.value.toFixed(2) : '');
    setFcf(fcfRaw ? fcfRaw.value.toFixed(2) : '');
    setNetCash(netCashRaw ? netCashRaw.value.toFixed(2) : '0');
    setDiscount(waccRaw ? waccRaw.value.toFixed(1) : '10');
    setTermGrowth('2');
    setYears('10');
  }, [priceRaw, fcfRaw, netCashRaw, waccRaw]);

  // Only auto-fill on first data load, not on every re-render. Same
  // pattern as the EPS panel above — one-shot init of editable form
  // state from raw data, so the setState-in-effect lint gets a
  // justified suppression.
  useEffect(() => {
    if (!initialized && (priceRaw || fcfRaw)) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      resetDefaults();
      setInitialized(true);
    }
  }, [initialized, priceRaw, fcfRaw, resetDefaults]);

  const priceNum = parseFloat(price);
  const fcfNum = parseFloat(fcf);
  const netCashNum = parseFloat(netCash) || 0;
  const discountNum = parseFloat(discount) / 100;
  const termGrowthNum = parseFloat(termGrowth) / 100;
  const yearsNum = parseInt(years);

  // Subtract net cash from price to get the operating value the DCF needs to justify
  const operatingValue = priceNum - netCashNum;

  const impliedGrowth = !isNaN(priceNum) && !isNaN(fcfNum) && !isNaN(discountNum) && !isNaN(termGrowthNum) && !isNaN(yearsNum)
    && fcfNum > 0 && operatingValue > 0 && discountNum > termGrowthNum && yearsNum > 0
    ? solveImpliedGrowth(fcfNum, operatingValue, discountNum, termGrowthNum, yearsNum)
    : null;

  const inputClass = "w-24 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-white font-mono text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none";
  const dataLabel = <span className="text-emerald-600 text-xs ml-1">DATA</span>;
  const assumptionLabel = <span className="text-amber-600 text-xs ml-1">ASSUMPTION</span>;

  return (
    <div className="space-y-5">
      {/* Inputs row */}
      <div className="flex flex-wrap items-end gap-5">
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">Share Price{dataLabel} <InfoTip text="Current market price per share. The DCF model solves for what FCF growth rate justifies this price." /></div>
          <input type="number" step="0.01" value={price} onChange={(e) => setPrice(e.target.value)} className={inputClass} />
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">FCF/share{dataLabel} <InfoTip text="Free Cash Flow per share for the most recent fiscal year. Starting point for projecting future cash flows in the DCF model." /></div>
          <input type="number" step="0.01" value={fcf} onChange={(e) => setFcf(e.target.value)} className={inputClass} />
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">Net Cash/sh{dataLabel} <InfoTip text="Cash minus debt per share. Subtracted from share price to isolate the operating value that FCF must justify." /></div>
          <input type="number" step="0.01" value={netCash} onChange={(e) => setNetCash(e.target.value)} className={inputClass} />
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">WACC %{waccRaw ? dataLabel : assumptionLabel} <InfoTip text="Weighted Average Cost of Capital — the discount rate. Blends cost of equity and cost of debt weighted by capital structure. Pre-filled from GuruFocus when available." /></div>
          <input type="number" step="0.5" value={discount} onChange={(e) => setDiscount(e.target.value)} className={inputClass} />
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">Terminal Growth %{assumptionLabel} <InfoTip text="Perpetual growth rate after the projection period (Gordon Growth Model). Typically 2-3%, roughly matching long-term GDP/inflation. Higher values dramatically increase valuation." /></div>
          <input type="number" step="0.5" value={termGrowth} onChange={(e) => setTermGrowth(e.target.value)} className={inputClass} />
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">Years{assumptionLabel} <InfoTip text="Number of years in the explicit projection period before terminal value kicks in. Standard is 10 years. Shorter periods put more weight on terminal value." /></div>
          <input type="number" step="1" value={years} onChange={(e) => setYears(e.target.value)} className={inputClass} />
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1">&nbsp;</div>
          <button onClick={resetDefaults} className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-white hover:bg-white/5 border border-gray-700 transition-colors">
            Reset
          </button>
        </div>
      </div>

      {/* Result */}
      <div className="flex items-end gap-8">
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">Implied FCF Growth <InfoTip text="The annual FCF growth rate the market is pricing in. Solved via binary search: what growth rate makes the DCF value equal the current share price? Green if at or below historic FCF growth (reasonable), red if above (optimistic)." /></div>
          <div className={`font-mono text-2xl font-semibold ${impliedGrowth != null && historicFcfGrowth ? (impliedGrowth <= historicFcfGrowth.value ? 'text-emerald-400' : 'text-rose-400') : 'text-white'}`}>
            {impliedGrowth != null ? fmtPct(impliedGrowth) : '—'}
          </div>
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">Historic FCF 5Y <InfoTip text="5-year historic FCF growth rate from LongEquity. Used as the benchmark to judge whether the implied growth rate is reasonable." /></div>
          <div className="font-mono text-2xl font-semibold text-gray-400">
            {historicFcfGrowth ? fmtPct(historicFcfGrowth.value) : '—'}
          </div>
        </div>
        <div>
          <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">GF Intrinsic Value <InfoTip text="GuruFocus intrinsic value based on projected FCF. An independent reference point — compare to the current share price to gauge over/undervaluation." /></div>
          <div className="font-mono text-lg text-gray-400">
            {gfIntrinsic ? `$${fmtNum(gfIntrinsic.value, 2)}` : '—'}
          </div>
        </div>
      </div>

      {/* Context metrics */}
      <div className="grid grid-cols-4 sm:grid-cols-8 gap-4 pt-2 border-t border-gray-800/40">
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">ROIC <InfoTip text="Return on Invested Capital. Measures how efficiently the company generates returns on all capital (debt + equity). Higher = better capital allocation." /></div>
          <div className="text-gray-300 font-mono text-sm">{roic ? `${fmtNum(roic.value)}%` : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">EBITDA 5Y Gr. <InfoTip text="5-year EBITDA growth rate per share. Indicates underlying business earnings power growth before interest, taxes, depreciation, and amortization." /></div>
          <div className="text-gray-300 font-mono text-sm">{ebitda5y ? fmtPct(ebitda5y.value / 100) : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">YoY Rev/sh Gr. <InfoTip text="Year-over-year revenue per share growth. Per-share basis adjusts for dilution from share issuance." /></div>
          <div className="text-gray-300 font-mono text-sm">{yoyRevGrowth ? fmtPct(yoyRevGrowth.value / 100) : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">Buyback <InfoTip text="Shares buyback ratio — percentage of shares repurchased. Positive = company is buying back shares (reduces share count, boosts per-share metrics)." /></div>
          <div className="text-gray-300 font-mono text-sm">{buybackRatio ? `${fmtNum(buybackRatio.value)}%` : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">Div Yield <InfoTip text="Annual dividend as a percentage of share price. Part of total shareholder return alongside price appreciation and buybacks." /></div>
          <div className="text-gray-300 font-mono text-sm">{divYield ? `${fmtNum(divYield.value)}%` : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">Piotroski <InfoTip text="Piotroski F-Score (0-9). Scores financial strength based on profitability, leverage, and operating efficiency. 8-9 is strong, 0-2 is weak." /></div>
          <div className="text-gray-300 font-mono text-sm">{piotroski ? fmtNum(piotroski.value, 0) : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">Altman Z <InfoTip text="Altman Z-Score predicts bankruptcy risk. Above 3.0 = safe zone, 1.8-3.0 = grey zone, below 1.8 = distress zone." /></div>
          <div className="text-gray-300 font-mono text-sm">{altmanZ ? fmtNum(altmanZ.value) : '—'}</div>
        </div>
        <div>
          <div className="text-gray-500 text-xs flex items-center gap-1">Beta <InfoTip text="Stock's volatility relative to the market. Beta = 1 means same as market, >1 = more volatile, <1 = less volatile. Used in CAPM to estimate cost of equity." /></div>
          <div className="text-gray-300 font-mono text-sm">{latestValue(metrics, MC.BETA) ? fmtNum(latestValue(metrics, MC.BETA)!.value) : '—'}</div>
        </div>
      </div>
    </div>
  );
}
