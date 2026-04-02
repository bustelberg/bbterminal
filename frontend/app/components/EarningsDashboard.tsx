'use client';

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid, Legend,
} from 'recharts';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Company = {
  company_id: number;
  primary_ticker: string;
  primary_exchange: string;
  company_name: string | null;
  country: string | null;
};

type MetricRow = {
  metric_code: string;
  target_date: string;
  numeric_value: number | null;
  is_prediction: boolean;
};

// ---------------------------------------------------------------------------
// Metric codes (matching old constants.py)
// ---------------------------------------------------------------------------

const MC = {
  FCF_YIELD: 'indicator_q_fcf_yield',
  PRICE: 'annuals__Per Share Data__Month End Stock Price',
  EPS_WO_NRI: 'annuals__Per Share Data__EPS without NRI',
  DIV_PS: 'annuals__Per Share Data__Dividends per Share',
  EPS_EST: 'annual_eps_nri_estimate',
  DIV_EST: 'annual_dividend_estimate',
  FCF_PS: 'annuals__Per Share Data__Free Cash Flow per Share',
  INTEREST_COVERAGE: 'indicator_q_interest_coverage',
  DEBT_TO_EQUITY: 'annuals__Balance Sheet__Debt-to-Equity',
  CAPEX_TO_REV: 'annuals__Ratios__Capex-to-Revenue',
  CAPEX_TO_OCF: 'annuals__Ratios__Capex-to-Operating-Cash-Flow',
  ROE: 'indicator_q_roe',
  ROIC: 'indicator_q_roic',
  GROSS_MARGIN: 'indicator_q_gross_margin',
  NET_MARGIN: 'indicator_q_net_margin',
  FWD_PE: 'indicator_q_forward_pe_ratio',
  PEG: 'indicator_q_peg_ratio',
  FCF: 'annuals__Cashflow Statement__Free Cash Flow',
  NET_INCOME: 'annuals__Income Statement__Net Income',
  EPS_DILUTED: 'annuals__Income Statement__EPS (Diluted)',
  EPS_FY1_EST: 'annual_per_share_eps_estimate',
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function latestValue(metrics: MetricRow[], code: string): { value: number; date: string } | null {
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (m.metric_code !== code || m.numeric_value == null) continue;
    if (!best || m.target_date > best.target_date) best = m;
  }
  return best ? { value: best.numeric_value!, date: best.target_date } : null;
}

function timeSeries(metrics: MetricRow[], code: string): { date: string; value: number }[] {
  return metrics
    .filter((m) => m.metric_code === code && m.numeric_value != null)
    .sort((a, b) => a.target_date.localeCompare(b.target_date))
    .map((m) => ({ date: m.target_date, value: m.numeric_value! }));
}

/** Collapse annual metrics to one point per calendar year (last date in each year). */
function annualSeries(metrics: MetricRow[], code: string): { date: string; value: number }[] {
  const raw = timeSeries(metrics, code);
  const byYear: Record<string, { date: string; value: number }> = {};
  for (const p of raw) {
    const yr = p.date.slice(0, 4);
    if (!byYear[yr] || p.date > byYear[yr].date) byYear[yr] = p;
  }
  return Object.values(byYear).sort((a, b) => a.date.localeCompare(b.date));
}

function computeCAGR(series: { date: string; value: number }[], requirePositive = true): number | null {
  if (series.length < 2) return null;
  const start = series[0];
  const end = series[series.length - 1];
  if (requirePositive && (start.value <= 0 || end.value <= 0)) return null;
  const years = (new Date(end.date).getTime() - new Date(start.date).getTime()) / (365.25 * 86400000);
  if (years < 0.5) return null;
  return Math.pow(end.value / start.value, 1 / years) - 1;
}

function computeCAGRWindow(series: { date: string; value: number }[], years: number): number | null {
  if (series.length < 2) return null;
  const endDate = new Date(series[series.length - 1].date);
  const cutoff = new Date(endDate);
  cutoff.setFullYear(cutoff.getFullYear() - years);
  const cutoffStr = cutoff.toISOString().slice(0, 10);
  // find closest point to cutoff
  let best = series[0];
  for (const p of series) {
    if (p.date <= cutoffStr) best = p;
  }
  if (best.date === series[series.length - 1].date) return null;
  return computeCAGR([best, series[series.length - 1]]);
}

function indexTo100(series: { date: string; value: number }[]): { date: string; value: number; raw: number }[] {
  const firstPositive = series.find((s) => s.value > 0);
  if (!firstPositive) return [];
  const base = firstPositive.value;
  const startIdx = series.indexOf(firstPositive);
  return series.slice(startIdx).map((s) => ({
    date: s.date,
    value: (s.value / base) * 100,
    raw: s.value,
  }));
}

function fmtPct(v: number | null): string {
  if (v == null) return '—';
  return `${(v * 100).toFixed(2)}%`;
}

function fmtNum(v: number | null, digits = 2): string {
  if (v == null) return '—';
  return v.toLocaleString(undefined, { maximumFractionDigits: digits });
}

function fmtPctPoints(v: number | null): string {
  if (v == null) return '—';
  return `${v.toFixed(2)}%`;
}

// ---------------------------------------------------------------------------
// SSE log reader
// ---------------------------------------------------------------------------

function useSSERefresh() {
  const [logs, setLogs] = useState<{ type: string; message: string }[]>([]);
  const [running, setRunning] = useState(false);
  const logEndRef = useRef<HTMLDivElement>(null);

  const start = useCallback((url: string, onDone?: () => void) => {
    setLogs([]);
    setRunning(true);

    fetch(url, { method: 'POST' }).then(async (res) => {
      if (!res.ok || !res.body) {
        setLogs([{ type: 'error', message: `HTTP ${res.status}` }]);
        setRunning(false);
        return;
      }
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';
        for (const line of lines) {
          if (!line.startsWith('data: ')) continue;
          try {
            const parsed = JSON.parse(line.slice(6));
            setLogs((prev) => [...prev, parsed]);
            if (parsed.type === 'done') {
              setRunning(false);
              onDone?.();
            }
          } catch { /* skip */ }
        }
      }
      setRunning(false);
      onDone?.();
    }).catch((err) => {
      setLogs((prev) => [...prev, { type: 'error', message: String(err) }]);
      setRunning(false);
    });
  }, []);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logs]);

  return { logs, running, start, logEndRef };
}

// ---------------------------------------------------------------------------
// Company picker with autocomplete
// ---------------------------------------------------------------------------

function CompanyPicker({
  companies,
  selected,
  onSelect,
}: {
  companies: Company[];
  selected: Company | null;
  onSelect: (c: Company) => void;
}) {
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const ref = useRef<HTMLDivElement>(null);

  const filtered = useMemo(() => {
    if (!query.trim()) return companies.slice(0, 50);
    const q = query.toLowerCase();
    return companies.filter(
      (c) =>
        (c.company_name || '').toLowerCase().includes(q) ||
        c.primary_ticker.toLowerCase().includes(q)
    ).slice(0, 50);
  }, [query, companies]);

  useEffect(() => {
    function handleClick(e: MouseEvent) {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    }
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, []);

  return (
    <div ref={ref} className="relative w-full max-w-md">
      <input
        type="text"
        value={query}
        onChange={(e) => { setQuery(e.target.value); setOpen(true); }}
        onFocus={() => setOpen(true)}
        placeholder={selected ? `${selected.company_name || selected.primary_ticker}` : 'Search company or ticker...'}
        className="w-full bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2.5 text-white placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
      />
      {open && filtered.length > 0 && (
        <div className="absolute z-50 mt-1 w-full max-h-64 overflow-y-auto bg-[#151821] border border-gray-700 rounded-lg shadow-xl">
          {filtered.map((c) => (
            <button
              key={c.company_id}
              onClick={() => {
                onSelect(c);
                setQuery('');
                setOpen(false);
              }}
              className="w-full px-3 py-2 text-left hover:bg-white/[0.04] transition-colors flex items-center gap-3"
            >
              <span className="font-mono text-indigo-400 text-sm">{c.primary_ticker}</span>
              <span className="text-gray-300 text-sm truncate">{c.company_name || '—'}</span>
              <span className="text-gray-600 text-xs ml-auto">{c.primary_exchange}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Shared UI components
// ---------------------------------------------------------------------------

function RefreshButton({ label, running, onClick }: { label: string; running: boolean; onClick: () => void }) {
  return (
    <button
      onClick={onClick}
      disabled={running}
      className="px-3 py-1.5 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
    >
      {running ? 'Refreshing...' : label}
    </button>
  );
}

function LogPanel({ logs, logEndRef }: { logs: { type: string; message: string }[]; logEndRef: React.RefObject<HTMLDivElement | null> }) {
  if (logs.length === 0) return null;
  return (
    <div className="mt-3 max-h-48 overflow-y-auto bg-[#0b0d13] border border-gray-800/40 rounded-lg p-3 font-mono text-xs">
      {logs.map((l, i) => (
        <div key={i} className={l.type === 'error' ? 'text-rose-400' : l.type === 'done' ? 'text-emerald-400' : 'text-gray-400'}>
          {l.message}
        </div>
      ))}
      <div ref={logEndRef} />
    </div>
  );
}

function StatCard({ label, value, sub }: { label: string; value: string; sub?: string | null }) {
  return (
    <div className="bg-[#0f1117] rounded-lg p-3 border border-gray-800/40">
      <div className="text-gray-500 text-xs mb-1">{label}</div>
      <div className="text-white font-mono text-lg">{value}</div>
      {sub && <div className="text-gray-600 text-xs mt-1">{sub}</div>}
    </div>
  );
}

function KPIRow({ items }: { items: { label: string; value: string }[] }) {
  return (
    <div className="flex gap-6 mb-3">
      {items.map((kpi) => (
        <div key={kpi.label}>
          <div className="text-gray-500 text-xs">{kpi.label}</div>
          <div className="text-white font-mono">{kpi.value}</div>
        </div>
      ))}
    </div>
  );
}

// Shared tooltip style
const tooltipStyle = { backgroundColor: '#151821', border: '1px solid #374151', borderRadius: '8px' };

// ---------------------------------------------------------------------------
// Snapshot stats (full, matching old dashboard layout)
// ---------------------------------------------------------------------------

function SnapshotStats({ metrics }: { metrics: MetricRow[] }) {
  const lv = useCallback((code: string) => latestValue(metrics, code), [metrics]);

  // Derived: EGM = (EPS_FY1_EST - EPS_DILUTED) / EPS_DILUTED
  const egm = useMemo(() => {
    const eps = lv(MC.EPS_DILUTED);
    const fy1 = lv(MC.EPS_FY1_EST);
    if (!eps || !fy1 || eps.value === 0) return null;
    return (fy1.value - eps.value) / eps.value;
  }, [lv]);

  // Derived: FCF / Net Income
  const fcfOverNi = useMemo(() => {
    const fcf = lv(MC.FCF);
    const ni = lv(MC.NET_INCOME);
    if (!fcf || !ni || ni.value === 0) return null;
    return fcf.value / ni.value;
  }, [lv]);

  // Derived: EPS 5Y CAGR
  const eps5y = useMemo(() => {
    const series = annualSeries(metrics, MC.EPS_WO_NRI);
    return computeCAGRWindow(series, 5);
  }, [metrics]);

  // Derived: Price CAGRs
  const priceSeries = useMemo(() => annualSeries(metrics, MC.PRICE), [metrics]);
  const price3y = useMemo(() => computeCAGRWindow(priceSeries, 3), [priceSeries]);
  const price5y = useMemo(() => computeCAGRWindow(priceSeries, 5), [priceSeries]);
  const priceAll = useMemo(() => computeCAGR(priceSeries), [priceSeries]);

  // Derived: FCF/share CAGR windows
  const fcfpsSeries = useMemo(() => annualSeries(metrics, MC.FCF_PS), [metrics]);
  const fcfSh5y = useMemo(() => computeCAGRWindow(fcfpsSeries, 5), [fcfpsSeries]);
  const fcfSh10y = useMemo(() => computeCAGRWindow(fcfpsSeries, 10), [fcfpsSeries]);

  // Dates for derived metrics
  const epsLatestDate = useMemo(() => {
    const series = annualSeries(metrics, MC.EPS_WO_NRI);
    return series.length > 0 ? series[series.length - 1].date : null;
  }, [metrics]);
  const priceLatestDate = useMemo(() => priceSeries.length > 0 ? priceSeries[priceSeries.length - 1].date : null, [priceSeries]);
  const fcfpsLatestDate = useMemo(() => fcfpsSeries.length > 0 ? fcfpsSeries[fcfpsSeries.length - 1].date : null, [fcfpsSeries]);

  type StatRow = { label: string; value: string; date?: string | null; info?: string };

  const leftSections: { title: string; rows: StatRow[] }[] = [
    {
      title: 'Balance Sheet',
      rows: [
        { label: 'Interest Coverage', value: fmtNum(lv(MC.INTEREST_COVERAGE)?.value ?? null), date: lv(MC.INTEREST_COVERAGE)?.date, info: 'EBIT / Interest Expense. Higher = more capacity to service debt. Below 3 is a warning sign.' },
        { label: 'Debt / Equity', value: fmtNum(lv(MC.DEBT_TO_EQUITY)?.value ?? null), date: lv(MC.DEBT_TO_EQUITY)?.date, info: 'Total Debt / Total Equity. Lower = less leverage. Above 2 warrants scrutiny.' },
      ],
    },
    {
      title: 'Capital Intensity',
      rows: [
        { label: 'CAPEX / Revenue', value: fmtNum(lv(MC.CAPEX_TO_REV)?.value ?? null), date: lv(MC.CAPEX_TO_REV)?.date, info: 'Capital expenditure as a share of revenue. Lower = more capital-light business model.' },
        { label: 'CAPEX / OCF', value: fmtNum(lv(MC.CAPEX_TO_OCF)?.value ?? null), date: lv(MC.CAPEX_TO_OCF)?.date, info: 'Capital expenditure as a share of operating cash flow. Lower = more cash left after reinvestment.' },
      ],
    },
    {
      title: 'Capital Allocation',
      rows: [
        { label: 'ROE', value: fmtPctPoints(lv(MC.ROE)?.value ?? null), date: lv(MC.ROE)?.date, info: 'Return on Equity = Net Income / Shareholders\' Equity. Measures profit generated per dollar of equity.' },
        { label: 'ROIC', value: fmtPctPoints(lv(MC.ROIC)?.value ?? null), date: lv(MC.ROIC)?.date, info: 'Return on Invested Capital = NOPAT / Invested Capital. Measures efficiency of all capital deployed.' },
      ],
    },
    {
      title: 'Profitability',
      rows: [
        { label: 'Gross Margin', value: fmtPctPoints(lv(MC.GROSS_MARGIN)?.value ?? null), date: lv(MC.GROSS_MARGIN)?.date, info: 'Gross Profit / Revenue. Indicates pricing power and cost of goods sold efficiency.' },
        { label: 'Net Margin', value: fmtPctPoints(lv(MC.NET_MARGIN)?.value ?? null), date: lv(MC.NET_MARGIN)?.date, info: 'Net Income / Revenue. Bottom-line profitability after all expenses.' },
      ],
    },
    {
      title: 'Historical Growth',
      rows: [
        { label: 'EPS 5Y CAGR', value: fmtPct(eps5y), date: epsLatestDate, info: 'Compound Annual Growth Rate of EPS (ex NRI) over the last 5 years.' },
      ],
    },
  ];

  const rightSections: { title: string; rows: StatRow[] }[] = [
    {
      title: 'Outlook',
      rows: [
        { label: 'EPS LT Growth EST', value: fmtPctPoints(lv(MC.EPS_EST)?.value ?? null), date: lv(MC.EPS_EST)?.date, info: 'Analyst consensus long-term EPS growth rate estimate (3-5 years forward).' },
      ],
    },
    {
      title: 'Valuation',
      rows: [
        { label: 'Forward P/E', value: fmtNum(lv(MC.FWD_PE)?.value ?? null), date: lv(MC.FWD_PE)?.date, info: 'Price / Forward EPS estimate. Lower = cheaper relative to expected earnings.' },
        { label: 'PEG', value: fmtNum(lv(MC.PEG)?.value ?? null), date: lv(MC.PEG)?.date, info: 'P/E / EPS Growth Rate. Below 1 suggests undervalued relative to growth; above 2 may be expensive.' },
      ],
    },
    {
      title: 'Value Creation',
      rows: [
        { label: 'CAGR 3Y', value: fmtPct(price3y), date: priceLatestDate, info: 'Compound Annual Growth Rate of stock price over the last 3 years.' },
        { label: 'CAGR 5Y', value: fmtPct(price5y), date: priceLatestDate, info: 'Compound Annual Growth Rate of stock price over the last 5 years.' },
        { label: 'CAGR All', value: fmtPct(priceAll), date: priceLatestDate, info: 'Compound Annual Growth Rate of stock price over the entire available history.' },
      ],
    },
    {
      title: 'Expected Return',
      rows: [
        { label: 'EGM', value: fmtPct(egm), date: lv(MC.EPS_FY1_EST)?.date, info: 'Earnings Growth Multiple = (FY1 EPS Est - Current EPS) / Current EPS. Indicates expected near-term earnings growth.' },
      ],
    },
    {
      title: 'Cashflow',
      rows: [
        { label: 'FCF / Net Income', value: fmtPct(fcfOverNi), date: lv(MC.FCF)?.date, info: 'Free Cash Flow / Net Income. Above 1 means cash earnings exceed accounting earnings (high quality).' },
        { label: 'FCF/sh 5Y CAGR', value: fmtPct(fcfSh5y), date: fcfpsLatestDate, info: 'Compound Annual Growth Rate of Free Cash Flow per Share over the last 5 years.' },
        { label: 'FCF/sh 10Y CAGR', value: fmtPct(fcfSh10y), date: fcfpsLatestDate, info: 'Compound Annual Growth Rate of Free Cash Flow per Share over the last 10 years.' },
      ],
    },
  ];

  function renderColumn(sections: { title: string; rows: StatRow[] }[]) {
    return (
      <div className="space-y-4">
        {sections.map((sec) => (
          <div key={sec.title}>
            <div className="text-gray-400 text-xs font-semibold uppercase tracking-wider mb-2">{sec.title}</div>
            <div className="space-y-1">
              {sec.rows.map((r) => (
                <div key={r.label} className="flex justify-between items-center">
                  <span className="text-gray-400 text-sm flex items-center gap-1.5">
                    {r.label}
                    {r.info && (
                      <span className="relative group cursor-help">
                        <span className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-gray-600 text-gray-500 text-[10px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors">i</span>
                        <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 w-56 px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-xs text-gray-300 leading-relaxed opacity-0 pointer-events-none group-hover:opacity-100 group-hover:pointer-events-auto transition-opacity z-50 shadow-xl">
                          {r.info}
                        </span>
                      </span>
                    )}
                  </span>
                  <span className="text-right">
                    <span className="text-white font-mono text-sm">{r.value}</span>
                    {r.date && (
                      <span className="block text-gray-500 text-[10px] font-mono">{r.date}</span>
                    )}
                  </span>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    );
  }

  return (
    <div className="grid grid-cols-2 gap-8">
      {renderColumn(leftSections)}
      {renderColumn(rightSections)}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Chart 1: FCF Yield % (line + mean)
// ---------------------------------------------------------------------------

function FCFYieldChart({ metrics }: { metrics: MetricRow[] }) {
  const data = useMemo(() => timeSeries(metrics, MC.FCF_YIELD), [metrics]);
  const mean = useMemo(() => {
    if (data.length === 0) return 0;
    return data.reduce((s, d) => s + d.value, 0) / data.length;
  }, [data]);

  if (data.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No FCF Yield data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2">All-time avg: <span className="text-rose-400 font-mono">{mean.toFixed(2)}%</span> (red dotted)</div>
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: number) => `${v.toFixed(1)}%`} />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: '#9ca3af' }} formatter={(v) => [`${Number(v).toFixed(2)}%`, 'FCF Yield']} />
          <ReferenceLine y={mean} stroke="#ef4444" strokeDasharray="5 5" />
          <Line type="monotone" dataKey="value" stroke="#818cf8" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

// ---------------------------------------------------------------------------
// Chart 2: Relative Growth (Price vs Owner Earnings) — log indexed
// ---------------------------------------------------------------------------

function RelativeGrowthChart({ metrics }: { metrics: MetricRow[] }) {
  const data = useMemo(() => {
    // Daily close prices for a smooth price line
    const dailyPrice = timeSeries(metrics, 'close_price');
    // Fall back to annual price if no daily data
    const annualPrice = annualSeries(metrics, MC.PRICE);
    const priceSource = dailyPrice.length > 0 ? dailyPrice : annualPrice;

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

    if (priceSource.length === 0 || oeActual.length === 0) return { chartData: [], cagrs: {} };

    // Find the earliest date where both price and OE actual exist and are positive.
    // For daily prices, find the first price date on or after the first positive OE date.
    const firstOE = oeActual.find((o) => o.value > 0);
    if (!firstOE) return { chartData: [], cagrs: {} };

    const firstPrice = priceSource.find((p) => p.date >= firstOE.date && p.value > 0);
    if (!firstPrice) return { chartData: [], cagrs: {} };

    const startDate = firstPrice.date;
    const priceBase = firstPrice.value;
    const oeBase = firstOE.value;

    // Build lookup maps
    const oeActMap: Record<string, number> = {};
    for (const o of oeActual) oeActMap[o.date] = o.value;
    const oeEstMap: Record<string, number> = {};
    for (const o of oeEst) oeEstMap[o.date] = o.value;

    // Find the last actual OE date to bridge the gap to estimates
    const lastActualDate = [...oeActual].filter((o) => o.date >= startDate && o.value > 0).pop()?.date;
    const lastActualIndexed = lastActualDate && oeActMap[lastActualDate] > 0
      ? (oeActMap[lastActualDate] / oeBase) * 100
      : undefined;

    // Build chart data from daily prices as the primary x-axis,
    // plus OE actual/estimate dates that may not coincide with a price date
    const allDates = new Set<string>();
    for (const p of priceSource) if (p.date >= startDate) allDates.add(p.date);
    for (const o of oeActual) if (o.date >= startDate) allDates.add(o.date);
    for (const o of oeEst) if (o.date >= startDate) allDates.add(o.date);
    const sortedDates = [...allDates].sort();

    // Price lookup for daily data
    const priceMap: Record<string, number> = {};
    for (const p of priceSource) priceMap[p.date] = p.value;

    const chartData = sortedDates.map((d) => {
      const oeEstVal = oeEstMap[d] != null && oeEstMap[d] > 0 ? (oeEstMap[d] / oeBase) * 100 : undefined;
      return {
        date: d,
        price: priceMap[d] != null ? (priceMap[d] / priceBase) * 100 : undefined,
        oe_actual: oeActMap[d] != null && oeActMap[d] > 0 ? (oeActMap[d] / oeBase) * 100 : undefined,
        // Bridge: at the last actual date, also set oe_est so the red line starts there
        oe_est: d === lastActualDate ? (oeEstVal ?? lastActualIndexed) : oeEstVal,
      };
    });

    // CAGRs
    const priceFiltered = priceSource.filter((p) => p.date >= startDate);
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
      <div className="text-gray-500 text-xs mb-2">Price vs Owner Earnings (Actual → Estimate), indexed to 100</div>
      <KPIRow items={[
        { label: 'CAGR Price', value: fmtPct(data.cagrs.price ?? null) },
        { label: 'CAGR OE Actual', value: fmtPct(data.cagrs.oe_act ?? null) },
        { label: 'CAGR OE Est', value: fmtPct(data.cagrs.oe_est ?? null) },
      ]} />
      <ResponsiveContainer width="100%" height={350}>
        <LineChart data={data.chartData} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 4)} />
          <YAxis
            scale="log"
            domain={['auto', 'auto']}
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => v.toFixed(0)}
          />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: '#9ca3af' }} formatter={(v) => [Number(v).toFixed(1), '']} />
          <Legend wrapperStyle={{ fontSize: 12 }} />
          <Line type="monotone" dataKey="price" name="Price" stroke="#6366f1" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="oe_actual" name="OE Actual" stroke="#34d399" strokeWidth={2} dot={false} connectNulls />
          <Line type="monotone" dataKey="oe_est" name="OE Estimate" stroke="#f87171" strokeWidth={2} dot={false} connectNulls />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

// ---------------------------------------------------------------------------
// Chart 3: FCF/share Growth (log indexed)
// ---------------------------------------------------------------------------

function FCFShareChart({ metrics }: { metrics: MetricRow[] }) {
  const { data, cagr, startDate, baseVal } = useMemo(() => {
    const series = annualSeries(metrics, MC.FCF_PS);
    const indexed = indexTo100(series);
    if (indexed.length === 0) return { data: [], cagr: null, startDate: null, baseVal: null };
    const positiveSeries = series.filter((s) => s.value > 0);
    return {
      data: indexed,
      cagr: computeCAGR(positiveSeries),
      startDate: indexed[0].date,
      baseVal: indexed[0].raw,
    };
  }, [metrics]);

  if (data.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No FCF/share data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2">Indexed to 100 at first positive point</div>
      <KPIRow items={[
        { label: 'CAGR FCF/sh', value: fmtPct(cagr) },
        { label: 'Start', value: startDate ?? '—' },
        { label: 'Base', value: baseVal != null ? fmtNum(baseVal, 4) : '—' },
      ]} />
      <ResponsiveContainer width="100%" height={300}>
        <LineChart data={data} margin={{ top: 10, right: 20, bottom: 10, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 4)} />
          <YAxis
            scale="log"
            domain={['auto', 'auto']}
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => v.toFixed(0)}
          />
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: '#9ca3af' }}
            formatter={(v, name) => {
              if (name === 'value') return [`${Number(v).toFixed(1)}`, 'Indexed'];
              return [`${Number(v).toFixed(2)}`, 'Raw'];
            }}
          />
          <Line type="monotone" dataKey="value" name="FCF/share (indexed)" stroke="#818cf8" strokeWidth={2} dot={false} />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

// ---------------------------------------------------------------------------
// Analyst Estimates grid
// ---------------------------------------------------------------------------

function AnalystEstimates({ metrics }: { metrics: MetricRow[] }) {
  const grouped = useMemo(() => {
    const estMetrics = metrics.filter((m) => m.metric_code.startsWith('annual_') && m.is_prediction && m.numeric_value != null);
    const map: Record<string, MetricRow[]> = {};
    for (const m of estMetrics) (map[m.metric_code] ||= []).push(m);
    return map;
  }, [metrics]);

  if (Object.keys(grouped).length === 0) {
    return <div className="text-gray-500 text-sm py-4 text-center">No analyst estimate data. Click Refresh Estimates to load.</div>;
  }

  return (
    <div className="grid grid-cols-3 gap-3">
      {Object.entries(grouped).slice(0, 12).map(([code, rows]) => {
        const sorted = [...rows].sort((a, b) => b.target_date.localeCompare(a.target_date));
        const latest = sorted[0];
        const label = code.replace('annual_', '').replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase());
        return (
          <StatCard
            key={code}
            label={label}
            value={latest.numeric_value?.toLocaleString(undefined, { maximumFractionDigits: 2 }) ?? '—'}
            sub={`as of ${latest.target_date}`}
          />
        );
      })}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function EarningsDashboard() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [selected, setSelected] = useState<Company | null>(null);
  const [metrics, setMetrics] = useState<MetricRow[]>([]);
  const [loadingMetrics, setLoadingMetrics] = useState(false);

  const snapshotSSE = useSSERefresh();
  const estimatesSSE = useSSERefresh();
  const allSSE = useSSERefresh();

  const anyRunning = snapshotSSE.running || estimatesSSE.running || allSSE.running;

  useEffect(() => {
    fetch(`${API_URL}/api/companies`)
      .then((r) => r.json())
      .then((data) => setCompanies(Array.isArray(data) ? data : []))
      .catch(() => {});
  }, []);

  const loadMetrics = useCallback(() => {
    if (!selected) return;
    setLoadingMetrics(true);
    fetch(`${API_URL}/api/earnings/${selected.company_id}/metrics`)
      .then((r) => r.json())
      .then((data) => setMetrics(Array.isArray(data) ? data : []))
      .catch(() => setMetrics([]))
      .finally(() => setLoadingMetrics(false));
  }, [selected]);

  useEffect(() => { loadMetrics(); }, [loadMetrics]);

  function refreshSnapshot() {
    if (!selected) return;
    snapshotSSE.start(`${API_URL}/api/earnings/${selected.company_id}/refresh-all?force=true`, loadMetrics);
  }

  function refreshEstimates() {
    if (!selected) return;
    estimatesSSE.start(`${API_URL}/api/earnings/${selected.company_id}/refresh/analyst_estimates?force=true`, loadMetrics);
  }

  function refreshAll() {
    if (!selected) return;
    allSSE.start(`${API_URL}/api/earnings/${selected.company_id}/refresh-all?force=true`, loadMetrics);
  }

  return (
    <div className="px-8 py-5 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Earnings Dashboard</h1>
      </div>

      {/* Company picker */}
      <div className="flex items-center gap-4">
        <CompanyPicker companies={companies} selected={selected} onSelect={setSelected} />
        {selected && <RefreshButton label="Refresh All" running={anyRunning} onClick={refreshAll} />}
      </div>

      {!selected && (
        <div className="text-gray-500 py-12 text-center">Select a company to view earnings data</div>
      )}

      {selected && (
        <>
          <div className="text-gray-400 text-sm">
            {selected.company_name || selected.primary_ticker} — {selected.primary_ticker}.{selected.primary_exchange}
            {loadingMetrics && <span className="ml-2 text-gray-600">Loading metrics...</span>}
          </div>

          <LogPanel logs={allSSE.logs} logEndRef={allSSE.logEndRef} />

          {/* Snapshot Stats */}
          <section className="bg-[#151821] rounded-xl border border-gray-800/40 p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-white font-medium">Snapshot Stats</h2>
              <RefreshButton label="Refresh Snapshot" running={anyRunning} onClick={refreshSnapshot} />
            </div>
            <LogPanel logs={snapshotSSE.logs} logEndRef={snapshotSSE.logEndRef} />
            <SnapshotStats metrics={metrics} />
          </section>

          {/* Charts in a 3-column grid */}
          <div className="grid grid-cols-1 xl:grid-cols-3 gap-6">
            {/* FCF Yield */}
            <section className="bg-[#151821] rounded-xl border border-gray-800/40 p-5 space-y-3">
              <h2 className="text-white font-medium">FCF Yield %</h2>
              <FCFYieldChart metrics={metrics} />
            </section>

            {/* Relative Growth */}
            <section className="bg-[#151821] rounded-xl border border-gray-800/40 p-5 space-y-3">
              <h2 className="text-white font-medium">Relative Growth (log)</h2>
              <RelativeGrowthChart metrics={metrics} />
            </section>

            {/* FCF/share Growth */}
            <section className="bg-[#151821] rounded-xl border border-gray-800/40 p-5 space-y-3">
              <h2 className="text-white font-medium">FCF/share Growth (log)</h2>
              <FCFShareChart metrics={metrics} />
            </section>
          </div>

          {/* Analyst Estimates */}
          <section className="bg-[#151821] rounded-xl border border-gray-800/40 p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-white font-medium">Analyst Estimates</h2>
              <RefreshButton label="Refresh Estimates" running={anyRunning} onClick={refreshEstimates} />
            </div>
            <LogPanel logs={estimatesSSE.logs} logEndRef={estimatesSSE.logEndRef} />
            <AnalystEstimates metrics={metrics} />
          </section>
        </>
      )}
    </div>
  );
}
