'use client';

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  ReferenceLine, CartesianGrid, Legend,
} from 'recharts';

import ApiUsageBadge, { type ApiUsageBadgeHandle } from './ApiUsageBadge';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type Company = {
  company_id: number;
  gurufocus_ticker: string;
  gurufocus_exchange: string;
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
  // Reverse DCF / WACC metrics
  WACC: 'annuals__Ratios__WACC %',
  ROIC_ANNUAL: 'annuals__Ratios__ROIC %',
  BETA: 'annuals__Valuation and Quality__Beta',
  NET_CASH_PS: 'annuals__Valuation and Quality__Net Cash per Share',
  GF_INTRINSIC: 'annuals__Valuation and Quality__Intrinsic Value: Projected FCF',
  PIOTROSKI: 'annuals__Valuation and Quality__Piotroski F-Score',
  ALTMAN_Z: 'annuals__Valuation and Quality__Altman Z-Score',
  BUYBACK_RATIO: 'annuals__Valuation and Quality__Shares Buyback Ratio %',
  YOY_REV_GROWTH: 'annuals__Valuation and Quality__YoY Rev. per Sh. Growth',
  EBITDA_5Y_GROWTH: 'annuals__Valuation and Quality__5-Year EBITDA Growth Rate (Per Share)',
  YOY_EPS_GROWTH: 'annuals__Valuation and Quality__YoY EPS Growth',
  DIV_YIELD: 'annuals__Valuation Ratios__Dividend Yield %',
  TAX_RATE: 'annuals__Income Statement__Tax Rate %',
  // LongEquity metrics
  SP_5Y_CAGR: 'share_price_5yr_cagr',
  SP_5Y_RSQ: 'share_price_5yr_rsq',
  SP_10Y_CAGR: 'share_price_10yr_cagr',
  SP_10Y_RSQ: 'share_price_10yr_rsq',
  REV_GROWTH_5Y: 'revenue_growth_5yr',
  REV_GROWTH_RSQ: 'revenue_growth_rsq',
  FCF_GROWTH_5Y: 'fcf_growth_5yr',
  FCF_GROWTH_SD: 'fcf_growth_sd',
  FCF_GROWTH_RSQ: 'fcf_growth_rsq',
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

/** Pick the earliest estimate after a reference date (for FY1 = next fiscal year). */
function earliestFutureValue(metrics: MetricRow[], code: string, afterDate: string): { value: number; date: string } | null {
  let best: MetricRow | null = null;
  for (const m of metrics) {
    if (m.metric_code !== code || m.numeric_value == null) continue;
    if (m.target_date <= afterDate) continue;
    if (!best || m.target_date < best.target_date) best = m;
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

function useSSERefresh(onApiCalls?: (region: string, count: number) => void) {
  const [logs, setLogs] = useState<{ type: string; message: string }[]>([]);
  const [running, setRunning] = useState(false);
  const logEndRef = useRef<HTMLDivElement>(null);
  const onApiCallsRef = useRef(onApiCalls);
  onApiCallsRef.current = onApiCalls;

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
            if (parsed.type === 'api_calls' && onApiCallsRef.current) {
              onApiCallsRef.current(parsed.region, parsed.count);
            }
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
    const el = logEndRef.current;
    if (el?.parentElement) {
      el.parentElement.scrollTop = el.parentElement.scrollHeight;
    }
  }, [logs]);

  const clearLogs = useCallback(() => setLogs([]), []);

  return { logs, running, start, logEndRef, clearLogs };
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
        c.gurufocus_ticker.toLowerCase().includes(q)
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
        placeholder={selected ? `${selected.company_name || selected.gurufocus_ticker}` : 'Search company or ticker...'}
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
              <span className="font-mono text-indigo-400 text-sm">{c.gurufocus_ticker}</span>
              <span className="text-gray-300 text-sm truncate">{c.company_name || '—'}</span>
              <span className="text-gray-600 text-xs ml-auto">{c.gurufocus_exchange}</span>
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

function LogPanel({ logs, logEndRef, running, onClose }: { logs: { type: string; message: string }[]; logEndRef: React.RefObject<HTMLDivElement | null>; running: boolean; onClose?: () => void }) {
  if (logs.length === 0) return null;
  const isDone = !running;
  return (
    <div className="bg-[#0b0d13] border border-gray-800/40 rounded-lg overflow-hidden">
      <div className="px-3 py-1.5 border-b border-gray-800/40 flex items-center gap-2">
        {isDone
          ? <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />
          : <div className="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse" />}
        <span className="text-gray-500 text-xs font-medium">{isDone ? 'Refresh Complete' : 'Refresh Progress'}</span>
        <button onClick={onClose} className="ml-auto text-gray-500 hover:text-gray-300 transition-colors" aria-label="Close">
          <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-3.5 h-3.5">
            <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
          </svg>
        </button>
      </div>
      <div className="max-h-[5.5rem] overflow-y-auto p-3 font-mono text-xs">
      {logs.map((l, i) => (
        <div key={i} className={l.type === 'error' ? 'text-rose-400' : l.type === 'done' ? 'text-emerald-400' : 'text-gray-400'}>
          {l.message}
        </div>
      ))}
      <div ref={logEndRef} />
      </div>
    </div>
  );
}

function InfoTip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number }>({ top: 0, left: 0 });
  const iconRef = useRef<HTMLSpanElement>(null);

  const tipWidth = 224; // w-56
  const margin = 8;

  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const clampedLeft = Math.max(margin + tipWidth / 2, Math.min(centerX, window.innerWidth - margin - tipWidth / 2));
      setPos({ top: rect.top - 8, left: clampedLeft });
    }
    setShow(true);
  };

  return (
    <span className="relative cursor-help" onMouseEnter={handleEnter} onMouseLeave={() => setShow(false)}>
      <span ref={iconRef} className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-gray-600 text-gray-500 text-[10px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors">i</span>
      {show && (
        <span
          className="fixed w-56 px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-xs text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, transform: 'translate(-50%, -100%)' }}
        >
          {text}
        </span>
      )}
    </span>
  );
}

function SectionLoader({ label }: { label: string }) {
  return (
    <div className="flex flex-col items-center justify-center py-10 gap-3">
      <div className="flex items-center gap-2">
        <svg className="animate-spin h-4 w-4 text-indigo-400" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
        <span className="text-gray-400 text-sm">Loading {label}...</span>
      </div>
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

  // Derived: FCF / Net Income
  const fcfOverNi = useMemo(() => {
    const fcf = lv(MC.FCF);
    const ni = lv(MC.NET_INCOME);
    if (!fcf || !ni || ni.value === 0) return null;
    return fcf.value / ni.value;
  }, [lv]);




  // Derived: FCF/share CAGR windows

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
      title: 'Value Creation',
      rows: [
        { label: 'Price 5Y CAGR', value: fmtPct(lv(MC.SP_5Y_CAGR)?.value ?? null), date: lv(MC.SP_5Y_CAGR)?.date, info: 'Compound Annual Growth Rate of share price over the last 5 years (LongEquity).' },
        { label: 'Price 5Y R²', value: fmtNum(lv(MC.SP_5Y_RSQ)?.value ?? null), date: lv(MC.SP_5Y_RSQ)?.date, info: 'R-squared of 5-year share price trend. Higher = more consistent growth.' },
        { label: 'Price 10Y CAGR', value: fmtPct(lv(MC.SP_10Y_CAGR)?.value ?? null), date: lv(MC.SP_10Y_CAGR)?.date, info: 'Compound Annual Growth Rate of share price over the last 10 years (LongEquity).' },
        { label: 'Price 10Y R²', value: fmtNum(lv(MC.SP_10Y_RSQ)?.value ?? null), date: lv(MC.SP_10Y_RSQ)?.date, info: 'R-squared of 10-year share price trend. Higher = more consistent growth.' },
      ],
    },
  ];

  const rightSections: { title: string; rows: StatRow[] }[] = [
    {
      title: 'Profitability',
      rows: [
        { label: 'Gross Margin', value: fmtPctPoints(lv(MC.GROSS_MARGIN)?.value ?? null), date: lv(MC.GROSS_MARGIN)?.date, info: 'Gross Profit / Revenue. Indicates pricing power and cost of goods sold efficiency.' },
        { label: 'Net Margin', value: fmtPctPoints(lv(MC.NET_MARGIN)?.value ?? null), date: lv(MC.NET_MARGIN)?.date, info: 'Net Income / Revenue. Bottom-line profitability after all expenses.' },
        { label: 'FCF / Net Income', value: fmtPct(fcfOverNi), date: lv(MC.FCF)?.date, info: 'Free Cash Flow / Net Income. Above 1 means cash earnings exceed accounting earnings (high quality).' },
      ],
    },
    {
      title: 'Historical Growth',
      rows: [
        { label: 'Revenue 5Y Growth', value: fmtPct(lv(MC.REV_GROWTH_5Y)?.value ?? null), date: lv(MC.REV_GROWTH_5Y)?.date, info: '5-year revenue growth rate (LongEquity).' },
        { label: 'Revenue R²', value: fmtNum(lv(MC.REV_GROWTH_RSQ)?.value ?? null), date: lv(MC.REV_GROWTH_RSQ)?.date, info: 'R-squared of revenue growth trend. Higher = more consistent growth.' },
        { label: 'FCF 5Y Growth', value: fmtPct(lv(MC.FCF_GROWTH_5Y)?.value ?? null), date: lv(MC.FCF_GROWTH_5Y)?.date, info: '5-year FCF growth rate (LongEquity).' },
        { label: 'FCF Growth R²', value: fmtNum(lv(MC.FCF_GROWTH_RSQ)?.value ?? null), date: lv(MC.FCF_GROWTH_RSQ)?.date, info: 'R-squared of FCF growth trend. Higher = more consistent growth.' },
        { label: 'FCF Growth SD', value: fmtNum(lv(MC.FCF_GROWTH_SD)?.value ?? null), date: lv(MC.FCF_GROWTH_SD)?.date, info: 'Standard deviation of FCF growth. Lower = more predictable.' },
      ],
    },
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
                    {r.info && <InfoTip text={r.info} />}
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
// Chart 1: Forward P/E (line + mean)
// ---------------------------------------------------------------------------

function ForwardPEChart({ metrics }: { metrics: MetricRow[] }) {
  const data = useMemo(() => timeSeries(metrics, MC.FWD_PE), [metrics]);
  const mean = useMemo(() => {
    if (data.length === 0) return 0;
    return data.reduce((s, d) => s + d.value, 0) / data.length;
  }, [data]);

  if (data.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No Forward P/E data. Refresh to load.</div>;
  }

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1">Period avg: <span className="text-rose-400 font-mono">{mean.toFixed(1)}x</span> (red dashed) <InfoTip text="Forward P/E = Price / Next-year EPS estimate. Lower = cheaper relative to expected earnings. The red dashed line shows the average across the visible period — useful for spotting when the stock trades above or below its typical valuation." /></div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 7)} />
          <YAxis tick={{ fontSize: 11, fill: '#6b7280' }} tickFormatter={(v: number) => `${v.toFixed(0)}x`} />
          <Tooltip contentStyle={tooltipStyle} labelStyle={{ color: '#9ca3af' }} formatter={(v) => [`${Number(v).toFixed(1)}x`, 'Fwd P/E']} />
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

// ---------------------------------------------------------------------------
// Chart 3: FCF/share Growth (log indexed)
// ---------------------------------------------------------------------------

function FCFShareChart({ metrics }: { metrics: MetricRow[] }) {
  const { data, cagr } = useMemo(() => {
    const series = annualSeries(metrics, MC.FCF_PS);
    if (series.length === 0) return { data: [], cagr: null };
    const positiveSeries = series.filter((s) => s.value > 0);
    return {
      data: series,
      cagr: computeCAGR(positiveSeries),
    };
  }, [metrics]);

  if (data.length === 0) {
    return <div className="text-gray-500 text-sm py-8 text-center">No FCF/share data. Refresh to load.</div>;
  }

  const hasNegative = data.some((d) => d.value < 0);

  return (
    <>
      <div className="text-gray-500 text-xs mb-2 flex items-center gap-1 flex-wrap">
        FCF per share (raw values) <InfoTip text="Free Cash Flow per share over time. Negative values are shaded red. CAGR is computed from positive values only." />
      </div>
      <div className="flex flex-wrap gap-x-4 gap-y-1 mb-2">
        <div className="flex items-center gap-1">
          <div className="text-gray-500 text-[11px]">CAGR (positive only)</div>
          <div className="text-white font-mono text-xs">{fmtPct(cagr)}</div>
        </div>
        <div className="flex items-center gap-1">
          <div className="text-gray-500 text-[11px]">Latest</div>
          <div className="text-white font-mono text-xs">{fmtNum(data[data.length - 1].value, 2)}</div>
        </div>
      </div>
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={data} margin={{ top: 5, right: 10, bottom: 5, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#1e2330" />
          <XAxis dataKey="date" tick={{ fontSize: 10, fill: '#6b7280' }} tickFormatter={(v: string) => v.slice(0, 4)} />
          <YAxis
            tick={{ fontSize: 11, fill: '#6b7280' }}
            tickFormatter={(v: number) => v.toFixed(1)}
          />
          {hasNegative && <ReferenceLine y={0} stroke="#6b7280" strokeDasharray="3 3" />}
          <Tooltip
            contentStyle={tooltipStyle}
            labelStyle={{ color: '#9ca3af' }}
            formatter={(v) => [Number(v).toFixed(2), 'FCF/share']}
          />
          <Line
            type="monotone"
            dataKey="value"
            name="FCF/share"
            stroke="#818cf8"
            strokeWidth={2}
            dot={(props: any) => {
              const { cx, cy, payload } = props;
              if (payload.value < 0) {
                return <circle cx={cx} cy={cy} r={3} fill="#f87171" stroke="#f87171" />;
              }
              return <circle cx={cx} cy={cy} r={0} fill="none" stroke="none" />;
            }}
          />
        </LineChart>
      </ResponsiveContainer>
    </>
  );
}

// ---------------------------------------------------------------------------
// EGM Calculator (interactive)
// ---------------------------------------------------------------------------

function EGMCalculator({ metrics }: { metrics: MetricRow[] }) {
  const epsRaw = latestValue(metrics, MC.EPS_DILUTED);
  const fy1Raw = epsRaw
    ? earliestFutureValue(metrics, MC.EPS_FY1_EST, epsRaw.date)
    : latestValue(metrics, MC.EPS_FY1_EST);
  const yoyEpsGrowth = latestValue(metrics, MC.YOY_EPS_GROWTH);

  const [eps, setEps] = useState<string>('');
  const [fy1, setFy1] = useState<string>('');
  const [initialized, setInitialized] = useState(false);

  const resetDefaults = useCallback(() => {
    setEps(epsRaw ? epsRaw.value.toFixed(2) : '');
    setFy1(fy1Raw ? fy1Raw.value.toFixed(2) : '');
  }, [epsRaw, fy1Raw]);

  useEffect(() => {
    if (!initialized && (epsRaw || fy1Raw)) {
      resetDefaults();
      setInitialized(true);
    }
  }, [initialized, epsRaw, fy1Raw, resetDefaults]);

  const epsNum = parseFloat(eps);
  const fy1Num = parseFloat(fy1);
  const egm = !isNaN(epsNum) && !isNaN(fy1Num) && epsNum !== 0
    ? (fy1Num - epsNum) / epsNum
    : null;

  const inputClass = "w-28 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-white font-mono text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none";

  return (
    <div className="flex items-center gap-6">
      <div>
        <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">
          Current EPS {epsRaw && <span className="text-gray-600">({epsRaw.date})</span>}
          <InfoTip text="Diluted EPS (excluding non-recurring items) for the most recent fiscal year. Used as the base for calculating expected growth." />
        </div>
        <input type="number" step="0.01" value={eps} onChange={(e) => setEps(e.target.value)} className={inputClass} />
      </div>
      <div>
        <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">
          FY1 EPS Est {fy1Raw && <span className="text-gray-600">({fy1Raw.date})</span>}
          <InfoTip text="Analyst consensus EPS estimate for the next fiscal year. The growth from Current EPS to this value determines the EGM." />
        </div>
        <input type="number" step="0.01" value={fy1} onChange={(e) => setFy1(e.target.value)} className={inputClass} />
      </div>
      <div>
        <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">
          EGM
          <InfoTip text="Earnings Growth Multiple = (FY1 EPS Est - Current EPS) / Current EPS. Shows expected year-over-year earnings growth. Green if above historic YoY EPS, red if below." />
        </div>
        <div className={`font-mono text-2xl font-semibold ${egm != null && yoyEpsGrowth ? (egm >= yoyEpsGrowth.value / 100 ? 'text-emerald-400' : 'text-rose-400') : 'text-white'}`}>
          {egm != null ? fmtPct(egm) : '—'}
        </div>
      </div>
      <div>
        <div className="text-gray-500 text-xs mb-1 flex items-center gap-1">
          Historic YoY EPS
          <InfoTip text={`Year-over-year EPS growth for the most recent fiscal year (${yoyEpsGrowth?.date ?? '—'}). Single year, not a multi-year average. Used as a benchmark for the EGM.`} />
        </div>
        <div className="font-mono text-2xl font-semibold text-gray-400">
          {yoyEpsGrowth ? fmtPct(yoyEpsGrowth.value / 100) : '—'}
        </div>
      </div>
      <div>
        <div className="text-gray-500 text-xs mb-1">&nbsp;</div>
        <button onClick={resetDefaults} className="px-3 py-1.5 rounded-lg text-sm text-gray-400 hover:text-white hover:bg-white/5 border border-gray-700 transition-colors">
          Reset
        </button>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Reverse DCF Calculator (interactive)
// ---------------------------------------------------------------------------

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

function ReverseDCF({ metrics }: { metrics: MetricRow[] }) {
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

  // Only auto-fill on first data load, not on every re-render
  useEffect(() => {
    if (!initialized && (priceRaw || fcfRaw)) {
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

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export default function EarningsDashboard() {
  const [companies, setCompanies] = useState<Company[]>([]);
  const [selected, setSelected] = useState<Company | null>(null);
  const [metrics, setMetrics] = useState<MetricRow[]>([]);
  const [loadingMetrics, setLoadingMetrics] = useState(false);
  const currentYear = new Date().getFullYear();
  const [startYear, setStartYear] = useState(2015);
  const [startYearInput, setStartYearInput] = useState('2015');
  const [startYearError, setStartYearError] = useState('');

  const applyStartYear = useCallback((raw: string) => {
    const v = parseInt(raw, 10);
    if (isNaN(v) || v < 2015) {
      setStartYearError('Min 2015');
    } else if (v > currentYear) {
      setStartYearError(`Max ${currentYear}`);
    } else {
      setStartYear(v);
      setStartYearInput(String(v));
      setStartYearError('');
    }
  }, [currentYear]);

  const nudgeStartYear = useCallback((delta: number) => {
    const next = startYear + delta;
    if (next >= 2015 && next <= currentYear) {
      setStartYear(next);
      setStartYearInput(String(next));
      setStartYearError('');
    }
  }, [startYear, currentYear]);

  const chartMetrics = useMemo(
    () => metrics.filter((m) => m.target_date >= `${startYear}-01-01`),
    [metrics, startYear],
  );

  const usageBadgeRef = useRef<ApiUsageBadgeHandle>(null);

  const sse = useSSERefresh((region, count) => {
    usageBadgeRef.current?.addSessionCalls(region, count);
  });

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

  useEffect(() => { sse.clearLogs(); loadMetrics(); }, [loadMetrics]);

  const refresh = (source: string) => {
    if (!selected) return;
    const endpoint = source === 'all' ? 'refresh-all' : `refresh/${source}`;
    sse.start(`${API_URL}/api/earnings/${selected.company_id}/${endpoint}?force=true`, () => {
      loadMetrics();
      usageBadgeRef.current?.refresh();
    });
  };

  return (
    <div className="px-8 py-5 space-y-6">
      <div className="flex items-center justify-between">
        <h1 className="text-xl font-semibold text-white">Earnings Dashboard</h1>
        <ApiUsageBadge ref={usageBadgeRef} />
      </div>

      {/* Company picker */}
      <div className="flex items-center gap-4">
        <CompanyPicker companies={companies} selected={selected} onSelect={setSelected} />
        {selected && <RefreshButton label="Refresh All" running={sse.running} onClick={() => refresh('all')} />}
      </div>

      {!selected && (
        <div className="text-gray-500 py-12 text-center">Select a company to view earnings data</div>
      )}

      {selected && (
        <>
          <div className="text-gray-400 text-sm">
            {selected.company_name || selected.gurufocus_ticker} — {selected.gurufocus_ticker}.{selected.gurufocus_exchange}
          </div>

          <LogPanel logs={sse.logs} logEndRef={sse.logEndRef} running={sse.running} onClose={sse.clearLogs} />

          {/* Snapshot Stats */}
          <section className="bg-[#151821] rounded-xl border border-indigo-500/20 p-5 space-y-4">
            <div className="flex items-center justify-between">
              <h2 className="text-white font-medium">Snapshot Stats</h2>
              <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
            </div>
            {loadingMetrics ? <SectionLoader label="snapshot stats" /> : <SnapshotStats metrics={metrics} />}
          </section>

          {/* Charts container */}
          <section className="bg-[#151821] rounded-xl border border-indigo-500/20 p-5 space-y-5">
            <div className="flex items-center gap-3">
              <h2 className="text-white font-medium">Charts</h2>
              <div className="flex items-center gap-1.5">
                <span className="text-gray-500 text-sm">From</span>
                <button
                  onClick={() => nudgeStartYear(-1)}
                  disabled={startYear <= 2015}
                  className="w-6 h-6 flex items-center justify-center rounded text-gray-400 hover:text-white hover:bg-white/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-sm"
                >&#9666;</button>
                <div className="relative">
                  <input
                    type="text"
                    value={startYearInput}
                    onChange={(e) => setStartYearInput(e.target.value)}
                    onBlur={() => applyStartYear(startYearInput)}
                    onKeyDown={(e) => { if (e.key === 'Enter') applyStartYear(startYearInput); }}
                    className={`w-16 bg-[#0f1117] border rounded-lg px-2 py-1 text-white text-sm font-mono text-center outline-none transition-colors ${startYearError ? 'border-rose-500 focus:border-rose-500 focus:ring-1 focus:ring-rose-500/30' : 'border-gray-700 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30'}`}
                  />
                  {startYearError && (
                    <div className="absolute top-full left-1/2 -translate-x-1/2 mt-1 text-rose-400 text-[10px] whitespace-nowrap">{startYearError}</div>
                  )}
                </div>
                <button
                  onClick={() => nudgeStartYear(1)}
                  disabled={startYear >= currentYear}
                  className="w-6 h-6 flex items-center justify-center rounded text-gray-400 hover:text-white hover:bg-white/10 transition-colors disabled:opacity-30 disabled:cursor-not-allowed text-sm"
                >&#9656;</button>
              </div>
            </div>
            <div className="grid grid-cols-1 xl:grid-cols-3 gap-5">
              {/* FCF Yield */}
              <div className="bg-[#0f1117] rounded-lg border border-indigo-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-white text-sm font-medium flex items-center gap-1.5"><span className="truncate">Forward P/E</span> <InfoTip text="Forward Price-to-Earnings ratio over time. Shows how much investors pay per dollar of expected earnings. Compare to the period average (red dashed) to spot relative cheapness or richness." /></h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('indicators')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Forward P/E" /> : <ForwardPEChart metrics={chartMetrics} />}
              </div>

              {/* Relative Growth */}
              <div className="bg-[#0f1117] rounded-lg border border-indigo-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-white text-sm font-medium flex items-center gap-1.5"><span className="truncate">Relative Growth (log)</span> <InfoTip text="Tracks whether the share price is growing in line with Owner Earnings (EPS + Dividends). On a log scale, parallel lines mean the valuation multiple is stable. Divergence signals re-rating." /></h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('prices')} />
                </div>
                {loadingMetrics ? <SectionLoader label="Relative Growth" /> : <RelativeGrowthChart metrics={chartMetrics} />}
              </div>

              {/* FCF/share Growth */}
              <div className="bg-[#0f1117] rounded-lg border border-indigo-500/20 p-4 space-y-2 overflow-hidden min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <h3 className="text-white text-sm font-medium flex items-center gap-1.5"><span className="truncate">FCF/share Growth</span> <InfoTip text="Free Cash Flow per share over time. Shows the trajectory of cash generation. Negative values are highlighted with red dots." /></h3>
                  <RefreshButton label="Refresh" running={sse.running} onClick={() => refresh('financials')} />
                </div>
                {loadingMetrics ? <SectionLoader label="FCF/share" /> : <FCFShareChart metrics={chartMetrics} />}
              </div>
            </div>
          </section>

          {/* EGM Calculator */}
          <section className="bg-[#151821] rounded-xl border border-indigo-500/20 p-5 space-y-4">
            <h2 className="text-white font-medium flex items-center gap-1.5">Expected Return (EGM) <InfoTip text="Earnings Growth Multiple — the projected year-over-year EPS growth from the current fiscal year to the next (FY1 estimate). Compares analyst expectations to the stock's actual recent EPS growth rate." /></h2>
            {loadingMetrics ? <SectionLoader label="EGM calculator" /> : <EGMCalculator metrics={metrics} />}
          </section>

          {/* Reverse DCF */}
          <section className="bg-[#151821] rounded-xl border border-indigo-500/20 p-5 space-y-4">
            <h2 className="text-white font-medium flex items-center gap-1.5">Reverse DCF <InfoTip text="Reverse Discounted Cash Flow — instead of estimating a fair value, it solves for the FCF growth rate the market is currently pricing in. If implied growth exceeds historic growth, the market expects acceleration (or the stock may be overvalued)." /></h2>
            {loadingMetrics ? <SectionLoader label="Reverse DCF" /> : <ReverseDCF metrics={metrics} />}
          </section>
        </>
      )}
    </div>
  );
}
