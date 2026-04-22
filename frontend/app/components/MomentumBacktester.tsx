'use client';

import { Fragment, useState, useEffect, useMemo, useRef } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceArea,
} from 'recharts';

import ApiUsageBadge from './ApiUsageBadge';
import { dialog } from '../../lib/dialog';
import {
  momentumStore,
  startBacktest,
  cancelBacktest,
  type DrawdownPeriod,
  type Summary,
} from '../../lib/stores/momentum';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type SignalDef = {
  key: string;
  label: string;
  description: string;
  default_weight: number;
  group?: string;
};

type SavedRun = {
  run_id: number;
  name: string;
  created_at: string;
  config: Record<string, unknown>;
  summary: Summary;
};

type BenchmarkOption = {
  benchmark_id: number;
  ticker: string;
  name: string;
};

type BenchmarkPrice = {
  target_date: string;
  price: number;
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const fmtPct = (v: number | null) => (v != null ? `${v >= 0 ? '+' : ''}${v.toFixed(2)}%` : '—');

const fmtPrice = (v: number | null | undefined) => {
  if (v == null) return '—';
  const d = Math.abs(v) >= 1000 ? 0 : Math.abs(v) >= 10 ? 2 : 4;
  return v.toFixed(d);
};

function guruFocusUrl(ticker: string, exchange: string): string {
  const USA = new Set(['NYSE', 'NASDAQ', 'US', 'AMEX']);
  const t = ticker.toUpperCase();
  const e = exchange.toUpperCase();
  if (!e || USA.has(e)) return `https://www.gurufocus.com/stock/${t}/summary`;
  return `https://www.gurufocus.com/stock/${e}:${t}/summary`;
}

const EXCHANGE_NAMES: Record<string, string> = {
  NYSE: 'New York Stock Exchange',
  NAS: 'NASDAQ',
  NASDAQ: 'NASDAQ',
  AMEX: 'NYSE American',
  OTCPK: 'OTC Markets Pink',
  OTCBB: 'OTC Bulletin Board',
  LSE: 'London Stock Exchange',
  XETR: 'Xetra (Deutsche Börse)',
  XETRA: 'Xetra (Deutsche Börse)',
  FRA: 'Frankfurt Stock Exchange',
  GER: 'Deutsche Börse',
  EPA: 'Euronext Paris',
  XPAR: 'Euronext Paris',
  AMS: 'Euronext Amsterdam',
  XAMS: 'Euronext Amsterdam',
  BRU: 'Euronext Brussels',
  XBRU: 'Euronext Brussels',
  LIS: 'Euronext Lisbon',
  XLIS: 'Euronext Lisbon',
  MIL: 'Borsa Italiana (Milan)',
  BIT: 'Borsa Italiana (Milan)',
  MCE: 'Bolsa de Madrid',
  BME: 'Bolsa de Madrid',
  SWX: 'SIX Swiss Exchange',
  SIX: 'SIX Swiss Exchange',
  VIE: 'Vienna Stock Exchange',
  WBO: 'Vienna Stock Exchange',
  WAR: 'Warsaw Stock Exchange',
  WSE: 'Warsaw Stock Exchange',
  IST: 'Borsa Istanbul',
  XIST: 'Borsa Istanbul',
  HEL: 'Nasdaq Helsinki',
  CPH: 'Nasdaq Copenhagen',
  STO: 'Nasdaq Stockholm',
  OSL: 'Oslo Stock Exchange',
  ICE: 'Nasdaq Iceland',
  DUB: 'Euronext Dublin',
  ATH: 'Athens Stock Exchange',
  BUD: 'Budapest Stock Exchange',
  PRA: 'Prague Stock Exchange',
  BUC: 'Bucharest Stock Exchange',
  MOEX: 'Moscow Exchange',
  TSX: 'Toronto Stock Exchange',
  TSXV: 'TSX Venture Exchange',
  CVE: 'TSX Venture Exchange',
  CNSX: 'Canadian Securities Exchange',
  MEX: 'Bolsa Mexicana de Valores',
  BCBA: 'Buenos Aires Stock Exchange',
  BVMF: 'B3 (São Paulo)',
  SAO: 'B3 (São Paulo)',
  TSE: 'Tokyo Stock Exchange',
  HKSE: 'Hong Kong Stock Exchange',
  SHSE: 'Shanghai Stock Exchange',
  SSE: 'Shanghai Stock Exchange',
  SZSE: 'Shenzhen Stock Exchange',
  TPE: 'Taiwan Stock Exchange',
  TWSE: 'Taiwan Stock Exchange',
  ROCO: 'Taipei Exchange',
  XKRX: 'Korea Exchange',
  KRX: 'Korea Exchange',
  NSE: 'National Stock Exchange of India',
  BSE: 'Bombay Stock Exchange',
  SGX: 'Singapore Exchange',
  XKLS: 'Bursa Malaysia',
  KLSE: 'Bursa Malaysia',
  BKK: 'Stock Exchange of Thailand',
  SET: 'Stock Exchange of Thailand',
  PHS: 'Philippine Stock Exchange',
  IDX: 'Indonesia Stock Exchange',
  ASX: 'Australian Securities Exchange',
  NZSE: 'New Zealand Exchange',
  NZX: 'New Zealand Exchange',
  JSE: 'Johannesburg Stock Exchange',
  TASE: 'Tel Aviv Stock Exchange',
  SAU: 'Saudi Stock Exchange (Tadawul)',
  DFM: 'Dubai Financial Market',
  ADX: 'Abu Dhabi Securities Exchange',
  QSE: 'Qatar Stock Exchange',
};

/** Compute top N non-overlapping drawdown periods from (date, value) pairs. */
function computeTopDrawdowns(values: { date: string; value: number }[], n: number = 3): DrawdownPeriod[] {
  if (values.length < 2) return [];

  const periods: DrawdownPeriod[] = [];
  let peakVal = values[0].value;
  let peakDate = values[0].date;
  let troughVal = peakVal;
  let troughDate = peakDate;
  let inDrawdown = false;

  for (let i = 1; i < values.length; i++) {
    const { date: dt, value: val } = values[i];
    if (val >= peakVal) {
      if (inDrawdown) {
        periods.push({
          drawdown_pct: Math.round((troughVal / peakVal - 1) * 10000) / 100,
          peak_date: peakDate,
          trough_date: troughDate,
          recovery_date: dt,
        });
        inDrawdown = false;
      }
      peakVal = val;
      peakDate = dt;
      troughVal = val;
      troughDate = dt;
    } else {
      inDrawdown = true;
      if (val < troughVal) {
        troughVal = val;
        troughDate = dt;
      }
    }
  }
  if (inDrawdown) {
    periods.push({
      drawdown_pct: Math.round((troughVal / peakVal - 1) * 10000) / 100,
      peak_date: peakDate,
      trough_date: troughDate,
      recovery_date: null,
    });
  }

  // Pick top N non-overlapping
  const sorted = [...periods].sort((a, b) => a.drawdown_pct - b.drawdown_pct);
  const selected: DrawdownPeriod[] = [];
  for (const p of sorted) {
    if (selected.length >= n) break;
    const pEnd = p.recovery_date ?? '9999-99';
    const overlaps = selected.some(s => {
      const sEnd = s.recovery_date ?? '9999-99';
      return p.peak_date <= sEnd && pEnd >= s.peak_date;
    });
    if (!overlaps) selected.push(p);
  }
  return selected;
}

const tooltipStyle = {
  contentStyle: { background: '#1a1d27', border: '1px solid rgba(75,85,99,0.4)', borderRadius: 8, fontSize: 13 },
  labelStyle: { color: '#9ca3af' },
  itemStyle: { color: '#e5e7eb' },
};

function CellInfoTip({ children }: { children: React.ReactNode }) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const iconRef = useRef<HTMLSpanElement>(null);
  const tipWidth = 220;
  const margin = 8;

  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const clampedLeft = Math.max(
        margin + tipWidth / 2,
        Math.min(centerX, window.innerWidth - margin - tipWidth / 2),
      );
      setPos({ top: rect.bottom + 6, left: clampedLeft });
    }
    setShow(true);
  };

  // Close on any scroll so the fixed-positioned tooltip doesn't float over
  // the sticky header after its anchor row has scrolled away.
  useEffect(() => {
    if (!show) return;
    const close = () => setShow(false);
    window.addEventListener('scroll', close, true);
    return () => window.removeEventListener('scroll', close, true);
  }, [show]);

  return (
    <span
      className="inline-block align-middle"
      onMouseEnter={handleEnter}
      onMouseLeave={() => setShow(false)}
    >
      <span
        ref={iconRef}
        className="inline-flex items-center justify-center w-3 h-3 ml-1 rounded-full border border-gray-700 text-gray-500 text-[8px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors cursor-help align-middle"
      >
        i
      </span>
      {show && (
        <span
          className="fixed px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-[11px] text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, width: tipWidth, transform: 'translate(-50%, 0)' }}
        >
          {children}
        </span>
      )}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function MomentumBacktester() {
  // Signal definitions from backend
  const [signalDefs, setSignalDefs] = useState<SignalDef[]>([]);
  const [weights, setWeights] = useState<Record<string, number>>({});
  const [categories, setCategories] = useState<string[]>([]);
  const [categoryWeights, setCategoryWeights] = useState<Record<string, number>>({});

  // Config
  const currentYear = new Date().getFullYear();
  const [startDate, setStartDate] = useState('2017-01');
  const [endDate, setEndDate] = useState(`${currentYear}-01`);
  const [topSectors, setTopSectors] = useState(4);
  const [topPerSector, setTopPerSector] = useState(6);
  const [skipPriceFetch, setSkipPriceFetch] = useState(false);
  const [maxCompanies, setMaxCompanies] = useState(0);

  // Backtest run state lives in a module-scoped store so the SSE stream
  // keeps running when the user navigates away from /momentum.
  const running = momentumStore.use((s) => s.running);
  const progress = momentumStore.use((s) => s.progress);
  const result = momentumStore.use((s) => s.result);
  const universe = momentumStore.use((s) => s.universe);
  const error = momentumStore.use((s) => s.error);
  const warnings = momentumStore.use((s) => s.warnings);
  const infos = momentumStore.use((s) => s.infos);
  const loadedRunId = momentumStore.use((s) => s.loadedRunId);

  const exchangeByCompany = useMemo(() => {
    const m = new Map<number, string>();
    for (const u of universe) m.set(u.company_id, u.exchange);
    return m;
  }, [universe]);

  const progressLogRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const el = progressLogRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [progress]);

  // Purely local UI state — safe to reset on navigation
  const [showWarnings, setShowWarnings] = useState(true);
  const [showInfos, setShowInfos] = useState(false);
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);

  // Save/load state
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [saveName, setSaveName] = useState('');
  const [saving, setSaving] = useState(false);

  // Benchmark state
  const [benchmarkOptions, setBenchmarkOptions] = useState<BenchmarkOption[]>([]);
  const [selectedBenchmarkId, setSelectedBenchmarkId] = useState<number | null>(null);
  const [benchmarkPrices, setBenchmarkPrices] = useState<BenchmarkPrice[]>([]);
  const [logScale, setLogScale] = useState(false);
  const [hoveredDrawdown, setHoveredDrawdown] = useState<number | null>(null);
  const [customFromMonth, setCustomFromMonth] = useState('');
  const [savedDropdownOpen, setSavedDropdownOpen] = useState(false);
  const savedDropdownRef = useRef<HTMLDivElement>(null);

  // Universe selection state — all universes live in the same table and are served
  // by /api/index-universe/indexes with enriched metadata.
  const [indexUniverses, setIndexUniverses] = useState<{ index_name: string; start_month: string; end_month: string; month_count: number; total_unique_tickers: number }[]>([]);
  const [selectedIndexUniverse, setSelectedIndexUniverse] = useState<string>('');
  const [universesLoading, setUniversesLoading] = useState(true);
  const [universesError, setUniversesError] = useState<string | null>(null);
  const [universesElapsed, setUniversesElapsed] = useState(0);

  // Load signal definitions + saved runs
  useEffect(() => {
    fetch(`${API_URL}/api/momentum/signals`)
      .then((r) => r.json())
      .then((d) => {
        const defs: SignalDef[] = d.signals ?? [];
        setSignalDefs(defs);
        const w: Record<string, number> = {};
        defs.forEach((s) => (w[s.key] = s.default_weight));
        setWeights(w);
        const cats: string[] = d.categories ?? [];
        setCategories(cats);
        const cw: Record<string, number> = {};
        cats.forEach((c) => (cw[c] = 50));
        setCategoryWeights(cw);
      })
      .catch(() => {});
    loadSavedRuns();
    fetch(`${API_URL}/api/benchmarks`)
      .then((r) => r.json())
      .then((data) => setBenchmarkOptions(data))
      .catch(() => {});
    const universesStart = Date.now();
    const tick = setInterval(() => setUniversesElapsed(Math.round((Date.now() - universesStart) / 1000)), 500);
    setUniversesLoading(true);
    setUniversesError(null);
    fetch(`${API_URL}/api/index-universe/indexes`)
      .then((r) => {
        if (!r.ok) throw new Error(`${r.status}`);
        return r.json();
      })
      .then((data) => setIndexUniverses(data))
      .catch((e) => setUniversesError(e instanceof Error ? e.message : String(e)))
      .finally(() => {
        clearInterval(tick);
        setUniversesLoading(false);
      });
    return () => clearInterval(tick);
  }, []);

  // When universe selection changes, auto-set start/end dates from the range
  const handleUniverseChange = (value: string) => {
    setSelectedIndexUniverse(value);
    if (value) {
      const entry = indexUniverses.find(i => i.index_name === value);
      if (entry) {
        setStartDate(entry.start_month);
        setEndDate(entry.end_month);
      }
    }
  };

  const universeDropdownValue = selectedIndexUniverse;

  useEffect(() => {
    if (!savedDropdownOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (savedDropdownRef.current && !savedDropdownRef.current.contains(e.target as Node)) {
        setSavedDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [savedDropdownOpen]);

  const loadSavedRuns = () => {
    fetch(`${API_URL}/api/momentum/backtests`)
      .then((r) => r.json())
      .then((data) => setSavedRuns(data))
      .catch(() => {});
  };

  // Fetch benchmark prices when selection or date range changes
  useEffect(() => {
    if (!selectedBenchmarkId || !result) {
      setBenchmarkPrices([]);
      return;
    }
    const dates = result.monthly_records.map((r) => r.date);
    if (dates.length === 0) return;
    // Extend end-date one month past the last record so the final forward-return
    // pair is available — strategy records hold return month[i] → month[i+1].
    const last = dates[dates.length - 1];
    const [ly, lm] = last.split('-').map(Number);
    const nm = lm === 12 ? 1 : lm + 1;
    const ny = lm === 12 ? ly + 1 : ly;
    const sd = `${dates[0]}-01`;
    const ed = `${ny}-${String(nm).padStart(2, '0')}-28`;
    fetch(`${API_URL}/api/benchmarks/${selectedBenchmarkId}/prices?start_date=${sd}&end_date=${ed}`)
      .then((r) => r.json())
      .then((data) => setBenchmarkPrices(data))
      .catch(() => setBenchmarkPrices([]));
  }, [selectedBenchmarkId, result]);

  // Compute benchmark monthly returns aligned to backtest months
  const benchmarkReturns = useMemo(() => {
    if (!result || benchmarkPrices.length === 0) return null;

    const months = result.monthly_records.map((r) => r.date);
    const priceByMonth = new Map<string, number>();

    // For each month, find the first available price on or after the 1st
    for (const bp of benchmarkPrices) {
      const month = bp.target_date.slice(0, 7);
      if (!priceByMonth.has(month)) {
        priceByMonth.set(month, bp.price);
      }
    }

    // Compute cumulative return aligned with the strategy:
    // The strategy record for month[i] includes the forward return earned
    // during that month (price change month[i] → month[i+1]).
    // So benchmark cumReturns[month[i]] should also include that same period's return.
    const cumReturns: Record<string, number> = {};
    let cumFactor = 1.0;
    const monthlyRets: number[] = [];

    const nextMonth = (ym: string) => {
      const [y, m] = ym.split('-').map(Number);
      const nm = m === 12 ? 1 : m + 1;
      const ny = m === 12 ? y + 1 : y;
      return `${ny}-${String(nm).padStart(2, '0')}`;
    };

    for (let i = 0; i < months.length; i++) {
      const p0 = priceByMonth.get(months[i]);
      const nextKey = i < months.length - 1 ? months[i + 1] : nextMonth(months[i]);
      const p1 = priceByMonth.get(nextKey);
      if (p0 && p1 && p0 > 0) {
        const ret = (p1 / p0 - 1) * 100;
        monthlyRets.push(ret);
        cumFactor *= (1 + ret / 100);
      }
      cumReturns[months[i]] = (cumFactor - 1) * 100;
    }

    // Summary stats
    const totalReturn = (cumFactor - 1) * 100;
    const years = monthlyRets.length / 12;
    const annualized = years > 0 ? (Math.pow(cumFactor, 1 / years) - 1) * 100 : 0;

    // Max drawdown
    let peak = 1.0;
    let maxDd = 0;
    let factor = 1.0;
    for (const ret of monthlyRets) {
      factor *= (1 + ret / 100);
      peak = Math.max(peak, factor);
      const dd = ((factor / peak) - 1) * 100;
      maxDd = Math.min(maxDd, dd);
    }

    // Sharpe
    let sharpe: number | null = null;
    if (monthlyRets.length >= 12) {
      const mean = monthlyRets.reduce((a, b) => a + b, 0) / monthlyRets.length;
      const std = Math.sqrt(monthlyRets.reduce((a, b) => a + (b - mean) ** 2, 0) / monthlyRets.length);
      if (std > 0) sharpe = (mean / std) * Math.sqrt(12);
    }

    return { cumReturns, totalReturn, annualized, maxDd, sharpe };
  }, [result, benchmarkPrices]);

  // Compute top 3 non-overlapping drawdowns (client-side, works for saved backtests too)
  const topDrawdowns: DrawdownPeriod[] = useMemo(() => {
    if (!result) return [];
    if (result.summary.top_drawdowns && result.summary.top_drawdowns.length > 0) {
      return result.summary.top_drawdowns;
    }
    const values = result.monthly_records.map(r => ({
      date: r.date,
      value: 1 + r.cumulative_return_pct / 100,
    }));
    return computeTopDrawdowns(values, 3);
  }, [result]);

  // Compute top 3 drawdowns for benchmark
  const benchmarkDrawdowns: DrawdownPeriod[] = useMemo(() => {
    if (!benchmarkReturns || !result) return [];
    const values = result.monthly_records
      .filter(r => benchmarkReturns.cumReturns[r.date] != null)
      .map(r => ({
        date: r.date,
        value: 1 + benchmarkReturns.cumReturns[r.date] / 100,
      }));
    return computeTopDrawdowns(values, 3);
  }, [benchmarkReturns, result]);

  // Yearly performance breakdown — for each calendar year, compound return
  // from end of prior year (or 0% at start) to last record in that year.
  const yearlyBreakdown = useMemo(() => {
    if (!result || result.monthly_records.length === 0) return [];

    const lastByYear = new Map<string, { stratCum: number; benchCum: number | null }>();
    for (const r of result.monthly_records) {
      const y = r.date.slice(0, 4);
      const benchCum = benchmarkReturns?.cumReturns[r.date] ?? null;
      lastByYear.set(y, { stratCum: r.cumulative_return_pct, benchCum });
    }

    const years = Array.from(lastByYear.keys()).sort();
    const rows: { year: string; strategy: number; benchmark: number | null }[] = [];
    let prevStrat = 0;
    let prevBench = 0;
    let hasPrevBench = false;

    for (const y of years) {
      const rec = lastByYear.get(y)!;
      const stratRet = ((1 + rec.stratCum / 100) / (1 + prevStrat / 100) - 1) * 100;
      let benchRet: number | null = null;
      if (rec.benchCum != null) {
        const startBench = hasPrevBench ? prevBench : 0;
        benchRet = ((1 + rec.benchCum / 100) / (1 + startBench / 100) - 1) * 100;
        prevBench = rec.benchCum;
        hasPrevBench = true;
      }
      rows.push({ year: y, strategy: stratRet, benchmark: benchRet });
      prevStrat = rec.stratCum;
    }
    return rows;
  }, [result, benchmarkReturns]);

  // Cumulative return from customFromMonth through end of backtest.
  const customRangeReturn = useMemo(() => {
    if (!result || !customFromMonth || result.monthly_records.length === 0) return null;

    const records = result.monthly_records;
    const last = records[records.length - 1];

    let stratStart = 0;
    for (const r of records) {
      if (r.date < customFromMonth) stratStart = r.cumulative_return_pct;
      else break;
    }
    const stratRet = ((1 + last.cumulative_return_pct / 100) / (1 + stratStart / 100) - 1) * 100;

    let benchRet: number | null = null;
    if (benchmarkReturns) {
      let benchStart = 0;
      let benchEnd: number | null = null;
      for (const r of records) {
        const v = benchmarkReturns.cumReturns[r.date];
        if (v == null) continue;
        if (r.date < customFromMonth) benchStart = v;
        benchEnd = v;
      }
      if (benchEnd != null) {
        benchRet = ((1 + benchEnd / 100) / (1 + benchStart / 100) - 1) * 100;
      }
    }

    return { strategy: stratRet, benchmark: benchRet, fromDate: customFromMonth, toDate: last.date };
  }, [result, benchmarkReturns, customFromMonth]);

  // Chart data — prepend a 0% origin so both lines start from the same point
  const chartData = useMemo(() => {
    if (!result) return [];
    const firstDate = result.monthly_records[0]?.date;
    const origin = firstDate
      ? { date: `${firstDate} (start)`, cumReturn: 0, monthReturn: null, benchmark: selectedBenchmarkId ? 0 : null }
      : null;
    const points = result.monthly_records.map((r) => ({
      date: r.date,
      cumReturn: r.cumulative_return_pct,
      monthReturn: r.portfolio_return_pct,
      benchmark: benchmarkReturns?.cumReturns[r.date] ?? null,
    }));
    return origin ? [origin, ...points] : points;
  }, [result, benchmarkReturns, selectedBenchmarkId]);

  // Log-scale chart data: ln(1 + cumReturn/100) * 100
  const displayChartData = useMemo(() => {
    if (!logScale) return chartData;
    return chartData.map((p) => ({
      ...p,
      cumReturn: p.cumReturn != null ? Math.log(1 + p.cumReturn / 100) * 100 : null,
      benchmark: p.benchmark != null ? Math.log(1 + p.benchmark / 100) * 100 : null,
    }));
  }, [chartData, logScale]);

  // Y-axis domain for chart — used by ReferenceArea to span full height
  const chartYDomain = useMemo<[number, number]>(() => {
    if (!displayChartData.length) return [-100, 100];
    let min = Infinity, max = -Infinity;
    for (const p of displayChartData) {
      if (p.cumReturn != null) { min = Math.min(min, p.cumReturn); max = Math.max(max, p.cumReturn); }
      if (p.benchmark != null) { min = Math.min(min, p.benchmark); max = Math.max(max, p.benchmark); }
    }
    // Add padding for nice spacing
    const pad = Math.max((max - min) * 0.05, 5);
    return [Math.floor(min - pad), Math.ceil(max + pad)];
  }, [displayChartData]);

  // Run backtest — delegates to the module-scoped momentumStore, which owns
  // the fetch/reader loop so it survives navigation away from /momentum.
  const runBacktest = () => {
    setExpandedMonth(null);
    return startBacktest({
      start_date: `${startDate}-01`,
      end_date: `${endDate}-01`,
      signal_weights: weights,
      category_weights: categoryWeights,
      top_n_sectors: topSectors,
      top_n_per_sector: topPerSector,
      skip_price_fetch: skipPriceFetch,
      max_companies: maxCompanies,
      universe_label: null,
      index_universe: selectedIndexUniverse || null,
    });
  };

  const saveBacktest = async () => {
    if (!result || !saveName.trim()) return;
    setSaving(true);
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: saveName.trim(),
          config: {
            start_date: `${startDate}-01`,
            end_date: `${endDate}-01`,
            signal_weights: weights,
            category_weights: categoryWeights,
            top_n_sectors: topSectors,
            top_n_per_sector: topPerSector,
            universe_label: null,
            index_universe: selectedIndexUniverse || null,
          },
          summary: result.summary,
          monthly_records: result.monthly_records,
          universe,
        }),
      });
      if (resp.ok) {
        const saved = await resp.json();
        setSaveName('');
        momentumStore.set({ loadedRunId: saved.run_id });
        loadSavedRuns();
      }
    } catch {}
    setSaving(false);
  };

  const loadBacktest = async (runId: number) => {
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`);
      if (!resp.ok) return;
      const data = await resp.json();

      // Restore config
      const cfg = data.config ?? {};
      if (cfg.start_date) setStartDate(cfg.start_date.slice(0, 7));
      if (cfg.end_date) setEndDate(cfg.end_date.slice(0, 7));
      if (cfg.signal_weights) setWeights(cfg.signal_weights);
      if (cfg.category_weights) setCategoryWeights(cfg.category_weights);
      if (cfg.top_n_sectors) setTopSectors(cfg.top_n_sectors);
      if (cfg.top_n_per_sector) setTopPerSector(cfg.top_n_per_sector);
      // Legacy saved runs may have used universe_label; both hit the same table now.
      setSelectedIndexUniverse(cfg.index_universe ?? cfg.universe_label ?? '');

      // Restore result — saved runs store the payload under `result`.
      const saved = data.result ?? data;
      momentumStore.set({
        result: {
          monthly_records: saved.monthly_records ?? [],
          summary: saved.summary ?? {
            total_return_pct: 0,
            annualized_return_pct: 0,
            max_drawdown_pct: 0,
            sharpe_ratio: null,
            avg_monthly_turnover_pct: 0,
            total_months: 0,
            avg_holdings: 0,
            top_drawdowns: [],
          },
        },
        universe: saved.universe ?? [],
        loadedRunId: runId,
        error: null,
        warnings: [],
        infos: [],
        progress: [],
      });
      setExpandedMonth(null);
    } catch {
      momentumStore.set({ error: 'Failed to load backtest' });
    }
  };

  const deleteBacktest = async (runId: number) => {
    setSavedRuns(prev => prev.filter(r => r.run_id !== runId));
    if (loadedRunId === runId) momentumStore.set({ loadedRunId: null });
    try {
      await fetch(`${API_URL}/api/momentum/backtests/${runId}`, { method: 'DELETE' });
    } catch {
      loadSavedRuns();
    }
  };

  const renameBacktest = async (runId: number, currentName: string) => {
    const next = await dialog.prompt('New name for this backtest:', {
      title: 'Rename backtest',
      defaultValue: currentName,
    });
    if (!next || next.trim() === '' || next === currentName) return;
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: next.trim() }),
      });
      if (!resp.ok) throw new Error(String(resp.status));
      loadSavedRuns();
    } catch (e) {
      dialog.alert(`Rename failed: ${e instanceof Error ? e.message : e}`, { title: 'Rename failed' });
    }
  };

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="px-8 py-5 border-b border-gray-800/60 flex items-center justify-between">
        <div>
          <h1 className="text-lg font-semibold text-white">Momentum Backtester</h1>
          <p className="text-xs text-gray-500 mt-0.5">
            Price momentum portfolio — equal-weight, monthly rebalancing, sector-filtered
          </p>
        </div>
        <div className="flex items-center gap-3">
          <ApiUsageBadge />
        {savedRuns.length > 0 && (
          <div className="relative" ref={savedDropdownRef}>
            <button
              type="button"
              onClick={() => setSavedDropdownOpen((o) => !o)}
              className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex items-center gap-2 hover:border-indigo-500 focus:outline-none focus:border-indigo-500 transition-colors min-w-[220px]"
            >
              <span className="truncate">
                {loadedRunId
                  ? savedRuns.find((r) => r.run_id === loadedRunId)?.name ?? 'Load saved backtest...'
                  : 'Load saved backtest...'}
              </span>
              <svg className={`w-3.5 h-3.5 text-gray-500 ml-auto transition-transform ${savedDropdownOpen ? 'rotate-180' : ''}`} viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M5.23 7.21a.75.75 0 011.06.02L10 11.06l3.71-3.83a.75.75 0 111.08 1.04l-4.25 4.39a.75.75 0 01-1.08 0L5.21 8.27a.75.75 0 01.02-1.06z" clipRule="evenodd" />
              </svg>
            </button>
            {savedDropdownOpen && (
              <div className="absolute right-0 mt-1 w-80 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-96 overflow-auto">
                {savedRuns.map((r) => {
                  const isActive = r.run_id === loadedRunId;
                  return (
                    <div
                      key={r.run_id}
                      className={`group flex items-center gap-2 px-3 py-2 border-b border-gray-800/40 last:border-b-0 hover:bg-white/[0.03] transition-colors ${isActive ? 'bg-indigo-500/10' : ''}`}
                    >
                      <button
                        type="button"
                        onClick={() => { loadBacktest(r.run_id); setSavedDropdownOpen(false); }}
                        className="flex-1 text-left min-w-0"
                      >
                        <div className={`text-sm truncate ${isActive ? 'text-indigo-300' : 'text-gray-200'}`}>{r.name}</div>
                        <div className="text-[10px] text-gray-500 font-mono">{new Date(r.created_at).toLocaleDateString()}</div>
                      </button>
                      <button
                        type="button"
                        onClick={(e) => { e.stopPropagation(); renameBacktest(r.run_id, r.name); }}
                        className="p-1.5 rounded text-gray-500 hover:text-indigo-400 hover:bg-white/5 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="Rename"
                      >
                        <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                          <path d="M13.586 3.586a2 2 0 112.828 2.828l-.793.793-2.828-2.828.793-.793zM11.379 5.793L3 14.172V17h2.828l8.38-8.379-2.83-2.828z" />
                        </svg>
                      </button>
                      <button
                        type="button"
                        onClick={async (e) => {
                          e.stopPropagation();
                          if (await dialog.confirm(`Delete "${r.name}"?`, { destructive: true, confirmLabel: 'Delete' })) {
                            deleteBacktest(r.run_id);
                          }
                        }}
                        className="p-1.5 rounded text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 opacity-0 group-hover:opacity-100 transition-opacity"
                        title="Delete"
                      >
                        <svg className="w-3.5 h-3.5" viewBox="0 0 20 20" fill="currentColor">
                          <path fillRule="evenodd" d="M9 2a1 1 0 00-.894.553L7.382 4H4a1 1 0 000 2v10a2 2 0 002 2h8a2 2 0 002-2V6a1 1 0 100-2h-3.382l-.724-1.447A1 1 0 0011 2H9zM7 8a1 1 0 012 0v6a1 1 0 11-2 0V8zm5-1a1 1 0 00-1 1v6a1 1 0 102 0V8a1 1 0 00-1-1z" clipRule="evenodd" />
                        </svg>
                      </button>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        )}
        </div>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {/* Config Panel */}
        <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
          <div className="flex flex-wrap items-end gap-5 mb-5">
            {/* Universe Label */}
            <div>
              <label className="text-gray-500 text-xs mb-1 flex items-center gap-2">
                <span>Universe</span>
                {universesLoading && (
                  <span className="flex items-center gap-1.5 text-indigo-400">
                    <span className="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse" />
                    <span className="text-[10px]">loading stats from DB… {universesElapsed}s</span>
                  </span>
                )}
                {!universesLoading && !universesError && indexUniverses.length > 0 && (
                  <span className="text-[10px] text-gray-600">{indexUniverses.length} loaded</span>
                )}
                {universesError && (
                  <span className="text-[10px] text-rose-400">failed: {universesError}</span>
                )}
              </label>
              <select
                value={universeDropdownValue}
                onChange={(e) => handleUniverseChange(e.target.value)}
                disabled={universesLoading}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none disabled:opacity-60 disabled:cursor-wait"
              >
                {universesLoading ? (
                  <option value="">Loading universes… ({universesElapsed}s)</option>
                ) : (
                  <>
                    <option value="">All companies</option>
                    {indexUniverses.map(i => (
                      <option key={i.index_name} value={i.index_name}>
                        {i.index_name} ({i.start_month} – {i.end_month}, {i.total_unique_tickers} tickers)
                      </option>
                    ))}
                  </>
                )}
              </select>
            </div>
            {/* Date Range */}
            <div>
              <label className="text-gray-500 text-xs block mb-1">Start</label>
              <input
                type="month"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">End</label>
              <input
                type="month"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Top Sectors</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topSectors}
                onChange={(e) => setTopSectors(Number(e.target.value))}
                className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Per Sector</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topPerSector}
                onChange={(e) => setTopPerSector(Number(e.target.value))}
                className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-gray-500 text-xs block mb-1">Max Companies</label>
              <input
                type="number"
                min={0}
                max={500}
                value={maxCompanies}
                onChange={(e) => setMaxCompanies(Number(e.target.value))}
                className="w-20 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                title="0 = all companies, otherwise limit alphabetically"
              />
              <span className="text-gray-600 text-xs ml-1">0 = all</span>
            </div>
            <label className="flex items-center gap-2 cursor-pointer self-center pt-4">
              <input
                type="checkbox"
                checked={skipPriceFetch}
                onChange={(e) => setSkipPriceFetch(e.target.checked)}
                className="accent-indigo-500 w-4 h-4 cursor-pointer"
              />
              <span className="text-gray-400 text-xs">Skip data fetch</span>
            </label>
            <button
              onClick={runBacktest}
              disabled={running}
              className="px-5 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {running ? 'Running...' : 'Run Backtest'}
            </button>
            {running && (
              <button
                onClick={cancelBacktest}
                className="px-4 py-2 rounded-lg text-sm font-medium text-gray-400 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Cancel
              </button>
            )}
          </div>

          {/* Signal Weights */}
          <div className="space-y-4">
            {['price', 'volume'].map((group) => {
              const groupSignals = signalDefs.filter((s) => (s.group ?? 'price') === group);
              if (groupSignals.length === 0) return null;
              return (
                <div key={group}>
                  <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">
                    {group === 'price' ? 'Price Momentum' : 'Volume Confirmation'}
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2.5">
                    {groupSignals.map((s) => (
                      <div key={s.key} className="flex items-center gap-3">
                        <div className="w-36 shrink-0 flex items-center gap-1.5">
                          <span className="text-gray-300 text-xs font-medium">{s.label}</span>
                          <span className="relative group/tip">
                            <span className="text-gray-600 hover:text-gray-400 cursor-help text-xs">&#9432;</span>
                            <span className="absolute bottom-full left-1/2 -translate-x-1/2 mb-1.5 hidden group-hover/tip:block w-64 px-3 py-2 rounded-lg bg-gray-800 border border-gray-700 text-gray-300 text-xs leading-relaxed shadow-xl z-50 pointer-events-none">
                              {s.description}
                            </span>
                          </span>
                        </div>
                        <input
                          type="range"
                          min={0}
                          max={10}
                          step={1}
                          value={weights[s.key] ?? 0}
                          onChange={(e) => setWeights((prev) => ({ ...prev, [s.key]: Number(e.target.value) }))}
                          className="flex-1 h-1 accent-indigo-500 cursor-pointer"
                        />
                        <span className="text-gray-500 text-xs w-5 text-right font-mono shrink-0">{weights[s.key] ?? 0}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
          {/* Category Weights */}
          {categories.length > 1 && (
            <div>
              <h3 className="text-gray-400 text-xs font-medium mb-2.5 uppercase tracking-wider">Category Weights</h3>
              <div className="flex items-center gap-6">
                {categories.map((cat) => (
                  <div key={cat} className="flex items-center gap-2">
                    <span className="text-gray-300 text-xs font-medium w-28">
                      {cat === 'price' ? 'Price Momentum' : cat === 'volume' ? 'Volume Confirmation' : cat}
                    </span>
                    <input
                      type="range"
                      min={0}
                      max={100}
                      step={5}
                      value={categoryWeights[cat] ?? 50}
                      onChange={(e) => setCategoryWeights((prev) => ({ ...prev, [cat]: Number(e.target.value) }))}
                      className="w-32 h-1 accent-indigo-500 cursor-pointer"
                    />
                    <span className="text-gray-500 text-xs w-8 text-right font-mono">{categoryWeights[cat] ?? 50}%</span>
                  </div>
                ))}
              </div>
            </div>
          )}
          </div>
        </div>

        {/* Progress */}
        {running && progress.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-4">
            <div className="flex items-center gap-3 mb-3">
              <div className="h-1.5 flex-1 bg-gray-800 rounded-full overflow-hidden">
                <div
                  className="h-full bg-indigo-500 rounded-full transition-all duration-300"
                  style={{ width: `${progress[progress.length - 1]?.pct ?? 0}%` }}
                />
              </div>
              <span className="text-gray-400 text-xs font-mono">{progress[progress.length - 1]?.pct ?? 0}%</span>
            </div>
            <div ref={progressLogRef} className="max-h-32 overflow-auto space-y-0.5">
              {progress.map((p, i) => (
                <div key={i} className="text-gray-500 text-xs">{p.message}</div>
              ))}
            </div>
          </div>
        )}

        {/* Error */}
        {error && (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg p-4 text-rose-400 text-sm">
            {error}
          </div>
        )}

        {/* Notifications — warnings on top (critical), info below (expected) */}
        {(warnings.length > 0 || infos.length > 0) && (
          <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden divide-y divide-gray-800/40">
            {warnings.length > 0 && (
              <div className="bg-amber-500/10">
                <button
                  type="button"
                  onClick={() => setShowWarnings((v) => !v)}
                  className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-amber-500/5 transition-colors"
                >
                  <span className="text-amber-300 text-sm font-medium">
                    {warnings.length} warning{warnings.length === 1 ? '' : 's'}
                  </span>
                  <span className="text-amber-400/70 text-xs font-mono">{showWarnings ? '▾' : '▸'}</span>
                </button>
                {showWarnings && (
                  <ul className="max-h-64 overflow-auto border-t border-amber-500/20 divide-y divide-amber-500/10">
                    {warnings.map((w, i) => (
                      <li key={i} className="px-4 py-2 text-xs text-amber-200 flex gap-2">
                        <span className="uppercase text-[10px] tracking-wider font-mono text-amber-400/70 shrink-0 w-16">
                          {w.scope}
                        </span>
                        <span className="break-words">{w.message}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
            {infos.length > 0 && (
              <div className="bg-sky-500/10">
                <button
                  type="button"
                  onClick={() => setShowInfos((v) => !v)}
                  className="w-full flex items-center justify-between px-4 py-2.5 text-left hover:bg-sky-500/5 transition-colors"
                >
                  <span className="text-sky-300 text-sm font-medium">
                    {infos.length} note{infos.length === 1 ? '' : 's'}
                  </span>
                  <span className="text-sky-400/70 text-xs font-mono">{showInfos ? '▾' : '▸'}</span>
                </button>
                {showInfos && (
                  <ul className="max-h-64 overflow-auto border-t border-sky-500/20 divide-y divide-sky-500/10">
                    {infos.map((n, i) => (
                      <li key={i} className="px-4 py-2 text-xs text-sky-200 flex gap-2">
                        <span className="uppercase text-[10px] tracking-wider font-mono text-sky-400/70 shrink-0 w-16">
                          {n.scope}
                        </span>
                        <span className="break-words">{n.message}</span>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
          </div>
        )}

        {/* Results */}
        {result && (
          <>
            {/* Benchmark selector */}
            <div className="flex items-center gap-3">
              <label className="text-gray-400 text-sm">Compare against</label>
              <select
                value={selectedBenchmarkId ?? ''}
                onChange={(e) => setSelectedBenchmarkId(e.target.value ? Number(e.target.value) : null)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              >
                <option value="">No benchmark</option>
                {benchmarkOptions.map((b) => (
                  <option key={b.benchmark_id} value={b.benchmark_id}>{b.ticker} — {b.name}</option>
                ))}
              </select>
              {selectedBenchmarkId && (
                <span className="text-gray-500 text-xs">
                  {benchmarkPrices.length > 0
                    ? `${benchmarkPrices.length} daily prices loaded (${benchmarkPrices[0]?.target_date} → ${benchmarkPrices[benchmarkPrices.length - 1]?.target_date})`
                    : 'Loading prices…'}
                </span>
              )}
            </div>

            {/* Summary Stats */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
                    <th className="px-4 py-2.5 text-left font-medium"></th>
                    <th className="px-3 py-2.5 text-right font-medium">Total Return</th>
                    <th className="px-3 py-2.5 text-right font-medium">Annualized</th>
                    <th className="px-3 py-2.5 text-right font-medium">Max Drawdown</th>
                    <th className="px-3 py-2.5 text-right font-medium">Sharpe</th>
                    <th className="px-3 py-2.5 text-right font-medium">Turnover</th>
                    <th className="px-3 py-2.5 text-right font-medium">Months</th>
                    <th className="px-4 py-2.5 text-right font-medium">Avg Holdings</th>
                  </tr>
                </thead>
                <tbody>
                  <tr className="border-b border-gray-800/30">
                    <td className="px-4 py-2.5 text-gray-200 font-medium">Strategy</td>
                    <td className={`px-3 py-2.5 text-right font-mono ${result.summary.total_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(result.summary.total_return_pct)}</td>
                    <td className={`px-3 py-2.5 text-right font-mono ${result.summary.annualized_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(result.summary.annualized_return_pct)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(result.summary.max_drawdown_pct)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-white">{result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—'}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-gray-300">{fmtPct(result.summary.avg_monthly_turnover_pct)}</td>
                    <td className="px-3 py-2.5 text-right font-mono text-gray-300">{result.summary.total_months}</td>
                    <td className="px-4 py-2.5 text-right font-mono text-gray-300">{result.summary.avg_holdings.toFixed(1)}</td>
                  </tr>
                  {benchmarkReturns && (
                    <tr className="border-b border-gray-800/30 bg-white/[0.01]">
                      <td className="px-4 py-2.5 text-amber-400 font-medium">
                        {benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'}
                      </td>
                      <td className={`px-3 py-2.5 text-right font-mono ${benchmarkReturns.totalReturn >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(benchmarkReturns.totalReturn)}</td>
                      <td className={`px-3 py-2.5 text-right font-mono ${benchmarkReturns.annualized >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(benchmarkReturns.annualized)}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(benchmarkReturns.maxDd)}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-white">{benchmarkReturns.sharpe != null ? benchmarkReturns.sharpe.toFixed(2) : '—'}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-600">—</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-300">{result.summary.total_months}</td>
                      <td className="px-4 py-2.5 text-right font-mono text-gray-600">—</td>
                    </tr>
                  )}
                </tbody>
              </table>
              {(topDrawdowns.length > 0 || benchmarkDrawdowns.length > 0) && (
                <div className="px-4 py-3 border-t border-gray-800/40 space-y-3">
                  {topDrawdowns.length > 0 && (
                    <div>
                      <div className="text-xs text-gray-500 font-medium mb-2">Strategy — Top Drawdowns</div>
                      <div className="grid grid-cols-3 gap-3">
                        {topDrawdowns.map((dd, i) => (
                          <div key={i} className="bg-[#0f1117] rounded-lg px-3 py-2">
                            <div className="flex items-center gap-2 mb-1">
                              <div className={`w-2 h-2 rounded-full ${i === 0 ? 'bg-rose-400' : i === 1 ? 'bg-rose-400/60' : 'bg-rose-400/30'}`} />
                              <span className="text-rose-400 font-mono text-sm font-medium">{dd.drawdown_pct.toFixed(1)}%</span>
                            </div>
                            <div className="text-[10px] text-gray-500 font-mono">
                              {dd.peak_date} to {dd.trough_date}
                              {dd.recovery_date ? ` (recovered ${dd.recovery_date})` : ' (ongoing)'}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                  {benchmarkDrawdowns.length > 0 && (
                    <div>
                      <div className="text-xs text-gray-500 font-medium mb-2">
                        {benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'} — Top Drawdowns
                      </div>
                      <div className="grid grid-cols-3 gap-3">
                        {benchmarkDrawdowns.map((dd, i) => (
                          <div key={i} className="bg-[#0f1117] rounded-lg px-3 py-2">
                            <div className="flex items-center gap-2 mb-1">
                              <div className={`w-2 h-2 rounded-full ${i === 0 ? 'bg-amber-400' : i === 1 ? 'bg-amber-400/60' : 'bg-amber-400/30'}`} />
                              <span className="text-amber-400 font-mono text-sm font-medium">{dd.drawdown_pct.toFixed(1)}%</span>
                            </div>
                            <div className="text-[10px] text-gray-500 font-mono">
                              {dd.peak_date} to {dd.trough_date}
                              {dd.recovery_date ? ` (recovered ${dd.recovery_date})` : ' (ongoing)'}
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              )}
            </div>

            {/* Yearly Performance + Custom Range */}
            {yearlyBreakdown.length > 0 && (
              <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
                <div className="px-5 py-3 border-b border-gray-800/40">
                  <h3 className="text-white text-sm font-medium">Yearly Performance</h3>
                </div>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
                      <th className="px-5 py-2.5 text-left font-medium">Year</th>
                      <th className="px-3 py-2.5 text-right font-medium">Strategy</th>
                      {benchmarkReturns && (
                        <>
                          <th className="px-3 py-2.5 text-right font-medium">
                            {benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'}
                          </th>
                          <th className="px-5 py-2.5 text-right font-medium">Diff</th>
                        </>
                      )}
                    </tr>
                  </thead>
                  <tbody>
                    {yearlyBreakdown.map((row) => {
                      const diff = row.benchmark != null ? row.strategy - row.benchmark : null;
                      return (
                        <tr key={row.year} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                          <td className="px-5 py-2 text-gray-200 font-mono">{row.year}</td>
                          <td className={`px-3 py-2 text-right font-mono ${row.strategy >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {fmtPct(row.strategy)}
                          </td>
                          {benchmarkReturns && (
                            <>
                              <td className={`px-3 py-2 text-right font-mono ${row.benchmark != null ? (row.benchmark >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                {row.benchmark != null ? fmtPct(row.benchmark) : '—'}
                              </td>
                              <td className={`px-5 py-2 text-right font-mono ${diff != null ? (diff >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                {diff != null ? fmtPct(diff) : '—'}
                              </td>
                            </>
                          )}
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                <div className="px-5 py-3 border-t border-gray-800/40 flex items-center gap-4 flex-wrap">
                  <label className="text-xs text-gray-400 font-medium">From month:</label>
                  <input
                    type="month"
                    value={customFromMonth}
                    min={result.monthly_records[0]?.date}
                    max={result.monthly_records[result.monthly_records.length - 1]?.date}
                    onChange={(e) => setCustomFromMonth(e.target.value)}
                    className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-xs text-gray-200 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                  />
                  {customFromMonth && (
                    <button
                      onClick={() => setCustomFromMonth('')}
                      className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
                    >
                      clear
                    </button>
                  )}
                  {customRangeReturn ? (
                    <div className="flex items-center gap-4 text-xs ml-auto">
                      <span className="text-gray-500 font-mono">{customRangeReturn.fromDate} → {customRangeReturn.toDate}</span>
                      <span className="text-gray-400">
                        Strategy:{' '}
                        <span className={`font-mono ${customRangeReturn.strategy >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                          {fmtPct(customRangeReturn.strategy)}
                        </span>
                      </span>
                      {customRangeReturn.benchmark != null && (
                        <span className="text-gray-400">
                          {benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'}:{' '}
                          <span className={`font-mono ${customRangeReturn.benchmark >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {fmtPct(customRangeReturn.benchmark)}
                          </span>
                        </span>
                      )}
                    </div>
                  ) : (
                    <span className="text-xs text-gray-500">Cumulative return from picked month through end of backtest.</span>
                  )}
                </div>
              </div>
            )}

            {/* Equity Curve */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-5">
              <div className="flex items-center justify-between mb-4">
                <h3 className="text-white text-sm font-medium">Equity Curve ({logScale ? 'Log' : 'Cumulative'} Return %)</h3>
                <label className="flex items-center gap-2 text-xs text-gray-400 cursor-pointer select-none">
                  <input
                    type="checkbox"
                    checked={logScale}
                    onChange={(e) => setLogScale(e.target.checked)}
                    className="accent-indigo-500 w-3.5 h-3.5"
                  />
                  Log scale
                </label>
              </div>
              <ResponsiveContainer width="100%" height={350}>
                <LineChart data={displayChartData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
                  <XAxis
                    dataKey="date"
                    tick={{ fill: '#6b7280', fontSize: 11 }}
                    tickLine={false}
                    interval={Math.max(0, Math.floor(displayChartData.length / 12) - 1)}
                  />
                  <YAxis
                    tick={{ fill: '#6b7280', fontSize: 11 }}
                    tickLine={false}
                    tickFormatter={(v: number) => `${v}%`}
                    domain={chartYDomain}
                  />
                  <Tooltip
                    {...tooltipStyle}
                    formatter={(value, name) => {
                      const v = Number(value);
                      const label = name === 'cumReturn' ? 'Strategy' : name === 'benchmark' ? (benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark') : String(name);
                      return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, label];
                    }}
                  />
                  {benchmarkReturns && (
                    <Legend
                      wrapperStyle={{ fontSize: 12, color: '#9ca3af' }}
                    />
                  )}
                  {(topDrawdowns).map((dd, i) => {
                    const base = [0.25, 0.15, 0.10];
                    const hovered = hoveredDrawdown === i;
                    const opacity = hovered ? (base[i] ?? 0.10) + 0.15 : (base[i] ?? 0.10);
                    return (
                      <ReferenceArea
                        key={`dd-${i}`}
                        x1={dd.peak_date}
                        x2={dd.recovery_date ?? displayChartData[displayChartData.length - 1]?.date}
                        y1={chartYDomain[0]}
                        y2={chartYDomain[1]}
                        fill={`rgba(244,63,94,${opacity})`}
                        strokeOpacity={0}
                        style={{ cursor: 'pointer' }}
                        onMouseEnter={() => setHoveredDrawdown(i)}
                        onMouseLeave={() => setHoveredDrawdown(null)}
                      />
                    );
                  })}
                  {benchmarkDrawdowns.map((dd, i) => {
                    const base = [0.12, 0.08, 0.05];
                    return (
                      <ReferenceArea
                        key={`bdd-${i}`}
                        x1={dd.peak_date}
                        x2={dd.recovery_date ?? displayChartData[displayChartData.length - 1]?.date}
                        y1={chartYDomain[0]}
                        y2={chartYDomain[1]}
                        fill={`rgba(245,158,11,${base[i] ?? 0.05})`}
                        strokeOpacity={0}
                      />
                    );
                  })}
                  <Line
                    type="monotone"
                    dataKey="cumReturn"
                    stroke="#818cf8"
                    strokeWidth={2}
                    dot={false}
                    name="Strategy"
                  />
                  {benchmarkReturns && (
                    <Line
                      type="monotone"
                      dataKey="benchmark"
                      stroke="#f59e0b"
                      strokeWidth={1.5}
                      strokeDasharray="4 3"
                      dot={false}
                      name={benchmarkOptions.find((b) => b.benchmark_id === selectedBenchmarkId)?.ticker ?? 'Benchmark'}
                      connectNulls
                    />
                  )}
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Monthly Portfolio Table */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40">
              <div className="px-5 py-4 border-b border-gray-800/40">
                <h3 className="text-white text-sm font-medium">Monthly Portfolios</h3>
              </div>
              <div className="max-h-[500px] overflow-auto">
                <table className="w-full text-sm">
                  <thead className="sticky top-0 bg-[#151821] z-20">
                    <tr className="text-gray-500 text-xs border-b border-gray-800/40">
                      <th className="text-left px-5 py-2.5 font-medium">Month</th>
                      <th className="text-right px-3 py-2.5 font-medium">Holdings</th>
                      <th className="text-right px-3 py-2.5 font-medium">Return</th>
                      <th className="text-right px-5 py-2.5 font-medium">Cumulative</th>
                    </tr>
                  </thead>
                  <tbody>
                    {result.monthly_records.map((r) => (
                      <Fragment key={r.date}>
                        <tr
                          className="border-b border-gray-800/20 hover:bg-white/[0.02] cursor-pointer transition-colors"
                          onClick={() => setExpandedMonth(expandedMonth === r.date ? null : r.date)}
                        >
                          <td className="px-5 py-2.5 text-gray-300 font-mono">
                            <span className="text-gray-600 mr-2">{expandedMonth === r.date ? '▾' : '▸'}</span>
                            {r.date}
                          </td>
                          <td className="text-right px-3 py-2.5 text-gray-400 font-mono">{r.holdings.length}</td>
                          <td className={`text-right px-3 py-2.5 font-mono ${r.portfolio_return_pct != null ? (r.portfolio_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                            {fmtPct(r.portfolio_return_pct)}
                          </td>
                          <td className={`text-right px-5 py-2.5 font-mono ${r.cumulative_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {fmtPct(r.cumulative_return_pct)}
                          </td>
                        </tr>
                        {expandedMonth === r.date && r.holdings.length > 0 && (
                          <tr key={`${r.date}-detail`}>
                            <td colSpan={4} className="bg-[#0f1117] px-5 py-3">
                              <table className="w-full text-xs">
                                <thead>
                                  <tr className="text-gray-600">
                                    <th className="text-left py-1 font-medium">Ticker</th>
                                    <th className="text-left py-1 font-medium">Company</th>
                                    <th className="text-left py-1 font-medium">Sector</th>
                                    {categories.map((cat) => (
                                      <th key={cat} className="text-right py-1 font-medium">
                                        {cat === 'price' ? 'Price' : cat === 'volume' ? 'Vol' : cat}
                                      </th>
                                    ))}
                                    <th className="text-right py-1 font-medium">Total</th>
                                    <th className="text-right py-1 font-medium pl-4">Start (local)</th>
                                    <th className="text-right py-1 font-medium">End (local)</th>
                                    <th className="text-right py-1 font-medium pl-4">Start (€)</th>
                                    <th className="text-right py-1 font-medium">End (€)</th>
                                    <th className="text-right py-1 font-medium pl-4">Return</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {[...r.holdings]
                                    .sort((a, b) => {
                                      const sec = a.sector.localeCompare(b.sector);
                                      return sec !== 0 ? sec : b.score - a.score;
                                    })
                                    .map((h) => {
                                      const exch = exchangeByCompany.get(h.company_id) ?? '';
                                      const href = guruFocusUrl(h.ticker, exch);
                                      return (
                                        <tr key={h.company_id} className="border-t border-gray-800/20">
                                          <td className="py-1.5 font-mono whitespace-nowrap">
                                            <a
                                              href={href}
                                              target="_blank"
                                              rel="noopener noreferrer"
                                              className="text-indigo-400 hover:text-indigo-300 hover:underline"
                                            >
                                              {h.ticker}
                                            </a>
                                            {exch && (
                                              <span
                                                className="ml-1 text-[10px] text-gray-500"
                                                title={EXCHANGE_NAMES[exch.toUpperCase()] ?? exch}
                                              >
                                                ({exch})
                                              </span>
                                            )}
                                          </td>
                                          <td className="py-1.5 truncate max-w-[200px]">
                                            <a
                                              href={href}
                                              target="_blank"
                                              rel="noopener noreferrer"
                                              className="text-gray-300 hover:text-indigo-300 hover:underline"
                                            >
                                              {h.company_name}
                                            </a>
                                          </td>
                                          <td className="py-1.5 text-gray-500">{h.sector}</td>
                                          {categories.map((cat) => (
                                            <td key={cat} className="text-right py-1.5 text-gray-400 font-mono">
                                              {h.category_scores?.[cat] != null ? h.category_scores[cat]!.toFixed(0) : '—'}
                                            </td>
                                          ))}
                                          <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                            {fmtPrice(h.entry_price_local)}
                                            {h.currency && <span className="text-gray-600 text-[10px] ml-1">{h.currency}</span>}
                                            {h.entry_date && (
                                              <CellInfoTip>
                                                <div className="text-gray-400">Trading date</div>
                                                <div className="font-mono text-gray-200">{h.entry_date}</div>
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono">
                                            {fmtPrice(h.exit_price_local)}
                                            {h.exit_date && (
                                              <CellInfoTip>
                                                <div className="text-gray-400">Trading date</div>
                                                <div className="font-mono text-gray-200">{h.exit_date}</div>
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono pl-4">
                                            {fmtPrice(h.entry_price_eur)}
                                            {(h.entry_date || (h.entry_price_eur != null && h.entry_price_local)) && (
                                              <CellInfoTip>
                                                {h.entry_date && (
                                                  <>
                                                    <div className="text-gray-400">Trading date</div>
                                                    <div className="font-mono text-gray-200 mb-1">{h.entry_date}</div>
                                                  </>
                                                )}
                                                {h.entry_price_eur != null && h.entry_price_local && h.entry_price_local > 0 && (
                                                  <>
                                                    <div className="text-gray-400">FX rate</div>
                                                    <div className="font-mono text-gray-200">
                                                      1 {h.currency ?? 'LCL'} = {(h.entry_price_eur / h.entry_price_local).toFixed(4)} EUR
                                                    </div>
                                                  </>
                                                )}
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className="text-right py-1.5 text-gray-400 font-mono">
                                            {fmtPrice(h.exit_price_eur)}
                                            {(h.exit_date || (h.exit_price_eur != null && h.exit_price_local)) && (
                                              <CellInfoTip>
                                                {h.exit_date && (
                                                  <>
                                                    <div className="text-gray-400">Trading date</div>
                                                    <div className="font-mono text-gray-200 mb-1">{h.exit_date}</div>
                                                  </>
                                                )}
                                                {h.exit_price_eur != null && h.exit_price_local && h.exit_price_local > 0 && (
                                                  <>
                                                    <div className="text-gray-400">FX rate</div>
                                                    <div className="font-mono text-gray-200">
                                                      1 {h.currency ?? 'LCL'} = {(h.exit_price_eur / h.exit_price_local).toFixed(4)} EUR
                                                    </div>
                                                  </>
                                                )}
                                              </CellInfoTip>
                                            )}
                                          </td>
                                          <td className={`text-right py-1.5 font-mono pl-4 ${h.forward_return_pct != null ? (h.forward_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}>
                                            {fmtPct(h.forward_return_pct)}
                                          </td>
                                        </tr>
                                      );
                                    })}
                                </tbody>
                              </table>
                            </td>
                          </tr>
                        )}
                        {expandedMonth === r.date && r.holdings.length === 0 && (
                          <tr key={`${r.date}-empty`}>
                            <td colSpan={4} className="bg-[#0f1117] px-5 py-4">
                              <div className="text-xs text-gray-500">
                                {r.empty_reason || 'No holdings for this month (unknown reason)'}
                              </div>
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* Save */}
            {!loadedRunId && (
              <div className="bg-[#151821] rounded-xl border border-gray-800/40 p-4 flex items-center gap-3">
                <input
                  value={saveName}
                  onChange={(e) => setSaveName(e.target.value)}
                  placeholder="Backtest name..."
                  className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-sm text-white flex-1 max-w-xs focus:outline-none focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 placeholder-gray-600 transition-colors"
                  onKeyDown={(e) => { if (e.key === 'Enter') saveBacktest(); }}
                />
                <button
                  onClick={saveBacktest}
                  disabled={saving || !saveName.trim()}
                  className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {saving ? 'Saving...' : 'Save Backtest'}
                </button>
              </div>
            )}
            {loadedRunId && (
              <div className="bg-indigo-500/10 border border-indigo-500/20 rounded-lg px-4 py-3 text-indigo-400 text-sm flex items-center gap-2">
                <span>Loaded from saved run</span>
                <span className="text-indigo-300 font-medium">
                  {savedRuns.find((r) => r.run_id === loadedRunId)?.name}
                </span>
              </div>
            )}

            {/* Disclaimer */}
            <p className="text-gray-600 text-xs">
              Note: Uses current company universe applied retroactively (survivorship bias). Returns are hypothetical and do not account for transaction costs.
            </p>
          </>
        )}
      </div>
    </div>
  );
}
