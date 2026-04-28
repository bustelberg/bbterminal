'use client';

import { Fragment, useState, useEffect, useMemo, useRef } from 'react';
import {
  LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid, Legend, ReferenceArea,
} from 'recharts';

import ApiUsageBadge from './ApiUsageBadge';
import { dialog } from '../../lib/dialog';
import ProgressTimeline from './ProgressTimeline';
import {
  momentumStore,
  startBacktest,
  cancelBacktest,
  loadCurrentPicksSnapshots,
  loadCurrentPicksSnapshot,
  refreshCurrentPicksMTD,
  type DrawdownPeriod,
  type MonthlyRecord,
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

// Comparison series — strategies and benchmarks shown side by side on the chart.
// `active` is implicit (derived from the live `result`) and is NOT stored here.
type ComparisonItem =
  | { id: string; kind: 'saved'; runId: number; label: string; monthly: MonthlyRecord[] }
  | { id: string; kind: 'benchmark'; benchmarkId: number; label: string; prices: BenchmarkPrice[] };

// Palette for series lines (index 0 = active strategy).
const SERIES_COLORS = [
  '#818cf8', // indigo
  '#f59e0b', // amber
  '#34d399', // emerald
  '#f472b6', // pink
  '#60a5fa', // sky
  '#a78bfa', // violet
  '#fb7185', // rose
  '#22d3ee', // cyan
];

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
  const [selectionMode, setSelectionMode] = useState<'momentum' | 'random'>('momentum');
  const [randomSeed, setRandomSeed] = useState<number>(42);
  const [nTrials, setNTrials] = useState<number>(1);

  // Backtest run state lives in a module-scoped store so the SSE stream
  // keeps running when the user navigates away from /momentum.
  const running = momentumStore.use((s) => s.running);
  const progress = momentumStore.use((s) => s.progress);
  const result = momentumStore.use((s) => s.result);
  const currentPortfolio = momentumStore.use((s) => s.currentPortfolio);
  const currentPicksSnapshots = momentumStore.use((s) => s.currentPicksSnapshots);
  const refreshingMTD = momentumStore.use((s) => s.refreshingMTD);
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

  // Purely local UI state — safe to reset on navigation
  const [showWarnings, setShowWarnings] = useState(true);
  const [showInfos, setShowInfos] = useState(false);
  const [expandedMonth, setExpandedMonth] = useState<string | null>(null);
  const [expandedDailyDate, setExpandedDailyDate] = useState<string | null>(null);

  // Save/load state
  const [savedRuns, setSavedRuns] = useState<SavedRun[]>([]);
  const [saveName, setSaveName] = useState('');
  const [saving, setSaving] = useState(false);

  // Benchmark options (for "add series" dropdown) + unified comparison list.
  const [benchmarkOptions, setBenchmarkOptions] = useState<BenchmarkOption[]>([]);
  const [comparisons, setComparisons] = useState<ComparisonItem[]>([]);
  const [addSeriesOpen, setAddSeriesOpen] = useState(false);
  const addSeriesRef = useRef<HTMLDivElement>(null);
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
    loadCurrentPicksSnapshots();
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

  // When universe selection changes, auto-set start/end dates from the range.
  // Some universes store start/end as YYYY-MM-DD instead of YYYY-MM — slice so
  // the <input type="month"> can accept the value.
  const handleUniverseChange = (value: string) => {
    setSelectedIndexUniverse(value);
    if (value) {
      const entry = indexUniverses.find(i => i.index_name === value);
      if (entry) {
        setStartDate(entry.start_month.slice(0, 7));
        setEndDate(entry.end_month.slice(0, 7));
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

  // Close "add series" dropdown on outside click
  useEffect(() => {
    if (!addSeriesOpen) return;
    const handleClick = (e: MouseEvent) => {
      if (addSeriesRef.current && !addSeriesRef.current.contains(e.target as Node)) {
        setAddSeriesOpen(false);
      }
    };
    document.addEventListener('mousedown', handleClick);
    return () => document.removeEventListener('mousedown', handleClick);
  }, [addSeriesOpen]);

  // Helpers to add/remove comparison series
  const addSavedSeries = async (runId: number) => {
    if (comparisons.some((c) => c.kind === 'saved' && c.runId === runId)) return;
    try {
      const resp = await fetch(`${API_URL}/api/momentum/backtests/${runId}`);
      if (!resp.ok) return;
      const data = await resp.json();
      const saved = data.result ?? data;
      const monthly: MonthlyRecord[] = saved.monthly_records ?? [];
      const label = data.name ?? `Backtest ${runId}`;
      setComparisons((prev) => [...prev, { id: `saved:${runId}`, kind: 'saved', runId, label, monthly }]);
    } catch {}
  };

  const addBenchmarkSeries = async (benchmarkId: number) => {
    if (comparisons.some((c) => c.kind === 'benchmark' && c.benchmarkId === benchmarkId)) return;
    const opt = benchmarkOptions.find((b) => b.benchmark_id === benchmarkId);
    const label = opt ? opt.ticker : `Benchmark ${benchmarkId}`;
    // Widen fetch window so any later series can still overlap.
    try {
      const resp = await fetch(
        `${API_URL}/api/benchmarks/${benchmarkId}/prices?start_date=1990-01-01&end_date=2099-12-31`,
      );
      if (!resp.ok) return;
      const prices: BenchmarkPrice[] = await resp.json();
      setComparisons((prev) => [
        ...prev,
        { id: `bench:${benchmarkId}`, kind: 'benchmark', benchmarkId, label, prices },
      ]);
    } catch {}
  };

  const removeSeries = (id: string) => {
    setComparisons((prev) => prev.filter((c) => c.id !== id));
  };

  // Resolve every series into a (YYYY-MM → growth factor) map, rebased to 1.0
  // at the series' first observed month. The factor embeds the forward return
  // earned during each month, so factor[month[i]] == cumProduct of returns
  // through month[i] (consistent with strategy `cumulative_return_pct`).
  type ResolvedSeries = {
    id: string;
    label: string;
    color: string;
    kind: 'active' | 'saved' | 'benchmark';
    removable: boolean;
    factorByMonth: Map<string, number>;
    months: string[]; // sorted
  };

  const resolvedSeries = useMemo<ResolvedSeries[]>(() => {
    const out: ResolvedSeries[] = [];
    let colorIdx = 0;
    const nextColor = () => SERIES_COLORS[colorIdx++ % SERIES_COLORS.length];

    const fromMonthly = (monthly: MonthlyRecord[]): { map: Map<string, number>; months: string[] } => {
      const map = new Map<string, number>();
      const months: string[] = [];
      for (const r of monthly) {
        const factor = 1 + r.cumulative_return_pct / 100;
        map.set(r.date, factor);
        months.push(r.date);
      }
      return { map, months };
    };

    const fromPrices = (prices: BenchmarkPrice[]): { map: Map<string, number>; months: string[] } => {
      // Pick first price per month.
      const firstByMonth = new Map<string, number>();
      for (const p of prices) {
        const ym = p.target_date.slice(0, 7);
        if (!firstByMonth.has(ym)) firstByMonth.set(ym, p.price);
      }
      const months = Array.from(firstByMonth.keys()).sort();
      if (months.length === 0) return { map: new Map(), months: [] };
      const map = new Map<string, number>();
      const p0 = firstByMonth.get(months[0])!;
      // Shift index so each month's "factor" reflects return through end of month
      // (same convention as strategy records: cumReturn at month[i] includes
      // the price change month[i] → month[i+1]).
      for (let i = 0; i < months.length - 1; i++) {
        const pn = firstByMonth.get(months[i + 1])!;
        map.set(months[i], pn / p0);
      }
      // Last month: no forward price available — leave out.
      return { map, months: months.slice(0, -1) };
    };

    if (result) {
      const { map, months } = fromMonthly(result.monthly_records);
      const activeName = loadedRunId != null
        ? savedRuns.find((r) => r.run_id === loadedRunId)?.name
        : undefined;
      out.push({
        id: 'active',
        label: activeName || 'Strategy',
        color: nextColor(),
        kind: 'active',
        removable: false,
        factorByMonth: map,
        months,
      });
    }

    for (const c of comparisons) {
      if (c.kind === 'saved') {
        const { map, months } = fromMonthly(c.monthly);
        out.push({
          id: c.id,
          label: c.label,
          color: nextColor(),
          kind: 'saved',
          removable: true,
          factorByMonth: map,
          months,
        });
      } else {
        const { map, months } = fromPrices(c.prices);
        out.push({
          id: c.id,
          label: c.label,
          color: nextColor(),
          kind: 'benchmark',
          removable: true,
          factorByMonth: map,
          months,
        });
      }
    }
    return out;
  }, [result, comparisons, loadedRunId, savedRuns]);

  // Determine the [maxStart, minEnd] alignment window over all series and
  // compute per-series aligned points + summary stats, rebased so each series
  // starts at 0% on windowStart.
  type SeriesPoint = { date: string; cumReturnPct: number | null };
  type SeriesStats = {
    totalReturn: number;
    annualized: number;
    maxDd: number;
    sharpe: number | null;
    months: number;
  };
  type AlignedSeries = ResolvedSeries & {
    points: SeriesPoint[];
    stats: SeriesStats;
    topDrawdowns: DrawdownPeriod[];
  };

  const alignedSeries = useMemo<{ series: AlignedSeries[]; windowStart: string | null; windowEnd: string | null; allMonths: string[] }>(() => {
    if (resolvedSeries.length === 0) return { series: [], windowStart: null, windowEnd: null, allMonths: [] };

    let maxStart = '';
    let minEnd = '9999-99';
    for (const s of resolvedSeries) {
      if (s.months.length === 0) continue;
      const first = s.months[0];
      const last = s.months[s.months.length - 1];
      if (first > maxStart) maxStart = first;
      if (last < minEnd) minEnd = last;
    }
    if (!maxStart || minEnd === '9999-99' || maxStart > minEnd) {
      return { series: [], windowStart: null, windowEnd: null, allMonths: [] };
    }

    // Union of all months each series has within [maxStart, minEnd]. If one
    // series lacks a given month, that series reports null for it.
    const monthSet = new Set<string>();
    for (const s of resolvedSeries) {
      for (const m of s.months) {
        if (m >= maxStart && m <= minEnd) monthSet.add(m);
      }
    }
    const allMonths = Array.from(monthSet).sort();

    const series: AlignedSeries[] = resolvedSeries.map((s) => {
      const baseFactor = s.factorByMonth.get(maxStart);
      const points: SeriesPoint[] = allMonths.map((m) => {
        const f = s.factorByMonth.get(m);
        if (f == null || baseFactor == null) return { date: m, cumReturnPct: null };
        return { date: m, cumReturnPct: (f / baseFactor - 1) * 100 };
      });

      // Monthly rebased returns for stats.
      const monthlyRets: number[] = [];
      let prev: number | null = null;
      for (const p of points) {
        if (p.cumReturnPct == null) continue;
        const factor = 1 + p.cumReturnPct / 100;
        if (prev != null && prev > 0) monthlyRets.push((factor / prev - 1) * 100);
        prev = factor;
      }
      const lastNonNull = [...points].reverse().find((p) => p.cumReturnPct != null);
      const totalReturn = lastNonNull?.cumReturnPct ?? 0;
      const years = monthlyRets.length / 12;
      const cumFactor = 1 + totalReturn / 100;
      const annualized = years > 0 ? (Math.pow(cumFactor, 1 / years) - 1) * 100 : 0;

      let peak = 1.0, maxDd = 0, factor = 1.0;
      for (const r of monthlyRets) {
        factor *= (1 + r / 100);
        peak = Math.max(peak, factor);
        const dd = (factor / peak - 1) * 100;
        maxDd = Math.min(maxDd, dd);
      }

      let sharpe: number | null = null;
      if (monthlyRets.length >= 12) {
        const mean = monthlyRets.reduce((a, b) => a + b, 0) / monthlyRets.length;
        const std = Math.sqrt(monthlyRets.reduce((a, b) => a + (b - mean) ** 2, 0) / monthlyRets.length);
        if (std > 0) sharpe = (mean / std) * Math.sqrt(12);
      }

      const ddValues = points
        .filter((p) => p.cumReturnPct != null)
        .map((p) => ({ date: p.date, value: 1 + (p.cumReturnPct as number) / 100 }));
      const topDrawdowns = computeTopDrawdowns(ddValues, 3);

      return {
        ...s,
        points,
        stats: { totalReturn, annualized, maxDd, sharpe, months: monthlyRets.length },
        topDrawdowns,
      };
    });

    return { series, windowStart: maxStart, windowEnd: minEnd, allMonths };
  }, [resolvedSeries]);

  // Yearly performance breakdown — per-series compound return for each calendar year.
  const yearlyBreakdown = useMemo(() => {
    const { series } = alignedSeries;
    if (series.length === 0) return { years: [] as string[], bySeries: {} as Record<string, Record<string, number | null>> };

    const yearsSet = new Set<string>();
    const bySeries: Record<string, Record<string, number | null>> = {};

    for (const s of series) {
      const lastByYear = new Map<string, number>();
      for (const p of s.points) {
        if (p.cumReturnPct == null) continue;
        const y = p.date.slice(0, 4);
        lastByYear.set(y, p.cumReturnPct);
      }
      const ys = Array.from(lastByYear.keys()).sort();
      let prev = 0;
      const rowMap: Record<string, number | null> = {};
      for (const y of ys) {
        const cum = lastByYear.get(y)!;
        rowMap[y] = ((1 + cum / 100) / (1 + prev / 100) - 1) * 100;
        prev = cum;
        yearsSet.add(y);
      }
      bySeries[s.id] = rowMap;
    }

    const years = Array.from(yearsSet).sort();
    // Backfill missing years with null so the column count matches.
    for (const s of series) {
      for (const y of years) if (!(y in bySeries[s.id])) bySeries[s.id][y] = null;
    }
    return { years, bySeries };
  }, [alignedSeries]);

  // Cumulative return from customFromMonth through end of aligned window, per series.
  const customRangeReturn = useMemo(() => {
    const { series } = alignedSeries;
    if (!customFromMonth || series.length === 0) return null;
    const last = series[0].points[series[0].points.length - 1];
    if (!last) return null;
    const perSeries = series.map((s) => {
      let start: number | null = null;
      let end: number | null = null;
      for (const p of s.points) {
        if (p.cumReturnPct == null) continue;
        if (p.date < customFromMonth) start = p.cumReturnPct;
        end = p.cumReturnPct;
      }
      if (end == null) return { id: s.id, label: s.label, color: s.color, ret: null };
      const s0 = start ?? 0;
      return { id: s.id, label: s.label, color: s.color, ret: ((1 + end / 100) / (1 + s0 / 100) - 1) * 100 };
    });
    return { perSeries, fromDate: customFromMonth, toDate: last.date };
  }, [alignedSeries, customFromMonth]);

  // Chart data — wide-format per month, one key per series id, plus a 0%
  // origin row so every line starts from the same reference point.
  const chartData = useMemo(() => {
    const { series, allMonths, windowStart } = alignedSeries;
    if (series.length === 0 || allMonths.length === 0) return [];

    const rows: Record<string, string | number | null>[] = [];
    if (windowStart) {
      const origin: Record<string, string | number | null> = { date: `${windowStart} (start)` };
      for (const s of series) origin[s.id] = 0;
      rows.push(origin);
    }
    for (const m of allMonths) {
      const row: Record<string, string | number | null> = { date: m };
      for (const s of series) {
        const pt = s.points.find((p) => p.date === m);
        row[s.id] = pt?.cumReturnPct ?? null;
      }
      rows.push(row);
    }
    return rows;
  }, [alignedSeries]);

  // One-way turnover per month: % of current holdings that weren't held last month.
  // First month has no prior portfolio → null.
  const turnoverByDate = useMemo<Record<string, number | null>>(() => {
    const map: Record<string, number | null> = {};
    if (!result) return map;
    let prevIds: Set<number> | null = null;
    for (const r of result.monthly_records) {
      const currIds = new Set(r.holdings.map(h => h.company_id));
      if (prevIds === null || currIds.size === 0) {
        map[r.date] = null;
      } else {
        let added = 0;
        for (const id of currIds) if (!prevIds.has(id)) added += 1;
        map[r.date] = (added / currIds.size) * 100;
      }
      prevIds = currIds;
    }
    return map;
  }, [result]);

  // Log-scale chart data: ln(1 + cumReturn/100) * 100 applied to every series key.
  const displayChartData = useMemo(() => {
    if (!logScale) return chartData;
    const seriesIds = alignedSeries.series.map((s) => s.id);
    return chartData.map((p) => {
      const out: Record<string, string | number | null> = { date: p.date as string };
      for (const id of seriesIds) {
        const v = p[id];
        out[id] = typeof v === 'number' ? Math.log(1 + v / 100) * 100 : null;
      }
      return out;
    });
  }, [chartData, logScale, alignedSeries]);

  // Y-axis domain for chart — used by ReferenceArea to span full height
  const chartYDomain = useMemo<[number, number]>(() => {
    if (!displayChartData.length) return [-100, 100];
    const seriesIds = alignedSeries.series.map((s) => s.id);
    let min = Infinity, max = -Infinity;
    for (const p of displayChartData) {
      for (const id of seriesIds) {
        const v = p[id];
        if (typeof v === 'number') {
          if (v < min) min = v;
          if (v > max) max = v;
        }
      }
    }
    if (min === Infinity || max === -Infinity) return [-100, 100];
    const pad = Math.max((max - min) * 0.05, 5);
    return [Math.floor(min - pad), Math.ceil(max + pad)];
  }, [displayChartData, alignedSeries]);

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
      selection_mode: selectionMode,
      random_seed: selectionMode === 'random' ? randomSeed : null,
      n_trials: selectionMode === 'random' ? Math.max(1, nTrials) : 1,
    });
  };

  // "What is my strategy holding right now?" — load the most recent saved
  // snapshot if one exists (instant), else trigger a fresh compute (slow).
  // Random mode is unsupported here.
  const showCurrentPicks = async () => {
    if (currentPicksSnapshots.length > 0) {
      await loadCurrentPicksSnapshot(currentPicksSnapshots[0].snapshot_id);
    } else {
      await recomputeCurrentPortfolio();
    }
  };

  // Force a fresh full compute. Slow (signals + scoring + price fetch),
  // but persists a new snapshot in the DB so future loads are instant.
  const recomputeCurrentPortfolio = async () => {
    await startBacktest({
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
      selection_mode: 'momentum',
      random_seed: null,
      n_trials: 1,
      mode: 'current_portfolio',
    });
    // Refresh the snapshot list so the new snapshot is available in the picker
    loadCurrentPicksSnapshots();
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
            selection_mode: selectionMode,
            random_seed: selectionMode === 'random' ? randomSeed : null,
            n_trials: selectionMode === 'random' ? Math.max(1, nTrials) : 1,
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
      if (cfg.selection_mode === 'random' || cfg.selection_mode === 'momentum') setSelectionMode(cfg.selection_mode);
      if (typeof cfg.random_seed === 'number') setRandomSeed(cfg.random_seed);
      if (typeof cfg.n_trials === 'number') setNTrials(cfg.n_trials);
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
                        {i.index_name} ({i.start_month.slice(0, 7)} – {i.end_month.slice(0, 7)}, {i.total_unique_tickers} tickers)
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
            <div>
              <label className="text-gray-500 text-xs block mb-1">Strategy</label>
              <select
                value={selectionMode}
                onChange={(e) => setSelectionMode(e.target.value as 'momentum' | 'random')}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                title="Random ignores all signal weights and picks sectors/stocks at random — use as a noise-floor baseline."
              >
                <option value="momentum">Momentum</option>
                <option value="random">Random (baseline)</option>
              </select>
            </div>
            {selectionMode === 'random' && (
              <>
                <div>
                  <label className="text-gray-500 text-xs block mb-1">Seed</label>
                  <input
                    type="number"
                    value={randomSeed}
                    onChange={(e) => setRandomSeed(Number(e.target.value))}
                    className="w-20 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                    title="Same seed reproduces the same random picks. With Trials > 1, trials use seed, seed+1, ..."
                  />
                </div>
                <div>
                  <label className="text-gray-500 text-xs block mb-1">Trials</label>
                  <input
                    type="number"
                    min={1}
                    max={100}
                    value={nTrials}
                    onChange={(e) => setNTrials(Number(e.target.value))}
                    className="w-16 bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-2 text-white text-sm font-mono text-center focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                    title="Number of independent random trials. Summary shows mean ± std across trials."
                  />
                </div>
              </>
            )}
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
            <button
              onClick={showCurrentPicks}
              disabled={running || selectionMode === 'random'}
              className="px-4 py-2 rounded-lg text-sm font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
              title={
                selectionMode === 'random'
                  ? 'Current Picks is unavailable for random selection mode'
                  : currentPicksSnapshots.length > 0
                    ? `Load most recent snapshot (${currentPicksSnapshots[0].as_of_date}, ${currentPicksSnapshots[0].triggered_by})`
                    : 'No saved snapshot yet — first click will run a full compute and save it'
              }
            >
              Current Picks
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
        {(running || error || progress.length > 0) && (
          <ProgressTimeline
            steps={[]}
            log={progress.map(p => p.message)}
            pct={progress[progress.length - 1]?.pct ?? 0}
            errorMessage={error}
            running={running}
            defaultLogOpen
            title="Backtest progress"
          />
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

        {/* Current Portfolio (MTD) — shown above backtest results, independent */}
        {currentPortfolio && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
            <div className="px-4 py-3 border-b border-gray-800/40 flex items-center justify-between flex-wrap gap-3">
              <div className="flex items-center gap-3 flex-wrap">
                <div>
                  <div className="text-sm font-medium text-white">Current Picks</div>
                  <div className="text-xs text-gray-500">
                    Rebalance as of <span className="font-mono text-gray-400">{currentPortfolio.as_of_date}</span>
                    {currentPortfolio.latest_price_date && (
                      <> · MTD through <span className="font-mono text-gray-400">{currentPortfolio.latest_price_date}</span></>
                    )}
                    {' · '}{currentPortfolio.holdings.length} holdings
                  </div>
                </div>
                {/* Snapshot picker */}
                {currentPicksSnapshots.length > 0 && (
                  <select
                    value={currentPortfolio.snapshot_id ?? ''}
                    onChange={(e) => {
                      const id = Number(e.target.value);
                      if (id) loadCurrentPicksSnapshot(id);
                    }}
                    className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-xs text-gray-300 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                    title="Switch to a historic snapshot"
                  >
                    {currentPortfolio.snapshot_id == null && <option value="">(unsaved)</option>}
                    {currentPicksSnapshots.map((s) => (
                      <option key={s.snapshot_id} value={s.snapshot_id}>
                        {s.created_at.slice(0, 16).replace('T', ' ')} · {s.triggered_by} · {s.as_of_date.slice(0, 7)}
                      </option>
                    ))}
                  </select>
                )}
                {/* Refresh MTD button — only meaningful when a saved snapshot is loaded */}
                {currentPortfolio.snapshot_id != null && (
                  <button
                    onClick={() => refreshCurrentPicksMTD(currentPortfolio.snapshot_id!)}
                    disabled={refreshingMTD || running}
                    className="px-2.5 py-1 rounded-lg text-xs font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed inline-flex items-center gap-1.5"
                    title="Refresh month-to-date returns using the latest available prices (does not re-run signals)"
                  >
                    <span className="text-emerald-400">✓</span>
                    {refreshingMTD ? 'Refreshing…' : 'Refresh MTD'}
                  </button>
                )}
                {/* Force a new full compute */}
                <button
                  onClick={recomputeCurrentPortfolio}
                  disabled={running}
                  className="px-2.5 py-1 rounded-lg text-xs font-medium border border-gray-700 text-gray-300 hover:bg-white/5 hover:text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  title="Run the full strategy now and save a new snapshot (slow)"
                >
                  Recompute
                </button>
              </div>
              {currentPortfolio.holdings.length > 0 && (() => {
                const validReturns = currentPortfolio.holdings
                  .map(h => h.forward_return_pct)
                  .filter((r): r is number => r != null);
                if (validReturns.length === 0) return null;
                const portMTD = validReturns.reduce((a, b) => a + b, 0) / validReturns.length;
                return (
                  <div className="text-right">
                    <div className="text-xs text-gray-500">Portfolio MTD (equal-weight)</div>
                    <div className={`text-lg font-mono font-medium ${portMTD >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {portMTD >= 0 ? '+' : ''}{portMTD.toFixed(2)}%
                    </div>
                  </div>
                );
              })()}
            </div>
            {currentPortfolio.holdings.length > 0 ? (
              <div className="bg-[#0f1117] px-5 py-3">
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
                    {[...currentPortfolio.holdings]
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
              </div>
            ) : (
              <div className="px-4 py-6 text-center text-sm text-gray-500">
                No holdings selected for this month — universe or signals returned empty.
              </div>
            )}
            {/* Daily picks — what the strategy WOULD have picked each trading
                day this month. Click a row to see the holdings for that date. */}
            {currentPortfolio.daily_picks && currentPortfolio.daily_picks.length > 0 && (
              <div className="border-t border-gray-800/40">
                <div className="px-4 py-3 border-b border-gray-800/40">
                  <div className="text-sm font-medium text-white">Daily picks ({currentPortfolio.daily_picks.length} trading days)</div>
                  <div className="text-xs text-gray-500 mt-0.5">
                    Hypothetical: what the strategy would pick if rebalancing on each day. Turnover compares to the previous day.
                  </div>
                </div>
                <table className="w-full text-xs">
                  <thead>
                    <tr className="border-b border-gray-800/40 text-gray-600">
                      <th className="text-left px-4 py-1.5 font-medium">Date</th>
                      <th className="text-right px-3 py-1.5 font-medium">Holdings</th>
                      <th className="text-right px-3 py-1.5 font-medium">Turnover</th>
                      <th className="text-right px-3 py-1.5 font-medium">%</th>
                    </tr>
                  </thead>
                  <tbody>
                    {currentPortfolio.daily_picks.map((dp) => (
                      <Fragment key={dp.date}>
                        <tr
                          className="border-b border-gray-800/30 hover:bg-white/[0.02] cursor-pointer"
                          onClick={() => setExpandedDailyDate(expandedDailyDate === dp.date ? null : dp.date)}
                        >
                          <td className="px-4 py-1.5 font-mono text-gray-200">
                            <span className="text-gray-600 mr-2">{expandedDailyDate === dp.date ? '▾' : '▸'}</span>
                            {dp.date}
                          </td>
                          <td className="px-3 py-1.5 text-right font-mono text-gray-300">{dp.holdings.length}</td>
                          <td className={`px-3 py-1.5 text-right font-mono ${dp.turnover_abs > 0 ? 'text-amber-400' : 'text-gray-600'}`}>
                            {dp.turnover_abs > 0 ? dp.turnover_abs : '—'}
                          </td>
                          <td className={`px-3 py-1.5 text-right font-mono ${dp.turnover_pct > 0 ? 'text-amber-400' : 'text-gray-600'}`}>
                            {dp.turnover_pct > 0 ? `${dp.turnover_pct.toFixed(2)}%` : '—'}
                          </td>
                        </tr>
                        {expandedDailyDate === dp.date && (
                          <tr>
                            <td colSpan={4} className="bg-[#0f1117] px-5 py-3">
                              <table className="w-full text-xs">
                                <thead>
                                  <tr className="text-gray-600">
                                    <th className="text-left py-1 font-medium">Ticker</th>
                                    <th className="text-left py-1 font-medium">Company</th>
                                    <th className="text-left py-1 font-medium">Sector</th>
                                    <th className="text-right py-1 font-medium">Score</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {[...dp.holdings]
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
                                          <td className="py-1.5 truncate max-w-[200px] text-gray-300">{h.company_name}</td>
                                          <td className="py-1.5 text-gray-500">{h.sector}</td>
                                          <td className="text-right py-1.5 text-white font-mono font-medium">{h.score.toFixed(1)}</td>
                                        </tr>
                                      );
                                    })}
                                </tbody>
                              </table>
                            </td>
                          </tr>
                        )}
                      </Fragment>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}

        {/* Results */}
        {result && (
          <>
            {/* Comparison panel — active strategy + any added backtests/benchmarks */}
            <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-4 py-3">
              <div className="flex items-center gap-3 flex-wrap">
                <span className="text-gray-400 text-sm mr-1">Comparison</span>
                {alignedSeries.series.map((s) => (
                  <span
                    key={s.id}
                    className="inline-flex items-center gap-2 bg-[#0f1117] border border-gray-800 rounded-full pl-2 pr-1 py-1 text-xs"
                  >
                    <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                    <span className="text-gray-200">{s.label}</span>
                    {s.removable && (
                      <button
                        type="button"
                        onClick={() => removeSeries(s.id)}
                        className="ml-0.5 w-4 h-4 rounded-full text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors flex items-center justify-center"
                        title="Remove from comparison"
                      >
                        <svg className="w-2.5 h-2.5" viewBox="0 0 20 20" fill="currentColor">
                          <path fillRule="evenodd" d="M4.293 4.293a1 1 0 011.414 0L10 8.586l4.293-4.293a1 1 0 111.414 1.414L11.414 10l4.293 4.293a1 1 0 01-1.414 1.414L10 11.414l-4.293 4.293a1 1 0 01-1.414-1.414L8.586 10 4.293 5.707a1 1 0 010-1.414z" clipRule="evenodd" />
                        </svg>
                      </button>
                    )}
                  </span>
                ))}
                <div className="relative" ref={addSeriesRef}>
                  <button
                    type="button"
                    onClick={() => setAddSeriesOpen((o) => !o)}
                    className="inline-flex items-center gap-1 text-xs text-indigo-300 hover:text-indigo-200 border border-indigo-500/40 hover:border-indigo-400/60 bg-indigo-500/10 hover:bg-indigo-500/20 rounded-full px-3 py-1 transition-colors"
                  >
                    + Add series
                  </button>
                  {addSeriesOpen && (
                    <div className="absolute left-0 mt-1 w-72 bg-[#151821] border border-gray-700 rounded-lg shadow-xl z-50 max-h-80 overflow-auto">
                      {benchmarkOptions.length > 0 && (
                        <div>
                          <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-gray-500 border-b border-gray-800/60">Benchmarks</div>
                          {benchmarkOptions.map((b) => {
                            const already = comparisons.some((c) => c.kind === 'benchmark' && c.benchmarkId === b.benchmark_id);
                            return (
                              <button
                                key={b.benchmark_id}
                                type="button"
                                disabled={already}
                                onClick={() => { addBenchmarkSeries(b.benchmark_id); setAddSeriesOpen(false); }}
                                className="w-full text-left px-3 py-2 text-xs hover:bg-white/[0.03] disabled:opacity-40 disabled:cursor-not-allowed text-gray-200 flex items-center gap-2"
                              >
                                <span className="font-mono text-amber-300">{b.ticker}</span>
                                <span className="text-gray-500 truncate">{b.name}</span>
                                {already && <span className="ml-auto text-[10px] text-gray-600">added</span>}
                              </button>
                            );
                          })}
                        </div>
                      )}
                      {savedRuns.length > 0 && (
                        <div>
                          <div className="px-3 py-1.5 text-[10px] uppercase tracking-wider text-gray-500 border-t border-b border-gray-800/60">Saved Backtests</div>
                          {savedRuns.map((r) => {
                            const already = comparisons.some((c) => c.kind === 'saved' && c.runId === r.run_id);
                            const isLoaded = r.run_id === loadedRunId;
                            return (
                              <button
                                key={r.run_id}
                                type="button"
                                disabled={already || isLoaded}
                                onClick={() => { addSavedSeries(r.run_id); setAddSeriesOpen(false); }}
                                className="w-full text-left px-3 py-2 text-xs hover:bg-white/[0.03] disabled:opacity-40 disabled:cursor-not-allowed text-gray-200 flex items-center gap-2"
                                title={isLoaded ? 'Currently loaded as the active strategy' : undefined}
                              >
                                <span className="truncate">{r.name}</span>
                                {isLoaded && <span className="ml-auto text-[10px] text-gray-600">active</span>}
                                {!isLoaded && already && <span className="ml-auto text-[10px] text-gray-600">added</span>}
                              </button>
                            );
                          })}
                        </div>
                      )}
                      {benchmarkOptions.length === 0 && savedRuns.length === 0 && (
                        <div className="px-3 py-4 text-xs text-gray-500">No benchmarks or saved backtests available.</div>
                      )}
                    </div>
                  )}
                </div>
                {alignedSeries.windowStart && alignedSeries.windowEnd && alignedSeries.series.length > 1 && (
                  <span className="text-[11px] text-gray-500 font-mono ml-auto">
                    aligned {alignedSeries.windowStart} → {alignedSeries.windowEnd}
                  </span>
                )}
              </div>
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
                    <th className="px-3 py-2.5 text-right font-medium">Months</th>
                  </tr>
                </thead>
                <tbody>
                  {alignedSeries.series.map((s) => (
                    <tr key={s.id} className="border-b border-gray-800/30">
                      <td className="px-4 py-2.5 font-medium flex items-center gap-2">
                        <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                        <span className="text-gray-200">{s.label}</span>
                      </td>
                      <td className={`px-3 py-2.5 text-right font-mono ${s.stats.totalReturn >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.totalReturn)}</td>
                      <td className={`px-3 py-2.5 text-right font-mono ${s.stats.annualized >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>{fmtPct(s.stats.annualized)}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-rose-400">{fmtPct(s.stats.maxDd)}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-white">{s.stats.sharpe != null ? s.stats.sharpe.toFixed(2) : '—'}</td>
                      <td className="px-3 py-2.5 text-right font-mono text-gray-300">{s.stats.months}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
              {/* Active strategy — raw (non-aligned) metrics, including turnover + holdings */}
              <div className="px-4 py-3 border-t border-gray-800/40 text-xs text-gray-500 flex flex-wrap gap-x-6 gap-y-1">
                <span>Strategy (full range): <span className="font-mono text-gray-300">Turnover {fmtPct(result.summary.avg_monthly_turnover_pct)}</span></span>
                <span><span className="font-mono text-gray-300">Avg Holdings {result.summary.avg_holdings.toFixed(1)}</span></span>
                <span><span className="font-mono text-gray-300">Months {result.summary.total_months}</span></span>
              </div>
              {/* Multi-trial cross-trial statistics — backend means ± std.
                  These are the numbers to compare a momentum run against,
                  NOT the per-series stats above (which derive from the mean
                  equity curve and understate volatility). */}
              {result.summary.n_trials != null && result.summary.n_trials > 1 && (
                <div className="px-4 py-3 border-t border-gray-800/40">
                  <div className="text-xs font-medium text-gray-400 mb-2">
                    Cross-trial statistics ({result.summary.n_trials} random trials, mean ± std)
                  </div>
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-3 text-xs">
                    <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                      <div className="text-gray-500">Total Return</div>
                      <div className="font-mono text-gray-200">
                        {fmtPct(result.summary.total_return_pct)}
                        <span className="text-gray-500"> ± {(result.summary.total_return_pct_std ?? 0).toFixed(2)}%</span>
                      </div>
                    </div>
                    <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                      <div className="text-gray-500">Annualized</div>
                      <div className="font-mono text-gray-200">
                        {fmtPct(result.summary.annualized_return_pct)}
                        <span className="text-gray-500"> ± {(result.summary.annualized_return_pct_std ?? 0).toFixed(2)}%</span>
                      </div>
                    </div>
                    <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                      <div className="text-gray-500">Max Drawdown</div>
                      <div className="font-mono text-gray-200">
                        {fmtPct(result.summary.max_drawdown_pct)}
                        <span className="text-gray-500"> ± {(result.summary.max_drawdown_pct_std ?? 0).toFixed(2)}%</span>
                      </div>
                    </div>
                    <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                      <div className="text-gray-500">Sharpe</div>
                      <div className="font-mono text-gray-200">
                        {result.summary.sharpe_ratio != null ? result.summary.sharpe_ratio.toFixed(2) : '—'}
                        {result.summary.sharpe_ratio_std != null && (
                          <span className="text-gray-500"> ± {result.summary.sharpe_ratio_std.toFixed(2)}</span>
                        )}
                      </div>
                    </div>
                    <div className="bg-[#0f1117] rounded-lg px-3 py-2">
                      <div className="text-gray-500">Turnover</div>
                      <div className="font-mono text-gray-200">
                        {fmtPct(result.summary.avg_monthly_turnover_pct)}
                        <span className="text-gray-500"> ± {(result.summary.avg_monthly_turnover_pct_std ?? 0).toFixed(2)}%</span>
                      </div>
                    </div>
                  </div>
                </div>
              )}
              {alignedSeries.series.some((s) => s.topDrawdowns.length > 0) && (
                <div className="px-4 py-3 border-t border-gray-800/40 space-y-3">
                  {alignedSeries.series.map((s) => (
                    s.topDrawdowns.length > 0 && (
                      <div key={s.id}>
                        <div className="text-xs font-medium mb-2 flex items-center gap-2">
                          <span className="inline-block w-2 h-2 rounded-full" style={{ background: s.color }} />
                          <span className="text-gray-400">{s.label} — Top Drawdowns</span>
                        </div>
                        <div className="grid grid-cols-3 gap-3">
                          {s.topDrawdowns.map((dd, i) => {
                            const alpha = [1.0, 0.6, 0.3][i] ?? 0.3;
                            return (
                              <div key={i} className="bg-[#0f1117] rounded-lg px-3 py-2">
                                <div className="flex items-center gap-2 mb-1">
                                  <div className="w-2 h-2 rounded-full" style={{ background: s.color, opacity: alpha }} />
                                  <span className="font-mono text-sm font-medium" style={{ color: s.color }}>{dd.drawdown_pct.toFixed(1)}%</span>
                                </div>
                                <div className="text-[10px] text-gray-500 font-mono">
                                  {dd.peak_date} to {dd.trough_date}
                                  {dd.recovery_date ? ` (recovered ${dd.recovery_date})` : ' (ongoing)'}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    )
                  ))}
                </div>
              )}
            </div>

            {/* Yearly Performance + Custom Range */}
            {yearlyBreakdown.years.length > 0 && (
              <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
                <div className="px-5 py-3 border-b border-gray-800/40">
                  <h3 className="text-white text-sm font-medium">Yearly Performance</h3>
                </div>
                <div className="overflow-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-800/40 text-gray-500 text-xs">
                        <th className="px-5 py-2.5 text-left font-medium">Year</th>
                        {alignedSeries.series.map((s) => (
                          <th key={s.id} className="px-3 py-2.5 text-right font-medium">
                            <span className="inline-flex items-center gap-1.5">
                              <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: s.color }} />
                              <span className="truncate max-w-[140px]">{s.label}</span>
                            </span>
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {yearlyBreakdown.years.map((y) => (
                        <tr key={y} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                          <td className="px-5 py-2 text-gray-200 font-mono">{y}</td>
                          {alignedSeries.series.map((s) => {
                            const v = yearlyBreakdown.bySeries[s.id]?.[y];
                            return (
                              <td
                                key={s.id}
                                className={`px-3 py-2 text-right font-mono ${v != null ? (v >= 0 ? 'text-emerald-400' : 'text-rose-400') : 'text-gray-600'}`}
                              >
                                {v != null ? fmtPct(v) : '—'}
                              </td>
                            );
                          })}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                <div className="px-5 py-3 border-t border-gray-800/40 flex items-center gap-4 flex-wrap">
                  <label className="text-xs text-gray-400 font-medium">From month:</label>
                  <input
                    type="month"
                    value={customFromMonth}
                    min={alignedSeries.windowStart ?? undefined}
                    max={alignedSeries.windowEnd ?? undefined}
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
                    <div className="flex items-center gap-4 text-xs ml-auto flex-wrap">
                      <span className="text-gray-500 font-mono">{customRangeReturn.fromDate} → {customRangeReturn.toDate}</span>
                      {customRangeReturn.perSeries.map((s) => (
                        <span key={s.id} className="text-gray-400 inline-flex items-center gap-1.5">
                          <span className="inline-block w-1.5 h-1.5 rounded-full" style={{ background: s.color }} />
                          {s.label}:{' '}
                          {s.ret != null ? (
                            <span className={`font-mono ${s.ret >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                              {fmtPct(s.ret)}
                            </span>
                          ) : (
                            <span className="font-mono text-gray-600">—</span>
                          )}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="text-xs text-gray-500">Cumulative return from picked month through end of aligned window.</span>
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
                      const s = alignedSeries.series.find((x) => x.id === name);
                      return [`${v >= 0 ? '+' : ''}${v.toFixed(2)}%`, s?.label ?? String(name)];
                    }}
                  />
                  {alignedSeries.series.length > 1 && (
                    <Legend
                      wrapperStyle={{ fontSize: 12, color: '#9ca3af' }}
                      formatter={(value) => {
                        const s = alignedSeries.series.find((x) => x.id === value);
                        return s?.label ?? String(value);
                      }}
                    />
                  )}
                  {/* Drawdown overlays: only for the active strategy (first series) */}
                  {alignedSeries.series[0]?.topDrawdowns.map((dd, i) => {
                    const base = [0.25, 0.15, 0.10];
                    const hovered = hoveredDrawdown === i;
                    const opacity = hovered ? (base[i] ?? 0.10) + 0.15 : (base[i] ?? 0.10);
                    return (
                      <ReferenceArea
                        key={`dd-${i}`}
                        x1={dd.peak_date}
                        x2={dd.recovery_date ?? (displayChartData[displayChartData.length - 1]?.date as string | undefined)}
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
                  {alignedSeries.series.map((s, i) => (
                    <Line
                      key={s.id}
                      type="monotone"
                      dataKey={s.id}
                      stroke={s.color}
                      strokeWidth={i === 0 ? 2 : 1.5}
                      strokeDasharray={i === 0 ? undefined : '4 3'}
                      dot={false}
                      name={s.id}
                      connectNulls
                    />
                  ))}
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
                      <th className="text-right px-3 py-2.5 font-medium" title="% of current holdings not held last month">Turnover</th>
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
                          <td className="text-right px-3 py-2.5 font-mono text-gray-400">
                            {turnoverByDate[r.date] != null ? `${turnoverByDate[r.date]!.toFixed(1)}%` : '—'}
                          </td>
                          <td className={`text-right px-5 py-2.5 font-mono ${r.cumulative_return_pct >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                            {fmtPct(r.cumulative_return_pct)}
                          </td>
                        </tr>
                        {expandedMonth === r.date && r.holdings.length > 0 && (
                          <tr key={`${r.date}-detail`}>
                            <td colSpan={5} className="bg-[#0f1117] px-5 py-3">
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
