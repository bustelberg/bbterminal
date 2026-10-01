'use client';

import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { colorForSector } from '../../../lib/sectorColors';
import InfoTip from '../InfoTip';
import SectionLoader from '../SectionLoader';
import { INFO_ICON } from '../../../lib/infoIcon';

type Row = { date: string; sector: string; rank: number; score: number | null; companies: number };
type Payload = {
  source: string; method: string; universe?: { size?: number };
  days: number; rows: Row[]; note?: string;
};
type Detail = {
  date: string; sector: string; rank: number; score: number | null;
  category_scores: Record<string, number | null>;
  category_weights: Record<string, number>;
  method: string;
  companies: Array<{
    analysis_id: number; name: string | null; ticker: string | null; score: number;
    price_score: number | null; volume_score: number | null;
    signals: Record<string, number | null>;
    signal_details: Record<string, {
      raw: number | null; normalized: number | null; universe_min: number | null;
      universe_max: number | null; weight: number; category: string;
      raw_price_legs: PriceLegs | null;
      raw_explanation: RawExplanation | null;
      universe_min_company: CompanyReference | null; universe_max_company: CompanyReference | null;
      universe_min_price_legs: PriceLegs | null; universe_max_price_legs: PriceLegs | null;
      universe_min_raw_explanation: RawExplanation | null; universe_max_raw_explanation: RawExplanation | null;
    }>;
  }>;
};
type CompanyReference = { analysis_id: number; name: string | null; ticker: string | null };
type PriceLegs = { start_date: string; start_price: number; end_date: string; end_price: number; return_pct: number };
type RawExplanation = { value: number | null; components: Array<{ label: string; value_str?: string }> };

const HISTORY_DAYS = 42;
const DATE_COLUMN_WIDTH = 96;
const DATE_TILE_WIDTH = DATE_COLUMN_WIDTH - 12;
const SIGNALS = {
  mom_12_1: { label: '12–1M', unit: 'pct' },
  mom_6m: { label: '6M', unit: 'pct' },
  volatility_adjusted_return_6m: { label: 'Vol-adj 6M', unit: 'ratio' },
  drawdown_from_recent_high_pct: { label: 'Drawdown', unit: 'pct' },
  above_200ma: { label: 'Above 200 MA', unit: 'binary' },
  vol_20d_vs_60d: { label: 'Volume surge', unit: 'ratio' },
  vol_trend_3m: { label: 'Volume trend', unit: 'pct' },
};
type CompanySortKey = 'company' | 'score' | 'price_score' | 'volume_score' | keyof typeof SIGNALS;
const HEADER_INFO: Record<CompanySortKey, string> = {
  company: 'Company selected from this sector. Its name opens the Yahoo Finance quote page.',
  score: 'Total score = 50% × price score + 50% × volume score.',
  price_score: 'Price score = weighted average of normalized price signals: (3×12–1M + 2×6M + 2×Vol-adj 6M + Drawdown + Above 200 MA) ÷ 9.',
  volume_score: 'Volume score = (normalized Volume surge + normalized Volume trend) ÷ 2.',
  mom_12_1: 'Return from 12 months ago to 1 month ago: (price one month ago ÷ price 12 months ago − 1) × 100.',
  mom_6m: 'Six-month return: (latest close ÷ close six months ago − 1) × 100.',
  volatility_adjusted_return_6m: 'Six-month return ÷ annualized volatility of daily returns over the latest 126 trading days.',
  drawdown_from_recent_high_pct: 'Distance from the highest close in the latest 252 trading days: (latest close ÷ recent high − 1) × 100.',
  above_200ma: 'Yes when the latest close is above the average of the latest 200 closes; otherwise No.',
  vol_20d_vs_60d: 'Average volume over the latest 20 trading days ÷ average volume over the latest 60 trading days.',
  vol_trend_3m: 'Change in average daily volume: latest 21 calendar days versus a 21-day window beginning three months ago.',
};

function formatSignal(value: number | null | undefined, unit: string): string {
  if (value == null) return '—';
  if (unit === 'pct') return `${value.toFixed(2)}%`;
  if (unit === 'ratio') return `${value.toFixed(2)}×`;
  if (unit === 'binary') return value >= 0.5 ? 'Yes' : 'No';
  return value.toFixed(2);
}

function normalizationFormula(item: Detail['companies'][number]['signal_details'][string], unit: string): string {
  if (item.raw == null || item.universe_min == null || item.universe_max == null || item.normalized == null) return 'No usable value for this signal.';
  if (item.universe_max === item.universe_min) return `Every company had ${formatSignal(item.raw, unit)}, so this signal receives the neutral 50.0.`;
  return `(${formatSignal(item.raw, unit)} − ${formatSignal(item.universe_min, unit)}) ÷ (${formatSignal(item.universe_max, unit)} − ${formatSignal(item.universe_min, unit)}) × 100 = ${item.normalized.toFixed(1)}`;
}

function CompanyReferenceLink({ company }: { company: CompanyReference | null }) {
  if (!company) return <>Asset identifier unavailable</>;
  const label = company.name ?? company.ticker ?? `Asset ${company.analysis_id}`;
  return company.ticker ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(company.ticker)}`} target="_blank" rel="noreferrer" className="text-accent-300 hover:underline">{label}{company.name ? ` (${company.ticker})` : ''}</a> : <>{label}</>;
}

function PriceReturnLegs({ legs }: { legs: PriceLegs | null }) {
  if (!legs) return null;
  return <div className="mt-2 space-y-1 border-t border-neutral-800/40 pt-2 text-[11px]">
    <div className="flex justify-between gap-3"><span className="text-fg-faint">Start · {formatDate(legs.start_date)}</span><span className="font-mono text-fg-muted">{legs.start_price.toFixed(2)}</span></div>
    <div className="flex justify-between gap-3"><span className="text-fg-faint">End · {formatDate(legs.end_date)}</span><span className="font-mono text-fg-muted">{legs.end_price.toFixed(2)}</span></div>
    <div className="flex justify-between gap-3 pt-1"><span className="text-fg-faint">Return</span><span className="font-mono text-fg-strong">{formatSignal(legs.return_pct, 'pct')}</span></div>
  </div>;
}

function ExplanationRows({ explanation }: { explanation: RawExplanation | null | undefined }) {
  return <>{explanation?.components.map((component, index) => <div key={`${component.label}-${index}`} className={index === explanation.components.length - 1 ? 'border-t border-neutral-800/50 pt-2' : ''}><p className="text-fg-faint">{component.label}</p>{component.value_str && <p className="mt-0.5 font-mono text-fg-muted">{component.value_str}</p>}</div>)}</>;
}

function RawCalculation({ signal, item }: {
  signalKey: string; signal: { label: string; unit: string };
  item: Detail['companies'][number]['signal_details'][string];
}) {
  return <div className="space-y-2"><p className="font-medium text-fg-strong">{signal.label} calculation</p><ExplanationRows explanation={item.raw_explanation} /></div>;
}

function SortableHeader({ label, column, active, direction, onSort, className = '' }: {
  label: string; column: CompanySortKey; active: CompanySortKey; direction: 'asc' | 'desc';
  onSort: (column: CompanySortKey) => void; className?: string;
}) {
  return <th className={`pb-2 pr-4 ${className}`}><span className="inline-flex items-center gap-1"><button type="button" onClick={() => onSort(column)} className="inline-flex items-center gap-1 hover:text-fg-strong">{label}<span className={active === column ? 'text-accent-300' : 'text-fg-faint'}>{active === column ? direction === 'asc' ? '↑' : '↓' : '↕'}</span></button><InfoTip text={HEADER_INFO[column]}><span className={INFO_ICON}>i</span></InfoTip></span></th>;
}

function formatDate(date: string): string {
  return new Intl.DateTimeFormat('en-GB', {
    day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
  }).format(new Date(`${date}T00:00:00Z`));
}

function amsterdamToday(): string {
  return new Intl.DateTimeFormat('sv-SE', { timeZone: 'Europe/Amsterdam' }).format(new Date());
}

function upcomingTradingDates(first: string, count: number): string[] {
  const cursor = new Date(`${first}T00:00:00Z`);
  const dates: string[] = [];
  while (dates.length < count) {
    const weekday = cursor.getUTCDay();
    if (weekday !== 0 && weekday !== 6) dates.push(cursor.toISOString().slice(0, 10));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
}

export default function SectorMomentumTimeline() {
  const [data, setData] = useState<Payload | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [hover, setHover] = useState<Row | null>(null);
  const [detail, setDetail] = useState<Detail | null>(null);
  const [detailLoading, setDetailLoading] = useState<Row | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [expandedCompanyId, setExpandedCompanyId] = useState<number | null>(null);
  const [companySort, setCompanySort] = useState<CompanySortKey>('score');
  const [companySortDirection, setCompanySortDirection] = useState<'asc' | 'desc'>('desc');
  const rankTableRef = useRef<HTMLDivElement>(null);
  const today = amsterdamToday();

  const load = useCallback(async () => {
    setLoading(true); setError(null); setHover(null);
    try {
      const response = await apiFetch(`${API_URL}/api/momentum/sector-timeline?days=${HISTORY_DAYS}`);
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
      setData(body as Payload);
    } catch (e) {
      setData(null); setError(e instanceof Error ? e.message : String(e));
    } finally { setLoading(false); }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const { dates, sectors, rankLookup, maxRank } = useMemo(() => {
    // Never paint an in-progress Amsterdam trading day, even if a cached API
    // response happens to contain a provisional vendor bar for it.
    const rows = (data?.rows ?? []).filter((row) => row.date < today);
    const ds = [...new Set(rows.map((r) => r.date))].sort();
    const bySector = new Map<string, Row[]>();
    const map = new Map<string, Row>();
    const byRank = new Map<string, Row>();
    for (const row of rows) {
      map.set(`${row.sector}|${row.date}`, row);
      byRank.set(`${row.date}|${row.rank}`, row);
      const list = bySector.get(row.sector) ?? [];
      list.push(row); bySector.set(row.sector, list);
    }
    const lastDate = ds.at(-1);
    const ss = [...bySector.keys()].sort((a, b) =>
      (map.get(`${a}|${lastDate}`)?.rank ?? 999) - (map.get(`${b}|${lastDate}`)?.rank ?? 999)
      || a.localeCompare(b));
    return {
      dates: ds,
      sectors: ss,
      rankLookup: byRank,
      maxRank: Math.max(1, ...rows.map((row) => row.rank)),
    };
  }, [data, today]);

  const futureDates = useMemo(() => upcomingTradingDates(today, 5), [today]);
  const displayDates = useMemo(
    () => [...dates, ...futureDates.filter((future) => !dates.includes(future))],
    [dates, futureDates],
  );
  const futureDateSet = useMemo(() => new Set(futureDates), [futureDates]);

  useEffect(() => {
    const table = rankTableRef.current;
    if (table) table.scrollLeft = table.scrollWidth;
  }, [displayDates]);

  const openDetail = useCallback(async (row: Row) => {
    setDetail(null); setDetailError(null); setDetailLoading(row); setExpandedCompanyId(null);
    try {
      const response = await apiFetch(`${API_URL}/api/momentum/sector-timeline/detail?date=${encodeURIComponent(row.date)}&sector=${encodeURIComponent(row.sector)}`);
      const body = await response.json().catch(() => null);
      if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
      setDetail(body as Detail);
    } catch (error) {
      setDetailError(error instanceof Error ? error.message : String(error));
    } finally {
      setDetailLoading(null);
    }
  }, []);

  const sortCompanies = useCallback((column: CompanySortKey) => {
    setCompanySortDirection((current) => companySort === column
      ? current === 'asc' ? 'desc' : 'asc'
      : column === 'company' ? 'asc' : 'desc');
    setCompanySort(column);
  }, [companySort]);

  const sortedCompanies = useMemo(() => {
    const companies = [...(detail?.companies ?? [])];
    const valueFor = (company: Detail['companies'][number]): string | number | null => {
      if (companySort === 'company') return company.name ?? company.ticker ?? '';
      if (companySort === 'score' || companySort === 'price_score' || companySort === 'volume_score') return company[companySort];
      return company.signals[companySort];
    };
    return companies.sort((left, right) => {
      const a = valueFor(left); const b = valueFor(right);
      if (a == null) return b == null ? 0 : 1;
      if (b == null) return -1;
      const comparison = typeof a === 'string' && typeof b === 'string' ? a.localeCompare(b) : Number(a) - Number(b);
      return companySortDirection === 'asc' ? comparison : -comparison;
    });
  }, [companySort, companySortDirection, detail]);

  return (
    <div className="min-h-screen bg-page text-fg">
      <div className="px-8 py-5 border-b border-neutral-800/40">
        <h1 className="text-xl font-semibold text-fg-strong">Sector Momentum</h1>
        <p className="text-sm text-fg-subtle mt-1">
          Daily sector rankings from Yahoo Finance closing prices.
        </p>
      </div>
      <div className="px-8 py-6 space-y-4">
        <div className="bg-card border border-neutral-800/40 rounded-xl p-4 flex items-center gap-3 flex-wrap">
          <span className="text-xs font-semibold text-fg-strong uppercase tracking-wide">History</span>
          <span className="text-xs text-fg-muted">Last 2 months</span>
          {data && <span className="text-xs text-fg-faint">{data.universe?.size?.toLocaleString() ?? '—'} liquid equities · {data.days} trading days</span>}
          <button type="button" onClick={() => void load()} className="ml-auto text-xs text-accent-300 hover:text-accent-200">Refresh</button>
        </div>

        {loading && <SectionLoader label="daily sector momentum" />}
        {error && <div className="text-sm text-neg-300 py-4">Could not load momentum: {error}</div>}
        {!loading && !error && data?.note && <div className="text-sm text-warn-300 py-4">{data.note}</div>}
        {!loading && !error && sectors.length > 0 && (
          <div className="bg-card border border-neutral-800/40 rounded-xl p-5 overflow-hidden">
            <div className="flex items-baseline justify-between gap-4 mb-4">
              <div><h2 className="font-medium text-fg-strong">Daily sector ranks</h2><p className="text-xs text-fg-faint mt-1">Each date is ordered vertically: strongest sector at the top, weakest at the bottom. Colors identify sectors; ranks blend Yahoo price and volume signals.</p></div>
              <span className="text-xs text-fg-faint shrink-0">Latest completed close</span>
            </div>
            <div ref={rankTableRef} className="overflow-x-auto">
              <div className="min-w-max">
                <div className="flex h-8 text-[10px] text-fg-faint">
                  <div className="w-16 shrink-0" />
                  <div className="flex gap-3">
                    {displayDates.map((d) => (
                      <div key={d} className="shrink-0 text-center whitespace-nowrap" style={{ width: DATE_TILE_WIDTH }} title={d}>{formatDate(d)}</div>
                    ))}
                  </div>
                </div>
                {Array.from({ length: maxRank }, (_, index) => index + 1).map((rank) => {
                  return <div key={rank} className="flex items-center h-8">
                    <div className="w-16 shrink-0 text-[11px] uppercase tracking-wide text-fg-faint">Rank {rank}</div>
                    <div className="flex gap-3">{displayDates.map((d) => {
                      const row = rankLookup.get(`${d}|${rank}`);
                      const isFuture = futureDateSet.has(d);
                      const color = row ? colorForSector(row.sector, sectors.indexOf(row.sector)) : 'transparent';
                      return <button key={d} type="button" aria-label={row ? `${row.sector}, ${d}, rank ${row.rank}` : isFuture ? `${d}, rank pending` : `${d}, no ranking`}
                        onMouseEnter={() => setHover(row ?? null)} onFocus={() => setHover(row ?? null)} onMouseLeave={() => setHover(null)}
                        onClick={() => { if (row) void openDetail(row); }}
                        disabled={!row}
                        className="h-6 shrink-0 rounded-sm text-xs text-fg-faint transition-transform hover:scale-110 focus:outline focus:outline-1 focus:outline-fg-strong disabled:cursor-default"
                        style={{ width: DATE_TILE_WIDTH, background: row ? color : 'transparent' }}>
                        {isFuture ? '?' : null}
                      </button>;
                    })}</div>
                  </div>;
                })}
              </div>
            </div>
            <div className="mt-4 flex flex-wrap gap-x-4 gap-y-1.5 text-xs text-fg-muted">
              {sectors.map((sector, index) => <span key={sector} className="flex items-center gap-1.5"><i className="w-2 h-2 rounded-sm shrink-0" style={{ background: colorForSector(sector, index) }} />{sector}</span>)}
            </div>
            <div className="mt-4 min-h-5 text-xs text-fg-muted">
              {hover ? <span><strong className="text-fg-strong">{hover.sector}</strong> · {hover.date} · rank #{hover.rank} of {maxRank} · score {hover.score?.toFixed(1) ?? '—'} · {hover.companies} companies · click for calculation</span> : 'Hover a square to inspect it, or click to see the calculation and companies.'}
            </div>
          </div>
        )}
      </div>
      {(detail || detailLoading || detailError) && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 p-4" role="dialog" aria-modal="true" aria-label="Sector rank calculation">
          <div className="w-[80vw] max-h-[85vh] overflow-auto rounded-xl border border-neutral-700 bg-elevated p-5 shadow-2xl">
            <div className="flex items-start justify-between gap-4">
              <div>
                <h2 className="text-lg font-semibold text-fg-strong">{detail?.sector ?? detailLoading?.sector ?? 'Sector calculation'}</h2>
                <p className="mt-1 text-sm text-fg-muted">{detail ? `${formatDate(detail.date)} · rank ${detail.rank} · sector score ${detail.score?.toFixed(2) ?? '—'}` : detailLoading ? <span className="inline-flex items-center gap-2"><i className="h-3.5 w-3.5 animate-spin rounded-full border-2 border-fg-faint border-t-accent-400" aria-hidden="true" />Loading {formatDate(detailLoading.date)}…</span> : 'Could not load calculation.'}</p>
              </div>
              <button type="button" onClick={() => { setDetail(null); setDetailLoading(null); setDetailError(null); }} className="text-fg-muted hover:text-fg-strong" aria-label="Close calculation">Close</button>
            </div>
            {detailError && <p className="mt-5 text-sm text-neg-300">{detailError}</p>}
            {detail && <>
              <p className="mt-4 text-sm text-fg-muted">{detail.method}</p>
              <div className="mt-3 flex gap-4 text-xs text-fg-muted">
                {Object.entries(detail.category_scores).map(([category, score]) => <span key={category}>{category}: <strong className="text-fg-strong">{score?.toFixed(1) ?? '—'}</strong></span>)}
              </div>
              <div className="mt-5 overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="text-left text-xs text-fg-faint"><tr><SortableHeader label="Company" column="company" active={companySort} direction={companySortDirection} onSort={sortCompanies} /><th className="pb-2 pr-4"><span className="inline-flex items-center gap-1">Why<InfoTip text="Open the company’s complete score calculation, including raw signals, normalization, weights, category scores, and total-score blend."><span className={INFO_ICON}>i</span></InfoTip></span></th><SortableHeader label="Total score" column="score" active={companySort} direction={companySortDirection} onSort={sortCompanies} className="text-right" /><SortableHeader label="Price score" column="price_score" active={companySort} direction={companySortDirection} onSort={sortCompanies} className="text-right" /><SortableHeader label="Volume score" column="volume_score" active={companySort} direction={companySortDirection} onSort={sortCompanies} className="text-right" />{Object.entries(SIGNALS).map(([key, signal]) => <SortableHeader key={key} label={`${signal.label}${signal.unit === 'pct' ? ' (%)' : signal.unit === 'ratio' ? ' (×)' : ''}`} column={key as keyof typeof SIGNALS} active={companySort} direction={companySortDirection} onSort={sortCompanies} className="text-right" />)}</tr></thead>
                  <tbody>{sortedCompanies.map((company) => <Fragment key={company.analysis_id}><tr className="border-t border-neutral-800/50"><td className="py-2 pr-4 text-fg-strong">{company.ticker ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(company.ticker)}`} target="_blank" rel="noreferrer" className="hover:text-accent-300 hover:underline">{company.name ?? company.ticker} {company.name ? <span className="text-fg-faint">{company.ticker}</span> : null}</a> : company.name ?? `Asset ${company.analysis_id}`}</td><td className="py-2 pr-4"><button type="button" onClick={() => setExpandedCompanyId((current) => current === company.analysis_id ? null : company.analysis_id)} className="text-xs text-accent-300 hover:text-accent-200">{expandedCompanyId === company.analysis_id ? 'Hide' : 'Explain'}</button></td><td className="py-2 pr-4 text-right font-mono text-fg-muted">{company.score.toFixed(2)}</td><td className="py-2 pr-4 text-right font-mono text-fg-muted">{company.price_score?.toFixed(2) ?? '—'}</td><td className="py-2 pr-4 text-right font-mono text-fg-muted">{company.volume_score?.toFixed(2) ?? '—'}</td>{Object.entries(SIGNALS).map(([key, signal]) => <td key={key} className="py-2 pr-4 text-right font-mono text-fg-muted">{formatSignal(company.signals[key], signal.unit)}</td>)}</tr>{expandedCompanyId === company.analysis_id && <tr className="border-t border-neutral-800/50 bg-inset/30"><td colSpan={5 + Object.keys(SIGNALS).length} className="p-4"><CompanyCalculation company={company} categoryWeights={detail.category_weights} /></td></tr>}</Fragment>)}</tbody>
                </table>
              </div>
            </>}
          </div>
        </div>
      )}
    </div>
  );
}

function CompanyCalculation({ company, categoryWeights }: {
  company: Detail['companies'][number]; categoryWeights: Detail['category_weights'];
}) {
  const groups = ['price', 'volume'];
  return <div className="space-y-3 text-xs">
    <p className="text-fg-muted">Total score = {groups.map((group) => `${((categoryWeights[group] ?? 0) * 100).toFixed(0)}% × ${group} score`).join(' + ')} = <strong className="font-mono text-fg-strong">{company.score.toFixed(2)}</strong>.</p>
    <div className="grid gap-3 md:grid-cols-2">{groups.map((group) => {
      const score = group === 'price' ? company.price_score : company.volume_score;
      return <div key={group} className="rounded-md border border-neutral-800/60 p-3">
        <div className="mb-2 flex justify-between"><strong className="capitalize text-fg-strong">{group} score</strong><span className="font-mono text-accent-300">{score?.toFixed(2) ?? '—'} / 100</span></div>
        <table className="w-full"><thead className="text-fg-faint"><tr><th className="text-left font-medium">Signal</th><th className="text-right font-medium">Raw</th><th className="text-right font-medium">Norm.</th><th className="text-right font-medium">Weight</th></tr></thead><tbody>
          {Object.entries(SIGNALS).filter(([key]) => company.signal_details[key]?.category === group).map(([key, signal]) => {
            const item = company.signal_details[key];
            if (!item) return null;
            return <tr key={key} className="border-t border-neutral-800/30"><td className="py-1 text-fg-soft">{signal.label}</td><td className="py-1 text-right font-mono text-fg-muted"><span className="inline-flex items-center gap-1">{formatSignal(item.raw, signal.unit)}{item.raw != null && <InfoTip wide content={<RawCalculation signalKey={key} signal={signal} item={item} />}><span className={INFO_ICON}>i</span></InfoTip>}</span></td><td className="py-1 text-right font-mono text-fg-muted"><span className="inline-flex items-center gap-1">{item.normalized?.toFixed(1) ?? '—'}{item.normalized != null && <InfoTip wide content={<div className="space-y-3"><div className="space-y-1"><p className="font-medium text-fg-strong">Normalization calculation</p><p className="font-mono text-fg-muted">Norm. = {normalizationFormula(item, signal.unit)}</p></div><div className="grid grid-cols-2 gap-3 border-t border-neutral-800/60 pt-3"><div className="space-y-2"><p className="text-fg-faint">Universe low · 0.0</p><CompanyReferenceLink company={item.universe_min_company} /><p className="font-mono text-fg-muted">{formatSignal(item.universe_min, signal.unit)}</p><PriceReturnLegs legs={item.universe_min_price_legs} /><div className="space-y-1 border-t border-neutral-800/40 pt-2"><p className="text-fg-faint">How this value is calculated</p><ExplanationRows explanation={item.universe_min_raw_explanation} /></div></div><div className="space-y-2"><p className="text-fg-faint">Universe high · 100.0</p><CompanyReferenceLink company={item.universe_max_company} /><p className="font-mono text-fg-muted">{formatSignal(item.universe_max, signal.unit)}</p><PriceReturnLegs legs={item.universe_max_price_legs} /><div className="space-y-1 border-t border-neutral-800/40 pt-2"><p className="text-fg-faint">How this value is calculated</p><ExplanationRows explanation={item.universe_max_raw_explanation} /></div></div></div></div>}><span className={INFO_ICON}>i</span></InfoTip>}</span></td><td className="py-1 text-right font-mono text-fg-muted">{item.weight}</td></tr>;
          })}
        </tbody></table>
      </div>;
    })}</div>
  </div>;
}
