'use client';

/**
 * The book's cumulative return through the year — 0% on 1 January, and what happened since.
 *
 *  It is AIRS's own `cumulatief_rendement`, READ AND NEVER DERIVED FROM THE VALUE SERIES. That
 * is the whole reason this chart can exist: AIRS's figure is FLOW-AWARE. AzTopSelectie goes from 0
 * to EUR 1,000,000 on 2026-06-30 because it was FUNDED that day, and this line stays at 0.00%
 * straight through it — measured. A curve computed from two of our own values would draw that
 * funding as a 100% gain in one session, and no ratio of two values can tell the two apart.
 *
 *  And it is the same column the scorecard beside it reads (`_airs_accounts._year_perf`), so the
 * chart's last point and the `Return` chip are one number by construction rather than by
 * coincidence. Deriving a second answer here would put two YTD figures in one row of one screen —
 * the failure the Analyse modal's benchmark tile already pays for once (see `benchmarkSourceNote`).
 *
 *  The zero is an anchor, not an observation. `cumulatief_rendement` restarts every January, so
 * the curve's origin is the opening of the first period AIRS published — pinned at exactly 0.0% by
 * the server, which reports its date as `return_from`. It is the one point on this line nobody
 * measured, and it is what makes every other point readable.
 *
 *  The hover is deliberately only date + the TWO returns: AIRS book and benchmark. Value and
 * holding-count diagnostics belong in the detailed tables; adding them here turns a quick
 * time-series read into a miniature ledger. The benchmark is sampled on these exact AIRS dates,
 * using the last market close on or before each date, so horizontal alignment has a real meaning.
 *
 *  The header is the return and nothing else (2026-09-01, on request): no value chip, no span
 * line. Both facts survive where a reader looks for them — the window on the x axis and in the ⓘ's
 * `when`, the value and the holding count in the tooltip — and this row is 24rem wide beside three
 * Scorecard chips, so every span in it is spent against the figure it is there to show.
 *
 *  It is not the drawdown panel's series and must not be read against it. That one rebuilds a
 * daily curve from the holdings as they stand today — look-ahead and survivorship included, as it
 * says — over years. This is what the book actually returned, on the dates AIRS has valued it.
 * Different objects, which is why this sits at the top of the modal rather than beside it.
 *
 *  Its own request. The Analyse modal is ONE payload with no partial paint, so its wall clock is
 * the reader's wait; a chart nobody has scrolled to yet does not belong in it. This fetches itself,
 * exactly as the Risk panels do.
 *
 *  And fetching itself is why it needs `refreshSeq` (2026-09-03, reported: the chip read +3.44%
 * and the chart +3.05%). The two ARE one column of one table — that part was never wrong — but the
 * modal's Refresh re-runs the AIRS scrape, which writes a NEW `airs_performance` row, and only the
 * modal's own payload was re-read afterwards. This effect depended on `portfolioId` alone, so the
 * chart kept the response it fetched when the modal opened: the chip moved to the row the scrape
 * had just written and the chart still held the one before it. "By construction" is a claim about
 * the COLUMN, and it says nothing about WHEN each side last read it.
 *
 *  It shares the scorecard's row, right of the excess tile, so its height is set against three
 * chips rather than against what a chart would like: 104px of plot, one line of chrome above and
 * one below. The caller fixes the WIDTH for the same reason — see the note at its call site.
 */
import { useEffect, useState } from 'react';
import {
  Area, AreaChart, CartesianGrid, Line, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis,
} from 'recharts';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { chartTheme } from '../../../lib/chartTheme';
import { AspectCard } from '../../../lib/tipCard';
import { v } from '../../../lib/dynamicValue';
import InfoTip from '../InfoTip';
import { traceError } from '../../../lib/debugTrace';
import type { BookValueSeries } from '../../../lib/types/api';
import allocationProfiles from '../../config/weightedBenchmarkAllocations.json';

type WeightedBenchmarkBlock = { bucket: string; weight_pct: number };

const weightedBenchmarkWeights = (blocks: readonly WeightedBenchmarkBlock[], variant?: string | null) => {
  // Policy benchmark weights are an explicit product promise, not a reflection
  // of whatever the book happens to hold today. The JSON is shipped with the
  // frontend, so changing the policy is a normal deploy with no DB migration.
  const configured = variant && variant in allocationProfiles
    ? allocationProfiles[variant as keyof typeof allocationProfiles]
    : null;
  if (configured) return configured;
  const names = new Set(['Stocks', 'Bonds', 'Alternatives', 'Cash']);
  const weights = Object.fromEntries(blocks
    .filter((block) => names.has(block.bucket) && Number.isFinite(block.weight_pct))
    .map((block) => [block.bucket, block.weight_pct]));
  return Object.keys(weights).length === 4 ? weights : null;
};

/** `2026-08-26` → a UTC timestamp.  UTC, not local: a date-only string parsed as local time
 *  shifts by an hour twice a year, which is enough to move a point across a tick. */
const ts = (d: string) => Date.parse(`${d}T00:00:00Z`);

/** True only for the literal last calendar day of a month — not the last observation we happen
 * to have in it. The return graph is a month-end report with one live point, not a sparse daily
 * history. */
const isCalendarMonthEnd = (date: string) => {
  const [year, month, day] = date.split('-').map(Number);
  return Number.isFinite(year) && Number.isFinite(month) && Number.isFinite(day)
    && day === new Date(Date.UTC(year, month, 0)).getUTCDate();
};

/** `26 Aug`, from a timestamp. */
const tick = (t: number) => new Date(t).toLocaleDateString('en-GB', {
  day: 'numeric', month: 'short', timeZone: 'UTC',
});

/**  ALWAYS SIGNED. On a curve pinned at zero the sign is the whole reading, and `2.4%` beside a
 *  line below the baseline is a contradiction the reader has to resolve by squinting. */
const pct = (v: number | null | undefined) =>
  (v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`);

const price = (value: number | null) => value == null ? '—' : value.toLocaleString('en-GB', {
  minimumFractionDigits: 2, maximumFractionDigits: 2,
});

function ReturnTooltip({ active, label, payload, benchmark }: {
  active?: boolean;
  label?: unknown;
  payload?: { value?: unknown; dataKey?: unknown }[];
  benchmark: string;
}) {
  if (!active || typeof label !== 'number') return null;
  const book = payload?.find((p) => (p.dataKey === 'positive' || p.dataKey === 'negative')
    && typeof p.value === 'number')?.value;
  const bench = payload?.find((p) => p.dataKey === 'benchmark'
    && typeof p.value === 'number')?.value;
  return (
    <div style={{ ...chartTheme.tooltipCard.contentStyle, padding: '7px 10px' }}>
      <p style={{ color: chartTheme.axisLabel, margin: 0 }}>
        {new Date(label).toLocaleDateString('en-GB', {
          day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
        })}
      </p>
      <p style={{ margin: '3px 0 0' }}>Portfolio: {pct(typeof book === 'number' ? book : null)}</p>
      {typeof bench === 'number' && (
        <p style={{ margin: '2px 0 0', color: chartTheme.compare }}>
          {benchmark}: {pct(bench)}
        </p>
      )}
    </div>
  );
}

/** A zero-crossing is added only to let the red and green paths meet. It is not an AIRS valuation,
 * so it must never be rendered as a date dot that looks selectable or measured. */
function ReturnDot({ cx, cy, fill, payload }: {
  cx?: number; cy?: number; fill?: string;
  payload?: { interpolated?: boolean };
}) {
  if (payload?.interpolated || cx == null || cy == null) return null;
  return <circle cx={cx} cy={cy} r={1.5} fill={fill} stroke={fill} />;
}

type MonthlyHolding = {
  isin: string; name: string; ticker: string | null; weight_pct: number;
  start_price: number | null; start_price_date: string | null;
  end_price: number | null; end_price_date: string | null;
  return_pct: number | null; vs_benchmark_pct: number | null;
};
type MonthlySortKey = 'portfolio' | 'name' | 'return_pct' | 'benchmark' | 'vs_benchmark_pct';
type ComparisonPortfolio = { portfolio_id: number; name: string; holdings: MonthlyHolding[] };
type ModelOption = { id: number; label: string };
type OverviewPortfolio = { fixed_portfolio_id: number | null; name: string };
type MonthlyDrilldown = {
  from_date: string; to_date: string; positions_date?: string | null; benchmark: string;
  benchmark_return_pct: number | null;
  benchmark_detail?: {
    ticker: string | null; start_price: number | null; start_price_date: string | null;
    end_price: number | null; end_price_date: string | null;
    components: { name: string; weight_pct: number; label: string | null; ticker: string | null;
      start_price: number | null; start_date: string | null; end_price: number | null; end_date: string | null; return_pct: number | null }[];
  };
  holdings: MonthlyHolding[]; comparison?: ComparisonPortfolio | null;
};

function MonthlyPerformance({ portfolioId, month, months, benchmark, weightsQuery, onSelect, onClose }: {
  portfolioId: number; month: string | null; months: { date: string; from: string }[]; benchmark: string; weightsQuery: string;
  onSelect: (month: string) => void; onClose: () => void;
}) {
  const [data, setData] = useState<MonthlyDrilldown | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [models, setModels] = useState<ModelOption[]>([]);
  const [comparisonId, setComparisonId] = useState('');
  const [sort, setSort] = useState<{ key: MonthlySortKey; descending: boolean }>({
    key: 'vs_benchmark_pct', descending: true,
  });
  useEffect(() => {
    if (!month) return;
    let alive = true;
    void apiFetch(`${API_URL}/api/airs/model-portfolios/${portfolioId}/monthly-yahoo-performance`
      + `?month=${encodeURIComponent(month)}&benchmark=${encodeURIComponent(benchmark)}${weightsQuery}`
      + (comparisonId ? `&compare_portfolio_id=${encodeURIComponent(comparisonId)}` : ''))
      .then(async (r) => ({ r, body: await r.json().catch(() => null) }))
      .then(({ r, body }) => {
        if (!alive) return;
        if (!r.ok) setError(body?.detail ?? `HTTP ${r.status}`);
        else { setError(null); setData(body as MonthlyDrilldown); }
      })
      .catch((e) => alive && setError(e instanceof Error ? e.message : String(e)));
    return () => { alive = false; };
  }, [portfolioId, month, benchmark, weightsQuery, comparisonId]);
  useEffect(() => {
    let alive = true;
    // Match the selector to the named strategies shown in the dashboard. The
    // raw model table includes FX/Dynamic twins and internal AIRS variants
    // which would produce duplicate choices here.
    void apiFetch(`${API_URL}/api/airs/portfolios/overview`).then(async (response) => {
      const body = await response.json().catch(() => []);
      if (alive && response.ok && Array.isArray(body)) {
        const options = new Map<number, ModelOption>();
        for (const row of body as OverviewPortfolio[]) {
          if (row.fixed_portfolio_id != null && row.name && !options.has(row.fixed_portfolio_id)) {
            options.set(row.fixed_portfolio_id, { id: row.fixed_portfolio_id, label: row.name });
          }
        }
        setModels([...options.values()].sort((a, b) => a.label.localeCompare(b.label)));
      }
    }).catch(() => undefined);
    return () => { alive = false; };
  }, []);
  // A browser can receive the updated frontend before the backend process is
  // restarted. Keep the drill-down usable for that older payload too.
  const benchmarkDetail = data?.benchmark_detail ?? {
    ticker: null, start_price: null, start_price_date: null,
    end_price: null, end_price_date: null, components: [],
  };
  const toggleSort = (key: MonthlySortKey) => setSort((current) =>
    current.key === key ? { key, descending: !current.descending } : { key, descending: key !== 'name' });
  // Keep the table correct while a browser is still receiving a response from
  // a backend that predates its ticker-level de-duplication.
  const uniqueHoldings = data ? [...new Map(data.holdings.map((holding) => [holding.ticker || holding.isin, holding])).values()] : [];
  const currentModel = models.find((model) => model.id === portfolioId);
  const currentName = currentModel?.label || 'This portfolio';
  const comparisonName = data?.comparison && (models.find((model) => model.id === data.comparison?.portfolio_id)?.label || data.comparison.name);
  const compareHoldings = data?.comparison ? [...new Map(data.comparison.holdings.map((holding) => [holding.ticker || holding.isin, holding])).values()] : [];
  const compareKeys = new Set(compareHoldings.map((holding) => holding.ticker || holding.isin));
  const currentKeys = new Set(uniqueHoldings.map((holding) => holding.ticker || holding.isin));
  const displayHoldings = data?.comparison ? [
    ...uniqueHoldings.filter((holding) => !compareKeys.has(holding.ticker || holding.isin)).map((holding) => ({ ...holding, portfolio: currentName })),
    ...compareHoldings.filter((holding) => !currentKeys.has(holding.ticker || holding.isin)).map((holding) => ({ ...holding, portfolio: comparisonName || 'Comparison portfolio' })),
  ] : uniqueHoldings.map((holding) => ({ ...holding, portfolio: currentName }));
  const sortedHoldings = data ? displayHoldings.sort((left, right) => {
    const a = sort.key === 'benchmark' ? data.benchmark_return_pct : left[sort.key];
    const b = sort.key === 'benchmark' ? data.benchmark_return_pct : right[sort.key];
    if (a == null) return b == null ? 0 : 1;
    if (b == null) return -1;
    const compared = typeof a === 'string' && typeof b === 'string' ? a.localeCompare(b) : Number(a) - Number(b);
    return sort.descending ? -compared : compared;
  }) : [];
  const SortHeader = ({ label, sortKey, align = 'right' }: { label: string; sortKey: MonthlySortKey; align?: 'left' | 'right' }) => <th className={align === 'left' ? 'px-3 py-2 text-left text-xs font-semibold' : 'px-2 py-2 text-right text-xs font-semibold'}>
    <button type="button" onClick={() => toggleSort(sortKey)} className="inline-flex items-center gap-1 hover:text-fg">
      {label}<span aria-hidden="true" className={sort.key === sortKey ? 'text-fg' : 'text-fg-faint'}>{sort.key === sortKey ? (sort.descending ? '↓' : '↑') : '↕'}</span>
    </button>
  </th>;
  const content = error ? <p className="p-4 text-[12px] text-neg-300">{error}</p>
    : !data ? <p className="p-4 text-[12px] text-fg-subtle">Loading Yahoo monthly performance…</p>
    : <div className="overflow-x-auto">
    {data.comparison && <p className="px-3 pt-3 text-xs text-fg-muted">Only holdings not shared by {currentName} and {comparisonName}.</p>}
    <table className="w-full min-w-[600px] text-[11px]">
      <thead className="text-fg-faint"><tr className="border-b border-neutral-800/50">
        {data.comparison && <SortHeader label="Portfolio" sortKey="portfolio" align="left" />}
        <SortHeader label="Holding" sortKey="name" align="left" />
        <SortHeader label="Yahoo return" sortKey="return_pct" /><SortHeader label="Benchmark" sortKey="benchmark" />
        <SortHeader label="vs benchmark" sortKey="vs_benchmark_pct" />
      </tr></thead>
      <tbody>{sortedHoldings.map((h) => <tr key={h.isin} className="border-b border-neutral-800/30 last:border-0">
        {data.comparison && <td className="px-3 py-1.5 text-fg-muted">{h.portfolio}</td>}
        <td className="px-3 py-1.5 text-fg"><span>{h.name}</span>{h.ticker && <span className="ml-1.5 font-mono text-fg-faint">{h.ticker}</span>}</td>
        <td className={`px-2 py-1.5 text-right font-mono ${h.return_pct != null && h.return_pct >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>
          <span className="inline-flex items-center justify-end gap-1">{pct(h.return_pct)}
            {h.ticker && h.start_price != null && h.end_price != null && <InfoTip wide content={<AspectCard
              what={`Yahoo return: ${pct(h.return_pct)}`}
              where={<a href={`https://finance.yahoo.com/quote/${encodeURIComponent(h.ticker)}/`} target="_blank" rel="noreferrer"
                className="text-accent-300 underline underline-offset-2 hover:text-accent-200">Yahoo Finance · {h.ticker} ↗</a>}
              when={<span>Start: {price(h.start_price)} on {h.start_price_date ?? '—'}<br />End: {price(h.end_price)} on {h.end_price_date ?? '—'}</span>}
              how="(end close ÷ start close − 1) × 100. Yahoo daily closes in the listing’s local currency." />}/>
            }
          </span>
        </td>
        <td className="px-2 py-1.5 text-right font-mono" style={{ color: chartTheme.compare }}>
          <span className="inline-flex items-center justify-end gap-1">{pct(data.benchmark_return_pct)}
            <InfoTip wide content={<AspectCard
              what={`Benchmark return: ${pct(data.benchmark_return_pct)}`}
              where={benchmarkDetail.ticker ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(benchmarkDetail.ticker)}/`} target="_blank" rel="noreferrer"
                className="text-accent-300 underline underline-offset-2 hover:text-accent-200">Yahoo Finance · {benchmarkDetail.ticker} ↗</a>
                : <span>Weighted Yahoo ETF basket</span>}
              when={benchmarkDetail.ticker ? <span>Start: {price(benchmarkDetail.start_price)} on {benchmarkDetail.start_price_date ?? '—'}<br />End: {price(benchmarkDetail.end_price)} on {benchmarkDetail.end_price_date ?? '—'}</span>
                : benchmarkDetail.components.length ? <span>{benchmarkDetail.components.map((component) => <span key={component.name} className="block">{component.weight_pct.toFixed(0)}% · {component.ticker ? <a href={`https://finance.yahoo.com/quote/${encodeURIComponent(component.ticker)}/`} target="_blank" rel="noreferrer" className="text-accent-300 underline underline-offset-2 hover:text-accent-200">{component.ticker} ↗</a> : component.label} · {price(component.start_price)} ({component.start_date ?? '—'}) → {price(component.end_price)} ({component.end_date ?? '—'})</span>)}</span>
                  : <span>Benchmark price provenance is available after the backend refreshes.</span>}
              how={benchmarkDetail.ticker
                ? 'Benchmark proxy return based on its Yahoo daily closes; FX is converted to EUR when applicable.'
                : 'Weighted sum of the Yahoo ETF proxy returns shown above; cash contributes 0%.'} />}/>
          </span>
        </td>
        <td className={`px-3 py-1.5 text-right font-mono font-medium ${h.vs_benchmark_pct != null && h.vs_benchmark_pct >= 0 ? 'text-pos-400' : 'text-neg-400'}`}>{pct(h.vs_benchmark_pct)}</td>
      </tr>)}</tbody>
    </table>
  </div>;
  return <div role="dialog" aria-modal="true" aria-label="Monthly Yahoo performance"
    className="fixed inset-0 z-[90] flex items-center justify-center bg-black/70 p-4" onMouseDown={onClose}>
    <div className="max-h-[85vh] w-full max-w-5xl overflow-auto rounded-xl border border-neutral-700 bg-card shadow-2xl"
      onMouseDown={(event) => event.stopPropagation()}>
      <div className="flex items-center justify-between border-b border-neutral-800 px-4 py-3">
        {month ? <div className="flex items-center gap-4"><button type="button" onClick={() => onSelect('')}
          className="inline-flex items-center gap-1.5 text-sm text-fg-muted hover:text-fg"><svg aria-hidden="true" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.8" className="h-4 w-4"><path d="M11.5 4.5 6 10l5.5 5.5M6.5 10H16" strokeLinecap="round" strokeLinejoin="round" /></svg>Months</button><h2 className="text-lg font-semibold text-fg">{new Date(`${month}T00:00:00Z`).toLocaleDateString('en-GB', {
            month: 'long', year: 'numeric', timeZone: 'UTC',
          })}</h2></div> : <span />}
        <button type="button" onClick={onClose}
        className="rounded px-2 py-1 text-fg-muted hover:bg-white/10 hover:text-fg" aria-label="Close">×</button></div>
      {!month ? <><div className="px-5 pt-2"><label className="block text-xs font-medium text-fg-muted" htmlFor="monthly-portfolio-compare">Compare with</label><select id="monthly-portfolio-compare" value={comparisonId} onChange={(event) => setComparisonId(event.target.value)}
        className="mt-1 w-full rounded-lg border border-neutral-700 bg-black/20 px-3 py-2 text-sm text-fg"><option value="">No comparison</option>{models.filter((model) => model.id !== portfolioId).map((model) => <option key={model.id} value={model.id}>{model.label}</option>)}</select></div><div className="grid grid-cols-2 gap-3 p-5 sm:grid-cols-3">
        {months.map(({ date }) => <button key={date} type="button" onClick={() => onSelect(date)}
          className="rounded-xl border border-neutral-700 bg-black/10 px-4 py-6 text-center transition hover:border-accent-400 hover:bg-accent-500/10">
          <span className="block text-sm font-medium text-fg">{new Date(`${date}T00:00:00Z`).toLocaleDateString('en-GB', { month: 'long', timeZone: 'UTC' })}</span>
          <span className="mt-1 block text-[11px] text-fg-faint">{new Date(`${date}T00:00:00Z`).getUTCFullYear()}</span>
        </button>)}
      </div></> : content}
    </div>
  </div>;
}

export default function BookReturnChart(
  {
    portfolioId,
    /**
     * Bumped by the modal's caller when a refresh finishes — the SAME counter the modal's own
     * payload effect takes.
     *
     *  A real dependency, not defensive padding. See the note at the top of this file: without it
     * this chart is the one surface in the block that never re-reads what the scrape just wrote,
     * and it disagrees with the chip above it by exactly one AIRS row.
     */
    refreshSeq = 0,
    benchmark,
    benchmarkBlocks = [],
    variant,
  }: { portfolioId: number; refreshSeq?: number; benchmark: string; benchmarkBlocks?: WeightedBenchmarkBlock[]; variant?: string | null },
) {
  const [data, setData] = useState<BookValueSeries | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [monthlyModalOpen, setMonthlyModalOpen] = useState(false);
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
  const weights = weightedBenchmarkWeights(benchmarkBlocks, variant);
  const weightsQuery = weights ? `&benchmark_weights=${encodeURIComponent(JSON.stringify(weights))}` : '';

  useEffect(() => {
    let alive = true;
    void (async () => {
      setData(null); setErr(null);
      setSelectedMonth(null);
      try {
        const r = await apiFetch(
          `${API_URL}/api/airs/model-portfolios/${portfolioId}/value-series`
          + `?benchmark=${encodeURIComponent(benchmark)}${weightsQuery}`);
        const b = await r.json().catch(() => null);
        if (!alive) return;
        if (!r.ok) { setErr(b?.detail ?? `HTTP ${r.status}`); return; }
        setData(b as BookValueSeries);
      } catch (e) {
        //  The full diagnostic goes to the console, one short line to the reader.
        traceError('analyse', 'value series', e);
        if (alive) setErr(e instanceof Error ? e.message : String(e));
      }
    })();
    return () => { alive = false; };
  }, [portfolioId, refreshSeq, benchmark, weightsQuery]);

  // Do not trust storage order for a charting rule. The server normally returns periods in date
  // order, but the latest observation must mean the latest DATE, never merely the final array
  // element (which could otherwise put a backfilled mid-month row back on the graph).
  const all = [...(data?.returns ?? [])].sort((a, b) => a.date.localeCompare(b.date));
  if (err) return <p className="text-[12px] text-neg-300">{err}</p>;
  if (!data) return <p className="text-[12px] text-fg-subtle">Loading the book’s return…</p>;
  //  The anchor alone is not a line. The server refuses to emit a lone pinned zero for exactly
  // that reason, so an empty series here means AIRS has published no return for this book yet —
  // or there is no book at all, which `reason` says.
  if (all.length < 2) {
    return (
      <p className="text-[12px] text-fg-faint">
        {data.reason ?? 'No return published for this book yet.'}
      </p>
    );
  }

  const last = all[all.length - 1];
  // Complete calendar month-ends, plus the single newest observation for the live month. This is
  // intentionally a complete display rule: no inception anchor, scrape-date or cash-flow marker
  // may add a visual point between month-ends.
  const points = all.filter((p) => isCalendarMonthEnd(p.date));
  if (points.at(-1)?.date !== last.date) points.push(last);
  const first = points[0] ?? last;
  const benchmarkByDate = new Map(
    (data.benchmark_returns ?? []).map((p) => [p.date, p.cum_pct] as const));
  const rows = points.map((p) => ({ ...p, t: ts(p.date),
    benchmark: benchmarkByDate.get(p.date) ?? null }));
  const up = (data.return_pct ?? 0) >= 0;
  /** Two series let Recharts colour each part of the return path by its own sign. A crossing gets
   * an interpolated 0% point in BOTH series: without it, the green/red segments stop at their last
   * sampled points and leave a visible gap exactly where the chart changes meaning. */
  const colouredRows = rows.reduce<Array<(typeof rows)[number] & {
    positive: number | null; negative: number | null; interpolated: boolean;
  }>>(
    (out, row, i) => {
      const previous = rows[i - 1];
      if (previous && ((previous.cum_pct < 0 && row.cum_pct > 0)
        || (previous.cum_pct > 0 && row.cum_pct < 0))) {
        const share = -previous.cum_pct / (row.cum_pct - previous.cum_pct);
        const crossingBenchmark = previous.benchmark != null && row.benchmark != null
          ? previous.benchmark + (row.benchmark - previous.benchmark) * share : null;
        out.push({ ...row, t: previous.t + (row.t - previous.t) * share,
          cum_pct: 0, benchmark: crossingBenchmark,
          positive: 0, negative: 0, interpolated: true });
      }
      out.push({ ...row,
        positive: row.cum_pct >= 0 ? row.cum_pct : null,
        negative: row.cum_pct <= 0 ? row.cum_pct : null, interpolated: false });
      return out;
    }, []);

  return (
    <div className="rounded-xl border border-neutral-800/40 bg-card p-3">
      <div className="flex items-baseline gap-2 flex-wrap mb-1">
        <span className="text-[11px] uppercase tracking-wider text-fg-faint">Return YTD</span>
        <span className={`font-mono ${up ? 'text-pos-400' : 'text-neg-400'}`}>
          {pct(data.return_pct)}
        </span>
        {data.benchmark_return_pct != null && (
          <span className="font-mono text-[12px]" style={{ color: chartTheme.compare }}>
            {data.benchmark ?? benchmark} {pct(data.benchmark_return_pct)}
          </span>
        )}
        {/*  NO VALUE CHIP AND NO SPAN LINE — removed on request (2026-09-01), and NEITHER FACT
            LEFT THE COMPONENT. The window is the ⓘ's `when` and it is written along the x axis;
            the book's value and that date's holding count are on every hover. What the header
            carries now is the one figure this chart exists to state, and a chip the reader has to
            step over to reach it is a cost with no reader. */}
        {/*  NO `how` AND NO `worked` (2026-09-03, on request: "it's simply copied straight from
            AIRS, no calculation needed"). This figure is READ — `cumulatief_rendement`, one column,
            one row — so a chained-product formula under it described an arithmetic nobody here
            performs, and the house rule already said so: no worked line over raw data. What is
            left is what / where / when, which is the whole of the Active Share shape when there is
            no maths to typeset.
             AND EVERY LIVE FIGURE IS BADGED (`v()`). The dates were interpolated bare, so a
            reader scanning the card could not tell this book's window from the sentence around it
            — which is the one question badging exists to answer, and the only reason the card
            carries them. The return itself is not repeated here: it is set in the header above. */}
        <InfoTip className="ml-auto" content={<AspectCard
          what="What the book has returned so far this year, from 0% at the start of it."
          where={`AIRS's own Rendementen sheet — the same figure as the Return tile beside this, `
            + `over ${v(all.length - 1)} published points.`}
          when={`${v(data.return_from ?? first.date)} to ${v(last.date)}.`} />} />
      </div>
      <div className="mb-11">
      <ResponsiveContainer width="100%" height={104}>
        <AreaChart data={colouredRows} margin={{ top: 4, right: 8, bottom: 0, left: 0 }}
          onClick={(state: { activeLabel?: unknown }) => {
            const point = colouredRows.find((row) => row.t === state.activeLabel && !row.interpolated);
            if (point && point.date !== first.date) { setSelectedMonth(null); setMonthlyModalOpen(true); }
          }} style={{ cursor: 'pointer' }}>
          <CartesianGrid strokeDasharray="3 3" stroke={chartTheme.gridEarnings} />
          {/*  A TIME AXIS, NOT A CATEGORY ONE, AND THE DIFFERENCE IS THE WHOLE SHAPE OF THE
              LINE. The points are irregular — AIRS publishes a month-end for each closed month and
              then a row per day the scrape ran — so on a categorical axis a five-month stretch of
              month-ends occupies as much of the chart as a fortnight of daily points, and a
              six-week gap is drawn the same width as an overnight one. Plotted against real time,
              distance means elapsed time everywhere. */}
          <XAxis dataKey="t" type="number" scale="time" domain={['dataMin', 'dataMax']}
            tickFormatter={tick} minTickGap={28}
            tick={{ fontSize: 11, fill: chartTheme.axisTick }} />
          {/*  ZERO IS ALWAYS IN THE DOMAIN, unlike the value chart this replaced. There the
              baseline was meaningless and an auto domain was the only way to see the shape; here
              zero is the reading — a curve floating entirely above or below a baseline that is off
              the plot says nothing about whether the book is up. */}
          <YAxis domain={[(min: number) => Math.min(0, min), (max: number) => Math.max(0, max)]}
            width={44} tick={{ fontSize: 11, fill: chartTheme.axisTick }}
            tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
          <Tooltip content={<ReturnTooltip benchmark={data.benchmark ?? benchmark} />}
            position={{ x: 8, y: 108 }} allowEscapeViewBox={{ y: true }}
            wrapperStyle={{ pointerEvents: 'none', zIndex: 10 }} />
          {/*  THE BASELINE IS DRAWN, not just included in the domain. "Start at 0%" is the whole
              claim of this chart, and a gridline the reader has to identify is not the same as a
              rule they can see the line cross. */}
          <ReferenceLine y={0} stroke={chartTheme.axisTick} strokeOpacity={0.55} />
          <Area dataKey="positive" type="monotone" stroke={chartTheme.pos} strokeWidth={2}
            fill={chartTheme.pos} fillOpacity={0.08}
            dot={<ReturnDot fill={chartTheme.pos} />} />
          <Area dataKey="negative" type="monotone" stroke={chartTheme.neg} strokeWidth={2}
            fill={chartTheme.neg} fillOpacity={0.08}
            dot={<ReturnDot fill={chartTheme.neg} />} />
          <Line dataKey="benchmark" type="monotone" stroke={chartTheme.compare} strokeWidth={1.8}
            strokeDasharray="5 3" dot={false} connectNulls={false} />
        </AreaChart>
      </ResponsiveContainer>
      </div>
      <p className="px-0.5 pt-1 text-[10px] text-fg-faint">Click a month to compare every holding&apos;s Yahoo return with the benchmark.</p>
      {monthlyModalOpen && <MonthlyPerformance portfolioId={portfolioId} month={selectedMonth}
        months={points.map((point, index) => ({ date: point.date, from: points[index - 1]?.date ?? '' })).slice(1).slice(-6)}
        benchmark={benchmark} weightsQuery={weightsQuery} onSelect={setSelectedMonth}
        onClose={() => { setMonthlyModalOpen(false); setSelectedMonth(null); }} />}
    </div>
  );
}
