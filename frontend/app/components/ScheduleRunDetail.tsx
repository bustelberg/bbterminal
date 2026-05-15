'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import type { IngestRun } from './Schedule';
import { runToTimelineProps, PIPELINE_STEPS } from './Schedule';
import ProgressTimeline from './ProgressTimeline';
import MonthlyHoldingsTable from './momentum/MonthlyHoldingsTable';
import type { BacktestResult, Holding, PeriodRecord, Summary } from '../../lib/stores/momentum';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type MembershipRow = {
  company_id: number;
  ticker: string;
  company_name: string;
  exchange: string;
  sector: string | null;
};

type SnapshotResponse = {
  snapshot_id: number;
  as_of_date: string;
  latest_price_date: string | null;
  config: Record<string, unknown> | null;
  holdings: Holding[];
  daily_picks: unknown[];
};

/** Per-section collapsible card with consistent styling — keeps the
 * three subsections of a run row (ACWI / membership / momentum) visually
 * uniform without dragging in CollapsibleCard's full chrome. */
function Section({
  title,
  children,
  rightSlot,
  defaultOpen = true,
}: {
  title: string;
  children: React.ReactNode;
  rightSlot?: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <div className="bg-[#0f1117] border border-gray-800/60 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full px-4 py-2 flex items-center justify-between hover:bg-white/[0.02] transition-colors"
      >
        <span className="flex items-center gap-2">
          <span className="text-gray-500 font-mono text-xs">{open ? '▾' : '▸'}</span>
          <span className="text-sm font-medium text-gray-200">{title}</span>
        </span>
        {rightSlot}
      </button>
      {open && <div className="px-4 py-3 border-t border-gray-800/40">{children}</div>}
    </div>
  );
}

/** Lists additions / removals / renames in expandable triplets. Counts
 * stay visible on the section header; the actual rows live behind a
 * click so the page doesn't render hundreds of names eagerly. */
function AcwiDiffSection({ run }: { run: IngestRun }) {
  const summary = run.acwi_summary;
  if (!summary) {
    if (run.current_phase === 'acwi' && run.status === 'running') {
      return <div className="text-xs text-gray-500">ACWI phase running…</div>;
    }
    return <div className="text-xs text-gray-500">No ACWI data captured for this run.</div>;
  }
  return (
    <div className="space-y-3 text-xs">
      <div className="text-gray-400 font-mono">
        Diff: {summary.this_month} vs {summary.prev_month}
      </div>
      <div className="grid gap-3 md:grid-cols-3">
        <DiffList
          color="emerald"
          label="Additions"
          count={summary.additions_count}
          items={summary.additions.map((a) => ({
            company_id: a.company_id,
            primary: a.ticker,
            secondary: a.name,
            sector: a.sector,
          }))}
        />
        <DiffList
          color="rose"
          label="Removals"
          count={summary.removals_count}
          items={summary.removals.map((a) => ({
            company_id: a.company_id,
            primary: a.ticker,
            secondary: a.name,
            sector: a.sector,
          }))}
        />
        <DiffList
          color="amber"
          label="Renames"
          count={summary.renames_count}
          items={summary.renames.map((r) => ({
            company_id: r.company_id,
            primary: `${r.old_ticker} → ${r.new_ticker}`,
            secondary: r.name,
            sector: null,
          }))}
        />
      </div>
    </div>
  );
}

function DiffList({
  color,
  label,
  count,
  items,
}: {
  color: 'emerald' | 'rose' | 'amber';
  label: string;
  count: number;
  items: Array<{ company_id: number; primary: string; secondary: string | null; sector: string | null }>;
}) {
  const [open, setOpen] = useState(false);
  const colorMap = {
    emerald: 'text-emerald-300 bg-emerald-500/10 border-emerald-500/30',
    rose: 'text-rose-300 bg-rose-500/10 border-rose-500/30',
    amber: 'text-amber-300 bg-amber-500/10 border-amber-500/30',
  };
  return (
    <div className="bg-[#151821] rounded-lg border border-gray-800/40">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        disabled={count === 0}
        className={`w-full px-3 py-2 flex items-center justify-between disabled:cursor-default ${count > 0 ? 'hover:bg-white/[0.02] cursor-pointer' : ''}`}
      >
        <span className="flex items-center gap-2">
          <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${colorMap[color]}`}>
            {label}
          </span>
          <span className="text-gray-200 font-mono">{count}</span>
        </span>
        {count > 0 && <span className="text-gray-500 font-mono text-xs">{open ? '▾' : '▸'}</span>}
      </button>
      {open && items.length > 0 && (
        <div className="border-t border-gray-800/40 max-h-72 overflow-y-auto divide-y divide-gray-800/20">
          {items.map((it) => (
            <div key={it.company_id} className="px-3 py-1.5">
              <div className="font-mono text-xs text-gray-200">{it.primary}</div>
              <div className="text-[10px] text-gray-500 truncate">
                {it.secondary ?? '—'}
                {it.sector && <span className="text-gray-600"> · {it.sector}</span>}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/** Searchable membership viewer — fetches on first open, filters client-
 * side. Capped at 500 rows; ACWI typically has 2,500+ holdings but the
 * 500 cap is plenty for "find a specific name" use. */
function MembershipSection({ run }: { run: IngestRun }) {
  const [rows, setRows] = useState<MembershipRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [search, setSearch] = useState('');
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    if (loaded || loading) return;
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/api/ingest/runs/${run.run_id}/acwi-membership?limit=5000`);
      if (!r.ok) {
        if (r.status === 404) {
          setError('No ACWI universe captured for this run.');
        } else {
          setError(`Failed to load (${r.status})`);
        }
        return;
      }
      const data = (await r.json()) as MembershipRow[];
      setRows(data);
      setLoaded(true);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, [run.run_id, loaded, loading]);

  useEffect(() => {
    void load();
  }, [load]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return rows;
    return rows.filter(
      (r) =>
        r.ticker.toLowerCase().includes(q) ||
        r.company_name.toLowerCase().includes(q) ||
        r.exchange.toLowerCase().includes(q) ||
        (r.sector ?? '').toLowerCase().includes(q),
    );
  }, [rows, search]);

  if (run.acwi_universe_id == null) {
    return <div className="text-xs text-gray-500">No ACWI universe captured for this run.</div>;
  }

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-3 flex-wrap">
        <input
          type="search"
          placeholder="Search by ticker, name, exchange, sector…"
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="bg-[#151821] border border-gray-700 rounded-lg px-3 py-1.5 text-xs text-gray-200 placeholder-gray-500 focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 focus:outline-none flex-1 min-w-[200px]"
        />
        <span className="text-xs text-gray-500 font-mono">
          {loading ? 'loading…' : `${filtered.length} / ${rows.length}`}
        </span>
      </div>
      {error && (
        <div className="text-xs text-rose-300">{error}</div>
      )}
      {filtered.length > 0 && (
        <div className="max-h-80 overflow-auto border border-gray-800/40 rounded-lg">
          <table className="w-full text-xs">
            <thead className="text-gray-500 text-[10px] uppercase sticky top-0 bg-[#151821]">
              <tr className="border-b border-gray-800/40">
                <th className="text-left px-3 py-1.5 font-medium">Ticker</th>
                <th className="text-left px-3 py-1.5 font-medium">Name</th>
                <th className="text-left px-3 py-1.5 font-medium">Exchange</th>
                <th className="text-left px-3 py-1.5 font-medium">Sector</th>
              </tr>
            </thead>
            <tbody>
              {filtered.map((r) => (
                <tr key={r.company_id} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                  <td className="px-3 py-1.5 font-mono text-gray-200">{r.ticker || '—'}</td>
                  <td className="px-3 py-1.5 text-gray-300 truncate max-w-[280px]">{r.company_name || '—'}</td>
                  <td className="px-3 py-1.5 font-mono text-gray-500">{r.exchange || '—'}</td>
                  <td className="px-3 py-1.5 text-gray-400">{r.sector || '—'}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

/** Loads the momentum snapshot for this run and renders the holdings
 * via the existing /momentum table. Synthesizes a single-period
 * BacktestResult so MonthlyHoldingsTable can be used as-is without
 * modification. */
function MomentumHoldingsSection({ run }: { run: IngestRun }) {
  const [snapshot, setSnapshot] = useState<SnapshotResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const snapshotId = run.momentum_snapshot_id;

  useEffect(() => {
    if (snapshotId == null) return;
    setLoading(true);
    fetch(`${API_URL}/api/momentum/current-picks/${snapshotId}`)
      .then((r) => (r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`))))
      .then((data: SnapshotResponse) => {
        setSnapshot(data);
        setError(null);
      })
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false));
  }, [snapshotId]);

  // Build a synthetic single-period BacktestResult from the snapshot so
  // MonthlyHoldingsTable renders one collapsible row whose "Holdings"
  // expansion shows the current picks with the same columns / search /
  // timeline modal as /momentum.
  const { result, categories, scoringConfig } = useMemo(() => {
    if (!snapshot) {
      return {
        result: null as BacktestResult | null,
        categories: [] as string[],
        scoringConfig: null as { universe_label: string | null; index_universe: string | null; signal_weights: Record<string, number>; category_weights: Record<string, number> } | null,
      };
    }
    const cfg = (snapshot.config ?? {}) as Record<string, unknown>;
    const signalWeights = (cfg.signal_weights as Record<string, number>) ?? {};
    const categoryWeights = (cfg.category_weights as Record<string, number>) ?? {};
    const cats = Object.keys(categoryWeights);
    const fallbackCats = cats.length > 0 ? cats : ['price', 'volume'];

    const holdings = (snapshot.holdings ?? []) as Holding[];
    // Aggregate stats from the holdings — equal-weighted mean of
    // forward_return_pct for the period return.
    const validReturns = holdings
      .map((h) => h.forward_return_pct)
      .filter((v): v is number => v != null && Number.isFinite(v));
    const meanReturn = validReturns.length > 0
      ? validReturns.reduce((a, b) => a + b, 0) / validReturns.length
      : null;

    const period: PeriodRecord = {
      date: snapshot.as_of_date,
      holdings,
      portfolio_return_pct: meanReturn,
      cumulative_return_pct: meanReturn ?? 0,
      is_open: true,
      as_of_date: snapshot.latest_price_date ?? undefined,
    };
    const summary: Summary = {
      total_return_pct: meanReturn ?? 0,
      annualized_return_pct: 0,
      max_drawdown_pct: 0,
      sharpe_ratio: null,
      avg_monthly_turnover_pct: 0,
      total_months: 1,
      avg_holdings: holdings.length,
      top_drawdowns: [],
    };
    const res: BacktestResult = {
      monthly_records: [period],
      summary,
      daily_records: [],
    };
    return {
      result: res,
      categories: fallbackCats,
      scoringConfig: {
        universe_label: (cfg.universe_label as string | null) ?? null,
        index_universe: (cfg.index_universe as string | null) ?? null,
        signal_weights: signalWeights,
        category_weights: categoryWeights,
      },
    };
  }, [snapshot]);

  if (snapshotId == null) {
    const phase = run.current_phase;
    if (phase === 'momentum' && run.status === 'running') {
      return <div className="text-xs text-gray-500">Momentum phase running…</div>;
    }
    return (
      <div className="text-xs text-gray-500">
        No momentum snapshot for this run. Pick a strategy in the &quot;Scheduled strategy&quot; card above to enable the momentum phase.
      </div>
    );
  }
  if (loading) {
    return <div className="text-xs text-gray-500">Loading snapshot…</div>;
  }
  if (error) {
    return <div className="text-xs text-rose-300">Failed to load snapshot: {error}</div>;
  }
  if (!result || !scoringConfig) {
    return <div className="text-xs text-gray-500">No data.</div>;
  }
  return (
    <div className="space-y-2">
      {run.momentum_summary && (
        <div className="text-xs text-gray-400 font-mono">
          {run.momentum_summary.holdings_count} holdings · strategy: {run.momentum_summary.strategy_name ?? '—'} (#{run.momentum_summary.strategy_run_id ?? '—'}) · latest {run.momentum_summary.latest_price_date ?? '—'}
        </div>
      )}
      {/* MonthlyHoldingsTable is the same component /momentum uses — we
          pass a 1-period synthetic result, and the user gets the same
          per-holding expansion with the company-search box and the
          timeline modal on click. exchangeByCompany is empty for now
          (we don't have a per-snapshot exchange map yet) so the
          exchange column reads blank, but everything else works. */}
      <MonthlyHoldingsTable
        result={result}
        categories={categories}
        exchangeByCompany={new Map()}
        scoringConfig={scoringConfig}
      />
    </div>
  );
}

export default function ScheduleRunDetail({ run }: { run: IngestRun }) {
  // Derive ProgressTimeline props from the run's per-phase state — same
  // helper the row's inline bar uses, so the two stay perfectly in
  // sync. Re-derives each render so polling updates animate naturally.
  const tp = runToTimelineProps(run);
  return (
    <div className="px-5 py-3 bg-[#0b0d13] space-y-3 border-t border-gray-800/30">
      <ProgressTimeline
        title={`Pipeline run #${run.run_id}`}
        steps={PIPELINE_STEPS}
        state={tp.state}
        pct={tp.pct}
        running={tp.running}
        doneSummary={tp.doneSummary}
        errorMessage={tp.errorMessage}
        totalElapsedMs={tp.totalElapsedMs}
      />
      <Section title="ACWI universe diff">
        <AcwiDiffSection run={run} />
      </Section>
      <Section title={`ACWI membership (${run.acwi_target_month ?? '—'})`} defaultOpen={false}>
        <MembershipSection run={run} />
      </Section>
      <Section title="Momentum holdings" defaultOpen={true}>
        <MomentumHoldingsSection run={run} />
      </Section>
    </div>
  );
}
