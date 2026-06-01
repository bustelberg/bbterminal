'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import type { IngestRun, MomentumStrategyResult, TemplateDiff } from './Schedule';
import { runToTimelineProps, PIPELINE_STEPS } from './Schedule';
import ProgressTimeline from './ProgressTimeline';
import SnapshotHoldings from './SnapshotHoldings';
import type { Column } from '../../lib/tableExport';
import TableDownloadButton from './TableDownloadButton';

import { API_URL } from '../../lib/apiUrl';

type MembershipRow = {
  company_id: number;
  ticker: string;
  company_name: string;
  exchange: string;
  sector: string | null;
};

/** Collapsible card matching the rest of the run-detail subsections. */
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

/** Diff + searchable membership viewer for one template entry in this
 * run's `templates_summary`. Each template that ran in this pipeline
 * tick gets its own card so multi-template runs (when SP500 lands) are
 * visually unambiguous. */
function TemplateRunSection({ run, t }: { run: IngestRun; t: TemplateDiff }) {
  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden">
      <div className="px-3 py-2 flex items-center gap-3 flex-wrap border-b border-gray-800/40">
        <span className="text-sm font-medium text-gray-200">{t.template_key}</span>
        {t.error ? (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-rose-500/10 text-rose-300 border-rose-500/30">
            error
          </span>
        ) : (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-emerald-500/15 text-emerald-300 border-emerald-500/30">
            ok
          </span>
        )}
        <span className="text-xs text-gray-500 font-mono">
          {t.this_month ?? '—'}{t.prev_month && <span className="text-gray-600"> vs {t.prev_month}</span>}
        </span>
        <span className="text-xs text-gray-400 font-mono ml-auto">
          +{t.additions_count} / −{t.removals_count}{t.renames_count > 0 && <span> / r{t.renames_count}</span>}
        </span>
      </div>
      <div className="px-3 py-3 space-y-3">
        {t.error && (
          <div className="text-xs text-rose-300 font-mono whitespace-pre-wrap">{t.error}</div>
        )}
        {!t.error && (
          <div className="grid gap-3 md:grid-cols-3">
            <DiffList
              color="emerald"
              label="Additions"
              count={t.additions_count}
              items={t.additions.map((a) => ({ company_id: a.company_id, primary: a.ticker, secondary: a.name, sector: a.sector }))}
            />
            <DiffList
              color="rose"
              label="Removals"
              count={t.removals_count}
              items={t.removals.map((a) => ({ company_id: a.company_id, primary: a.ticker, secondary: a.name, sector: a.sector }))}
            />
            <DiffList
              color="amber"
              label="Renames"
              count={t.renames_count}
              items={t.renames.map((r) => ({ company_id: r.company_id, primary: `${r.old_ticker} → ${r.new_ticker}`, secondary: r.name, sector: null }))}
            />
          </div>
        )}
        {!t.error && (t.unresolved_additions?.length ?? 0) > 0 && (
          <UnresolvedAdditionsSection items={t.unresolved_additions ?? []} />
        )}
        {!t.error && t.universe_id != null && t.this_month && (
          <TemplateMembership runId={run.run_id} templateKey={t.template_key} targetMonth={t.this_month} />
        )}
      </div>
    </div>
  );
}

/** Post-XLS MSCI additions the pipeline couldn't verify on GuruFocus.
 * Surfaced prominently in amber so the user notices on the next
 * /schedule visit and can add a manual override (typically: extend
 * `_ISHARES_TO_GF` / `_EXCHCODE_MAP`, or wait for the next iShares XLS
 * commit which usually fills the gap). */
function UnresolvedAdditionsSection({
  items,
}: {
  items: NonNullable<TemplateDiff['unresolved_additions']>;
}) {
  const [open, setOpen] = useState(true);
  return (
    <div className="bg-amber-500/[0.06] border border-amber-500/30 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full px-3 py-2 flex items-center justify-between hover:bg-amber-500/[0.04] cursor-pointer"
      >
        <span className="flex items-center gap-2">
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border text-amber-300 bg-amber-500/10 border-amber-500/40">
            Unresolved post-XLS additions
          </span>
          <span className="text-amber-200 font-mono">{items.length}</span>
          <span className="text-[11px] text-amber-200/70">
            need a manual GuruFocus link before the next pipeline tick can include them
          </span>
        </span>
        <span className="text-amber-300 font-mono text-xs">{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <div className="border-t border-amber-500/30 max-h-96 overflow-y-auto divide-y divide-amber-500/15">
          {items.map((u, idx) => (
            <div key={`${u.msci_href ?? u.name}-${idx}`} className="px-3 py-2 text-xs space-y-1">
              <div className="flex items-baseline gap-2 flex-wrap">
                <span className="font-mono text-gray-200">{u.name}</span>
                <span className="text-[10px] uppercase tracking-wider px-1 py-0.5 rounded border border-gray-700 text-gray-400">
                  {u.country}
                </span>
                {u.eff_date && (
                  <span className="text-[10px] text-gray-500 font-mono">eff {u.eff_date}</span>
                )}
                <span className="text-[10px] uppercase tracking-wider px-1 py-0.5 rounded border border-rose-500/30 text-rose-300 bg-rose-500/10 ml-auto">
                  {u.reason}
                </span>
              </div>
              {u.openfigi_candidate && (
                <div className="text-[11px] text-gray-400 font-mono">
                  OpenFIGI says: {u.openfigi_candidate.exch_code ?? '?'}:{u.openfigi_candidate.ticker ?? '?'}
                  {u.openfigi_candidate.name && (
                    <span className="text-gray-500"> ({u.openfigi_candidate.name})</span>
                  )}
                </div>
              )}
              {u.gf_url && (
                <a
                  href={u.gf_url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-[11px] text-amber-300 hover:text-amber-200 underline font-mono break-all inline-block"
                >
                  {u.gf_url}
                </a>
              )}
              {u.detail && (
                <div className="text-[10px] text-gray-500 italic">{u.detail}</div>
              )}
            </div>
          ))}
        </div>
      )}
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
    <div className="bg-[#0f1117] rounded-lg border border-gray-800/40">
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

/** Lazy-loaded searchable membership table for one template in this run.
 * Fetches `/api/ingest/runs/{run_id}/templates/{template_key}/membership`.
 * Capped at 5000 rows (ACWI typically has ~2,500-3,000). */
function TemplateMembership({
  runId, templateKey, targetMonth,
}: { runId: number; templateKey: string; targetMonth: string }) {
  const [rows, setRows] = useState<MembershipRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const [search, setSearch] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);

  const load = useCallback(async () => {
    if (loaded || loading) return;
    setLoading(true);
    try {
      const r = await fetch(`${API_URL}/api/ingest/runs/${runId}/templates/${encodeURIComponent(templateKey)}/membership?limit=5000`);
      if (!r.ok) {
        setError(`Failed to load (${r.status})`);
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
  }, [runId, templateKey, loaded, loading]);

  useEffect(() => {
    if (open && !loaded) void load();
  }, [open, loaded, load]);

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

  const membershipExportColumns = useMemo<Column<MembershipRow>[]>(() => [
    { key: 'ticker', header: 'Ticker', accessor: (r) => r.ticker },
    { key: 'company_name', header: 'Name', accessor: (r) => r.company_name },
    { key: 'exchange', header: 'Exchange', accessor: (r) => r.exchange },
    { key: 'sector', header: 'Sector', accessor: (r) => r.sector ?? '' },
    { key: 'company_id', header: 'Company ID', accessor: (r) => r.company_id },
  ], []);

  return (
    <div className="border border-gray-800/40 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full px-3 py-2 flex items-center justify-between text-left hover:bg-white/[0.02] transition-colors"
      >
        <span className="flex items-center gap-2">
          <span className="text-gray-500 font-mono text-xs">{open ? '▾' : '▸'}</span>
          <span className="text-sm text-gray-200">Membership ({targetMonth})</span>
        </span>
        <span className="text-xs text-gray-500 font-mono">{loaded ? `${rows.length} holdings` : 'click to load'}</span>
      </button>
      {open && (
        <div className="px-3 py-2 border-t border-gray-800/40 space-y-2">
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
            <TableDownloadButton
              rows={filtered}
              columns={membershipExportColumns}
              filename={`run_${runId}_${templateKey}_${targetMonth}`}
              title={`Download ${filtered.length} holdings as CSV / XLSX`}
            />
          </div>
          {error && <div className="text-xs text-rose-300">{error}</div>}
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
      )}
    </div>
  );
}

function MomentumStrategyEntry({ result }: { result: MomentumStrategyResult }) {
  const [open, setOpen] = useState(false);
  const [tbOpen, setTbOpen] = useState(false);
  const [cfgOpen, setCfgOpen] = useState(false);
  const isError = result.status === 'error';
  const statusCls = isError
    ? 'bg-rose-500/10 text-rose-300 border-rose-500/30'
    : 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30';
  // Always allow expansion — even on a hard error there's debug info to
  // surface (error message, traceback, the config that ran).
  return (
    <div className="bg-[#151821] border border-gray-800/40 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full px-3 py-2 flex items-center gap-3 hover:bg-white/[0.02] transition-colors"
      >
        <span className="text-gray-500 font-mono text-xs w-4">{open ? '▾' : '▸'}</span>
        <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${statusCls}`}>
          {result.status}
        </span>
        <span className="text-sm text-gray-200 font-medium truncate">{result.strategy_name}</span>
        {result.frequency && (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-indigo-500/10 text-indigo-300 border-indigo-500/30">
            {result.frequency}
          </span>
        )}
        <span className="text-xs text-gray-500 font-mono ml-auto">
          {result.holdings_count > 0 && `${result.holdings_count} holdings · `}
          {result.latest_price_date && `as of ${result.latest_price_date}`}
        </span>
      </button>
      {open && (
        <div className="border-t border-gray-800/40 bg-[#0f1117] divide-y divide-gray-800/30">
          {isError && (
            <div className="px-3 py-3 space-y-2">
              <div className="text-xs text-rose-300 font-mono whitespace-pre-wrap">
                {result.error_message ?? 'Unknown error'}
              </div>
              {result.error_traceback && (
                <div>
                  <button
                    type="button"
                    onClick={() => setTbOpen((v) => !v)}
                    className="text-[10px] uppercase tracking-wider text-gray-500 hover:text-gray-300 transition-colors"
                  >
                    {tbOpen ? '▾' : '▸'} Full traceback
                  </button>
                  {tbOpen && (
                    <pre className="mt-2 bg-[#0b0d13] border border-gray-800/60 rounded-lg px-3 py-2 text-rose-200 text-[11px] font-mono whitespace-pre-wrap overflow-x-auto max-h-72 overflow-y-auto">
                      {result.error_traceback}
                    </pre>
                  )}
                </div>
              )}
            </div>
          )}
          {!isError && result.snapshot_id != null && (
            <div className="px-3 py-3">
              <SnapshotHoldings snapshotId={result.snapshot_id} />
            </div>
          )}
          {!isError && result.snapshot_id == null && (
            <div className="px-3 py-3 text-xs text-gray-500">No snapshot saved.</div>
          )}
          {result.config && Object.keys(result.config).length > 0 && (
            <div className="px-3 py-3">
              <button
                type="button"
                onClick={() => setCfgOpen((v) => !v)}
                className="text-[10px] uppercase tracking-wider text-gray-500 hover:text-gray-300 transition-colors"
              >
                {cfgOpen ? '▾' : '▸'} Raw stored config
              </button>
              {cfgOpen && (
                <pre className="mt-2 bg-[#0b0d13] border border-gray-800/60 rounded-lg px-3 py-2 text-gray-300 text-[11px] font-mono whitespace-pre-wrap overflow-x-auto max-h-72 overflow-y-auto">
                  {JSON.stringify(result.config, null, 2)}
                </pre>
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function TemplatesSection({ run }: { run: IngestRun }) {
  const templates = run.templates_summary ?? [];
  if (templates.length === 0) {
    const phase = run.current_phase;
    if (phase === 'templates' && run.status === 'running') {
      return <div className="text-xs text-gray-500">Templates phase running…</div>;
    }
    return (
      <div className="text-xs text-gray-500">
        No template universes were refreshed for this run. (Are any registered?)
      </div>
    );
  }
  return (
    <div className="space-y-3">
      {templates.map((t, idx) => (
        <TemplateRunSection key={`${t.template_key}-${idx}`} run={run} t={t} />
      ))}
    </div>
  );
}

function MomentumSection({ run }: { run: IngestRun }) {
  const results = run.momentum_summary ?? [];
  if (results.length === 0) {
    const phase = run.current_phase;
    if (phase === 'momentum' && run.status === 'running') {
      return <div className="text-xs text-gray-500">Momentum phase running…</div>;
    }
    return (
      <div className="text-xs text-gray-500">
        No strategies were scheduled when this pipeline ran. Add one to the schedule above and the next run will compute holdings for it.
      </div>
    );
  }
  return (
    <div className="space-y-2">
      {results.map((r, idx) => (
        <MomentumStrategyEntry key={`${r.strategy_id ?? 'x'}-${idx}`} result={r} />
      ))}
    </div>
  );
}

export default function ScheduleRunDetail({ run }: { run: IngestRun }) {
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
      <Section title={`Template universes (${run.templates_summary?.length ?? 0})`}>
        <TemplatesSection run={run} />
      </Section>
      <Section title={`Momentum holdings (${run.momentum_summary?.length ?? 0} strateg${(run.momentum_summary?.length ?? 0) === 1 ? 'y' : 'ies'})`} defaultOpen={true}>
        <MomentumSection run={run} />
      </Section>
    </div>
  );
}
