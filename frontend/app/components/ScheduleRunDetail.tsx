'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import type { IngestRun, MomentumStrategyResult, TemplateDiff } from './schedule/types';
import { runToTimelineProps, PIPELINE_STEPS } from './schedule/timeline';
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
    <div className="bg-page border border-neutral-800/60 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full px-4 py-2 flex items-center justify-between hover:bg-overlay/[0.02] transition-colors"
      >
        <span className="flex items-center gap-2">
          <span className="text-fg-subtle font-mono text-xs">{open ? '▾' : '▸'}</span>
          <span className="text-sm font-medium text-fg">{title}</span>
        </span>
        {rightSlot}
      </button>
      {open && <div className="px-4 py-3 border-t border-neutral-800/40">{children}</div>}
    </div>
  );
}

/** Diff + searchable membership viewer for one template entry in this
 * run's `templates_summary`. Each template that ran in this pipeline
 * tick gets its own card so multi-template runs (when SP500 lands) are
 * visually unambiguous. */
function TemplateRunSection({ run, t }: { run: IngestRun; t: TemplateDiff }) {
  return (
    <div className="bg-card border border-neutral-800/40 rounded-lg overflow-hidden">
      <div className="px-3 py-2 flex items-center gap-3 flex-wrap border-b border-neutral-800/40">
        <span className="text-sm font-medium text-fg">{t.template_key}</span>
        {t.error ? (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-neg-500/10 text-neg-300 border-neg-500/30">
            error
          </span>
        ) : (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-pos-500/15 text-pos-300 border-pos-500/30">
            ok
          </span>
        )}
        <span className="text-xs text-fg-subtle font-mono">
          {t.this_month ?? '—'}{t.prev_month && <span className="text-fg-faint"> vs {t.prev_month}</span>}
        </span>
        <span className="text-xs text-fg-muted font-mono ml-auto">
          +{t.additions_count} / −{t.removals_count}{t.renames_count > 0 && <span> / r{t.renames_count}</span>}
        </span>
      </div>
      <div className="px-3 py-3 space-y-3">
        {t.error && (
          <div className="text-xs text-neg-300 font-mono whitespace-pre-wrap">{t.error}</div>
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
    <div className="bg-warn-500/[0.06] border border-warn-500/30 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        className="w-full px-3 py-2 flex items-center justify-between hover:bg-warn-500/[0.04] cursor-pointer"
      >
        <span className="flex items-center gap-2">
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border text-warn-300 bg-warn-500/10 border-warn-500/40">
            Unresolved post-XLS additions
          </span>
          <span className="text-warn-200 font-mono">{items.length}</span>
          <span className="text-[11px] text-warn-200/70">
            need a manual GuruFocus link before the next pipeline tick can include them
          </span>
        </span>
        <span className="text-warn-300 font-mono text-xs">{open ? '▾' : '▸'}</span>
      </button>
      {open && (
        <div className="border-t border-warn-500/30 max-h-96 overflow-y-auto divide-y divide-warn-500/15">
          {items.map((u, idx) => (
            <div key={`${u.msci_href ?? u.name}-${idx}`} className="px-3 py-2 text-xs space-y-1">
              <div className="flex items-baseline gap-2 flex-wrap">
                <span className="font-mono text-fg">{u.name}</span>
                <span className="text-[10px] uppercase tracking-wider px-1 py-0.5 rounded border border-neutral-700 text-fg-muted">
                  {u.country}
                </span>
                {u.eff_date && (
                  <span className="text-[10px] text-fg-subtle font-mono">eff {u.eff_date}</span>
                )}
                <span className="text-[10px] uppercase tracking-wider px-1 py-0.5 rounded border border-neg-500/30 text-neg-300 bg-neg-500/10 ml-auto">
                  {u.reason}
                </span>
              </div>
              {u.openfigi_candidate && (
                <div className="text-[11px] text-fg-muted font-mono">
                  OpenFIGI says: {u.openfigi_candidate.exch_code ?? '?'}:{u.openfigi_candidate.ticker ?? '?'}
                  {u.openfigi_candidate.name && (
                    <span className="text-fg-subtle"> ({u.openfigi_candidate.name})</span>
                  )}
                </div>
              )}
              {u.gf_url && (
                <a
                  href={u.gf_url}
                  target="_blank"
                  rel="noreferrer"
                  className="text-[11px] text-warn-300 hover:text-warn-200 underline font-mono break-all inline-block"
                >
                  {u.gf_url}
                </a>
              )}
              {u.detail && (
                <div className="text-[10px] text-fg-subtle italic">{u.detail}</div>
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
    emerald: 'text-pos-300 bg-pos-500/10 border-pos-500/30',
    rose: 'text-neg-300 bg-neg-500/10 border-neg-500/30',
    amber: 'text-warn-300 bg-warn-500/10 border-warn-500/30',
  };
  return (
    <div className="bg-page rounded-lg border border-neutral-800/40">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        disabled={count === 0}
        className={`w-full px-3 py-2 flex items-center justify-between disabled:cursor-default ${count > 0 ? 'hover:bg-overlay/[0.02] cursor-pointer' : ''}`}
      >
        <span className="flex items-center gap-2">
          <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${colorMap[color]}`}>
            {label}
          </span>
          <span className="text-fg font-mono">{count}</span>
        </span>
        {count > 0 && <span className="text-fg-subtle font-mono text-xs">{open ? '▾' : '▸'}</span>}
      </button>
      {open && items.length > 0 && (
        <div className="border-t border-neutral-800/40 max-h-72 overflow-y-auto divide-y divide-neutral-800/20">
          {items.map((it) => (
            <div key={it.company_id} className="px-3 py-1.5">
              <div className="font-mono text-xs text-fg">{it.primary}</div>
              <div className="text-[10px] text-fg-subtle truncate">
                {it.secondary ?? '—'}
                {it.sector && <span className="text-fg-faint"> · {it.sector}</span>}
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
    <div className="border border-neutral-800/40 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full px-3 py-2 flex items-center justify-between text-left hover:bg-overlay/[0.02] transition-colors"
      >
        <span className="flex items-center gap-2">
          <span className="text-fg-subtle font-mono text-xs">{open ? '▾' : '▸'}</span>
          <span className="text-sm text-fg">Membership ({targetMonth})</span>
        </span>
        <span className="text-xs text-fg-subtle font-mono">{loaded ? `${rows.length} holdings` : 'click to load'}</span>
      </button>
      {open && (
        <div className="px-3 py-2 border-t border-neutral-800/40 space-y-2">
          <div className="flex items-center gap-3 flex-wrap">
            <input
              type="search"
              placeholder="Search by ticker, name, exchange, sector…"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              className="bg-card border border-neutral-700 rounded-lg px-3 py-1.5 text-xs text-fg placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 focus:outline-none flex-1 min-w-[200px]"
            />
            <span className="text-xs text-fg-subtle font-mono">
              {loading ? 'loading…' : `${filtered.length} / ${rows.length}`}
            </span>
            <TableDownloadButton
              rows={filtered}
              columns={membershipExportColumns}
              filename={`run_${runId}_${templateKey}_${targetMonth}`}
              title={`Download ${filtered.length} holdings as CSV / XLSX`}
            />
          </div>
          {error && <div className="text-xs text-neg-300">{error}</div>}
          {filtered.length > 0 && (
            <div className="max-h-80 overflow-auto border border-neutral-800/40 rounded-lg">
              <table className="w-full text-xs">
                <thead className="text-fg-subtle text-[10px] uppercase sticky top-0 bg-card">
                  <tr className="border-b border-neutral-800/40">
                    <th className="text-left px-3 py-1.5 font-medium">Ticker</th>
                    <th className="text-left px-3 py-1.5 font-medium">Name</th>
                    <th className="text-left px-3 py-1.5 font-medium">Exchange</th>
                    <th className="text-left px-3 py-1.5 font-medium">Sector</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((r) => (
                    <tr key={r.company_id} className="border-b border-neutral-800/20 hover:bg-overlay/[0.02]">
                      <td className="px-3 py-1.5 font-mono text-fg">{r.ticker || '—'}</td>
                      <td className="px-3 py-1.5 text-fg-soft truncate max-w-[280px]">{r.company_name || '—'}</td>
                      <td className="px-3 py-1.5 font-mono text-fg-subtle">{r.exchange || '—'}</td>
                      <td className="px-3 py-1.5 text-fg-muted">{r.sector || '—'}</td>
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
    ? 'bg-neg-500/10 text-neg-300 border-neg-500/30'
    : 'bg-pos-500/15 text-pos-300 border-pos-500/30';
  // Always allow expansion — even on a hard error there's debug info to
  // surface (error message, traceback, the config that ran).
  return (
    <div className="bg-card border border-neutral-800/40 rounded-lg overflow-hidden">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full px-3 py-2 flex items-center gap-3 hover:bg-overlay/[0.02] transition-colors"
      >
        <span className="text-fg-subtle font-mono text-xs w-4">{open ? '▾' : '▸'}</span>
        <span className={`text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border ${statusCls}`}>
          {result.status}
        </span>
        <span className="text-sm text-fg font-medium truncate">{result.strategy_name}</span>
        {result.frequency && (
          <span className="text-[10px] uppercase tracking-wider px-1.5 py-0.5 rounded border bg-accent-500/10 text-accent-300 border-accent-500/30">
            {result.frequency}
          </span>
        )}
        <span className="text-xs text-fg-subtle font-mono ml-auto">
          {result.holdings_count > 0 && `${result.holdings_count} holdings · `}
          {result.latest_price_date && `as of ${result.latest_price_date}`}
        </span>
      </button>
      {open && (
        <div className="border-t border-neutral-800/40 bg-page divide-y divide-neutral-800/30">
          {isError && (
            <div className="px-3 py-3 space-y-2">
              <div className="text-xs text-neg-300 font-mono whitespace-pre-wrap">
                {result.error_message ?? 'Unknown error'}
              </div>
              {result.error_traceback && (
                <div>
                  <button
                    type="button"
                    onClick={() => setTbOpen((v) => !v)}
                    className="text-[10px] uppercase tracking-wider text-fg-subtle hover:text-fg-soft transition-colors"
                  >
                    {tbOpen ? '▾' : '▸'} Full traceback
                  </button>
                  {tbOpen && (
                    <pre className="mt-2 bg-sidebar border border-neutral-800/60 rounded-lg px-3 py-2 text-neg-200 text-[11px] font-mono whitespace-pre-wrap overflow-x-auto max-h-72 overflow-y-auto">
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
            <div className="px-3 py-3 text-xs text-fg-subtle">No snapshot saved.</div>
          )}
          {result.config && Object.keys(result.config).length > 0 && (
            <div className="px-3 py-3">
              <button
                type="button"
                onClick={() => setCfgOpen((v) => !v)}
                className="text-[10px] uppercase tracking-wider text-fg-subtle hover:text-fg-soft transition-colors"
              >
                {cfgOpen ? '▾' : '▸'} Raw stored config
              </button>
              {cfgOpen && (
                <pre className="mt-2 bg-sidebar border border-neutral-800/60 rounded-lg px-3 py-2 text-fg-soft text-[11px] font-mono whitespace-pre-wrap overflow-x-auto max-h-72 overflow-y-auto">
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
      return <div className="text-xs text-fg-subtle">Templates phase running…</div>;
    }
    return (
      <div className="text-xs text-fg-subtle">
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
      return <div className="text-xs text-fg-subtle">Momentum phase running…</div>;
    }
    return (
      <div className="text-xs text-fg-subtle">
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
    <div className="px-5 py-3 bg-sidebar space-y-3 border-t border-neutral-800/30">
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
