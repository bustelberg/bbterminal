'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  momentumStore,
  setActiveVariant,
  VARIANT_DEFS,
  type VariantKey,
  type VariantOutcome,
  type VariantsRunState,
} from '../../../lib/stores/momentum';
import type { Column } from '../../../lib/tableExport';
import TableDownloadButton from '../TableDownloadButton';
import CollapsibleCard from './CollapsibleCard';
import { fmtPct } from './utils';
import { computeNetStats, parenPct } from './feeStats';
import { useExchangeFeeMap } from '../../../lib/hooks/apiData';
import { API_URL } from '../../../lib/apiUrl';

// One row per VARIANT_DEFS entry. Click to make that variant the active one
// (the detail views below — equity curve, holdings table, sector timeline —
// switch to it). Pending/running rows show a spinner; errored rows surface
// the message inline so the user doesn't have to open the network tab.
//
// Variants in this table are always rendered in VARIANT_DEFS order; the
// active row is highlighted and clickable, everything else is muted but
// stays visible so the running sweep is observable.

type Props = {
  /** Per-company exchange lookup, needed to compute the (net) parenthetical
   * for each variant's stats. Optional — when omitted the table just shows
   * gross figures. */
  exchangeByCompany?: Map<number, string>;
};

export default function VariantSummaryTable({ exchangeByCompany }: Props) {
  const variants = momentumStore.use((s) => s.variants);
  const active = momentumStore.use((s) => s.activeVariantKey);
  const run = momentumStore.use((s) => s.variantsRun);

  // Per-exchange fees — shared cached hook, null when no non-zero fees
  // are configured so each variant row just shows gross figures.
  const feesByExchange = useExchangeFeeMap();

  // Hide entirely when no sweep has ever run. The wrapper component decides
  // when to render us based on whether `variants` has any entries.
  const anyVariant = Object.values(variants).some((v) => v != null);

  // Flatten each ready variant's summary into one row for the export.
  type VariantExportRow = {
    variant: string;
    start: string;
    end: string;
    annualized_pct: number | null;
    sharpe: number | null;
    total_return_pct: number | null;
    max_drawdown_pct: number | null;
  };
  const exportRows = useMemo<VariantExportRow[]>(() => {
    const out: VariantExportRow[] = [];
    for (const v of VARIANT_DEFS) {
      const o = variants[v.key];
      if (!o || o.status !== 'ok') continue;
      const r = o.result;
      const months = r.monthly_records ?? [];
      out.push({
        variant: v.label,
        start: months[0]?.date ?? '',
        end: months[months.length - 1]?.date ?? '',
        annualized_pct: r.summary?.annualized_return_pct ?? null,
        sharpe: r.summary?.sharpe_ratio ?? null,
        total_return_pct: r.summary?.total_return_pct ?? null,
        max_drawdown_pct: r.summary?.max_drawdown_pct ?? null,
      });
    }
    return out;
  }, [variants]);
  const exportColumns = useMemo<Column<VariantExportRow>[]>(() => [
    { key: 'variant', header: 'Variant', accessor: (r) => r.variant },
    { key: 'start', header: 'Start', accessor: (r) => r.start },
    { key: 'end', header: 'End', accessor: (r) => r.end },
    { key: 'annualized_pct', header: 'Annualized return (%)', accessor: (r) => r.annualized_pct },
    { key: 'sharpe', header: 'Sharpe', accessor: (r) => r.sharpe },
    { key: 'total_return_pct', header: 'Total return (%)', accessor: (r) => r.total_return_pct },
    { key: 'max_drawdown_pct', header: 'Max drawdown (%)', accessor: (r) => r.max_drawdown_pct },
  ], []);

  if (!anyVariant) return null;

  return (
    <CollapsibleCard
      title="Variants"
      rightSlot={
        <div className="flex items-center gap-3">
          <span>Click a row to switch the equity curve, holdings, and sector timeline below.</span>
          {run && <SweepStatus run={run} />}
          <TableDownloadButton
            rows={exportRows}
            columns={exportColumns}
            filename="variant_summary"
            title={`Download ${exportRows.length} variant rows as CSV / XLSX`}
          />
        </div>
      }
    >
      <table className="w-full text-sm border-t border-gray-800/40">
        <thead>
          <tr className="text-gray-500 text-xs border-b border-gray-800/40">
            <th className="text-left font-medium px-3 py-2">Variant</th>
            <th className="text-right font-medium px-3 py-2">Start</th>
            <th className="text-right font-medium px-3 py-2">End</th>
            <th className="text-right font-medium px-3 py-2">Annualized return</th>
            <th className="text-right font-medium px-3 py-2">Sharpe</th>
            <th className="text-right font-medium px-3 py-2">Total return</th>
            <th className="text-right font-medium px-3 py-2">Max drawdown</th>
          </tr>
        </thead>
        <tbody>
          {VARIANT_DEFS.filter((v) => variants[v.key] != null).map((v) => (
            <VariantRow
              key={v.key}
              variantKey={v.key}
              label={v.label}
              outcome={variants[v.key]}
              isActive={active === v.key}
              feesByExchange={feesByExchange}
              exchangeByCompany={exchangeByCompany}
            />
          ))}
        </tbody>
      </table>
    </CollapsibleCard>
  );
}

function SweepStatus({ run }: { run: VariantsRunState }) {
  // Tick once per second while a variant is in flight so the elapsed
  // counter updates between variant transitions. Reading Date.now()
  // directly during render would be impure (and ESLint's react-hooks/purity
  // rule rightly flags it).
  const [now, setNow] = useState<number>(() => run.startedAt);
  const isRunning = run.current != null;
  // The leading setNow snaps the elapsed counter to "live time" the
  // instant a sweep starts (otherwise it lags by up to one full second
  // before the first interval tick). React 19's set-state-in-effect
  // lint dislikes this synchronous setter, but the alternative — a
  // ref-based "first tick" toggle — adds more noise than it saves.
  useEffect(() => {
    if (!isRunning) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setNow(Date.now());
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [isRunning]);
  const elapsed = Math.round((now - run.startedAt) / 1000);
  const done = run.completed;
  const total = run.total;
  return (
    <div className="text-xs text-gray-400 flex items-center gap-2">
      {isRunning && (
        <svg className="animate-spin w-3 h-3 text-indigo-400" viewBox="0 0 24 24" fill="none">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
        </svg>
      )}
      <span className="font-mono">
        {done}/{total}
        {isRunning && <span className="text-gray-600"> · {elapsed}s</span>}
      </span>
    </div>
  );
}

function VariantRow({
  variantKey,
  label,
  outcome,
  isActive,
  feesByExchange,
  exchangeByCompany,
}: {
  variantKey: VariantKey;
  label: string;
  outcome: VariantOutcome | undefined;
  isActive: boolean;
  feesByExchange: Map<string, number> | null;
  exchangeByCompany: Map<number, string> | undefined;
}) {
  const status = outcome?.status ?? 'pending';
  const clickable = status === 'ok';
  const handleClick = () => {
    if (clickable) setActiveVariant(variantKey);
  };

  // Active row: chevron indicator + slightly brighter label, no left border
  // and no background tint (those were the "ugly selector").
  // Clickable but not active: subtle hover bg.
  // Pending/running/errored: no hover.
  const rowClass = clickable && !isActive
    ? 'cursor-pointer hover:bg-white/[0.02]'
    : '';

  return (
    <tr className={`border-b border-gray-800/20 ${rowClass}`} onClick={handleClick}>
      <td className="px-3 py-2 text-gray-200">
        <div className="flex items-center gap-2">
          <span className={`inline-block w-3 text-indigo-400 ${isActive ? '' : 'opacity-0'}`}>
            ›
          </span>
          <StatusBadge status={status} />
          <span className={isActive ? 'text-white font-medium' : ''}>{label}</span>
          {status === 'cancelled' && (
            <span className="text-[10px] text-gray-500 italic">cancelled</span>
          )}
        </div>
        {status === 'error' && outcome?.status === 'error' && (
          <div className="text-[10px] text-rose-400 mt-0.5 font-mono">{outcome.message}</div>
        )}
      </td>
      <SummaryCells
        outcome={outcome}
        feesByExchange={feesByExchange}
        exchangeByCompany={exchangeByCompany}
      />
    </tr>
  );
}

function SummaryCells({
  outcome,
  feesByExchange,
  exchangeByCompany,
}: {
  outcome: VariantOutcome | undefined;
  feesByExchange: Map<string, number> | null;
  exchangeByCompany: Map<number, string> | undefined;
}) {
  // 6 placeholder cells (Start, End, Annualized, Sharpe, Total, DD) so the
  // row width stays stable across statuses.
  // Note: hooks must be called unconditionally — `net` is computed before
  // any early return.
  const net = useMemo(() => {
    if (outcome?.status !== 'ok' || !feesByExchange || !exchangeByCompany) return null;
    return computeNetStats(outcome.result.monthly_records, feesByExchange, exchangeByCompany, outcome.result.daily_records);
  }, [outcome, feesByExchange, exchangeByCompany]);

  if (outcome?.status !== 'ok') {
    return (
      <>
        {[0, 1, 2, 3, 4, 5].map((i) => (
          <td key={i} className="px-3 py-2 text-right text-gray-700 font-mono">—</td>
        ))}
      </>
    );
  }
  const s = outcome.result.summary;
  const records = outcome.result.monthly_records;
  // First/last record dates → YYYY-MM. Records are emitted in chronological
  // order by the backtest loop, so first = period start, last = period end.
  const firstDate = records.length > 0 ? records[0].date.slice(0, 7) : '—';
  const lastDate = records.length > 0 ? records[records.length - 1].date.slice(0, 7) : '—';
  const colorize = (v: number | null | undefined) =>
    v == null ? 'text-gray-500'
      : v >= 0 ? 'text-emerald-400'
      : 'text-rose-400';
  return (
    <>
      <td className="px-3 py-2 text-right font-mono text-gray-400">{firstDate}</td>
      <td className="px-3 py-2 text-right font-mono text-gray-400">{lastDate}</td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.annualized_return_pct)}`}>
        {fmtPct(s.annualized_return_pct)}
        <span className="text-gray-500">{parenPct(net?.annualized_return_pct)}</span>
      </td>
      <td className="px-3 py-2 text-right font-mono text-gray-200">
        {s.sharpe_ratio != null ? s.sharpe_ratio.toFixed(2) : '—'}
        <span className="text-gray-500">{net?.sharpe_ratio != null ? ` (${net.sharpe_ratio.toFixed(2)})` : ''}</span>
      </td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.total_return_pct)}`}>
        {fmtPct(s.total_return_pct)}
        <span className="text-gray-500">{parenPct(net?.total_return_pct)}</span>
      </td>
      <td className="px-3 py-2 text-right font-mono text-rose-400">
        {fmtPct(s.max_drawdown_pct)}
        <span className="text-gray-500">{parenPct(net?.max_drawdown_pct)}</span>
      </td>
    </>
  );
}

function StatusBadge({ status }: { status: VariantOutcome['status'] }) {
  if (status === 'pending') {
    return <span className="inline-block w-2 h-2 rounded-full bg-gray-700" title="Pending" />;
  }
  if (status === 'running') {
    return (
      <svg className="animate-spin w-3 h-3 text-indigo-400" viewBox="0 0 24 24" fill="none" aria-label="Running">
        <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
        <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
      </svg>
    );
  }
  if (status === 'ok') {
    return <span className="inline-block w-2 h-2 rounded-full bg-emerald-400" title="Done" />;
  }
  if (status === 'cancelled') {
    return <span className="inline-block w-2 h-2 rounded-full bg-gray-500" title="Cancelled" />;
  }
  return <span className="inline-block w-2 h-2 rounded-full bg-rose-500" title="Error" />;
}
