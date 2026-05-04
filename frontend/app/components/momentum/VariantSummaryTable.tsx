'use client';

import { useEffect, useState } from 'react';
import {
  momentumStore,
  setActiveVariant,
  VARIANT_DEFS,
  type VariantKey,
  type VariantOutcome,
  type VariantsRunState,
} from '../../../lib/stores/momentum';
import { fmtPct } from './utils';

// One row per VARIANT_DEFS entry. Click to make that variant the active one
// (the detail views below — equity curve, holdings table, sector timeline —
// switch to it). Pending/running rows show a spinner; errored rows surface
// the message inline so the user doesn't have to open the network tab.
//
// Variants in this table are always rendered in VARIANT_DEFS order; the
// active row is highlighted and clickable, everything else is muted but
// stays visible so the running sweep is observable.

export default function VariantSummaryTable() {
  const variants = momentumStore.use((s) => s.variants);
  const active = momentumStore.use((s) => s.activeVariantKey);
  const run = momentumStore.use((s) => s.variantsRun);

  // Hide entirely when no sweep has ever run. The wrapper component decides
  // when to render us based on whether `variants` has any entries.
  const anyVariant = Object.values(variants).some((v) => v != null);
  if (!anyVariant) return null;

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
      <div className="px-4 py-3 border-b border-gray-800/40 flex items-center justify-between">
        <div>
          <div className="text-sm font-medium text-white">Variants</div>
          <div className="text-xs text-gray-500">
            Click a row to switch the equity curve, holdings, and sector timeline below.
          </div>
        </div>
        {run && <SweepStatus run={run} />}
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-gray-500 text-xs border-b border-gray-800/40">
            <th className="text-left font-medium px-3 py-2">Variant</th>
            <th className="text-right font-medium px-3 py-2">Annualized return</th>
            <th className="text-right font-medium px-3 py-2">Sharpe</th>
            <th className="text-right font-medium px-3 py-2">Total return</th>
            <th className="text-right font-medium px-3 py-2">Max drawdown</th>
          </tr>
        </thead>
        <tbody>
          {VARIANT_DEFS.map((v) => (
            <VariantRow
              key={v.key}
              variantKey={v.key}
              label={v.label}
              outcome={variants[v.key]}
              isActive={active === v.key}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

function SweepStatus({ run }: { run: VariantsRunState }) {
  // Tick once per second while a variant is in flight so the elapsed
  // counter updates between variant transitions. Reading Date.now()
  // directly during render would be impure (and ESLint's react-hooks/purity
  // rule rightly flags it).
  const [now, setNow] = useState<number>(() => run.startedAt);
  const isRunning = run.current != null;
  useEffect(() => {
    if (!isRunning) return;
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
}: {
  variantKey: VariantKey;
  label: string;
  outcome: VariantOutcome | undefined;
  isActive: boolean;
}) {
  const status = outcome?.status ?? 'pending';
  const clickable = status === 'ok';
  const handleClick = () => {
    if (clickable) setActiveVariant(variantKey);
  };

  // Active row: stronger highlight + indigo left border.
  // Clickable but not active: hover effect.
  // Pending/running/errored: muted background, no hover.
  const rowClass = isActive
    ? 'bg-indigo-500/10 border-l-2 border-indigo-400'
    : clickable
      ? 'cursor-pointer hover:bg-white/[0.02] border-l-2 border-transparent'
      : 'border-l-2 border-transparent';

  return (
    <tr className={`border-b border-gray-800/20 ${rowClass}`} onClick={handleClick}>
      <td className="px-3 py-2 text-gray-200">
        <div className="flex items-center gap-2">
          <StatusBadge status={status} />
          <span className={isActive ? 'text-white font-medium' : ''}>{label}</span>
        </div>
        {status === 'error' && outcome?.status === 'error' && (
          <div className="text-[10px] text-rose-400 mt-0.5 font-mono">{outcome.message}</div>
        )}
      </td>
      <SummaryCells outcome={outcome} />
    </tr>
  );
}

function SummaryCells({ outcome }: { outcome: VariantOutcome | undefined }) {
  if (outcome?.status !== 'ok') {
    // 4 placeholder cells so the row width stays stable across statuses.
    return (
      <>
        {[0, 1, 2, 3].map((i) => (
          <td key={i} className="px-3 py-2 text-right text-gray-700 font-mono">—</td>
        ))}
      </>
    );
  }
  const s = outcome.result.summary;
  const colorize = (v: number | null | undefined) =>
    v == null ? 'text-gray-500'
      : v >= 0 ? 'text-emerald-400'
      : 'text-rose-400';
  return (
    <>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.annualized_return_pct)}`}>
        {fmtPct(s.annualized_return_pct)}
      </td>
      <td className="px-3 py-2 text-right font-mono text-gray-200">
        {s.sharpe_ratio != null ? s.sharpe_ratio.toFixed(2) : '—'}
      </td>
      <td className={`px-3 py-2 text-right font-mono ${colorize(s.total_return_pct)}`}>
        {fmtPct(s.total_return_pct)}
      </td>
      <td className="px-3 py-2 text-right font-mono text-rose-400">
        {fmtPct(s.max_drawdown_pct)}
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
  return <span className="inline-block w-2 h-2 rounded-full bg-rose-500" title="Error" />;
}
