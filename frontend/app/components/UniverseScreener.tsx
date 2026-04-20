'use client';

import { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import {
  AreaChart, Area, XAxis, YAxis, Tooltip, ResponsiveContainer,
  CartesianGrid,
} from 'recharts';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type CriterionDef = { key: string; label: string; description?: string; min_years?: number };

type SectorCount = { sector: string; count: number };
type MonthlyCount = { month: string; count: number };
type UniverseRow = {
  universe_id: number;
  label: string;
  description: string | null;
  created_at: string;
  start_month: string | null;
  end_month: string | null;
  month_count: number;
  total_rows: number;
  unique_companies: number;
  unique_tickers: number;
  avg_per_month: number;
  first_month_count: number;
  last_month_count: number;
  monthly_counts: MonthlyCount[];
  sectors: SectorCount[];
};

export default function UniverseScreener() {
  const [criteria, setCriteria] = useState<CriterionDef[]>([]);
  const [universes, setUniverses] = useState<UniverseRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [renamingId, setRenamingId] = useState<number | null>(null);
  const [renameValue, setRenameValue] = useState('');
  const [busyLabel, setBusyLabel] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState<string | null>(null);
  const [confirmDeleteAll, setConfirmDeleteAll] = useState(false);
  const [expandedId, setExpandedId] = useState<number | null>(null);

  const loadUniverses = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await fetch(`${API_URL}/api/universe/labels`);
      if (!r.ok) throw new Error(`${r.status}`);
      const data: UniverseRow[] = await r.json();
      setUniverses(data);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetch(`${API_URL}/api/universe/criteria`)
      .then(r => r.json())
      .then(setCriteria)
      .catch(() => {});
    loadUniverses();
  }, [loadUniverses]);

  const startRename = (u: UniverseRow) => {
    setRenamingId(u.universe_id);
    setRenameValue(u.label);
  };

  const cancelRename = () => {
    setRenamingId(null);
    setRenameValue('');
  };

  const saveRename = async (u: UniverseRow) => {
    const newLabel = renameValue.trim();
    if (!newLabel || newLabel === u.label) {
      cancelRename();
      return;
    }
    setBusyLabel(u.label);
    try {
      const r = await fetch(`${API_URL}/api/universe/labels/${encodeURIComponent(u.label)}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ new_label: newLabel }),
      });
      if (!r.ok) {
        const body = await r.text();
        throw new Error(body || `${r.status}`);
      }
      cancelRename();
      await loadUniverses();
    } catch (e) {
      alert(`Rename failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setBusyLabel(null);
    }
  };

  const deleteOne = async (label: string) => {
    setBusyLabel(label);
    try {
      const r = await fetch(`${API_URL}/api/universe/labels/${encodeURIComponent(label)}`, {
        method: 'DELETE',
      });
      if (!r.ok) throw new Error(`${r.status}`);
      setConfirmDelete(null);
      setUniverses(prev => prev.filter(u => u.label !== label));
    } catch (e) {
      alert(`Delete failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setBusyLabel(null);
    }
  };

  const deleteAll = async () => {
    setBusyLabel('__all__');
    try {
      const r = await fetch(`${API_URL}/api/universe/labels`, { method: 'DELETE' });
      if (!r.ok) throw new Error(`${r.status}`);
      setConfirmDeleteAll(false);
      await loadUniverses();
    } catch (e) {
      alert(`Delete all failed: ${e instanceof Error ? e.message : e}`);
    } finally {
      setBusyLabel(null);
    }
  };

  return (
    <div className="h-full flex flex-col bg-[#0f1117]">
      <div className="px-8 py-5 border-b border-gray-800/60">
        <h1 className="text-white text-xl font-semibold">Universe Overview</h1>
        <p className="text-gray-500 text-sm mt-1">Quality criteria reference and detailed stats for every saved universe.</p>
      </div>

      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {criteria.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4">
            <h2 className="text-white text-sm font-medium mb-3">Quality Criteria (score 1 point each, need &gt;= 1 to qualify)</h2>
            <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
              {criteria.map(c => (
                <div key={c.key} className="text-xs text-gray-400 flex items-center gap-1.5">
                  <div className="w-1.5 h-1.5 rounded-full bg-indigo-500/60 shrink-0" />
                  {c.label}
                  <span className="text-gray-600 font-mono text-[10px]">{c.min_years ?? 1}y</span>
                  {c.description && <InfoTip text={c.description} />}
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="flex items-center justify-between">
          <h2 className="text-white text-sm font-medium">
            Saved Universes {universes.length > 0 && <span className="text-gray-500 font-normal">({universes.length})</span>}
          </h2>
          {universes.length > 0 && (
            confirmDeleteAll ? (
              <div className="flex items-center gap-2">
                <span className="text-sm text-rose-400">Delete all {universes.length}?</span>
                <button
                  onClick={deleteAll}
                  disabled={busyLabel === '__all__'}
                  className="px-3 py-1.5 rounded-lg text-xs font-medium bg-rose-600 hover:bg-rose-500 text-white transition-colors disabled:opacity-50"
                >
                  {busyLabel === '__all__' ? 'Deleting...' : 'Yes, delete all'}
                </button>
                <button
                  onClick={() => setConfirmDeleteAll(false)}
                  className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
                >
                  Cancel
                </button>
              </div>
            ) : (
              <button
                onClick={() => setConfirmDeleteAll(true)}
                className="px-3 py-1.5 rounded-lg text-xs font-medium text-gray-500 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Delete all
              </button>
            )
          )}
        </div>

        {loading ? (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-8 text-sm text-gray-500">
            Loading...
          </div>
        ) : error ? (
          <div className="bg-rose-500/10 border border-rose-500/20 rounded-lg px-5 py-4 text-sm text-rose-400">
            Failed to load: {error}
          </div>
        ) : universes.length === 0 ? (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-8 text-sm text-gray-500">
            No universes saved yet.
          </div>
        ) : (
          <div className="space-y-3">
            {universes.map(u => (
              <UniverseCard
                key={u.universe_id}
                u={u}
                expanded={expandedId === u.universe_id}
                onToggle={() => setExpandedId(expandedId === u.universe_id ? null : u.universe_id)}
                renamingId={renamingId}
                renameValue={renameValue}
                setRenameValue={setRenameValue}
                startRename={startRename}
                cancelRename={cancelRename}
                saveRename={saveRename}
                confirmDelete={confirmDelete}
                setConfirmDelete={setConfirmDelete}
                deleteOne={deleteOne}
                busyLabel={busyLabel}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

type UniverseCardProps = {
  u: UniverseRow;
  expanded: boolean;
  onToggle: () => void;
  renamingId: number | null;
  renameValue: string;
  setRenameValue: (v: string) => void;
  startRename: (u: UniverseRow) => void;
  cancelRename: () => void;
  saveRename: (u: UniverseRow) => void;
  confirmDelete: string | null;
  setConfirmDelete: (label: string | null) => void;
  deleteOne: (label: string) => void;
  busyLabel: string | null;
};

function UniverseCard({
  u, expanded, onToggle,
  renamingId, renameValue, setRenameValue, startRename, cancelRename, saveRename,
  confirmDelete, setConfirmDelete, deleteOne, busyLabel,
}: UniverseCardProps) {
  const isRenaming = renamingId === u.universe_id;
  const isConfirming = confirmDelete === u.label;
  const isBusy = busyLabel === u.label;

  const createdLabel = u.created_at ? new Date(u.created_at).toISOString().slice(0, 10) : '—';
  const monthRange = u.start_month && u.end_month
    ? (u.start_month === u.end_month ? u.start_month : `${u.start_month} → ${u.end_month}`)
    : '—';

  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
      <div className="flex items-start justify-between gap-3 px-5 py-4">
        <div className="flex items-start gap-3 min-w-0 flex-1">
          <button
            type="button"
            onClick={onToggle}
            className="mt-0.5 text-gray-500 hover:text-gray-300 transition-colors shrink-0"
            aria-label={expanded ? 'Collapse' : 'Expand'}
          >
            <span className="font-mono text-sm">{expanded ? '▾' : '▸'}</span>
          </button>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-3 flex-wrap">
              {isRenaming ? (
                <input
                  autoFocus
                  value={renameValue}
                  onChange={e => setRenameValue(e.target.value)}
                  onKeyDown={e => {
                    if (e.key === 'Enter') saveRename(u);
                    if (e.key === 'Escape') cancelRename();
                  }}
                  className="bg-[#0f1117] border border-gray-700 rounded-lg px-2 py-1 text-base text-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
                />
              ) : (
                <h3 className="text-white text-base font-semibold">{u.label}</h3>
              )}
              <span className="text-gray-500 text-xs font-mono">id:{u.universe_id}</span>
              <span className="text-gray-500 text-xs">created {createdLabel}</span>
            </div>
            {u.description && (
              <p className="text-gray-400 text-xs mt-1">{u.description}</p>
            )}
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-3 mt-3">
              <Stat label="Months" value={u.month_count} />
              <Stat label="Range" value={monthRange} mono />
              <Stat label="Unique companies" value={u.unique_companies} />
              <Stat label="Unique tickers" value={u.unique_tickers} />
              <Stat label="Total rows" value={u.total_rows} />
              <Stat label="Avg / month" value={u.avg_per_month} />
              <Stat label="First / last mo" value={`${u.first_month_count} / ${u.last_month_count}`} />
            </div>
          </div>
        </div>

        <div className="flex items-center gap-1 shrink-0">
          {isRenaming ? (
            <>
              <button
                onClick={() => saveRename(u)}
                disabled={isBusy}
                className="px-2.5 py-1 rounded-lg text-xs font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50"
              >
                Save
              </button>
              <button
                onClick={cancelRename}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
              >
                Cancel
              </button>
            </>
          ) : isConfirming ? (
            <>
              <span className="text-xs text-rose-400">Delete?</span>
              <button
                onClick={() => deleteOne(u.label)}
                disabled={isBusy}
                className="px-2.5 py-1 rounded-lg text-xs font-medium bg-rose-600 hover:bg-rose-500 text-white transition-colors disabled:opacity-50"
              >
                {isBusy ? '...' : 'Yes'}
              </button>
              <button
                onClick={() => setConfirmDelete(null)}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
              >
                Cancel
              </button>
            </>
          ) : (
            <>
              <button
                onClick={() => startRename(u)}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-gray-400 hover:text-white hover:bg-white/5 transition-colors"
              >
                Rename
              </button>
              <button
                onClick={() => setConfirmDelete(u.label)}
                className="px-2.5 py-1 rounded-lg text-xs font-medium text-gray-400 hover:text-rose-400 hover:bg-rose-500/10 transition-colors"
              >
                Delete
              </button>
            </>
          )}
        </div>
      </div>

      {expanded && (
        <div className="border-t border-gray-800/40 grid grid-cols-1 lg:grid-cols-2 gap-0">
          <MonthlySparkline monthly={u.monthly_counts} />
          <SectorBreakdown sectors={u.sectors} totalRows={u.total_rows} />
        </div>
      )}
    </div>
  );
}

function Stat({ label, value, mono }: { label: string; value: string | number; mono?: boolean }) {
  return (
    <div>
      <div className="text-gray-500 text-[10px] uppercase tracking-wider">{label}</div>
      <div className={`text-gray-200 text-sm mt-0.5 ${mono ? 'font-mono text-xs' : ''}`}>{value}</div>
    </div>
  );
}

function MonthlySparkline({ monthly }: { monthly: MonthlyCount[] }) {
  const stats = useMemo(() => {
    if (!monthly.length) return null;
    const counts = monthly.map(m => m.count);
    const min = Math.min(...counts);
    const max = Math.max(...counts);
    const avg = counts.reduce((a, b) => a + b, 0) / counts.length;
    return { min, max, avg };
  }, [monthly]);

  const yDomain = useMemo(() => {
    if (!stats) return [0, 1] as [number, number];
    const range = stats.max - stats.min || Math.max(stats.max, 1) * 0.1;
    const pad = range * 0.1;
    return [Math.max(0, Math.floor(stats.min - pad)), Math.ceil(stats.max + pad)] as [number, number];
  }, [stats]);

  if (!monthly.length || !stats) {
    return (
      <div className="p-5 text-xs text-gray-500">No monthly data.</div>
    );
  }

  return (
    <div className="p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="text-gray-400 text-xs font-medium">Monthly membership count</div>
        <div className="text-gray-600 text-[10px] font-mono">
          min {stats.min} · avg {stats.avg.toFixed(0)} · max {stats.max}
        </div>
      </div>
      <ResponsiveContainer width="100%" height={180}>
        <AreaChart data={monthly} margin={{ top: 5, right: 8, left: 0, bottom: 0 }}>
          <defs>
            <linearGradient id="universeMonthlyGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#818cf8" stopOpacity={0.3} />
              <stop offset="95%" stopColor="#818cf8" stopOpacity={0} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="3 3" stroke="#1f2937" />
          <XAxis
            dataKey="month"
            tick={{ fill: '#6b7280', fontSize: 11 }}
            tickLine={false}
            interval={Math.max(0, Math.floor(monthly.length / 8) - 1)}
          />
          <YAxis
            tick={{ fill: '#6b7280', fontSize: 11 }}
            tickLine={false}
            domain={yDomain}
            allowDecimals={false}
            width={45}
          />
          <Tooltip
            contentStyle={{
              backgroundColor: '#1e2230',
              border: '1px solid rgba(107,114,128,0.3)',
              borderRadius: '8px',
              fontSize: 12,
            }}
            labelStyle={{ color: '#9ca3af' }}
            formatter={(value: number) => [value.toLocaleString(), 'Companies']}
            labelFormatter={(l) => String(l)}
          />
          <Area
            type="monotone"
            dataKey="count"
            stroke="#818cf8"
            strokeWidth={1.5}
            fill="url(#universeMonthlyGradient)"
            dot={false}
            activeDot={{ r: 4, fill: '#818cf8', stroke: '#1e2230', strokeWidth: 2 }}
          />
        </AreaChart>
      </ResponsiveContainer>
      <details className="mt-3">
        <summary className="text-gray-500 text-xs cursor-pointer hover:text-gray-300">Monthly counts ({monthly.length} months)</summary>
        <div className="mt-2 max-h-48 overflow-auto text-xs font-mono text-gray-400 grid grid-cols-3 md:grid-cols-4 gap-x-4 gap-y-1">
          {monthly.map(m => (
            <div key={m.month} className="flex justify-between">
              <span>{m.month}</span>
              <span className="text-gray-500">{m.count}</span>
            </div>
          ))}
        </div>
      </details>
    </div>
  );
}

function SectorBreakdown({ sectors, totalRows }: { sectors: SectorCount[]; totalRows: number }) {
  if (!sectors.length) {
    return <div className="p-5 text-xs text-gray-500 border-t lg:border-t-0 lg:border-l border-gray-800/40">No sector data.</div>;
  }
  const maxCount = sectors[0]?.count || 1;
  return (
    <div className="p-5 border-t lg:border-t-0 lg:border-l border-gray-800/40">
      <div className="text-gray-400 text-xs font-medium mb-2">Sector breakdown ({sectors.length})</div>
      <div className="max-h-48 overflow-auto space-y-1">
        {sectors.map(s => {
          const pct = totalRows ? (s.count / totalRows) * 100 : 0;
          const barPct = (s.count / maxCount) * 100;
          return (
            <div key={s.sector} className="text-xs">
              <div className="flex items-center justify-between text-gray-400">
                <span className="truncate pr-2">{s.sector}</span>
                <span className="font-mono text-gray-500 shrink-0">{s.count} · {pct.toFixed(1)}%</span>
              </div>
              <div className="mt-0.5 h-1 bg-gray-800 rounded-full overflow-hidden">
                <div className="h-full bg-indigo-500/60" style={{ width: `${barPct}%` }} />
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}

function InfoTip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number }>({ top: 0, left: 0 });
  const iconRef = useRef<HTMLSpanElement>(null);

  const tipWidth = 224;
  const margin = 8;

  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const clampedLeft = Math.max(margin + tipWidth / 2, Math.min(centerX, window.innerWidth - margin - tipWidth / 2));
      setPos({ top: rect.bottom + 8, left: clampedLeft });
    }
    setShow(true);
  };

  return (
    <span className="relative cursor-help" onMouseEnter={handleEnter} onMouseLeave={() => setShow(false)}>
      <span ref={iconRef} className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-gray-600 text-gray-500 text-[10px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors">i</span>
      {show && (
        <span
          className="fixed w-56 px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-xs text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, transform: 'translate(-50%, 0)' }}
        >
          {text}
        </span>
      )}
    </span>
  );
}
