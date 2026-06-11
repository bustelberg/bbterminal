'use client';

import { useState } from 'react';

import { dialog } from '../../../lib/dialog';
import Spinner from '../Spinner';
import CompanyPicker from './CompanyPicker';
import type { Company } from './types';
import type { Portfolio, PortfolioMemberInput } from './usePortfolios';

type Row = { company_id: number; ticker: string; name: string; weight: number };

/** Admin-only portfolio manager: list existing portfolios (rename / delete /
 * edit) and create new ones from existing companies with adjustable weights
 * (equal by default). Shares the parent's `usePortfolios` instance via props so
 * the dashboard's pickers refresh on every mutation. */
export default function PortfolioManagerModal({
  companies,
  portfolios,
  create,
  update,
  remove,
  onSaved,
  onClose,
}: {
  companies: Company[];
  portfolios: Portfolio[];
  create: (name: string, members: PortfolioMemberInput[]) => Promise<Portfolio>;
  update: (id: number, patch: { name?: string; members?: PortfolioMemberInput[] }) => Promise<Portfolio>;
  remove: (id: number) => Promise<void>;
  /** Called with the portfolio after a successful create / save — lets the
   * caller auto-select it onto the comparison side that opened the manager. */
  onSaved?: (p: Portfolio) => void;
  onClose: () => void;
}) {
  const [editingId, setEditingId] = useState<number | null>(null);
  const [name, setName] = useState('');
  const [rows, setRows] = useState<Row[]>([]);
  const [saving, setSaving] = useState(false);
  const [busyId, setBusyId] = useState<number | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);

  const weightSum = rows.reduce((s, r) => s + (r.weight || 0), 0);

  const equalize = (list: Row[]): Row[] => {
    const n = list.length;
    return n === 0 ? list : list.map((r) => ({ ...r, weight: 1 / n }));
  };

  const resetForm = () => { setEditingId(null); setName(''); setRows([]); setErr(null); };

  const addCompany = (c: Company) => {
    setNotice(null);
    if (rows.some((r) => r.company_id === c.company_id)) return;
    // Equal-weight by default: re-equalize the whole basket on each add.
    setRows((prev) => equalize([
      ...prev,
      { company_id: c.company_id, ticker: c.gurufocus_ticker, name: c.company_name ?? c.gurufocus_ticker, weight: 0 },
    ]));
  };

  const removeRow = (cid: number) => setRows((prev) => prev.filter((r) => r.company_id !== cid));
  const setWeight = (cid: number, w: number) =>
    setRows((prev) => prev.map((r) => (r.company_id === cid ? { ...r, weight: w } : r)));

  const startEdit = (p: Portfolio) => {
    setNotice(null);
    setEditingId(p.id);
    setName(p.name);
    setRows(p.members.map((m) => ({
      company_id: m.company_id,
      ticker: m.ticker ?? '',
      name: m.name ?? m.ticker ?? String(m.company_id),
      weight: m.weight,
    })));
    setErr(null);
  };

  const save = async () => {
    if (!name.trim()) { setErr('Name is required'); return; }
    if (rows.length === 0) { setErr('Add at least one company'); return; }
    setSaving(true);
    setErr(null);
    try {
      const members: PortfolioMemberInput[] = rows.map((r) => ({ company_id: r.company_id, weight: r.weight }));
      const wasEditing = editingId != null;
      const saved = wasEditing
        ? await update(editingId!, { name: name.trim(), members })
        : await create(name.trim(), members);
      onSaved?.(saved);
      resetForm();
      // Explicit confirmation for the header-managed flow (the side flow
      // closes the modal, so this is only seen when staying open).
      setNotice(`${wasEditing ? 'Updated' : 'Saved'} “${saved.name}”`);
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(false);
    }
  };

  const rename = async (p: Portfolio) => {
    const next = await dialog.prompt('Rename portfolio', { defaultValue: p.name, confirmLabel: 'Rename' });
    if (next == null || !next.trim() || next.trim() === p.name) return;
    setBusyId(p.id);
    try { await update(p.id, { name: next.trim() }); }
    catch (e) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setBusyId(null); }
  };

  const del = async (p: Portfolio) => {
    if (!(await dialog.confirm(`Delete "${p.name}"? This cannot be undone.`, { destructive: true, confirmLabel: 'Delete' }))) return;
    setBusyId(p.id);
    try {
      await remove(p.id);
      if (editingId === p.id) resetForm();
    } catch (e) { setErr(e instanceof Error ? e.message : String(e)); }
    finally { setBusyId(null); }
  };

  const addable = companies.filter((c) => !rows.some((r) => r.company_id === c.company_id));

  return (
    <div className="fixed inset-0 z-50 flex items-start justify-center bg-scrim/60 p-6 overflow-y-auto" onClick={onClose}>
      <div
        className="bg-card border border-neutral-800/60 rounded-xl shadow-2xl w-full max-w-3xl my-8"
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-5 py-4 border-b border-neutral-800/60">
          <h2 className="text-fg-strong font-semibold">Portfolios</h2>
          <button onClick={onClose} className="text-fg-subtle hover:text-fg-strong transition-colors text-xl leading-none px-2">×</button>
        </div>

        {err && (
          <div className="mx-5 mt-4 text-sm text-neg-300 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2">{err}</div>
        )}
        {notice && !err && (
          <div className="mx-5 mt-4 text-sm text-pos-300 bg-pos-500/10 border border-pos-500/20 rounded-lg px-3 py-2">{notice} ✓</div>
        )}

        <div className="grid grid-cols-1 md:grid-cols-2 gap-5 p-5">
          {/* Existing portfolios */}
          <div className="space-y-2">
            <h3 className="text-fg-soft text-sm font-medium">Saved portfolios</h3>
            {portfolios.length === 0 && <p className="text-fg-subtle text-sm py-4">None yet — create one on the right.</p>}
            <div className="space-y-1.5">
              {portfolios.map((p) => (
                <div key={p.id} className={`flex items-center gap-2 px-3 py-2 rounded-lg border transition-colors ${editingId === p.id ? 'border-accent-500/40 bg-accent-500/5' : 'border-neutral-800/40 hover:bg-overlay/[0.02]'}`}>
                  <div className="min-w-0 flex-1">
                    <div className="text-fg-strong text-sm truncate">{p.name}</div>
                    <div className="text-fg-faint text-xs">{p.members.length} compan{p.members.length === 1 ? 'y' : 'ies'}</div>
                  </div>
                  {busyId === p.id ? <Spinner size={12} /> : (
                    <div className="flex gap-1">
                      <button onClick={() => startEdit(p)} className="px-2 py-1 text-xs text-fg-muted hover:text-fg-strong hover:bg-overlay/5 rounded transition-colors">Edit</button>
                      <button onClick={() => rename(p)} className="px-2 py-1 text-xs text-fg-muted hover:text-fg-strong hover:bg-overlay/5 rounded transition-colors">Rename</button>
                      <button onClick={() => del(p)} className="px-2 py-1 text-xs text-fg-faint hover:text-neg-400 hover:bg-neg-500/10 rounded transition-colors">Delete</button>
                    </div>
                  )}
                </div>
              ))}
            </div>
          </div>

          {/* Create / edit form */}
          <div className="space-y-3">
            <div className="flex items-center justify-between">
              <h3 className="text-fg-soft text-sm font-medium">{editingId != null ? 'Edit portfolio' : 'New portfolio'}</h3>
              {editingId != null && (
                <button onClick={resetForm} className="text-xs text-fg-subtle hover:text-fg-strong transition-colors">+ New instead</button>
              )}
            </div>
            <input
              value={name}
              onChange={(e) => { setName(e.target.value); setNotice(null); }}
              placeholder="Portfolio name"
              className="w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong placeholder-fg-subtle outline-none focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 transition-colors"
            />
            <CompanyPicker companies={addable} selected={null} onSelect={addCompany} className="w-full" />

            {rows.length > 0 && (
              <div className="space-y-1.5">
                <div className="flex items-center justify-between">
                  <span className="text-fg-faint text-xs">{rows.length} compan{rows.length === 1 ? 'y' : 'ies'} · weights normalize to 100%</span>
                  <button onClick={() => setRows((prev) => equalize(prev))} className="text-xs text-accent-400 hover:text-accent-300 transition-colors">Equalize</button>
                </div>
                <div className="space-y-1 max-h-56 overflow-y-auto">
                  {rows.map((r) => (
                    <div key={r.company_id} className="flex items-center gap-2 text-sm">
                      <span className="font-mono text-accent-400 text-xs w-16 truncate">{r.ticker}</span>
                      <span className="text-fg-soft text-xs truncate flex-1 min-w-0">{r.name}</span>
                      <input
                        type="number"
                        min={0}
                        step="0.05"
                        value={r.weight}
                        onChange={(e) => setWeight(r.company_id, parseFloat(e.target.value) || 0)}
                        className="w-20 bg-page border border-neutral-700 rounded px-2 py-1 text-xs text-fg-strong font-mono outline-none focus:border-accent-500"
                      />
                      <span className="text-fg-faint text-xs font-mono w-12 text-right">
                        {weightSum > 0 ? `${((r.weight / weightSum) * 100).toFixed(0)}%` : '—'}
                      </span>
                      <button onClick={() => removeRow(r.company_id)} className="text-fg-faint hover:text-neg-400 transition-colors px-1">×</button>
                    </div>
                  ))}
                </div>
              </div>
            )}

            <div className="flex gap-2 pt-1">
              <button
                onClick={save}
                disabled={saving || !name.trim() || rows.length === 0}
                className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong disabled:opacity-50 disabled:cursor-not-allowed transition-colors inline-flex items-center gap-2"
              >
                {saving && <Spinner size={12} className="h-3 w-3 text-fg-strong" />}
                {editingId != null ? 'Save changes' : 'Create portfolio'}
              </button>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
