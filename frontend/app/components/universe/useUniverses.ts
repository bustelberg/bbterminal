/**
 * `useUniverses` — the controller for the `/universe` overview page: loads
 * the screening criteria, derived-metric specs, and saved universes; owns
 * the rename / delete-one / delete-all mutations and their coupled
 * interaction state; and groups base universes with their derived children.
 *
 * Lifted out of `UniverseScreener.tsx` so the god-component's ~13 useState
 * slots + four async mutations + the grouping memo become one cohesive
 * concern. The presentational pieces (card, tighten panel, sparkline, …)
 * consume the returned values; the orchestrator just threads this object
 * into `SavedUniverses`.
 */
import { useCallback, useEffect, useMemo, useState } from 'react';

import { dialog } from '../../../lib/dialog';
import { trackedFetch } from '../../../lib/loading';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import type { CriterionDef, DerivedCriterionSpec, FilterConfig, UniverseRow } from './types';

export type UseUniversesResult = ReturnType<typeof useUniverses>;

export function useUniverses() {
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
  const [tighteningId, setTighteningId] = useState<number | null>(null);

  const [derivedSpecs, setDerivedSpecs] = useState<DerivedCriterionSpec[]>([]);
  const [defaultConfig, setDefaultConfig] = useState<FilterConfig>({});

  const loadUniverses = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const r = await trackedFetch('Loading saved universes', `${API_URL}/api/universe/labels`);
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
    trackedFetch('Loading screening criteria', `${API_URL}/api/universe/criteria`)
      .then(r => r.json())
      .then(setCriteria)
      .catch(() => {});
    trackedFetch('Loading derived-metric specs', `${API_URL}/api/universe/derived-metrics/criteria`)
      .then(r => r.json())
      .then(d => {
        setDerivedSpecs(d.specs || []);
        setDefaultConfig(d.default_filter_config || {});
      })
      .catch(() => {});
    loadUniverses();
  }, [loadUniverses]);

  const startRename = useCallback((u: UniverseRow) => {
    setRenamingId(u.universe_id);
    setRenameValue(u.label);
  }, []);

  const cancelRename = useCallback(() => {
    setRenamingId(null);
    setRenameValue('');
  }, []);

  const saveRename = useCallback(async (u: UniverseRow) => {
    const newLabel = renameValue.trim();
    if (!newLabel || newLabel === u.label) {
      cancelRename();
      return;
    }
    setBusyLabel(u.label);
    try {
      const r = await apiFetch(`${API_URL}/api/universe/labels/${encodeURIComponent(u.label)}`, {
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
      dialog.alert(`Rename failed: ${e instanceof Error ? e.message : e}`, { title: 'Rename failed' });
    } finally {
      setBusyLabel(null);
    }
  }, [renameValue, cancelRename, loadUniverses]);

  const deleteOne = useCallback(async (label: string) => {
    setBusyLabel(label);
    try {
      const r = await apiFetch(`${API_URL}/api/universe/labels/${encodeURIComponent(label)}`, {
        method: 'DELETE',
      });
      if (!r.ok) throw new Error(`${r.status}`);
      const data = await r.json().catch(() => ({}));
      const childLabels: string[] = Array.isArray(data?.children) ? data.children : [];
      const toRemove = new Set<string>([label, ...childLabels]);
      setConfirmDelete(null);
      setUniverses(prev => prev.filter(u => !toRemove.has(u.label)));
    } catch (e) {
      dialog.alert(`Delete failed: ${e instanceof Error ? e.message : e}`, { title: 'Delete failed' });
    } finally {
      setBusyLabel(null);
    }
  }, []);

  const deleteAll = useCallback(async () => {
    setBusyLabel('__all__');
    try {
      const r = await apiFetch(`${API_URL}/api/universe/labels`, { method: 'DELETE' });
      if (!r.ok) throw new Error(`${r.status}`);
      setConfirmDeleteAll(false);
      await loadUniverses();
    } catch (e) {
      dialog.alert(`Delete all failed: ${e instanceof Error ? e.message : e}`, { title: 'Delete all failed' });
    } finally {
      setBusyLabel(null);
    }
  }, [loadUniverses]);

  // Group base universes (with their derived children inline beneath them)
  const grouped = useMemo(() => {
    const baseRows = universes.filter(u => !u.is_derived);
    const childrenByParent = new Map<number, UniverseRow[]>();
    for (const u of universes) {
      if (u.is_derived && u.parent_universe_id != null) {
        const arr = childrenByParent.get(u.parent_universe_id) ?? [];
        arr.push(u);
        childrenByParent.set(u.parent_universe_id, arr);
      }
    }
    const orphans = universes.filter(
      u => u.is_derived && (u.parent_universe_id == null || !baseRows.some(b => b.universe_id === u.parent_universe_id))
    );
    return { baseRows, childrenByParent, orphans };
  }, [universes]);

  return {
    // data
    criteria,
    universes,
    loading,
    error,
    derivedSpecs,
    defaultConfig,
    grouped,
    loadUniverses,
    // expand / tighten selection
    expandedId,
    setExpandedId,
    tighteningId,
    setTighteningId,
    // rename
    renamingId,
    renameValue,
    setRenameValue,
    startRename,
    cancelRename,
    saveRename,
    // delete
    confirmDelete,
    setConfirmDelete,
    deleteOne,
    confirmDeleteAll,
    setConfirmDeleteAll,
    deleteAll,
    busyLabel,
  };
}
