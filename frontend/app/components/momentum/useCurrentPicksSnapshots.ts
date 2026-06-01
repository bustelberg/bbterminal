/**
 * `useCurrentPicksSnapshots` — CRUD for saved current-picks snapshots on
 * `/backtest`'s "Load saved current picks" dropdown: label, rename, delete
 * (single + bulk), and the multi-select set. Self-contained — reads the
 * snapshot list from the store and owns the selection + bulk-delete state.
 * Lifted out of `MomentumBacktester.tsx`.
 */
import { useState } from 'react';

import { dialog } from '../../../lib/dialog';
import {
  deleteCurrentPicksSnapshot,
  renameCurrentPicksSnapshot,
} from '../../../lib/stores/momentum';

export function useCurrentPicksSnapshots() {
  const [selectedSnapshotIds, setSelectedSnapshotIds] = useState<Set<number>>(new Set());
  const [bulkDeletingSnapshots, setBulkDeletingSnapshots] = useState(false);

  const snapshotLabel = (s: { name?: string | null; created_at: string; triggered_by: string; as_of_date: string }): string => {
    const trimmed = (s.name ?? '').trim();
    if (trimmed) return trimmed;
    return `${s.created_at.slice(0, 10)} · ${s.triggered_by} · ${s.as_of_date.slice(0, 7)}`;
  };

  const renameSnapshot = async (snapshotId: number, currentName: string | null | undefined) => {
    const next = await dialog.prompt('Name for this snapshot (leave empty to clear):', {
      title: 'Rename snapshot',
      defaultValue: currentName ?? '',
    });
    if (next == null) return; // user cancelled
    const trimmed = next.trim();
    if (trimmed === (currentName ?? '').trim()) return; // no change
    await renameCurrentPicksSnapshot(snapshotId, trimmed === '' ? null : trimmed);
  };

  const confirmDeleteSnapshot = async (s: { snapshot_id: number; name?: string | null; created_at: string; triggered_by: string; as_of_date: string }) => {
    const label = snapshotLabel(s);
    if (await dialog.confirm(`Delete snapshot "${label}"?`, { destructive: true, confirmLabel: 'Delete' })) {
      await deleteCurrentPicksSnapshot(s.snapshot_id);
    }
  };

  /** Bulk-delete handler fires all DELETE requests in parallel. The
   * dropdown component owns selection state and passes the selected
   * `ids` in; we confirm once up front, fire in parallel, then prune the
   * list + clear `loadedRunId` if the active run was caught in the
   * delete. */

  const bulkDeleteSnapshots = async () => {
    const ids = Array.from(selectedSnapshotIds);
    if (ids.length === 0) return;
    const ok = await dialog.confirm(
      `Delete ${ids.length} current-picks snapshot${ids.length === 1 ? '' : 's'}?`,
      { destructive: true, confirmLabel: `Delete ${ids.length}` },
    );
    if (!ok) return;
    setBulkDeletingSnapshots(true);
    try {
      // deleteCurrentPicksSnapshot already updates the store (removes
      // from currentPicksSnapshots, clears currentPortfolio if matched).
      // Run in parallel for speed.
      await Promise.all(ids.map((id) => deleteCurrentPicksSnapshot(id)));
      setSelectedSnapshotIds(new Set());
    } finally {
      setBulkDeletingSnapshots(false);
    }
  };

  const toggleSnapshotSelected = (snapshotId: number) => {
    setSelectedSnapshotIds((prev) => {
      const next = new Set(prev);
      if (next.has(snapshotId)) next.delete(snapshotId); else next.add(snapshotId);
      return next;
    });
  };

  return {
    selectedSnapshotIds, setSelectedSnapshotIds, bulkDeletingSnapshots,
    snapshotLabel, renameSnapshot, confirmDeleteSnapshot, bulkDeleteSnapshots, toggleSnapshotSelected,
  };
}
