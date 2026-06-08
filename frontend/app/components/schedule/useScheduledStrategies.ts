/**
 * `useScheduledStrategies` — all state + server mutations for the
 * "Scheduled strategies" card on `/schedule`.
 *
 * Lifted out of `Schedule.tsx` so the page component stops owning the
 * strategy list, its loading/error flags, the per-strategy run-history
 * cache, the latest-price-date probe, and the five PATCH/DELETE
 * mutations as one cohesive concern (mirrors the `momentum/` hooks).
 * The card consumes this via a single hook call; every handler keeps the
 * same signature it had as an inline `useCallback`.
 */
import { useCallback, useEffect, useState } from 'react';
import { apiFetch } from '../../../lib/apiFetch';
import { dialog } from '../../../lib/dialog';
import { API_URL } from '../../../lib/apiUrl';
import type { StrategyRunHistory } from '../ScheduledStrategyDetail';
import type { ScheduledStrategy } from './types';

export type UseScheduledStrategiesResult = ReturnType<typeof useScheduledStrategies>;

export function useScheduledStrategies() {
  const [strategies, setStrategies] = useState<ScheduledStrategy[]>([]);
  const [strategiesLoading, setStrategiesLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [expandedStrategyId, setExpandedStrategyId] = useState<number | null>(null);
  // Latest available close-price date across all companies (the freshest
  // data the pipeline could compute against). Shown on every strategy row
  // so the user can tell at a glance how current the underlying data is.
  const [latestPriceDate, setLatestPriceDate] = useState<string | null>(null);
  // Per-strategy run-history cache. Survives collapse/re-expand so the
  // detail view renders instantly on a second click; the detail still
  // fires a silent revalidate fetch on every mount to pick up updates.
  const [historyCache, setHistoryCache] = useState<Map<number, StrategyRunHistory>>(new Map());

  const cacheRunHistory = useCallback((id: number, data: StrategyRunHistory) => {
    setHistoryCache((prev) => {
      const next = new Map(prev);
      next.set(id, data);
      return next;
    });
  }, []);

  const loadStrategies = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies`);
      if (!r.ok) return;
      const data = (await r.json()) as ScheduledStrategy[];
      setStrategies(Array.isArray(data) ? data : []);
    } catch {
      // Silent — strategies card just shows empty state
    } finally {
      setStrategiesLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadStrategies();
  }, [loadStrategies]);

  // Fetch the latest available close-price date once on mount.
  useEffect(() => {
    let cancelled = false;
    apiFetch(`${API_URL}/api/data/latest-price-date`)
      .then((r) => (r.ok ? r.json() : null))
      .then((d: { date?: string | null } | null) => {
        if (!cancelled && d?.date) setLatestPriceDate(d.date);
      })
      .catch(() => { /* non-critical — row just omits the date */ });
    return () => { cancelled = true; };
  }, []);

  const toggleStrategy = useCallback(async (id: number, enabled: boolean) => {
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ enabled }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Toggle failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [loadStrategies]);

  const renameStrategy = useCallback(async (id: number, currentName: string) => {
    const next = await dialog.prompt('New name for this scheduled strategy:', {
      title: 'Rename strategy',
      defaultValue: currentName,
    });
    if (next == null) return; // cancelled
    const trimmed = next.trim();
    if (!trimmed || trimmed === currentName) return; // no change
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: trimmed }),
      });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Rename failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [loadStrategies]);

  const removeStrategy = useCallback(async (id: number) => {
    const ok = await dialog.confirm(
      'Remove this strategy from the schedule? Existing snapshots will be preserved.',
      { title: 'Remove scheduled strategy', confirmLabel: 'Remove', destructive: true },
    );
    if (!ok) return;
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies/${id}`, { method: 'DELETE' });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Delete failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      if (expandedStrategyId === id) setExpandedStrategyId(null);
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [expandedStrategyId, loadStrategies]);

  const removeAllStrategies = useCallback(async () => {
    const count = strategies.length;
    if (count === 0) return;
    const ok = await dialog.confirm(
      `Remove all ${count} scheduled strateg${count === 1 ? 'y' : 'ies'}? Existing snapshots will be preserved (their schedule-strategy link goes NULL via cascade, but the holdings stay inspectable).`,
      { title: 'Remove all', confirmLabel: `Remove ${count}`, destructive: true },
    );
    if (!ok) return;
    try {
      const r = await apiFetch(`${API_URL}/api/scheduled-strategies`, { method: 'DELETE' });
      if (!r.ok) {
        const body = await r.text().catch(() => '');
        setError(`Delete all failed: ${r.status} ${body.slice(0, 200)}`);
        return;
      }
      setExpandedStrategyId(null);
      await loadStrategies();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, [strategies.length, loadStrategies]);

  return {
    strategies,
    strategiesLoading,
    error,
    setError,
    expandedStrategyId,
    setExpandedStrategyId,
    latestPriceDate,
    historyCache,
    cacheRunHistory,
    loadStrategies,
    toggleStrategy,
    renameStrategy,
    removeStrategy,
    removeAllStrategies,
  };
}
