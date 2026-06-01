/**
 * `useSectorEtfs` — sector → benchmark_id map for
 * `selection_mode='sector_etf'`. Loaded lazily from /api/benchmarks
 * when the user picks Sector ETF mode (and refreshed whenever they pop
 * back to that mode in case they've edited mappings on /benchmarks in
 * another tab).
 *
 * Lives in its own hook so the giant backtester component stops owning
 * the inline effect — and so callers that load a saved config can still
 * write the map directly via `setSectorEtfs`.
 *
 * `sectorEtfsLoading` / `sectorEtfsError` pass through from the shared
 * benchmarks hook (deriving instead of mirroring into useState — the
 * react-hooks/set-state-in-effect rule flags the mirror pattern).
 */
import { useEffect, useMemo, useState } from 'react';

import { useBenchmarks } from '../../../lib/hooks/apiData';

export function useSectorEtfs({ active }: { active: boolean }) {
  const [sectorEtfs, setSectorEtfs] = useState<Record<string, number>>({});

  const { data: rows, loading, error } = useBenchmarks({ enabled: active });

  // Build the map whenever the fetched rows change. The benchmarks hook
  // idles when `active` is false, so `rows` stays null in that case —
  // we still drop the map so we don't keep a stale snapshot around when
  // the user toggles mode. We can't switch to a pure useMemo because
  // the saved-config loader downstream writes to `sectorEtfs` directly
  // (overriding the derived value); the writable slot has to be a real
  // useState.
  useEffect(() => {
    if (!rows) {
      // eslint-disable-next-line react-hooks/set-state-in-effect
      if (!active) setSectorEtfs({});
      return;
    }
    const map: Record<string, number> = {};
    for (const r of rows) {
      if (r.sector) map[r.sector] = r.benchmark_id;
    }
    setSectorEtfs(map);
  }, [rows, active]);

  return useMemo(
    () => ({
      sectorEtfs,
      setSectorEtfs,
      sectorEtfsLoading: loading,
      sectorEtfsError: error,
    }),
    [sectorEtfs, loading, error],
  );
}
