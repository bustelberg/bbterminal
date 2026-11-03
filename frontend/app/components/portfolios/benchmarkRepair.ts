'use client';

import { useEffect, useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { invalidateReadCache } from '../../../lib/readCache';
import { startJob } from '../../../lib/stores/jobs';
import type { BenchTarget } from './benchSeries';

const recent = new Map<string, { at: number; promise: Promise<void> }>();
const EVENT = 'bb:benchmark-data-updated';

/** One verified repair shared by every chart, including concurrent mounts. */
export function ensureBenchmarkData(universe: string, cadence: string): Promise<void> {
  const key = `${universe}:${cadence}`;
  const existing = recent.get(key);
  if (existing && Date.now() - existing.at < 24 * 60 * 60_000) return existing.promise;
  const promise = (async () => {
    const { done } = await startJob(
      `${API_URL}/api/benchmarks/index/${encodeURIComponent(universe)}/fundamentals/repair/job?cadence=${cadence}`,
      `Checking ${universe} data`);
    const job = await done;
    // A partially failed/cancelled fill can still have saved useful figures.
    if (job.dataChanged !== false) {
      invalidateReadCache('benchmark data check finished');
      window.dispatchEvent(new CustomEvent(EVENT, { detail: key }));
    }
    if (job.status !== 'done') {
      const item = recent.get(key);
      if (item) item.at = Date.now() - (24 * 60 * 60_000 - 15 * 60_000);
    }
  })().catch((error) => {
    console.warn(`[benchmark repair] ${key}`, error);
    // Keep a short cooldown on transport errors rather than triggering once per chart.
    const item = recent.get(key);
    if (item) item.at = Date.now() - (24 * 60 * 60_000 - 60_000);
  });
  recent.set(key, { at: Date.now(), promise });
  return promise;
}

export function useBenchmarkRepair(target: BenchTarget | null | undefined): number {
  const [revision, setRevision] = useState(0);
  const universe = target && 'universe' in target ? target.universe : null;
  const cadence = target?.cadence ?? 'annual';
  useEffect(() => {
    if (!universe) return;
    const key = `${universe}:${cadence}`;
    const updated = (event: Event) => {
      if ((event as CustomEvent<string>).detail === key) setRevision((value) => value + 1);
    };
    window.addEventListener(EVENT, updated);
    // Acquisition belongs to the weekly server job. Opening a chart only reads its saved result.
    return () => { window.removeEventListener(EVENT, updated); };
  }, [universe, cadence]);
  return revision;
}
