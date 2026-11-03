import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';

export type SavedSeries = { points: [number, number][]; coverage: { period: string; companies: number; weight_pct: number }[] };
export type ChartSnapshot = {
  version: number; label: string; cadence: string; built_at: string;
  growth: { metrics: unknown[]; blend_notes?: unknown; member_counts?: unknown; coverage?: unknown };
  series: Record<string, Record<string, SavedSeries>>;
};

// apiFetch shares the one GET across all chart hooks and invalidates it after data writes.
export async function readBenchmarkSnapshot(label: string, cadence: string): Promise<ChartSnapshot> {
  const response = await apiFetch(`${API_URL}/api/benchmarks/index/${encodeURIComponent(label)}/chart-snapshot?cadence=${cadence}`);
  const body = await response.json();
  if (!response.ok) throw new Error(body?.detail ?? `HTTP ${response.status}`);
  return body as ChartSnapshot;
}

/** A saved line is authoritative, including an empty one. Companies still use their raw inputs. */
export function savedBenchmarkSeries(data: unknown, variant: string, fallback: () => Map<number, number>): Map<number, number> {
  const saved = (data as { saved_series?: Record<string, SavedSeries> })?.saved_series;
  if (!saved) return fallback();
  const series = saved[variant];
  if (!series) throw new Error(`Saved benchmark variant is missing: ${variant}`);
  return new Map(series.points);
}
