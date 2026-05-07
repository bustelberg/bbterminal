import type { PeriodRecord, Summary } from '../../../lib/stores/momentum';

export type SignalDef = {
  key: string;
  label: string;
  description: string;
  default_weight: number;
  group?: string;
};

export type SavedRun = {
  run_id: number;
  name: string;
  created_at: string;
  config: Record<string, unknown>;
  summary: Summary;
};

export type BenchmarkOption = {
  benchmark_id: number;
  ticker: string;
  name: string;
};

export type BenchmarkPrice = {
  target_date: string;
  price: number;
};

// Comparison series — strategies and benchmarks shown side by side on the chart.
// `active` is implicit (derived from the live `result`) and is NOT stored here.
export type ComparisonItem =
  | { id: string; kind: 'saved'; runId: number; label: string; monthly: PeriodRecord[] }
  | { id: string; kind: 'benchmark'; benchmarkId: number; label: string; prices: BenchmarkPrice[] };
