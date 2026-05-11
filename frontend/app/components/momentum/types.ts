import type { DailyRecord, PeriodRecord, Summary } from '../../../lib/stores/momentum';

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
// `daily` is optional on saved runs — present for backtests saved after the
// daily-curve refactor, absent for older payloads (the chart falls back to
// `monthly` then).
/** One variant inside a saved variant bundle. Mirrors what
 * MomentumBacktester saveVariantsBundle persists. */
export type SavedVariant = {
  key: string;
  label: string;
  summary: Summary;
  monthly_records: PeriodRecord[];
  daily_records?: DailyRecord[];
};

export type ComparisonItem =
  | {
      id: string;
      kind: 'saved';
      runId: number;
      label: string;
      monthly: PeriodRecord[];
      daily?: DailyRecord[];
      summary?: Summary;
      // Populated when the saved run is a variant bundle. The badge UI
      // exposes a picker over these so the user can switch which variant
      // is overlaid without re-adding from the "Add series" dropdown.
      // `monthly` / `daily` / `summary` / `label` are derived from
      // `allVariants[variantIndex]` at switch time.
      allVariants?: SavedVariant[];
      variantIndex?: number;
      baseLabel?: string; // user-facing saved-run name (without variant suffix)
    }
  | { id: string; kind: 'benchmark'; benchmarkId: number; label: string; prices: BenchmarkPrice[] };
