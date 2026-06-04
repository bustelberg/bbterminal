/**
 * Shared types for the `/universe` overview (screener) page. Lifted out of
 * `UniverseScreener.tsx` so the controller hook, the card, and the tighten
 * panel share one definition.
 */

export type CriterionDef = { key: string; label: string; description?: string; min_years?: number };

export type ComponentSpec = { label: string; code: string; default: number };

export type DerivedCriterionSpec = {
  key: string;
  label: string;
  default_threshold: number;
  default_enabled: boolean;
  metric?: string;
  op?: string;
  components?: ComponentSpec[];
};

export type FilterConfigEntry = {
  enabled: boolean;
  threshold?: number;
  components?: Record<string, number>;
};

export type FilterConfig = Record<string, FilterConfigEntry>;

export type SectorCount = { sector: string; count: number };

export type MonthlyCount = { month: string; count: number; base_count?: number };

export type UniverseRow = {
  universe_id: number;
  label: string;
  description: string | null;
  created_at: string;
  parent_universe_id: number | null;
  parent_label: string | null;
  filter_config: FilterConfig | null;
  is_derived: boolean;
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

/** Result of `POST /api/universe/derive/preview` — the dry-run row counts
 * the tighten panel shows before the user commits to creating a universe. */
export type Preview = {
  monthly_counts: MonthlyCount[];
  base_rows: number;
  passed_rows: number;
  missing_metrics: number;
  base_label: string;
};
