/**
 * Pure helpers for the derived-universe filter config: the human-readable
 * pills shown on a derived card, label shortening, a deep-clone for the
 * tighten-panel's editable copy, and the derive-pipeline step definitions.
 */
import type { StepDef } from '../ProgressTimeline';
import type { DerivedCriterionSpec, FilterConfig } from './types';

/** The ordered phases of the derive SSE pipeline (drives ProgressTimeline). */
export const DERIVE_STEPS: StepDef[] = [
  { key: 'validate', label: 'Validate inputs' },
  { key: 'load_base', label: 'Load base memberships' },
  { key: 'precompute', label: 'Precompute derived metrics' },
  { key: 'filter', label: 'Apply filter' },
  { key: 'create', label: 'Create universe row' },
  { key: 'insert', label: 'Insert memberships' },
];

export function shortLabel(s: string): string {
  return s.replace(/\s*\(max\)\s*/i, '').trim();
}

/** Render the active filter entries of a derived universe as compact pills
 * (e.g. `Net debt/EBITDA <= 3`, or component lists for multi-part specs). */
export function buildFilterPills(cfg: FilterConfig, specs: DerivedCriterionSpec[]): string[] {
  const out: string[] = [];
  for (const spec of specs) {
    const entry = cfg[spec.key];
    if (!entry || !entry.enabled) continue;
    if (spec.components) {
      const parts = spec.components.map(c => {
        const v = entry.components?.[c.code] ?? c.default;
        return `${shortLabel(c.label)}≤${v}`;
      });
      out.push(`${spec.label}: ${parts.join(', ')}`);
    } else {
      const v = entry.threshold ?? spec.default_threshold;
      out.push(`${spec.label} ${spec.op ?? '>='} ${v}`);
    }
  }
  return out;
}

/** Deep clone the default filter config so the tighten panel can edit an
 * isolated copy without mutating the shared defaults. */
export function deepCloneConfig(c: FilterConfig): FilterConfig {
  const out: FilterConfig = {};
  for (const [k, v] of Object.entries(c)) {
    out[k] = {
      enabled: v.enabled,
      ...(v.threshold != null ? { threshold: v.threshold } : {}),
      ...(v.components ? { components: { ...v.components } } : {}),
    };
  }
  return out;
}
