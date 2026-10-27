import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('cash and certificate accounting', () => {
  it('keeps certificate adjustments visible and in the complete book total', () => {
    expect(source).toContain('const certificateAdjustments = realised?.certificate_adjustments ?? []');
    expect(source).toContain('heldSum.contribution = add2(heldSum.contribution, sum(certificateAdjustments.map((a) => a.contribution_pct)))');
    expect(source).toContain('heldSum.result = add2(heldSum.result, sum(certificateAdjustments.map((a) => a.result_eur)))');
    expect(source).toContain('certificateAdjustments.map((adjustment)');
    expect(source).toContain('Certificate result — not attributed to cash');
  });

  it('uses accounting contributions in the sleeve, not current weight times an unrelated return', () => {
    const sleeve = source.slice(source.indexOf('function SleeveBreakdown('), source.indexOf('const BENCHMARKS'));
    expect(sleeve).toContain('contrib: h.contribution_pct ?? null');
    expect(sleeve).toContain('const rows = holdings.filter((h) => h.bucket === bucket)');
    expect(sleeve).not.toContain('h.weight_pct != null');
    expect(sleeve).not.toContain('(w / 100) * h.own_return_pct');
    expect(sleeve).not.toContain('const c = it.contrib ?? 0');
  });
});
