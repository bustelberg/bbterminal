/**
 * The Tables tab's comparison target — /research-dashboard's company B.
 *
 * ⚠⚠ WHAT THIS GUARDS IS A SCREEN THAT LOOKED RIGHT. `LongEquityTab` took `compare` and `TablesTab`
 * did not, so on /research-dashboard the Graphs tab drew company A against company B while the
 * Tables tab beside it summarised A against ACWI — two tabs of one modal, one pair of picked names,
 * two different comparisons, and nothing on either tab saying so. `tablesCopy` already records the
 * same rule for the index default: "a summary measured against a different index from the charts it
 * summarises is a summary of something else."
 *
 * Pure — the two facts checked here are a shared constant and a source-level invariant, both of
 * which a component test could not see and `tsc` cannot either.
 */
import { readFileSync } from 'node:fs';
import { join } from 'node:path';

import { describe, expect, it } from 'vitest';

import { benchBody, isUniverseTarget, type BenchTarget } from './benchSeries';

const src = (f: string) => readFileSync(join(__dirname, f), 'utf8');

describe('the comparison sentinel', () => {
  it('⚠⚠ is the SAME string in both tabs, which share one selector shape', () => {
    // Each tab keeps its own copy (one is a heavy chart module, this is a string), so the equality
    // is asserted rather than imported. They collide in one `useState` with the index names, so a
    // drift here is a tab that silently falls back to an index.
    const grab = (f: string) => /const COMPARE_VALUE = '([^']+)'/.exec(src(f))?.[1];
    expect(grab('LongEquityTab.tsx')).toBe('__compare__');
    expect(grab('TablesTab.tsx')).toBe('__compare__');
  });

  it('⚠ is not a name any index could take', () => {
    // It shares a state slot with `CAGR_BENCHMARKS`; an index literally called `__compare__` would
    // make the two indistinguishable.
    expect(src('CagrTable.tsx')).not.toContain('__compare__');
  });
});

describe('⚠⚠ the column header reads the TARGET, never the selection', () => {
  /**
   * `bench` is now either an index name or the sentinel. Printing it puts the literal string
   * `__compare__` in a column header the moment a company is picked — which is exactly the trap
   * `BenchTarget` was shaped to prevent: it carries its own `label` because the universe string
   * was being read as the display name in 47 places.
   */
  it('TablesTab renders no bare `{bench}`', () => {
    const s = src('TablesTab.tsx');
    const offenders = s.split('\n')
      .map((line, i) => ({ line: line.trim(), n: i + 1 }))
      .filter(({ line }) => !line.startsWith('*') && !line.startsWith('//')
        && /\{bench\}/.test(line));
    expect(offenders.map((o) => `TablesTab.tsx:${o.n} ${o.line.slice(0, 60)}`)).toEqual([]);
  });

  it('and derives its label from the target', () => {
    expect(src('TablesTab.tsx')).toContain('const benchLabel = benchTarget.label;');
  });
});

describe('a company target is a one-holding book, an index is a universe', () => {
  /** ⚠ THE REASON THIS NEEDED NO NEW ENDPOINT. Both arms of the union already serialise. */
  const company: BenchTarget = { isin: 'US67066G1040', label: 'NVIDIA Corporation', cadence: 'annual' };
  const index: BenchTarget = { universe: 'ACWI', label: 'ACWI', cadence: 'annual' };

  it('sends the company as a weight-1 holding', () => {
    expect(JSON.parse(benchBody(company))).toEqual({
      holdings: [{ isin: 'US67066G1040', name: 'NVIDIA Corporation', weight: 1 }],
      cadence: 'annual',
    });
  });

  it('sends the index as a universe', () => {
    expect(JSON.parse(benchBody(index))).toEqual({ universe: 'ACWI', cadence: 'annual' });
  });

  it('⚠ and the ONE discriminator tells them apart', () => {
    expect(isUniverseTarget(index)).toBe(true);
    expect(isUniverseTarget(company)).toBe(false);
  });
});

describe('the modal hands the same `compare` to both tabs', () => {
  it('⚠ or Graphs and Tables answer two different questions on one screen', () => {
    const s = src('OwnerEarningsModal.tsx');
    // Both call sites must carry it. Counted rather than located, so a reorder does not fail this.
    expect((s.match(/^\s*compare=\{compare\}/gm) ?? []).length).toBe(2);
  });
});
