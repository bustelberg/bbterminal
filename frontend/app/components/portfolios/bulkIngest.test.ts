import { describe, it, expect } from 'vitest';
import {
  badgeFor, hasMetric, planIngest, summarize,
  type CoverageRow, type MatrixRow, type PlannedHolding,
} from './bulkIngest';

const cov = (isin: string | null, name: string, reason: string, served_by?: string): CoverageRow =>
  ({ isin, name, reason, served_by, weight_pct: 1 });
const mat = (isin: string, years: Record<string, number | null>): MatrixRow =>
  ({ isin, name: isin, status: 'ok', revenue: years });

describe('hasMetric', () => {
  it('reads the values, not the status', () => {
    expect(hasMetric(mat('A', { 2024: 1.2 }))).toBe(true);
    expect(hasMetric(mat('A', {}))).toBe(false);
    expect(hasMetric(mat('A', { 2024: null }))).toBe(false);   // present-but-empty is not data
  });
});

describe('planIngest', () => {
  it('queues the holdings a fetch could fix', () => {
    const p = planIngest([cov('A', 'Alpha', 'no_metrics'), cov('B', 'Beta', 'no_company')], []);
    expect(p.queue.map((q) => q.isin)).toEqual(['A', 'B']);
    expect(p.rows.every((r) => r.state === 'fetch')).toBe(true);
  });

  it('lists holdings a fetch can never fix, each with its reason', () => {
    const p = planIngest(
      [cov(null, 'Cash EUR', 'cash'), cov('C', 'iShares Core', 'fund'), cov('D', 'Bund 2030', 'not_equity')],
      [],
    );
    expect(p.queue).toEqual([]);
    expect(p.rows.map((r) => r.state)).toEqual(['never', 'never', 'never']);
    expect(p.rows.every((r) => (r.note ?? '').length > 0)).toBe(true);
  });

  it('does not re-fetch a holding that already has the metric', () => {
    const p = planIngest([cov('A', 'Alpha', 'covered'), cov('B', 'Beta', 'covered')],
      [mat('A', { 2024: 3 })]);
    expect(p.rows.map((r) => r.state)).toEqual(['present', 'fetch']);   // covered ≠ has THIS metric
    expect(p.queue.map((q) => q.isin)).toEqual(['B']);
  });

  it('is one row per instrument, keyed on the canonical ISIN', () => {
    // The same stock held directly and inside a linked certificate; and an ADR aliased to its
    // home line (`served_by`), which is the id the matrix reports.
    const p = planIngest(
      [cov('A', 'Alpha', 'no_metrics'), cov('A', 'Alpha via cert', 'no_metrics'),
        cov('US874039', 'TSMC ADR', 'no_metrics', 'TW000233')],
      [],
    );
    expect(p.rows.map((r) => r.key)).toEqual(['A', 'TW000233']);
    // The ISIN sent is the holding's own — the backend canonicalises it.
    expect(p.rows[1].isin).toBe('US874039');
  });

  it('counts an aliased holding as present when its canonical has the metric', () => {
    const p = planIngest([cov('US874039', 'TSMC ADR', 'covered', 'TW000233')],
      [mat('TW000233', { 2024: 9 })]);
    expect(p.rows[0].state).toBe('present');
    expect(p.queue).toEqual([]);
  });

  it('keeps two cash lines apart (no ISIN to key on)', () => {
    const p = planIngest([cov(null, 'Cash EUR', 'cash'), cov(null, 'Cash USD', 'cash')], []);
    expect(p.rows.map((r) => r.name)).toEqual(['Cash EUR', 'Cash USD']);
  });
});

describe('badgeFor', () => {
  const row: PlannedHolding = { key: 'A', isin: 'A', name: 'Alpha', state: 'fetch' };

  it('the re-probe outranks the ingest status', () => {
    const b = badgeFor(row, { key: 'A', status: 'ingested' }, true);
    expect(b).toMatchObject({ label: 'present', tone: 'ok' });
  });

  it('a fetch that yielded no line reads as absent, with the reason in the note', () => {
    const b = badgeFor(row, { key: 'A', status: 'ingested', detail: 'loaded 40 rows' }, false);
    expect(b.label).toBe('—');
    expect(b.tone).toBe('muted');
    expect(b.note).toContain('reports none of this line');
  });

  it('calls out unsubscribed — the one absence an ingest can never fix', () => {
    expect(badgeFor(row, { key: 'A', status: 'unsubscribed' }, false))
      .toMatchObject({ label: 'unsubscribed', tone: 'warn' });
  });

  it('a holding not yet reached is pending, not absent', () => {
    expect(badgeFor(row, undefined, false)).toMatchObject({ label: '…', tone: 'pending' });
  });

  it('a never-fetchable holding keeps its own reason', () => {
    const b = badgeFor({ ...row, state: 'never', note: 'cash — no ISIN to look up' }, undefined, false);
    expect(b.note).toBe('cash — no ISIN to look up');
  });
});

describe('summarize', () => {
  it('counts present off the re-probe, not off the statuses', () => {
    const rows: PlannedHolding[] = [
      { key: 'A', isin: 'A', name: 'Alpha', state: 'fetch' },
      { key: 'B', isin: 'B', name: 'Beta', state: 'fetch' },     // ingested, still no metric
      { key: 'C', isin: 'C', name: 'Gamma', state: 'present' },
    ];
    expect(summarize(rows, (k) => k === 'A')).toEqual({ total: 3, present: 2 });
  });
});
