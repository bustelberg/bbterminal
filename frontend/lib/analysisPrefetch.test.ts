import { describe, expect, it, vi } from 'vitest';
import { analysisDetailsKey, analysisRequestKey, loadPrefetchedAnalysis, prefetchAnalysis, readPrefetchedAnalysis } from './analysisPrefetch';

describe('Analyse request sharing', () => {
  it('paints completed results on reopen but never exposes expired or pending data', async () => {
    vi.useFakeTimers();
    try {
      const key = analysisRequestKey('id:reopen');
      let finish!: (value: object) => void;
      const pending = loadPrefetchedAnalysis(key, () => new Promise<object>((resolve) => { finish = resolve; }));
      expect(readPrefetchedAnalysis(key)).toBeUndefined();
      const result = { book_holdings: ['Stock'], details_pending: false };
      finish(result);
      await pending;
      expect(readPrefetchedAnalysis(key)).toBe(result);
      expect(readPrefetchedAnalysis(analysisRequestKey('id:reopen', 'ACWI', 'book', '', false, 1))).toBeUndefined();
      vi.advanceTimersByTime(30_001);
      expect(readPrefetchedAnalysis(key)).toBeUndefined();
    } finally { vi.useRealTimers(); }
  });
  it('shares in-flight holdings between sleeves and separates every data-changing input', async () => {
    const base = analysisDetailsKey('id:4242', 'ACWI', 'book', false, 0, 0, false);
    let finish!: (value: object) => void;
    const load = vi.fn(() => new Promise<object>((resolve) => { finish = resolve; }));
    const overview = loadPrefetchedAnalysis(`${base}|holdings`, load);
    const bonds = loadPrefetchedAnalysis(`${base}|holdings`, load);
    finish({ book_holdings: ['same positions'] });
    expect(await overview).toBe(await bonds);
    expect(load).toHaveBeenCalledOnce();
    expect(new Set([
      base,
      analysisDetailsKey('id:4242', 'AEX', 'book', false, 0, 0, false),
      analysisDetailsKey('id:4242', 'ACWI', 'model', false, 0, 0, false),
      analysisDetailsKey('id:4242', 'ACWI', 'book', true, 0, 0, false),
      analysisDetailsKey('id:4242', 'ACWI', 'book', false, 1, 0, false),
      analysisDetailsKey('id:4242', 'ACWI', 'book', false, 0, 1, false),
      analysisDetailsKey('id:4242', 'ACWI', 'book', false, 0, 0, true),
    ]).size).toBe(7);
  });
  it('uses exactly the modal key for the default hover prefetch', async () => {
    const key = analysisRequestKey('id:98765');
    expect(key).toBe(analysisRequestKey('id:98765', 'ACWI', 'book', '', false, 0, 0));
    expect(analysisRequestKey('id:98765', 'ACWI', 'book', '', false, 0, 1)).not.toBe(key);
    let finish!: (value: object) => void;
    const load = vi.fn(() => new Promise<object>((resolve) => { finish = resolve; }));
    prefetchAnalysis(key, load);
    const second = vi.fn();
    const waiting = loadPrefetchedAnalysis(analysisRequestKey('id:98765'), second);
    finish({ returns: 3.75 });
    expect(await waiting).toEqual({ returns: 3.75 });
    expect(load).toHaveBeenCalledOnce();
    expect(second).not.toHaveBeenCalled();
  });
});
