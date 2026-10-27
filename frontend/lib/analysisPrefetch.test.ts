import { describe, expect, it, vi } from 'vitest';
import { analysisRequestKey, loadPrefetchedAnalysis, prefetchAnalysis } from './analysisPrefetch';

describe('Analyse request sharing', () => {
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
