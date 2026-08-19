import { describe, expect, it } from 'vitest';
import { createLiveReload } from './liveReload';

/**
 * The failure this guards is not "it reloaded too often" — it is a table that goes BACKWARDS while
 * the scan goes forwards, because two whole-table reads overlapped and the older one returned last.
 */
function rig(everyMs = 1000) {
  let t = 0;
  const pending: (() => void)[] = [];
  const timers: { fn: () => void; at: number }[] = [];
  let started = 0;
  const lr = createLiveReload(
    () => { started += 1; return new Promise<void>((res) => pending.push(res)); },
    everyMs,
    () => t,
    (fn, ms) => { timers.push({ fn, at: t + ms }); },
  );
  return {
    lr,
    started: () => started,
    /** Resolve the oldest in-flight reload. */
    settle: async () => { pending.shift()?.(); await Promise.resolve(); await Promise.resolve(); },
    tick: (ms: number) => {
      t += ms;
      for (const x of timers.splice(0, timers.length)) {
        if (x.at <= t) x.fn(); else timers.push(x);
      }
    },
  };
}

describe('createLiveReload', () => {
  it('reloads on the first advance', () => {
    const r = rig();
    r.lr.onProgress(1);
    expect(r.started()).toBe(1);
  });

  it('⚠⚠ NEVER RUNS TWO AT ONCE — the whole reason it exists', async () => {
    const r = rig();
    r.lr.onProgress(1);
    for (let i = 2; i <= 20; i += 1) r.lr.onProgress(i);
    // Nineteen more progress lines arrived while the first read was still in flight.
    expect(r.started()).toBe(1);
    expect(r.lr.busy()).toBe(true);
  });

  it('coalesces a burst into exactly ONE follow-up, not one per line', async () => {
    const r = rig(1000);
    r.lr.onProgress(1);
    for (let i = 2; i <= 20; i += 1) r.lr.onProgress(i);
    await r.settle();          // the first read lands; one tail is scheduled
    r.tick(1000);
    expect(r.started()).toBe(2);
    await r.settle();
    r.tick(1000);
    // Nothing new arrived after the burst, so it stops rather than polling forever.
    expect(r.started()).toBe(2);
  });

  it('ignores narration and repeats — a reload costs a whole-table read', () => {
    const r = rig();
    r.lr.onProgress(1);
    expect(r.started()).toBe(1);
    r.lr.onProgress(undefined);
    r.lr.onProgress(1);
    expect(r.started()).toBe(1);
  });

  it('⚠ ignores a LOWER count, so an out-of-order frame cannot walk it backwards', async () => {
    const r = rig(1000);
    r.lr.onProgress(10);
    await r.settle();
    r.tick(1000);
    r.lr.onProgress(4);        // a late frame from earlier in the run
    expect(r.started()).toBe(1);
  });

  it('throttles: a second advance inside the window waits rather than firing', async () => {
    const r = rig(1000);
    r.lr.onProgress(1);
    await r.settle();
    r.lr.onProgress(2);        // < 1000ms since the last one
    expect(r.started()).toBe(1);
    r.tick(1000);
    expect(r.started()).toBe(2);
  });

  it('⚠ a FAILED reload does not stop the next one', async () => {
    let started = 0;
    let t = 0;
    const timers: (() => void)[] = [];
    const lr = createLiveReload(
      () => { started += 1; return Promise.reject(new Error('boom')); },
      0, () => t, (fn) => { timers.push(fn); },
    );
    lr.onProgress(1);
    await Promise.resolve(); await Promise.resolve(); await Promise.resolve();
    expect(started).toBe(1);
    expect(lr.busy()).toBe(false);   // the rejection cleared the in-flight flag
    t += 10;
    lr.onProgress(2);
    expect(started).toBe(2);
  });

  it('never rejects into its caller — it runs inside a progress stream', async () => {
    const lr = createLiveReload(() => Promise.reject(new Error('boom')), 0);
    expect(() => lr.onProgress(1)).not.toThrow();
    await new Promise((res) => { setTimeout(res, 0); });
  });
});
