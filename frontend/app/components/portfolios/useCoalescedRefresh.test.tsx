// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { retrySeriesRead, useCoalescedRefresh } from './useCoalescedRefresh';

it('coalesces progress, preserves in-flight reads, and reads again after the last update', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const root = createRoot(document.createElement('div'));
  const pending: Array<() => void> = [];
  const signals: AbortSignal[] = [];
  const load = vi.fn((signal: AbortSignal) => { signals.push(signal); return new Promise<void>((resolve) => pending.push(resolve)); });
  function Probe({ revision, id = 'Adobe' }: { revision: number; id?: string }) {
    useCoalescedRefresh(id, revision, true, load); return null;
  }
  try {
    await act(async () => root.render(<Probe revision={0} />));
    await act(async () => root.render(<Probe revision={1} />));
    await act(async () => root.render(<Probe revision={30} />));
    expect(load).toHaveBeenCalledTimes(1);
    expect(signals[0].aborted).toBe(false);
    await act(async () => pending.shift()!());
    expect(load).toHaveBeenCalledTimes(2);
    await act(async () => pending.shift()!());
    expect(load).toHaveBeenCalledTimes(2);
    await act(async () => root.render(<Probe revision={30} id="Other" />));
    expect(signals[0].aborted).toBe(true);
    expect(load).toHaveBeenCalledTimes(3);
  } finally {
    await act(async () => root.unmount());
    pending.forEach((resolve) => resolve());
  }
});

it('retries transport failures and server errors, but stops on authentication errors', async () => {
  vi.useFakeTimers();
  const signal = new AbortController().signal;
  const read = vi.fn().mockRejectedValueOnce(new TypeError('Failed to fetch'))
    .mockRejectedValueOnce(Object.assign(new Error('Unavailable'), { status: 503 })).mockResolvedValue('saved Adobe history');
  try {
    const result = retrySeriesRead(read, signal);
    await vi.runAllTimersAsync();
    expect(await result).toBe('saved Adobe history');
    expect(read).toHaveBeenCalledTimes(3);
    const forbidden = vi.fn().mockRejectedValue(Object.assign(new Error('Unauthorized'), { status: 401 }));
    await expect(retrySeriesRead(forbidden, signal)).rejects.toThrow('Unauthorized');
    expect(forbidden).toHaveBeenCalledTimes(1);
  } finally { vi.useRealTimers(); }
});
