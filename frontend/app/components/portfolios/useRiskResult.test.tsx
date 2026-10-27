// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { useRiskResult } from './useRiskResult';

it('automatically retries missing benchmark history rather than caching an unavailable answer', async () => {
  vi.useFakeTimers();
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const root = createRoot(document.createElement('div'));
  const load = vi.fn()
    .mockResolvedValueOnce({ available: false, reason: 'The benchmark does not have complete price history from 2021-01-01.' })
    .mockResolvedValue({ available: true });
  function Probe() {
    const { data } = useRiskResult('missing-benchmark-auto-retry', load, 'test', 'failed');
    return <span>{String(data?.available)}</span>;
  }
  try {
    await act(async () => root.render(<Probe />));
    expect(load).toHaveBeenCalledTimes(1);
    await act(async () => vi.advanceTimersByTime(60_000));
    expect(load).toHaveBeenCalledTimes(2);
    await act(async () => vi.advanceTimersByTime(120_000));
    expect(load).toHaveBeenCalledTimes(2);
  } finally {
    await act(async () => root.unmount());
    vi.useRealTimers();
  }
});

it('does not fetch inactive tabs, then loads them when selected', async () => {
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const host = document.createElement('div');
  const root = createRoot(host);
  const load = vi.fn(async () => ({ value: 'loaded' }));
  function Probe({ enabled, request }: { enabled: boolean; request: string }) {
    const { data } = useRiskResult(request, load, 'test', 'load failed', enabled);
    return <span>{data?.value}</span>;
  }
  try {
    await act(async () => root.render(<Probe enabled={false} request="inactive-risk-one" />));
    expect(load).not.toHaveBeenCalled();
    await act(async () => root.render(<Probe enabled request="inactive-risk-one" />));
    expect(load).toHaveBeenCalledTimes(1);
    expect(host.textContent).toBe('loaded');
    await act(async () => root.render(<Probe enabled={false} request="inactive-risk-two" />));
    expect(load).toHaveBeenCalledTimes(1);
    await act(async () => root.render(<Probe enabled request="inactive-risk-two" />));
    expect(load).toHaveBeenCalledTimes(2);
    await act(async () => root.render(<Probe enabled request="inactive-risk-one" />));
    expect(load).toHaveBeenCalledTimes(2);
  } finally {
    await act(async () => root.unmount());
  }
});
