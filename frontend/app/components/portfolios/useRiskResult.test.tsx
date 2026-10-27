// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { useRiskResult } from './useRiskResult';

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
