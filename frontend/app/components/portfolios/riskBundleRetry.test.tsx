// @vitest-environment happy-dom
import React, { act } from 'react';
import { createRoot } from 'react-dom/client';
import { expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { useRiskBundle } from './riskBundle';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));

it('retries a pending bundle and stops polling when risk becomes ready', async () => {
  vi.useFakeTimers();
  Object.assign(globalThis, { IS_REACT_ACT_ENVIRONMENT: true });
  const reply = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
  vi.mocked(apiFetch)
    .mockResolvedValueOnce(reply({ ready: false, detail: 'Still updating', views: { 'weekly-returns': {} } }, 202))
    .mockResolvedValue(reply({ ready: true, views: { volatility: {} } }));
  const host = document.createElement('div');
  const root = createRoot(host);
  function Probe() {
    const result = useRiskBundle('retry-test');
    return <span>{result.error ?? 'Ready'} {Object.keys(result.views).join(',')}</span>;
  }
  try {
    await act(async () => root.render(<Probe />));
    expect(host.textContent).toContain('weekly-returns');
    expect(host.textContent).toContain('Still updating');
    await act(async () => vi.advanceTimersByTime(10_000));
    expect(apiFetch).toHaveBeenCalledTimes(2);
    expect(host.textContent).toBe('Ready volatility');
    await act(async () => vi.advanceTimersByTime(60_000));
    expect(apiFetch).toHaveBeenCalledTimes(2);
  } finally {
    await act(async () => root.unmount());
    vi.useRealTimers();
    vi.clearAllMocks();
  }
});
