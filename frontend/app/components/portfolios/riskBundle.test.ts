import { beforeEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { invalidateReadCache } from '../../../lib/readCache';
import { readRiskResult } from './useRiskResult';
import { loadRiskBundle } from './riskBundle';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
beforeEach(() => {
  vi.resetAllMocks();
  invalidateReadCache('risk bundle test');
});
const reply = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });

it('loads all views in one request and shares concurrent reads', async () => {
  const result = { ready: true, views: { volatility: { available: true }, 'weekly-returns': { instruments: {} } } };
  vi.mocked(apiFetch).mockResolvedValue(reply(result));
  const first = loadRiskBundle('{}');
  expect(loadRiskBundle('{}')).toBe(first);
  expect(await first).toEqual(result);
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(readRiskResult('risk-bundle\n{}')).toEqual(result);
});

it('surfaces a failed preparation without another request', async () => {
  vi.mocked(apiFetch).mockResolvedValue(reply({ detail: 'Risk data is still updating.' }, 503));
  await expect(loadRiskBundle('{}')).rejects.toThrow('still updating');
  expect(apiFetch).toHaveBeenCalledOnce();
});

it('returns saved AIRS history during risk updates without caching an incomplete bundle', async () => {
  const partial = { ready: false, available: false, detail: 'Risk data is still updating.',
    views: { 'weekly-returns': { options: [{ id: 'A' }] } } };
  vi.mocked(apiFetch).mockResolvedValue(reply(partial, 202));
  expect(await loadRiskBundle('{}')).toEqual(partial);
  expect(readRiskResult('risk-bundle\n{}')).toBeNull();
});

it('does not cache a response completed after the client data generation changed', async () => {
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockImplementation(() => new Promise<Response>((resolve) => { finish = resolve; }));
  const pending = loadRiskBundle('changed-body');
  invalidateReadCache('new holdings');
  finish(reply({ ready: true, views: {} }));
  await pending;
  expect(readRiskResult('risk-bundle\nchanged-body')).toBeNull();
});
