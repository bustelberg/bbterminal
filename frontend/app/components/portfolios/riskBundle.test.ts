import { beforeEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { watchJob } from '../../../lib/stores/jobs';
import { runSSE } from '../../../lib/stream';
import { invalidateReadCache } from '../../../lib/readCache';
import { readRiskResult } from './useRiskResult';
import { loadRiskBundle, prefetchRiskBundle } from './riskBundle';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../../lib/stores/jobs', () => ({ watchJob: vi.fn() }));
vi.mock('../../../lib/stream', () => ({ runSSE: vi.fn() }));
beforeEach(() => {
  vi.resetAllMocks();
  invalidateReadCache('risk warmup test');
  vi.mocked(runSSE).mockImplementation(async (_url, _init, onEvent) => { onEvent({ type: 'job', status: 'done' }); });
});
const reply = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });

it('loads a warm bundle with one request and no preparation job', async () => {
  const result = { ready: true, views: { volatility: { available: true } } };
  vi.mocked(apiFetch).mockResolvedValue(reply(result));
  expect(await loadRiskBundle('{}', 'Portfolio')).toEqual(result);
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(watchJob).not.toHaveBeenCalled();
});

it('follows a new preparation automatically if data changes between completion and readback', async () => {
  vi.mocked(apiFetch).mockResolvedValueOnce(reply({ ready: false, job_id: 'first' }, 202))
    .mockResolvedValueOnce(reply({ ready: false, job_id: 'replacement' }, 202))
    .mockResolvedValueOnce(reply({ ready: true, views: { volatility: { generation: 2 } } }));
  expect((await loadRiskBundle('{}', 'Portfolio')).views.volatility).toEqual({ generation: 2 });
  expect(String(vi.mocked(runSSE).mock.calls[0][0])).toContain('/first/stream');
  expect(String(vi.mocked(runSSE).mock.calls[1][0])).toContain('/replacement/stream');
  expect(watchJob).not.toHaveBeenCalled();
});

it('bounds continuous invalidation without silently creating another job on the final check', async () => {
  vi.mocked(apiFetch).mockImplementation(async (url) => String(url).includes('prepare_if_missing=false')
    ? reply({ ready: false }, 202) : reply({ ready: false, job_id: 'job' }, 202));
  await expect(loadRiskBundle('{}', 'Portfolio')).rejects.toThrow('still updating');
  expect(runSSE).toHaveBeenCalledTimes(3);
  expect(apiFetch).toHaveBeenCalledTimes(4);
  expect(String(vi.mocked(apiFetch).mock.lastCall?.[0])).toContain('prepare_if_missing=false');
});

it('does not retry a genuine failed preparation indefinitely', async () => {
  vi.mocked(apiFetch).mockResolvedValue(reply({ ready: false, job_id: 'failed' }, 202));
  vi.mocked(runSSE).mockImplementation(async (_url, _init, onEvent) => { onEvent({ type: 'job', status: 'failed' }); });
  await expect(loadRiskBundle('{}', 'Portfolio')).rejects.toThrow('could not be prepared');
  expect(apiFetch).toHaveBeenCalledOnce();
});

it('shares a silent warmup with an early Risk click and seeds its immediately readable result', async () => {
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockImplementation(() => new Promise<Response>((resolve) => { finish = resolve; }));
  prefetchRiskBundle('warm-body', 'Portfolio');
  const visible = loadRiskBundle('warm-body', 'Portfolio');
  expect(apiFetch).toHaveBeenCalledOnce();
  finish(reply({ ready: true, views: { concentration: { available: true } } }));
  const result = await visible;
  expect(readRiskResult('risk-bundle\nwarm-body')).toEqual(result);
  prefetchRiskBundle('warm-body', 'Portfolio');
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(watchJob).not.toHaveBeenCalled();
});

it('does not publish a warmup completed after the client data generation changed', async () => {
  let finish!: (response: Response) => void;
  vi.mocked(apiFetch).mockImplementation(() => new Promise<Response>((resolve) => { finish = resolve; }));
  const warm = loadRiskBundle('changed-body', 'Portfolio');
  invalidateReadCache('new holdings');
  finish(reply({ ready: true, views: {} }));
  await warm;
  expect(readRiskResult('risk-bundle\nchanged-body')).toBeNull();
});
