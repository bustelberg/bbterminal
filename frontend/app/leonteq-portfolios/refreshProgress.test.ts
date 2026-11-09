import { afterEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../lib/apiFetch';
import { startJob, jobsStore, type JobToast } from '../../lib/stores/jobs';
import { refreshRequest } from './refreshProgress';

vi.mock('../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
vi.mock('../../lib/stores/jobs', async (importOriginal) => ({
  ...await importOriginal<typeof import('../../lib/stores/jobs')>(), startJob: vi.fn(),
}));
afterEach(() => { vi.clearAllMocks(); jobsStore.set({ jobs: [] }); });

it.each([
  ['/api/leonteq/portfolios/login', {}, 'list', undefined],
  ['/api/leonteq/portfolios/composition', { name: 'AI' }, 'composition', undefined],
  ['/api/leonteq/airs/model-portfolios/7/composition?refresh=true&datum=2026-11-06', {}, 'airs', 7],
  ['/api/leonteq/portfolios/monthly-performance', { name: 'AI', airs_portfolio_id: 7 }, 'monthly', 7],
  ['/api/leonteq/price-checks', {}, 'prices', undefined],
])('runs %s through the shared background popup and then reads its result', async (url, body, operation, portfolioId) => {
  vi.mocked(startJob).mockResolvedValue({ id: 'job-1', done: Promise.resolve({ status: 'done' } as JobToast), body: {} });
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ status: 200, payload: { rows: ['saved'] } })));
  const result = await refreshRequest(url, { method: 'POST', body: JSON.stringify(body) }, 'Refresh AI', 'AI', true);
  expect(startJob).toHaveBeenCalledOnce();
  const request = JSON.parse(vi.mocked(startJob).mock.calls[0][2]!.body as string);
  expect(request).toMatchObject({ operation, name: 'AI', lang: 'nl' });
  expect(request.portfolio_id).toBe(portfolioId);
  if (operation === 'airs') expect(request.datum).toBe('2026-11-06');
  expect(apiFetch).toHaveBeenCalledWith(expect.stringContaining('/refresh/jobs/job-1/result'), { cache: 'no-store', noReadCache: true });
  expect(await result.json()).toEqual({ rows: ['saved'] });
});

it('preserves saved fallback data when the background refresh reports failure', async () => {
  vi.mocked(startJob).mockResolvedValue({ id: 'failed', done: Promise.resolve({ status: 'failed', summary: 'Source unavailable' } as JobToast), body: {} });
  const payload = { success: true, cached: true, refresh_error: 'busy', portfolios: [{ name: 'AI' }] };
  vi.mocked(apiFetch).mockResolvedValue(new Response(JSON.stringify({ status: 200, payload })));
  expect(await (await refreshRequest('/api/leonteq/portfolios/login', undefined, 'Leonteq')).json()).toEqual(payload);
});

it('does not read or display an unverified result after cancellation', async () => {
  vi.mocked(startJob).mockResolvedValue({ id: 'cancelled', done: Promise.resolve({ status: 'cancelled' } as JobToast), body: {} });
  await expect(refreshRequest('/api/leonteq/portfolios/login', undefined, 'Leonteq')).rejects.toThrow('Refresh stopped');
  expect(apiFetch).not.toHaveBeenCalled();
});

it('also reports a failure to start in the generic popup', async () => {
  vi.mocked(startJob).mockRejectedValue(new Error('Network unavailable'));
  await expect(refreshRequest('/api/leonteq/portfolios/login', undefined, 'Leonteq')).rejects.toThrow('Network unavailable');
  expect(jobsStore.get().jobs.at(-1)).toMatchObject({ title: 'Leonteq', status: 'failed',
    summary: 'The refresh could not be started. Check the connection and retry.' });
  expect(apiFetch).not.toHaveBeenCalled();
});

it('shows a failed load popup when a completed source result cannot reach the page', async () => {
  vi.mocked(startJob).mockResolvedValue({ id: 'finished', done: Promise.resolve({ status: 'done' } as JobToast), body: {} });
  vi.mocked(apiFetch).mockRejectedValue(new Error('Connection lost'));
  await expect(refreshRequest('/api/leonteq/portfolios/login', undefined, 'Leonteq')).rejects.toThrow('updated data could not be loaded');
  expect(jobsStore.get().jobs.at(-1)).toMatchObject({ status: 'failed', kind: 'leonteq.refresh.result' });
});
