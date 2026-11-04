import { beforeEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { invalidateReadCache } from '../../../lib/readCache';
import { loadPerformanceBundle, readPerformanceBundle } from './monthlyReportPrefetch';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
beforeEach(() => { vi.resetAllMocks(); invalidateReadCache('performance warmup test'); });

it('shares one bundle request between warmup and an early Performance click', async () => {
  let finish!: (value: Response) => void;
  vi.mocked(apiFetch).mockImplementation(() => new Promise((resolve) => { finish = resolve; }));
  const warm = loadPerformanceBundle('BOOK');
  const clicked = loadPerformanceBundle('BOOK');
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(String(vi.mocked(apiFetch).mock.calls[0][0])).toContain('/accounts/BOOK/performance');
  finish(new Response(JSON.stringify({ current_month: '2026-11', years: {} })));
  expect(await clicked).toEqual(await warm);
  expect(readPerformanceBundle('BOOK')?.current_month).toBe('2026-11');
  invalidateReadCache('refreshed AIRS');
  expect(readPerformanceBundle('BOOK')).toBeNull();
});

it('does not retain a failed bundle request', async () => {
  vi.mocked(apiFetch).mockResolvedValueOnce(new Response(JSON.stringify({ detail: 'Unavailable' }), { status: 503 }))
    .mockResolvedValueOnce(new Response(JSON.stringify({ current_month: '2026-11', years: {} })));
  await expect(loadPerformanceBundle('BOOK')).rejects.toThrow('Unavailable');
  expect(readPerformanceBundle('BOOK')).toBeNull();
  expect((await loadPerformanceBundle('BOOK')).current_month).toBe('2026-11');
  expect(apiFetch).toHaveBeenCalledTimes(2);
});
