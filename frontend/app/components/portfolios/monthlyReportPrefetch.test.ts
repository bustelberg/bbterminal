import { beforeEach, expect, it, vi } from 'vitest';
import { apiFetch } from '../../../lib/apiFetch';
import { invalidateReadCache } from '../../../lib/readCache';
import { loadPerformanceTiles, readPerformanceTiles } from './monthlyReportPrefetch';

vi.mock('../../../lib/apiFetch', () => ({ apiFetch: vi.fn() }));
beforeEach(() => { vi.resetAllMocks(); invalidateReadCache('performance warmup test'); });

it('shares one lightweight request between warmup and an early Performance click', async () => {
  let finish!: (value: Response) => void;
  vi.mocked(apiFetch).mockImplementation(() => new Promise((resolve) => { finish = resolve; }));
  const warm = loadPerformanceTiles('BOOK', '2026-11');
  const clicked = loadPerformanceTiles('BOOK', '2026-11');
  expect(apiFetch).toHaveBeenCalledOnce();
  expect(String(vi.mocked(apiFetch).mock.calls[0][0])).toContain('include_details=false');
  finish(new Response(JSON.stringify({ month: '2026-11', snapshot: null, months: [] })));
  expect(await clicked).toEqual(await warm);
  expect(readPerformanceTiles('BOOK', '2026-11')?.month).toBe('2026-11');
  invalidateReadCache('refreshed AIRS');
  expect(readPerformanceTiles('BOOK', '2026-11')).toBeNull();
});

it('does not retain a failed tile request', async () => {
  vi.mocked(apiFetch).mockResolvedValueOnce(new Response(JSON.stringify({ detail: 'Unavailable' }), { status: 503 }))
    .mockResolvedValueOnce(new Response(JSON.stringify({ month: '2026-11', snapshot: null, months: [] })));
  await expect(loadPerformanceTiles('BOOK', '2026-11')).rejects.toThrow('Unavailable');
  expect(readPerformanceTiles('BOOK', '2026-11')).toBeNull();
  expect((await loadPerformanceTiles('BOOK', '2026-11')).month).toBe('2026-11');
  expect(apiFetch).toHaveBeenCalledTimes(2);
});
