import { afterEach, beforeEach, expect, it, vi } from 'vitest';

const mocks = vi.hoisted(() => ({ start: vi.fn(), invalidate: vi.fn(), dispatch: vi.fn() }));
vi.mock('../../../lib/stores/jobs', () => ({ startJob: mocks.start }));
vi.mock('../../../lib/readCache', () => ({ invalidateReadCache: mocks.invalidate }));
vi.mock('../../../lib/apiUrl', () => ({ API_URL: 'http://test' }));
afterEach(() => { vi.unstubAllGlobals(); vi.restoreAllMocks(); });

beforeEach(() => {
  vi.resetModules();
  vi.clearAllMocks();
  vi.stubGlobal('window', { dispatchEvent: mocks.dispatch });
  vi.stubGlobal('CustomEvent', class { detail: unknown;
    constructor(_name: string, init: { detail: unknown }) { this.detail = init.detail; }
  });
});

it('shares one repair across concurrent charts and reloads readers after it finishes', async () => {
  let finish!: (value: { status: string }) => void;
  const done = new Promise<{ status: string }>((resolve) => { finish = resolve; });
  mocks.start.mockResolvedValue({ done });
  const { ensureBenchmarkData } = await import('./benchmarkRepair');
  const a = ensureBenchmarkData('ACWI', 'annual');
  const b = ensureBenchmarkData('ACWI', 'annual');
  expect(a).toBe(b);
  expect(mocks.start).toHaveBeenCalledTimes(1);
  expect(mocks.invalidate).not.toHaveBeenCalled();
  finish({ status: 'done' });
  await a;
  expect(mocks.invalidate).toHaveBeenCalledTimes(1);
  expect(mocks.dispatch).toHaveBeenCalledWith(expect.objectContaining({ detail: 'ACWI:annual' }));
  await ensureBenchmarkData('ACWI', 'annual');
  expect(mocks.start).toHaveBeenCalledTimes(1);
});

it('keeps a transport-error cooldown instead of repeatedly starting a job per chart', async () => {
  mocks.start.mockRejectedValue(new Error('offline'));
  vi.spyOn(console, 'warn').mockImplementation(() => {});
  const { ensureBenchmarkData } = await import('./benchmarkRepair');
  await ensureBenchmarkData('ACWI', 'annual');
  await ensureBenchmarkData('ACWI', 'annual');
  expect(mocks.start).toHaveBeenCalledTimes(1);
  expect(mocks.invalidate).not.toHaveBeenCalled();
  vi.restoreAllMocks();
});

it('does not invalidate or reload charts when the audit confirms no fetch was needed', async () => {
  mocks.start.mockResolvedValue({ done: Promise.resolve({ status: 'done', dataChanged: false }) });
  const { ensureBenchmarkData } = await import('./benchmarkRepair');
  await ensureBenchmarkData('ACWI', 'annual');
  expect(mocks.invalidate).not.toHaveBeenCalled();
  expect(mocks.dispatch).not.toHaveBeenCalled();
});

it('waits fifteen minutes before retrying a failed data fill', async () => {
  let now = Date.now();
  vi.spyOn(Date, 'now').mockImplementation(() => now);
  mocks.start.mockResolvedValue({ done: Promise.resolve({ status: 'failed', dataChanged: false }) });
  const { ensureBenchmarkData } = await import('./benchmarkRepair');
  await ensureBenchmarkData('ACWI', 'annual');
  await ensureBenchmarkData('ACWI', 'annual');
  expect(mocks.start).toHaveBeenCalledTimes(1);
  expect(mocks.invalidate).not.toHaveBeenCalled();
  now += 15 * 60_000 + 1;
  await ensureBenchmarkData('ACWI', 'annual');
  expect(mocks.start).toHaveBeenCalledTimes(2);
});
