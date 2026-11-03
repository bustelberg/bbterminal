import { afterEach, expect, it, vi } from 'vitest';
import { runSSE } from '../stream';
import { jobsStore, watchJob, type JobProgress } from './jobs';

vi.mock('../stream', () => ({ runSSE: vi.fn() }));

afterEach(() => {
  jobsStore.set({ jobs: [] });
  vi.clearAllMocks();
});

it('distinguishes a saved AIRS account from start/report narration on the progress stream', async () => {
  vi.mocked(runSSE).mockImplementation(async (_url, _init, onEvent) => {
    onEvent({ type: 'progress', done: 1, total: 22, message: 'Starting Bustelberg' });
    onEvent({ type: 'progress', done: 1, total: 22, message: 'Reports saved',
      account: 'BUS_Offensief_Dyn', account_done: true });
    onEvent({ type: 'job', status: 'done', summary: 'Finished' });
  });
  const progress: JobProgress[] = [];
  await watchJob('airs-refresh', 'Refresh all', 0, undefined, (event) => progress.push(event));

  expect(progress).toHaveLength(2);
  expect(progress[0].accountDone).toBeUndefined();
  expect(progress[1]).toMatchObject({ account: 'BUS_Offensief_Dyn', accountDone: true });
});

it('retains diagnostic lines for Details without replacing simple progress or firing progress listeners', async () => {
  vi.mocked(runSSE).mockImplementation(async (_url, _init, onEvent) => {
    onEvent({ type: 'job', kind: 'fundamentals.benchmark-repair', status: 'running' });
    onEvent({ type: 'event', kind: 'progress', message: 'Checking ACWI data…' });
    onEvent({ type: 'event', kind: 'diagnostic', message: 'metric_presence_rpc_ms: 120.5', profile: { database_ms: 120.5 } });
  });
  const progress: JobProgress[] = [];
  const job = await watchJob('benchmark', 'Checking ACWI', 0, undefined, (event) => progress.push(event));
  expect(job.message).toBe('Checking ACWI data…');
  expect(job.kind).toBe('fundamentals.benchmark-repair');
  expect(job.details).toContain('metric_presence_rpc_ms: 120.5');
  expect(progress).toHaveLength(1);
});
