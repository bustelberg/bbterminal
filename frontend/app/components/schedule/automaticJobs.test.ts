import { describe, expect, it } from 'vitest';
import { jobFailure, summaryLine, type JobRow } from './automaticJobs';

describe('scheduled job receipt summary', () => {
  it('keeps counts and simple failure reasons readable', () => {
    expect(summaryLine({ filled: 4, errors: ['missing statements'], empty: [] }))
      .toBe('filled 4 · errors missing statements');
  });

  it('summarizes nested receipts without coercing objects to strings', () => {
    const line = summaryLine({
      accounts: [{ portefeuille: 'BOOK', issues: ['missing close'] }],
      errors: { audit: { receipt: { incomplete: 1 } } },
      fx_dates: { USD: '2026-10-30', GBP: '2026-10-30' },
    });
    expect(line).toBe('accounts 1 entries · errors 1 entries · fx dates 2 entries');
    expect(line).not.toContain('[object Object]');
  });
});

it('copies compact causes even when scheduler registration overrides the run status', () => {
  const job: JobRow = {
    id: 'daily', label: 'Daily refresh', fills: '', cadence: '', registered: false,
    observable: true, runnable: true, status: 'missing', reason: 'Not registered',
    last_status: 'error', last_run_at: '2026-11-04T05:00:00Z', last_detail: 'Refresh failed',
    last_summary: { errors: { audit: { missing: ['BOOK'] } } },
  };
  expect(jobFailure(job)).toEqual({
    id: 'daily', status: 'missing', last_status: 'error',
    last_run_at: job.last_run_at, reason: 'Not registered', detail: 'Refresh failed',
    causes: [{ message: 'BOOK', affected: ['missing'], affected_count: 1 }],
  });
  expect(jobFailure({ ...job, last_status: 'done' })).toBeNull();
  expect(jobFailure({ ...job, status: 'error', last_status: 'running' })?.detail).toBe('Refresh failed');
});
