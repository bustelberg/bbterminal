import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it } from 'vitest';
import ValuationJobOutcome from './ValuationJobOutcome';
import { JobCard } from './JobToaster';
import type { JobToast } from '../../../lib/stores/jobs';

it.each([
  ['19/20 companies refreshed', '1 failed', 'Successful updates saved.'],
  ['19/20 bedrijven vernieuwd', '1 mislukt', 'Succesvolle updates zijn opgeslagen.'],
])('colors successes and failures separately for %s', (success, failure, remainder) => {
  const html = renderToStaticMarkup(<ValuationJobOutcome message={`${success}; ${failure}. ${remainder}`} />);
  expect(html).toContain(`<span class="text-pos-400">${success}</span>`);
  expect(html).toContain(`<span class="text-neg-400">${failure}</span>`);
  expect(html).toContain(`</span>. ${remainder}`);
});

it('keeps zero failures neutral', () => {
  const html = renderToStaticMarkup(<ValuationJobOutcome message="20/20 companies refreshed; 0 failed. Successful updates saved." />);
  expect(html).toContain('<span class="text-fg-muted">0 failed</span>');
  expect(html).not.toContain('text-neg-400');
});

it('uses a neutral valuation bar and omits the overall failure badge', () => {
  const job: JobToast = { id: 'test', title: 'Refresh valuation', kind: 'valuation.portfolio', label: 'test',
    status: 'failed', done: 20, total: 20, message: '', apiCalls: 0, cancelRequested: false, dismissed: false,
    summary: '19/20 companies refreshed; 1 failed. Successful updates saved.' };
  const html = renderToStaticMarkup(<JobCard job={job} lang="en" />);
  expect(html).toContain('h-full bg-overlay/30');
  expect(html).not.toContain('bg-neg-500');
  expect(html).not.toContain('Failed ·');
  expect(html).toContain('<span class="text-neg-400">1 failed</span>');
  expect(html).toContain('<span class="text-pos-400">19/20 companies refreshed</span>');
  const other = renderToStaticMarkup(<JobCard job={{ ...job, kind: 'other' }} lang="en" />);
  expect(other).toContain('bg-neg-500');
  expect(other).toContain('Failed ·');
});
