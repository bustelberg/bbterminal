import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it } from 'vitest';
import { BenchmarkOverview } from './JobDetails';

it('explains full membership, selection, available data and individual vendor responses', () => {
  const events = [{ seq: 1, kind: 'diagnostic', message: 'Audit', details: {
    label: 'ACWI', cadence: 'annual', companies: 1948,
    present_companies_by_metric: { revenue: 1654 }, statements_to_fetch: [2], estimates_to_fetch: [],
    unavailable: [{ company_id: 3, company: 'Restricted Co', reason: 'Unsubscribed exchange' }],
    selection: [{ company_id: 2, company: 'Missing Co', ticker: 'MISS', exchange: 'XTER',
      missing_metrics: ['revenue'], statements: 'request', estimates: 'present' }],
    raw_reporting_by_year: [{ metric: 'revenue', year: 2025, companies: 1600 }],
    display_policy: 'At least 500 usable companies per metric and period',
  } }, { seq: 2, kind: 'diagnostic', message: 'Result', details: {
    feed: 'statements', receipt: { attempted: 1, failed: 1, deferred: 0, calls: 1, rows: 0,
      outcomes: [{ company_id: 2, company: 'Missing Co', ticker: 'MISS', exchange: 'XTER',
        status: 'unavailable', reason: 'Empty financial statements', rows: 0, calls: 1 }] },
  } }];
  const html = renderToStaticMarkup(<BenchmarkOverview events={events} />);
  for (const text of ['1,948 companies checked', '1654', '1600', '500 usable', 'Missing Co',
    'XTER', 'Unsubscribed exchange', 'Empty financial statements', '1 attempted', '0 deferred']) {
    expect(html).toContain(text);
  }
});
