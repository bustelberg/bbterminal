import { expect, it } from 'vitest';
import fixtures from '../../../../backend/tests/fixtures/chart_statistics_parity.json';
import { logLinearFit } from '../../../lib/trendFit';
import { endpointCagr } from './lineCagr';
import { savedFit, type ChartStatistics } from './chartStatistics';

it.each(fixtures)('precomputed $name statistics agree with the existing chart formulas', (fixture) => {
  const prepared = fixture.statistics as ChartStatistics;
  const fit = logLinearFit(fixture.points);
  const actual = savedFit(prepared, fixture.points);
  for (const key of ['r2', 'slope', 'intercept', 'cagr'] as const) {
    if (fit[key] == null) expect(actual[key]).toBeNull();
    else expect(actual[key]).toBeCloseTo(fit[key]!, 9);
  }
  expect(actual.n).toBe(fit.n);
  expect(actual.dropped).toBe(fit.dropped);
  const endpoint = endpointCagr(fixture.points);
  if (endpoint.pct == null) expect(prepared.cagr.pct).toBeNull();
  else expect(prepared.cagr.pct).toBeCloseTo(endpoint.pct, 9);
});
