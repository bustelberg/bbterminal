import { describe, expect, it } from 'vitest';
import { levelFromBlend, type BlendMetricRow } from './fundamentalBlend';

/**
 * READING THE SERVER'S LINE — the change that stopped the `Tables` tab answering from a
 * reconstruction.
 *
 * ⚠⚠ THE TWO TABS OF ONE MODAL DISAGREED FOUR TIMES IN A DAY: share price 10.91 against 10.89,
 * EPS 15.95 against 16.82, FCF/share 18.85 against 18.90, EPS again 8.31 against 8.32. Not a bug
 * in either implementation — `Graphs` asked `/fundamental-blend-metrics` for the blended line while
 * `Tables` asked `/portfolio-revenue-matrix` for the RAW per-company figures and rebuilt it with
 * `buildBlend`. Two sources for one series.
 *
 * ⚠ AND THE REBUILD COULD NEVER HAVE BEEN EXACT: the matrix is specified as the ground data behind
 * the chart, not as the blend's inputs, so it is a strict subset of them. The `LTM` point, a period
 * carried past the coverage floor and the member list itself were each missing at some point.
 *
 * Verified against a live ACWI payload the day this shipped — every row landed on the server's own
 * figure to the printed decimal: eps_nri +8.31%, fcf_ps +7.52%, price_ps +10.91%.
 */
const row = (metric_code: string, target_date: string,
             numeric_value: number | null): BlendMetricRow =>
  ({ metric_code, target_date, numeric_value });

const FCF = 'annuals__Per Share Data__Free Cash Flow per Share';
const FCF_LOWER = 'annuals__per_share_data__Free Cash Flow per Share';
const EPS = 'annuals__Per Share Data__EPS without NRI';
const EST = 'annual_eps_nri_estimate';

describe('levelFromBlend', () => {
  it('keys a filed period by its fiscal year', () => {
    const level = levelFromBlend(
      [row(FCF, '2015-12-31', 100), row(FCF, '2016-12-31', 110)], [FCF]);
    expect(level).toEqual({ 2015: { value: 100 }, 2016: { value: 110 } });
  });

  it('⚠ suffixes a consensus with `e`, which is the vocabulary `lineCagr` speaks', () => {
    // The server ships the forecast under its own metric code with an ordinary date, so the suffix
    // is the ONLY thing separating a consensus from a filed year downstream — and `lineCagr`
    // refuses an estimate endpoint precisely so a forecast cannot wear a track record's clothes.
    const level = levelFromBlend(
      [row(EPS, '2025-12-31', 200), row(EST, '2026-12-31', 240)], [EPS], [EST]);
    expect(level).toEqual({ 2025: { value: 200 }, '2026e': { value: 240 } });
  });

  it('⚠ ignores the LTM, which ships under its own code', () => {
    // Every consumer (`lineCagr`, `forwardCagr`, `commonEndPeriod`) filters to periods
    // `periodYear` can parse, so carrying it would add a key nothing reads.
    const level = levelFromBlend(
      [row(FCF, '2025-12-31', 100), row('ltm__Per Share Data__Free Cash Flow per Share',
                                        '2026-06-30', 130)], [FCF]);
    expect(level).toEqual({ 2025: { value: 100 } });
  });

  it('⚠ takes the LATEST date when one period arrives twice', () => {
    // A metric with two section spellings can carry one period under both — the vendor renamed its
    // sections and `metric_data` holds whichever was current per company. Taking whichever arrived
    // first would make the line depend on row order.
    const level = levelFromBlend([
      row(FCF, '2016-06-30', 1), row(FCF_LOWER, '2016-12-31', 2),
    ], [FCF, FCF_LOWER]);
    expect(level).toEqual({ 2016: { value: 2 } });
  });

  it('ignores codes it was not asked for, and nulls', () => {
    const level = levelFromBlend([
      row(EPS, '2015-12-31', 50),           // a different metric's line
      row(FCF, '2015-12-31', null),         // no figure
      row(FCF, '2016-12-31', 10),
    ], [FCF]);
    expect(level).toEqual({ 2016: { value: 10 } });
  });

  it('⚠ is empty rather than partial when the payload carries none of it', () => {
    // The caller renders an empty level as "no line", which is a different state from a line drawn
    // over one point — and a row of dashes with a reason beats a confident wrong rate.
    expect(levelFromBlend([row(EPS, '2015-12-31', 50)], [FCF])).toEqual({});
    expect(levelFromBlend([], [FCF])).toEqual({});
  });
});
