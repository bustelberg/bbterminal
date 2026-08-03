import { describe, expect, it } from 'vitest';
import {
  explainHeadline, explainLegRows, explainWarnings, type ExplainTrace,
} from './ytdExplain';

const trace = (over: Partial<ExplainTrace['portfolio']> = {},
               legs: ExplainTrace['legs'] = []): ExplainTrace => ({
  load: {},
  portfolio: {
    portfolio_id: 2015,
    name: 'AITopSelectie OFF FX',
    positions_datum: '2025-12-30',
    positions_scanned_at: '2026-07-13T14:41:42Z',
    ytd_anchor: '2026-01-01',
    anchor_is_inception: false,
    total_weight: 100,
    priced_weight: 100,
    covered_pct: 100,
    low_coverage: false,
    resolved_holdings: 20,
    unresolved_holdings: 0,
    interpolated_holdings: 0,
    ytd_pct: 37.9632,
    sum_of_contributions_pp: 37.9632,
    reconciles: true,
    ...over,
  },
  legs,
});

describe('explainHeadline', () => {
  it('names the model, the window it opened on, and the number', () => {
    expect(explainHeadline(trace())).toBe(
      'YTD derivation — AITopSelectie OFF FX (#2015) = +37.96% from 2026-01-01');
  });

  it('says when the window is the inception, not the year', () => {
    // 27 of 56 models are younger than the year. A partial-year figure that reads like a full
    // one is the whole reason `ytd_anchor` exists — the headline has to carry it.
    expect(explainHeadline(trace({ ytd_anchor: '2026-07-05', anchor_is_inception: true })))
      .toContain('from 2026-07-05 (inception, PARTIAL year)');
  });

  it('does not pretend to explain a trace it does not have', () => {
    expect(explainHeadline({ error: 'no such model portfolio' }))
      .toBe('YTD derivation unavailable — no such model portfolio');
  });
});

describe('explainWarnings', () => {
  it('is silent when the model is fully priced and reconciles', () => {
    expect(explainWarnings(trace())).toEqual([]);
  });

  it('leads with the reconciliation failure', () => {
    // Contributions that do not sum to the YTD mean the legs below are not this number's
    // arithmetic — the dump explains something else, and saying so is the only honest output.
    const w = explainWarnings(trace({ sum_of_contributions_pp: 30, reconciles: false }));
    expect(w[0]).toContain('do NOT sum to the YTD');
  });

  it('flags partial coverage as the reason two environments can disagree', () => {
    // The AITopSelectie case exactly: 19 of 20 priced, renormalised over 95%.
    const w = explainWarnings(trace({
      covered_pct: 95, resolved_holdings: 19, unresolved_holdings: 1,
    }));
    expect(w.join(' ')).toContain('95.0%');
    expect(w.join(' ')).toContain('19 of 20');
  });

  it('separates "no number at all" from "a number over less than everything"', () => {
    const w = explainWarnings(trace({ covered_pct: 41, low_coverage: true }));
    expect(w.join(' ')).toContain('under the floor');
    expect(w.join(' ')).not.toContain('renormalised over');
  });

  it('says when part of the return rests on interpolated opening prices', () => {
    expect(explainWarnings(trace({ interpolated_holdings: 2 })).join(' '))
      .toContain('2 holding(s) were marked at an INTERPOLATED opening price');
  });
});

describe('explainLegRows', () => {
  const legs: ExplainTrace['legs'] = [
    { isin: 'US0079031078', fonds: 'AMD', weight: 5, status: 'priced',
      yahoo_symbol: 'AMD', currency: 'USD', start_date: '2025-12-31',
      start_price_eur: 100, end_date: '2026-07-30', end_price_eur: 232,
      return_pct: 132, weight_pct_of_priced: 5, contribution_pp: 6.6, series_bars: 400 },
    { isin: 'US8740391003', fonds: 'Taiwan Semiconductor', weight: 5,
      status: 'no_mark_at_anchor', series_bars: 6606, series_first: '2026-05-27',
      series_last: '2026-07-29', weight_pct_of_priced: null, contribution_pp: null },
    { isin: null, fonds: 'Liquiditeiten', weight: 2, status: 'cash',
      return_pct: 0, weight_pct_of_priced: 2, contribution_pp: 0 },
  ];

  it('ranks by contribution and puts the unpriced legs last', () => {
    // Unpriced legs have no contribution to rank by, but they ARE the renormalisation — the
    // reason a return differs between two deployments — so they are shown, never dropped.
    const rows = explainLegRows(trace({}, legs));
    expect(rows.map((r) => r.fonds)).toEqual(['AMD', 'Liquiditeiten', 'Taiwan Semiconductor']);
  });

  it('shows an unpriced leg the extent of the series it does have', () => {
    const tsmc = explainLegRows(trace({}, legs))[2];
    expect(tsmc.status).toBe('no_mark_at_anchor');
    expect(tsmc.start).toBe('(first bar 2026-05-27)');
    expect(tsmc.bars).toBe(6606);
    // No invented marks: a leg with no window has no start price, and null renders as an
    // empty console cell rather than a zero.
    expect(tsmc['start €']).toBeNull();
    expect(tsmc['contrib pp']).toBeNull();
  });

  it('marks a look-through leg as one', () => {
    // A certificate wrapping another model has no traded price; its numbers come from the
    // basket behind it, and a reader comparing two environments must be able to see that.
    const rows = explainLegRows(trace({}, [
      { ...legs[0], fonds: 'Star Selection Index', lookthrough: true },
    ]));
    expect(rows[0].status).toBe('priced (look-through)');
  });

  it('carries the renormalised weight, not just the model percentage', () => {
    // `weight` is what AIRS stores; `norm %` is what the return actually carries once the
    // unpriceable legs are out. They differ exactly when coverage < 100%, which is when the
    // number is worth questioning.
    const rows = explainLegRows(trace({}, [
      { ...legs[0], weight: 5, weight_pct_of_priced: 5.263 },
    ]));
    expect(rows[0]['weight %']).toBe(5);
    expect(rows[0]['norm %']).toBe(5.263);
  });
});
