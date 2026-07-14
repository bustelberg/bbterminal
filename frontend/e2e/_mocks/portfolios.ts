import type { Page } from '@playwright/test';

/**
 * Fixtures for /portfolios. Four rows, chosen because each one renders the performance
 * columns DIFFERENTLY — and the whole point of those columns is that "we have no number"
 * and "the number is zero" must never look alike.
 *
 *   HEALTHY   a model that predates the year: real YTD, real since-inception, real ratios.
 *   HINDSIGHT the MoTopSelectie_FX case — defined 8 days ago, so its YTD is a backtest (⚠)
 *             and its ratios are withheld: 5 daily returns is not a Sharpe.
 *   THIN      under the 60% coverage floor: no YTD, no since, no ratios (TOPS_OFF_BEH, which
 *             once reported "+0.00%" off its 1% cash line).
 *   NOMODEL   a `normaal` portfolio: AIRS stores no composition at all, so it has no
 *             performance row at all.
 */
export const FIXTURE_PORTFOLIOS = [
  {
    id: 2015,
    name: 'AITopSelectie OFF FX',
    truncated: false,
    omschrijving: 'AI top selection',
    portfolio_type: 'fixed (14.5)',
    fixed_datum: '2025-12-29',        // deliberately != positions_datum, the 39-of-56 case
    positions_datum: '2025-12-30',
    positions_scanned_at: '2026-07-13T10:00:00Z',
    has_fixed_model: true,
    no_snapshot: false,
    holdings: 24,
    scanned_at: '2026-07-13T10:00:00Z',
  },
  {
    id: 2088,
    name: 'MoTopSelectie_FX',
    truncated: false,
    omschrijving: 'Momentum top selection',
    portfolio_type: 'fixed (0)',
    fixed_datum: '2026-07-05',
    positions_datum: '2026-07-05',
    positions_scanned_at: '2026-07-13T10:00:00Z',
    has_fixed_model: true,
    no_snapshot: false,
    holdings: 26,
    scanned_at: '2026-07-13T10:00:00Z',
  },
  {
    id: 2101,
    name: 'TOPS_OFF_BEH',
    truncated: false,
    omschrijving: 'Structured products',
    portfolio_type: 'fixed (0)',
    fixed_datum: '2026-05-29',
    positions_datum: '2026-05-29',
    positions_scanned_at: '2026-07-13T10:00:00Z',
    has_fixed_model: true,
    no_snapshot: false,
    holdings: 10,
    scanned_at: '2026-07-13T10:00:00Z',
  },
  {
    // A MATURE model — running over a year, so it is the one row that can honestly carry a CAGR.
    id: 2030,
    name: 'BUS_Risicodragend',
    truncated: false,
    omschrijving: 'Risk-bearing',
    portfolio_type: 'fixed (0)',
    fixed_datum: '2025-05-26',
    positions_datum: '2025-04-08',
    positions_scanned_at: '2026-07-13T10:00:00Z',
    has_fixed_model: true,
    no_snapshot: false,
    holdings: 30,
    scanned_at: '2026-07-13T10:00:00Z',
  },
  {
    // A single-ETF wrapper — a COUNTED model holding one instrument. Hidden by default by
    // "Hide small portfolios", and the row that proves the filter keys on the count and not on
    // the absence of one.
    id: 2140,
    name: 'TOPS_MTS_L',
    truncated: false,
    omschrijving: 'Single-instrument wrapper',
    portfolio_type: 'fixed (0)',
    fixed_datum: '2025-01-02',
    positions_datum: '2025-01-02',
    positions_scanned_at: '2026-07-13T10:00:00Z',
    has_fixed_model: true,
    no_snapshot: false,
    holdings: 1,
    scanned_at: '2026-07-13T10:00:00Z',
  },
  {
    id: 2120,
    name: 'BUS_Neutraal_Dyn',
    truncated: false,
    omschrijving: 'Benchmark wrapper',
    portfolio_type: 'normaal',
    fixed_datum: '2023-07-21',
    positions_datum: null,
    positions_scanned_at: '2026-07-13T10:00:00Z',
    has_fixed_model: false,
    no_snapshot: false,
    holdings: null,
    scanned_at: '2026-07-13T10:00:00Z',
  },
];

export const FIXTURE_PERFORMANCE = [
  {
    // Running 1.27 years, so its rate is MEASURED rather than extrapolated: +48.26% compounded
    // over that long IS +35.97% a year. The only row here entitled to a CAGR.
    portfolio_id: 2030,
    name: 'BUS_Risicodragend',
    model_effective: '2025-04-08',
    model_changed_in_period: false,
    ytd_pct: 8.16,
    ytd_from: '2026-01-01',
    since_model_pct: 48.26,
    sharpe: 1.98,
    sortino: 3.25,
    cagr_pct: 35.97,
    years_running: 1.27,
    ann_vol_pct: 18.2,
    stat_days: 323,
    resolved_holdings: 30,
    unresolved_holdings: 0,
    priced_holdings: 30,
    unpriced_holdings: 0,
    covered_pct: 100,
    since_covered_pct: 100,
    low_coverage: false,
    partial_coverage: false,
    interpolated_holdings: 0,
    cash_pct: 0,
  },
  {
    portfolio_id: 2015,
    name: 'AITopSelectie OFF FX',
    model_effective: '2025-12-30',
    model_changed_in_period: false,
    ytd_pct: 51.48,
    ytd_from: '2026-01-01',        // predates the year -> a real, full YTD
    since_model_pct: 50.61,
    sharpe: 3.65,
    sortino: 5.61,
    // Running 0.54 years — UNDER ONE. So no CAGR: annualizing its +50.61% over 135 trading days
    // would print +114.8%, beside models that earned their rate over two years.
    cagr_pct: null,
    years_running: 0.54,
    ann_vol_pct: 31.47,
    stat_days: 135,
    resolved_holdings: 24,        // every instrument has a Yahoo series
    unresolved_holdings: 0,
    priced_holdings: 24,
    unpriced_holdings: 0,
    covered_pct: 100,
    since_covered_pct: 100,
    low_coverage: false,
    partial_coverage: false,
    cash_pct: 0,
  },
  {
    // Defined 8 days before it was measured. Its YTD window opens at the INCEPTION, not 1 Jan
    // (priced back to January it would read +75.78% on weights it never held), so the YTD is a
    // partial year and equals the since-inception return, by construction. One daily return is
    // still not a Sharpe.
    portfolio_id: 2088,
    name: 'MoTopSelectie_FX',
    model_effective: '2026-07-05',
    model_changed_in_period: true,
    ytd_pct: 0.51,
    ytd_from: '2026-07-05',
    since_model_pct: 0.51,
    sharpe: null,
    sortino: null,
    cagr_pct: null,
    years_running: 0.02,          // eight days. Nothing is annualizable from that.
    ann_vol_pct: null,
    stat_days: 1,
    resolved_holdings: 26,
    unresolved_holdings: 0,
    priced_holdings: 26,          // 26 instruments + a cash line
    unpriced_holdings: 0,
    covered_pct: 100,
    since_covered_pct: 100,
    low_coverage: false,
    partial_coverage: false,
    cash_pct: 20,
  },
  {
    // 1% of its weight is priceable (its cash line). Every number is refused.
    portfolio_id: 2101,
    name: 'TOPS_OFF_BEH',
    model_effective: '2026-05-29',
    model_changed_in_period: true,
    ytd_pct: null,
    ytd_from: null,
    since_model_pct: null,
    sharpe: null,
    sortino: null,
    cagr_pct: null,
    years_running: 0.13,
    ann_vol_pct: null,
    stat_days: 0,
    // Nine structured products, none of them priceable, plus the 1% cash line that is.
    resolved_holdings: 0,
    unresolved_holdings: 9,
    priced_holdings: 1,
    unpriced_holdings: 9,
    covered_pct: 1,
    since_covered_pct: 0,
    low_coverage: true,
    partial_coverage: false,
    cash_pct: 1,
  },
];

/**
 * One portfolio's positions, as the expand fetches them. Four rows, because the five price
 * columns have four distinct states and only one of them is "a number":
 *
 *   priced      — an EUR entry mark, an EUR exit mark, and the return between them.
 *   stale end   — its last close LAGS the others (vendors publish unevenly). Not an error: the
 *                 position is marked at its last known price, and the date says so.
 *   unresolved  — no Yahoo series at all (a queued ETF, an in-house fund). No marks, and NOT a
 *                 0% return.
 *   cash        — no ISIN, no series. Priced at a flat 0% inside the portfolio figure.
 */
export const FIXTURE_POSITIONS = {
  portfolio: 'AITopSelectie OFF FX',
  portfolio_id: 2015,
  datum: '2025-12-30',
  dates: ['2025-12-30'],
  ytd_from: '2026-01-01',
  matched: 2,
  unmatched: 1,
  cached_at: '2026-07-13T10:00:00Z',
  rows: [
    {
      fonds: 'Amazon', isin: 'US0231351067', percentage: 5, valuta: 'USD',
      categorie: 'Aandelen', sector: 'Consumer', regio: 'US', known_instrument: true,
      currency: 'USD',
      start_date: '2025-12-31', start_price_eur: 196.44, start_price_local: 224.92,
      end_date: '2026-07-02', end_price_eur: 212.89, end_price_local: 249.61,
      return_pct: 8.37,
    },
    {
      fonds: 'Apple', isin: 'US0378331005', percentage: 5, valuta: 'USD',
      categorie: 'Aandelen', sector: 'Tech', regio: 'US', known_instrument: true,
      currency: 'USD',
      start_date: '2025-12-31', start_price_eur: 411.59, start_price_local: 471.28,
      end_date: '2026-06-30', end_price_eur: 342.57, end_price_local: 401.65,  // a lagging close
      return_pct: -16.77,
    },
    {
      // A queued ETF: in the model, not in our price grid. No marks — and NOT a 0% return.
      fonds: 'Global X SuperDividend', isin: 'IE00077FRP95', percentage: 5, valuta: 'USD',
      categorie: 'ETF', sector: 'Dividend', regio: 'World', known_instrument: false,
    },
    {
      // The iShares Euro HY case: mapped to a listing that trades a handful of times a year, so
      // there is no close anywhere near 1 Jan. Its opening price is INTERPOLATED between the two
      // real closes bracketing the date — 127 days apart — and must say so.
      fonds: 'Ishares Euro HY Corp Bd', isin: 'IE00B66F4759', percentage: 23.6, valuta: 'EUR',
      categorie: 'BUS-OBL-HighY', sector: 'Bonds', regio: 'BU-Europa', known_instrument: true,
      currency: 'USD',
      start_date: '2026-01-01', start_price_eur: 92.63, start_price_local: null,
      start_interpolated: true, start_gap_days: 127,
      end_date: '2026-06-02', end_price_eur: 91.42, end_price_local: 106.5,
      return_pct: -1.31,
    },
    {
      fonds: 'Liquiditeiten', isin: null, percentage: 5, valuta: 'EUR',
      categorie: 'Liquiditeiten', sector: null, regio: null, known_instrument: false,
    },
  ],
};

/**
 * The Analyse modal's payload. The buckets are the point: a real sector, a FUND bucket (we hold
 * no look-through — an ETF's listing tells you nothing about its contents), and Cash.
 */
export const FIXTURE_ANALYSIS = {
  portfolio_id: 2015,
  name: 'AITopSelectie OFF FX',
  as_of: '2025-12-30',
  benchmark: 'SP500',
  benchmark_members: 491,
  holdings: 24,
  covered_pct: 100,
  benchmark_covered_pct: 100,
  foreign_listings: 1,
  benchmark_foreign_listings: 33,
  // The S&P rebuilds almost completely in the asset world (487 of 493). ACWI does not — see the
  // coverage warning, which is the whole reason this field exists.
  benchmark_universe_members: 493,
  benchmark_priced: 488,
  benchmark_coverage_pct: 99,
  // ⚠ The index is priced over the MODEL's windows. This model predates the year, so its YTD
  // really does open on 1 Jan — but a younger one's would not, and the benchmark would move with
  // it. (See `ytd_is_since`.)
  returns: {
    ytd_from: '2026-01-01',
    since_from: '2025-12-30',
    portfolio_ytd_pct: 51.48,
    benchmark_ytd_pct: 12.41,
    ytd_excess_pct: 39.07,
    portfolio_since_pct: 50.61,
    benchmark_since_pct: 12.77,
    since_excess_pct: 37.84,
    ytd_is_since: false,
  },
  axes: [
    {
      axis: 'sector',
      rows: [
        { bucket: 'Technology', portfolio_pct: 45, benchmark_pct: 36.3, diff_pct: 8.7 },
        { bucket: 'Fund (not looked through)', portfolio_pct: 20, benchmark_pct: 0, diff_pct: 20 },
        { bucket: 'Industrials', portfolio_pct: 25, benchmark_pct: 7.3, diff_pct: 17.7 },
        { bucket: 'Healthcare', portfolio_pct: 0, benchmark_pct: 8.8, diff_pct: -8.8 },
        { bucket: 'Cash', portfolio_pct: 10, benchmark_pct: 0, diff_pct: 10 },
      ],
    },
    {
      // The S&P's 2.1% Europe is REAL — Linde, Accenture, Medtronic, Chubb, NXP are genuinely
      // domiciled abroad. It is NOT the 7.2% the first version showed, which was US megacaps
      // (Eli Lilly on Stuttgart, IBM on Hamburg) inheriting their LISTING venue's country.
      axis: 'region',
      rows: [
        { bucket: 'North America', portfolio_pct: 60, benchmark_pct: 97.5, diff_pct: -37.5 },
        { bucket: 'Europe', portfolio_pct: 30, benchmark_pct: 2.1, diff_pct: 27.9 },
        { bucket: 'Cash', portfolio_pct: 10, benchmark_pct: 0, diff_pct: 10 },
      ],
    },
    {
      axis: 'currency',
      rows: [
        { bucket: 'USD', portfolio_pct: 60, benchmark_pct: 96.9, diff_pct: -36.9 },
        { bucket: 'EUR', portfolio_pct: 30, benchmark_pct: 0.8, diff_pct: 29.2 },
        { bucket: 'Cash', portfolio_pct: 10, benchmark_pct: 0, diff_pct: 10 },
      ],
    },
  ],
};

export async function mockPortfolios(page: Page) {
  await page.route('**/api/airs/model-portfolios/*/attribution**', async (route) => {
    // ECHO THE REQUESTED AXIS. The real endpoint does, and the panel words itself from what the
    // server COMPUTED — so a mock that always says "sector" would hide the exact bug this
    // catches: the table header saying "region" while the legend still says "sector".
    const axis = new URL(route.request().url()).searchParams.get('axis') ?? 'sector';
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ ...FIXTURE_ATTRIBUTION, axis }),
    });
  });
  await page.route('**/api/airs/model-portfolios/*/analysis**', async (route) => {
    // The benchmark is a query param, and ACWI is the one that cannot be fully rebuilt — its
    // missing constituents are a whole country at a time, so the coverage warning must appear.
    const acwi = new URL(route.request().url()).searchParams.get('benchmark') === 'ACWI';
    const body = acwi
      ? {
        ...FIXTURE_ANALYSIS,
        benchmark: 'ACWI',
        benchmark_universe_members: 1998,
        benchmark_priced: 1346,
        benchmark_coverage_pct: 67,
      }
      : FIXTURE_ANALYSIS;
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(body),
    });
  });
  await page.route('**/api/airs/model-portfolios/*/positions**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_POSITIONS),
    });
  });
  await page.route('**/api/airs/model-portfolios', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_PORTFOLIOS),
    });
  });
  await page.route('**/api/airs/model-portfolios/performance**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_PERFORMANCE),
    });
  });
  // The page also renders the benchmarks panel below the table; stub it so nothing hangs
  // against the unreachable mock host.
  await page.route('**/api/benchmarks**', async (route) => {
    await route.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
  });
}

/**
 * Brinson-Fachler attribution. The fixture is built so the identity HOLDS:
 * allocation + selection + interaction sums to the excess, exactly — because a table whose
 * effects do not sum to the excess is not a decomposition of it, and the panel says so.
 */
export const FIXTURE_ATTRIBUTION = {
  portfolio_id: 2015,
  name: 'AITopSelectie OFF FX',
  benchmark: 'ACWI',
  benchmark_coverage_pct: 84,
  window: 'since',
  axis: 'sector',
  start: '2025-04-11',
  portfolio_return_pct: 21.57,
  benchmark_return_pct: 36.91,
  excess_pct: -15.34,
  attributed_pct: -15.34,
  residual_pct: 0,
  reconciles: true,
  attributable_pct: 94,
  excluded_pct: 6,
  excluded_return_pct: null,
  // ⚠ NOT the same as excluded: a real Healthcare position we cannot price. Its sector reads as
  // UNOWNED in the table, so that row's allocation effect is a FALSE finding.
  unpriced_pct: 6,
  unpriced_buckets: ['Healthcare'],
  excluded: [
    { bucket: 'Healthcare', name: 'Some Pharma', isin: 'XX0000000001', weight_pct: 6, return_pct: null, reason: 'unpriced' },
  ],
  rows: [
    { bucket: 'Industrials', portfolio_weight_pct: 10.6, benchmark_weight_pct: 9.4, portfolio_return_pct: -30.1, benchmark_return_pct: 38.2, allocation_pct: 0.12, selection_pct: -7.51, interaction_pct: -0.97, total_pct: -8.36 },
    { bucket: 'Consumer Cyclical', portfolio_weight_pct: 34.0, benchmark_weight_pct: 13.0, portfolio_return_pct: 12.4, benchmark_return_pct: 21.2, allocation_pct: -5.07, selection_pct: -1.14, interaction_pct: -1.84, total_pct: -8.05 },
    { bucket: 'Technology', portfolio_weight_pct: 20.2, benchmark_weight_pct: 22.7, portfolio_return_pct: 30.0, benchmark_return_pct: 52.9, allocation_pct: -0.98, selection_pct: -5.19, interaction_pct: 0.56, total_pct: -5.61 },
    { bucket: 'Healthcare', portfolio_weight_pct: 0, benchmark_weight_pct: 9.7, portfolio_return_pct: null, benchmark_return_pct: 19.1, allocation_pct: 1.73, selection_pct: 0, interaction_pct: 0, total_pct: 1.73 },
    { bucket: 'Communication Services', portfolio_weight_pct: 12.8, benchmark_weight_pct: 9.0, portfolio_return_pct: 60.2, benchmark_return_pct: 46.7, allocation_pct: 0.15, selection_pct: 1.21, interaction_pct: 0.52, total_pct: 1.88 },
    { bucket: 'Energy', portfolio_weight_pct: 0, benchmark_weight_pct: 5.4, portfolio_return_pct: null, benchmark_return_pct: 21.6, allocation_pct: 0.82, selection_pct: 0, interaction_pct: 0, total_pct: 0.82 },
    { bucket: 'Utilities', portfolio_weight_pct: 0, benchmark_weight_pct: 2.9, portfolio_return_pct: null, benchmark_return_pct: 16.6, allocation_pct: 0.58, selection_pct: 0, interaction_pct: 0, total_pct: 0.58 },
    { bucket: 'Financials', portfolio_weight_pct: 14.9, benchmark_weight_pct: 15.9, portfolio_return_pct: 39.2, benchmark_return_pct: 37.5, allocation_pct: 0.08, selection_pct: 0.27, interaction_pct: -0.02, total_pct: 0.33 },
    { bucket: 'Consumer Defensive', portfolio_weight_pct: 7.4, benchmark_weight_pct: 6.9, portfolio_return_pct: 22.5, benchmark_return_pct: 10.4, allocation_pct: -0.18, selection_pct: 0.84, interaction_pct: 0.06, total_pct: 0.72 },
  ],
  top_contributors: [
    { isin: 'US02079K1079', name: 'Alphabet - C', ticker: 'GOOG', weight_pct: 10, return_pct: 59.2, contribution_pct: 5.92 },
    { isin: 'US0404131064', name: 'Arista Networks', ticker: 'ANET', weight_pct: 8, return_pct: 55.4, contribution_pct: 4.43 },
  ],
  top_detractors: [
    { isin: 'US22160N1090', name: 'Copart', ticker: 'CPRT', weight_pct: 8, return_pct: -34.0, contribution_pct: -2.72 },
    { isin: 'US21036P1084', name: 'Constellation Software', ticker: 'CSU', weight_pct: 6, return_pct: -32.5, contribution_pct: -1.95 },
  ],
  // Alphabet is NOT here: the model holds class C and the index holds class A. Two ISINs, one
  // company — matching on the ISIN once reported it as a winner they MISSED, at +3.23pp, while it
  // was their single biggest contributor.
  missed_winners: [
    { isin: 'US0378331005', name: 'Apple Inc', ticker: 'AAPL', weight_pct: 4.2, return_pct: 51.0, contribution_pct: 2.14 },
    { isin: 'US8740391003', name: 'Taiwan Semiconductor', ticker: 'TSM', weight_pct: 2.1, return_pct: 84.3, contribution_pct: 1.77 },
  ],
};
