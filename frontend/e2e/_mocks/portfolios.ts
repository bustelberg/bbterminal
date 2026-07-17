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
      // NOT AN INSTRUMENT — another model portfolio, wrapped as a Leonteq certificate so it can
      // be held like a security. Yahoo has no listing for a structured product, so it can never
      // be priced DIRECTLY (`known_instrument: false`); the LINK is what lets us look through to
      // the model behind it. The guess is confident here, and the badge says so. And because it
      // is linked, its Start/End/Return are a LOOK-THROUGH: the wrapped model's basket indexed to
      // 100 at the window open — only the Return is a real number, so the row wears the accent
      // colour and a ↳ rather than reading as a share price.
      fonds: 'Star Selection Index', isin: 'CH1381833321', percentage: 10, valuta: 'EUR',
      categorie: 'Aandelen', sector: null, regio: null, known_instrument: false,
      linked_portfolio_id: 2094, linked_portfolio_name: 'StarTopSelectie OFF FX',
      link_source: 'auto', link_confidence: 0.99,
      link_reason: "name 'Star Selection Index' matches 'StarTopSelectie OFF FX'",
      lookthrough: true, currency: null,
      start_date: '2026-01-01', start_price_eur: 100.0, start_price_local: null,
      end_date: '2026-07-13', end_price_eur: 107.3, end_price_local: null,
      return_pct: 7.3,
    },
    {
      fonds: 'Liquiditeiten', isin: null, percentage: 5, valuta: 'EUR',
      categorie: 'Liquiditeiten', sector: null, regio: null, known_instrument: false,
    },
  ],
};

/**
 * What the row dropdowns may offer. `excluded_by_isin` is the CYCLE guard: TOPS_STS_L (2082)
 * holds the Star certificate at 100%, so linking the certificate to it walks straight back to
 * the row you started from. The owning portfolio (2015) is already absent from `options`.
 */
export const FIXTURE_LINKABLE = {
  options: [
    { id: 2094, name: 'StarTopSelectie OFF FX', omschrijving: 'StarTopSelectie Offensief fixed', positions: 24 },
    { id: 2082, name: 'TOPS_STS_L', omschrijving: 'StarTopSelectie', positions: 1 },
    { id: 1973, name: 'BUS_EUR_OFF_FX', omschrijving: 'Europa Offensief FX', positions: 27 },
  ],
  excluded_by_isin: { CH1381833321: [2082] },
};

/**
 * Accounts -> models. Real names and real shapes (AirSPMS, 2026-07-17).
 *
 * The `source` field is what this fixture exists to pin. A `guess` is a name pattern; a
 * `manual` is a person. They must never look alike, because the wrong risk profile of a
 * strategy holds nearly the same instruments — BUS_FTS_Bepoff/DEF/NEU_AFS hold the IDENTICAL
 * 27 ISINs — so a wrong link reads as perfectly plausible and nobody re-checks it.
 */
export const FIXTURE_ACCOUNT_LINKS = {
  accounts: [
    // Confirmed by a human — always wins over any guess.
    {
      portefeuille: 'StarTopSelectie OFF DYN', ytd_pct: 2.4, months: 7,
      model_portfolio_id: 2094, model_name: 'StarTopSelectie OFF FX', model_positions: 24,
      source: 'manual', reason: null,
    },
    // Pattern-matched only. `BUS_FTS_OFF_DYN` <-> `BUS_FTS_OFF_AFS` — the suffix-REPLACED
    // convention, which is the one that broke the first matcher.
    {
      portefeuille: 'BUS_FTS_OFF_DYN', ytd_pct: -6.13, months: 7,
      model_portfolio_id: 1990, model_name: 'BUS_FTS_OFF_AFS', model_positions: 25,
      source: 'guess', reason: "exact stem match on 'busftsoff'",
    },
    // AIRS mangles the word itself (AAND -> AAN + _d), so there is no guess to make.
    {
      portefeuille: 'BUS_BM_AAN_kw_EUR_2026_d', ytd_pct: 13.93, months: 7,
      model_portfolio_id: null, model_name: null, model_positions: null,
      source: 'none', reason: "no model has the stem 'busbmaankweur2026'",
    },
    // The cycle: an account that is ALSO a one-line model row, so it matched itself.
    {
      portefeuille: 'TOPS_AZTS_L', ytd_pct: 0.0, months: 7,
      model_portfolio_id: null, model_name: null, model_positions: null,
      source: 'none', reason: 'the only stem match is the account itself',
    },
  ],
  models: [
    { id: 2094, name: 'StarTopSelectie OFF FX', positions: 24 },
    { id: 1990, name: 'BUS_FTS_OFF_AFS', positions: 25 },
    { id: 1991, name: 'BUS_FTS_NEU_AFS', positions: 27 },
    { id: 1810, name: 'BUS_BM_AAND_kw_EUR_2026', positions: 1 },
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
  weight_basis: 'model',
  weight_note: null,
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
    // The book (AIRS) beside the strategy (yfinance). Windows match here, so the gap is real
    // drift: the strategy read +51.48% while the book AIRS holds made +46.12%.
    book_portefeuille: 'BUS_Defensief_Dyn',
    book_ytd_pct: 46.12,
    book_comparable: true,
    book_gap_pct: 5.36,
    book_reason: null,
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
    const params = new URL(route.request().url()).searchParams;
    // The benchmark is a query param, and ACWI is the one that cannot be fully rebuilt — its
    // missing constituents are a whole country at a time, so the coverage warning must appear.
    const acwi = params.get('benchmark') === 'ACWI';
    // Book weights change ONLY the portfolio bars, never the benchmark — the fixture below has
    // Technology at 45% under the model, 52% under the book, so a test can see the toggle bite.
    const book = params.get('weight_by') === 'book';
    let body = acwi
      ? {
        ...FIXTURE_ANALYSIS,
        benchmark: 'ACWI',
        benchmark_universe_members: 1998,
        benchmark_priced: 1346,
        benchmark_coverage_pct: 67,
      }
      : FIXTURE_ANALYSIS;
    if (book) {
      body = {
        ...body,
        weight_basis: 'book',
        holdings: 41,
        axes: body.axes.map((ax) => ax.axis !== 'sector' ? ax : {
          ...ax,
          rows: ax.rows.map((r) => r.bucket !== 'Technology' ? r
            : { ...r, portfolio_pct: 52, diff_pct: 52 - r.benchmark_pct }),
        }),
      };
    }
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
  await page.route('**/api/airs/model-portfolios/*/linkable**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_LINKABLE),
    });
  });
  await page.route('**/api/airs/model-portfolios', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_PORTFOLIOS),
    });
  });
  /**
   * The overview — the pair composed. Name from the Fixed side, numbers from the Dynamic side.
   *
   * `link_source` is the point of the fixture: a `guess`-named row must never look like a
   * confirmed one, because a mis-pairing files a real book's money under another strategy's
   * name and nothing else on the row would look wrong.
   */
  await page.route('**/api/airs/portfolios/overview**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([
        {
          name: 'Bustelberg Defensief', description: 'Defensief FX',
          dynamic_portefeuille: 'BUS_Defensief_Dyn', fixed_name: 'BUS_Defensief_FX',
          fixed_portfolio_id: 1933, fixed_type: 'fixed (8.5)', isins: 40,
          link_source: 'guess', link_reason: "exact stem match on 'busdefensief'",
          as_of: '2026-07-16', periode: '2026-07-16', months: 7,
          ytd_pct: 2.68, latest_month_pct: -0.12, price_result_eur: 14851,
          income_eur: 15514, investment_result_eur: 30447, deposits_eur: 0, withdrawals_eur: 0,
          begin_value_eur: 1135000, end_value_eur: 1165895, holdings: 41,
          reconciles: true, residual_eur: 0,
        },
        {
          name: 'EuropaTopSelectie Offensief', description: 'Europa Offensief FX',
          dynamic_portefeuille: 'EuropaTopSelect OFF DYN', fixed_name: 'BUS_EUR_OFF_FX',
          fixed_portfolio_id: 1973, fixed_type: 'fixed (0)', isins: 26,
          link_source: 'manual', link_reason: null,
          as_of: '2026-07-16', periode: '2026-07-16', months: 7,
          ytd_pct: -1.28, latest_month_pct: 0.4, price_result_eur: -12000,
          income_eur: 3000, investment_result_eur: -9000, deposits_eur: 0, withdrawals_eur: 0,
          begin_value_eur: 1000000, end_value_eur: 991000, holdings: 27,
          reconciles: true, residual_eur: 0,
        },
        // Unlinked: real AIRS numbers, no nickname, no ISINs. Hidden by default, not deleted.
        {
          name: 'BUS_BM_AAN_kw_EUR_2026_d', description: null,
          dynamic_portefeuille: 'BUS_BM_AAN_kw_EUR_2026_d', fixed_name: null,
          fixed_portfolio_id: null, fixed_type: null, isins: null,
          link_source: 'none', link_reason: "no model has the stem 'busbmaankweur2026'",
          as_of: '2026-07-16', periode: '2026-07-16', months: 7,
          ytd_pct: 13.93, latest_month_pct: 1.1, price_result_eur: 139300,
          income_eur: 0, investment_result_eur: 139300, deposits_eur: 0, withdrawals_eur: 0,
          begin_value_eur: 1000000, end_value_eur: 1139300, holdings: 1,
          reconciles: true, residual_eur: 0,
        },
      ]),
    });
  });
  // The accounts list. Every money figure is the YEAR's (summed across AIRS's monthly rows) —
  // `latest_month_pct` is a different window, not a rival YTD.
  await page.route('**/api/airs/accounts', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([{
        portefeuille: 'BUS_Defensief_Dyn', periode: '2026-07-16', as_of: '2026-07-16', months: 7,
        begin_value_eur: 1135000, end_value_eur: 1165895, ytd_pct: 2.68, latest_month_pct: -0.12,
        price_result_eur: 14851, income_eur: 15514, investment_result_eur: 30447,
        deposits_eur: 0, withdrawals_eur: 0, residual_eur: 0, reconciles: true, holdings: 41,
      }]),
    });
  });
  await page.route('**/api/airs/accounts/*/holdings**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        portefeuille: 'BUS_Defensief_Dyn', as_of: '2026-07-16', ytd_pct: 2.68,
        price_result_eur: 14851, income_eur: 15514,
        rows: [
          { holding_name: 'ASML Holding', quantity: 27, currency: 'EUR', weight: 0.0359,
            start_value_eur: 24878, current_value_eur: 41834, ytd_return_eur: 16956,
            ytd_return_pct: 0.6816, ytd_return_local_pct: 0.6816 },
          { holding_name: 'iShares Global Corp Bond ETF EUR H Dist', quantity: 42841,
            currency: 'EUR', weight: 0.1534, start_value_eur: 183505, current_value_eur: 178797,
            ytd_return_eur: -4708, ytd_return_pct: -0.0257, ytd_return_local_pct: -0.0257 },
          { holding_name: 'Star Selection Index', quantity: 21100, currency: 'EUR',
            weight: 0.0176, start_value_eur: 22003, current_value_eur: 20522,
            ytd_return_eur: -1481, ytd_return_pct: -0.0673, ytd_return_local_pct: -0.0673 },
          { holding_name: 'Aedifica', quantity: 400, currency: 'EUR', weight: 0.029,
            start_value_eur: 33416, current_value_eur: 33787, ytd_return_eur: 371,
            ytd_return_pct: 0.0111, ytd_return_local_pct: 0.0111 },
          // ⚠ No opening value: its return is UNDEFINED, and its weight still counts.
          { holding_name: 'Effectenrekening', quantity: 0, currency: 'EUR', weight: 0.0141,
            start_value_eur: 0, current_value_eur: 16468, ytd_return_eur: null,
            ytd_return_pct: null, ytd_return_local_pct: null },
        ],
      }),
    });
  });
  /**
   * An account's holdings with ISINs attached. Real rows from BUS_Defensief_Dyn.
   *
   * The three verdicts must stay visibly apart. `unpriced` is the subtle one: the name matched
   * and NOTHING checked it, which is precisely where the Acc/Inc share-class trap lives — so it
   * must not read as a pass.
   */
  await page.route('**/api/airs/accounts/*/isins**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        portefeuille: 'BUS_Defensief_Dyn',
        model_name: 'BUS_Defensief_FX',
        model_source: 'guess',
        as_of: '2026-07-16',
        // AIRS's own Beleggingscategorie. ⚠ Bonds is 88% ETF — an "ETF" bucket would empty it.
        segments: [
          { asset_class: 'Equity', holdings: 1, value_eur: 41834, start_value_eur: 24878, weight_pct: 3.59,
            gain_eur: 16956, return_pct: 68.16, priced_value_eur: 41834, etf_value_eur: 0 },
          { asset_class: 'Bonds', holdings: 1, value_eur: 178797, start_value_eur: 183505, weight_pct: 15.34,
            gain_eur: -4708, return_pct: -2.57, priced_value_eur: 178797, etf_value_eur: 178797 },
          { asset_class: 'Real estate', holdings: 1, value_eur: 33787, start_value_eur: 33416, weight_pct: 2.9,
            gain_eur: 371, return_pct: 1.11, priced_value_eur: 33787, etf_value_eur: 0 },
          { asset_class: 'Alternatives', holdings: 1, value_eur: 20522, start_value_eur: 22003, weight_pct: 1.76,
            gain_eur: -1481, return_pct: -6.73, priced_value_eur: 20522, etf_value_eur: 0 },
          // ⚠ Real exposure, undefined return: it counts in the weight, never in the return.
          { asset_class: 'Cash', holdings: 1, value_eur: 16468, start_value_eur: 0, weight_pct: 1.41,
            gain_eur: null, return_pct: null, priced_value_eur: 0, etf_value_eur: 0 },
        ],
        rows: [
          // Confirmed by the price: implied €1549 against ASML's own close.
          {
            holding_name: 'ASML Holding', lines: 1, quantity: 27, currency: 'EUR',
            asset_class: 'Equity', categorie: 'AAND', is_etf: false,
            isin: 'NL0010273215', model_fonds: 'ASML Holding', name_score: 100,
            implied_price_eur: 1549.4, our_price_eur: 1510.89, price_ratio: 1.03,
            verdict: 'ok', our_instrument: 'ASML Holding N.V.',
          },
          // ⚠ The model's ISIN is the fund's USD (Dist) class at €77.94; the book holds the
          // EUR-hedged class at €4.17. 19x apart, both quoted EUR — FX cannot explain it.
          {
            holding_name: 'iShares Global Corp Bond ETF EUR H Dist', lines: 1, quantity: 42841,
            currency: 'EUR', asset_class: 'Bonds', categorie: 'OBL', is_etf: true,
            isin: 'IE00BJSFQW37', model_fonds: 'iShs Glb Crp Bond ETF EUR',
            name_score: 80, implied_price_eur: 4.1735, our_price_eur: 77.938, price_ratio: 0.0535,
            verdict: 'price_mismatch',
            our_instrument: 'iShares Global Corp Bond UCITS ETF USD (Dist)',
          },
          // Name-matched only — a Leonteq AMC Yahoo cannot price. Nothing confirms it.
          {
            holding_name: 'Star Selection Index', lines: 1, quantity: 21100, currency: 'EUR',
            asset_class: 'Alternatives', categorie: 'ALTBEL', is_etf: false,
            isin: 'CH1381833321', model_fonds: 'Star Selection Index', name_score: 100,
            implied_price_eur: null, our_price_eur: null, price_ratio: null,
            verdict: 'unpriced', our_instrument: null,
          },
          { holding_name: 'Aedifica', lines: 1, quantity: 400, currency: 'EUR',
            asset_class: 'Real estate', categorie: 'VAS', is_etf: false, isin: 'BE0003851681',
            name_score: 100, verdict: 'ok', our_instrument: 'Aedifica NV/SA' },
          { holding_name: 'Effectenrekening', lines: 1, quantity: 0, currency: 'EUR',
            asset_class: 'Cash', categorie: null, is_etf: false, isin: null,
            name_score: 28, verdict: 'unpriced', our_instrument: null },
        ],
        // The model holds it; the book does not. Real drift.
        unmatched_model_positions: [
          { fonds: 'Ish DJS GSD 100', isin: 'DE000A0F5UH1', percentage: 0.9 },
        ],
      }),
    });
  });
  // Accounts -> models. The three `source` values are the point of the fixture: they must stay
  // visibly different, because "a human confirmed this" and "we pattern-matched a name" are
  // not the same claim about which strategy a book is running.
  await page.route('**/api/airs/account-model-links**', async (route) => {
    if (route.request().method() !== 'GET') {
      await route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_ACCOUNT_LINKS),
    });
  });
  await page.route('**/api/airs/model-portfolios/performance**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_PERFORMANCE),
    });
  });
  // The correlation matrix sits between the table and the benchmarks panel; stub it too.
  await page.route('**/api/airs/model-portfolios/correlations**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_CORRELATIONS),
    });
  });
  // The page also renders the benchmarks panel below the table; stub it so nothing hangs
  // against the unreachable mock host.
  // The four soundness charts, launched from the positions table's  button.
  // ⚠ The price line here is DAILY and yfinance's; the fair values are GuruFocus's, already
  // converted to the same EUR by the backend. A fixture that made them share a currency by
  // accident would not exercise the thing that matters.
  await page.route('**/api/asset-pipeline/fundamentals/isin/**', async (route) => {
    await route.fulfill({
      status: 200, contentType: 'application/json',
      body: JSON.stringify(FIXTURE_FUNDAMENTALS),
    });
  });

  await page.route('**/api/benchmarks**', async (route) => {
    await route.fulfill({ status: 200, contentType: 'application/json', body: '[]' });
  });
}

/**
 * A 3×3 correlation fixture. Symmetric, diagonal 1.0, and one null pair (C is too young to
 * overlap A) so the hatched "too few overlapping days" cell is exercised.
 */
// Deliberately DISTINCT labels from the perf-table fixtures above — the real matrix shows the
// same portfolio names as the table, but reusing them here would make the other tests' broad
// getByText selectors match twice (the table row AND the matrix header) and fail strict mode.
export const FIXTURE_CORRELATIONS = {
  portfolio_ids: [2015, 2001, 2099],
  // ⚠ WILDLY UNEVEN LENGTHS, ON PURPOSE. The row-header column is auto-width, so it is as wide as
  // its LONGEST label — and the hover-stability test below only means something if bolding the
  // longest one could actually move the column. Three same-length labels would pass that test
  // whether or not the bug is fixed.
  labels: ['Corr Alpha FX', 'Corr Beta Considerably Longer Name FX', 'Corr Gamma FX'],
  // AIRS's own codes, shown in the tooltip beside a chosen name.
  codes: ['CORR_A_FX', 'CORR_B_FX', 'CORR_G_FX'],
  // Risk profiles — Beta has none, so the "not offered at a profile" path is exercised too.
  variants: ['Offensief', null, 'Neutraal'],
  as_of: '2026-07-15',
  min_overlap_days: 20,
  ytd: [
    [1.0, 0.82, null],
    [0.82, 1.0, -0.31],
    [null, -0.31, 1.0],
  ],
  ytd_obs: [136, 137, 8],
  trailing_12m: [
    [1.0, 0.79, 0.12],
    [0.79, 1.0, -0.24],
    [0.12, -0.24, 1.0],
  ],
  trailing_12m_obs: [257, 257, 8],
};

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
    {
      bucket: 'Technology', portfolio_weight_pct: 20.2, benchmark_weight_pct: 22.7, portfolio_return_pct: 30.0, benchmark_return_pct: 52.9, allocation_pct: -0.98, selection_pct: -5.19, interaction_pct: 0.56, total_pct: -5.61,
      // The names behind the bar — the click-through detail. Your holdings (with contribution)
      // and the index's top constituents in the same bucket, capped with the true count.
      portfolio_holdings: [
        { isin: 'US0404131064', name: 'Arista Networks', ticker: null, weight_pct: 8, return_pct: 55.4, contribution_pct: 4.43, in_both: false },
        { isin: 'NL0010273215', name: 'ASML Holding', ticker: null, weight_pct: 6, return_pct: -4.0, contribution_pct: -0.24, in_both: false },
        // Held on BOTH sides — you hold "Microsoft", the index holds "Microsoft Corp".
        { isin: 'US5949181045', name: 'Microsoft', ticker: null, weight_pct: 6, return_pct: 14.0, contribution_pct: 0.84, in_both: true },
      ],
      benchmark_holdings: [
        { isin: 'US67066G1040', name: 'NVIDIA Corp', ticker: 'NVDA', weight_pct: 7.1, return_pct: 31.0, contribution_pct: 2.2, in_both: false },
        { isin: 'US5949181045', name: 'Microsoft Corp', ticker: 'MSFT', weight_pct: 6.2, return_pct: 14.0, contribution_pct: 0.87, in_both: true },
      ],
      benchmark_holdings_count: 82,
    },
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

/**
 * One company's soundness payload. Small but SHAPED like the real thing:
 *  - the price is daily (three consecutive days), the ratios are annual — different cadences on
 *    one chart is the actual layout problem;
 *  -  is non-zero on a fair value, because that is the field whose whole job is
 *    admitting a gap (Apple loses 13 of 40 periods to thin FX history).
 */
export const FIXTURE_FUNDAMENTALS = {
  isin: 'US0231351067',
  symbol: 'NASDAQ:AMZN',
  company_id: 42,
  currency: 'USD',
  yahoo_symbol: 'AMZN',
  price_currency: 'USD',
  is_home: true,
  template: 'normal',
  cadence: 'annuals',
  period_count: 4,
  fetched: false,
  price_eur: [
    { date: '2026-07-01', value: 190.1 },
    { date: '2026-07-02', value: 192.4 },
    { date: '2026-07-03', value: 191.2 },
  ],
  fair_values_eur: [
    { field: 'Graham Number', label: 'Graham Number',
      points: [{ date: '2024-12-31', value: 88.0 }, { date: '2025-12-31', value: 95.5 }],
      period_count: 4, dropped: 2 },
    // ⚠ A NEGATIVE fair value — Peter Lynch needs positive earnings growth, so a loss year has
    // none. Log cannot plot it; the line must BREAK, not bridge. A fixture without one would
    // never exercise that path.
    { field: 'Peter Lynch Fair Value', label: 'Peter Lynch',
      points: [{ date: '2023-12-31', value: -4.0 }, { date: '2024-12-31', value: 150.0 }, { date: '2025-12-31', value: 162.0 }],
      period_count: 4, dropped: 1, non_positive: 1 },
  ],
  price_crosscheck_eur: [{ date: '2025-12-31', value: 188.0 }],
  yields: [
    { field: 'FCF Yield %', label: 'FCF yield',
      points: [{ date: '2024-12-31', value: 2.1 }, { date: '2025-12-31', value: 2.63 }],
      period_count: 4, dropped: 0 },
  ],
  returns: [
    { field: 'ROIC %', label: 'ROIC',
      points: [{ date: '2024-12-31', value: 30.2 }, { date: '2025-12-31', value: 39.38 }],
      period_count: 4, dropped: 0 },
    { field: 'WACC %', label: 'WACC',
      points: [{ date: '2024-12-31', value: 9.1 }, { date: '2025-12-31', value: 9.86 }],
      period_count: 4, dropped: 0 },
  ],
  safety: [
    { field: 'Piotroski F-Score', label: 'Piotroski F',
      points: [{ date: '2024-12-31', value: 7 }, { date: '2025-12-31', value: 8 }],
      period_count: 4, dropped: 0 },
    { field: 'Altman Z-Score', label: 'Altman Z',
      points: [{ date: '2024-12-31', value: 9.4 }, { date: '2025-12-31', value: 10.17 }],
      period_count: 4, dropped: 0 },
  ],
  // ⚠ ONE OF EACH STATUS. A fixture where all four pass would never show that the strip keeps
  // fail / n_a / unknown visibly APART — and n_a (a bank has no ROIC at all) reading as a
  // failure is the specific thing that would make the card untrustworthy.
  quality: [
    { key: 'spread', label: 'ROIC − WACC', unit: 'pp', value: 18.8, periods: 10, status: 'ok', note: '10y median.' },
    { key: 'trend', label: 'ROIC trend', unit: 'pp', value: -17.1, periods: 10, status: 'fail', note: 'The moat is melting.' },
    { key: 'conversion', label: 'FCF / net income', unit: 'x', value: 1.07, periods: 10, status: 'ok', note: '10y median.' },
    { key: 'gm_sd', label: 'Gross margin σ', unit: 'pp', value: null, periods: 0, status: 'n_a', note: 'No gross margin for this template.' },
  ],
  has_roic: true,
  has_earnings_yield: true,
};
