import type { Page } from '@playwright/test';

/**
 * Fixture rows for the /asset-pipeline grid. Mirrors `GET /api/asset-pipeline/grid`
 * (the `asset_grid` view) closely enough for `AssetPipelineTable` to render.
 *
 * The set is chosen to exercise the geography contract specifically:
 *   * AAPL   — domicile == listing, so NOT cross-listed.
 *   * TSM    — Taiwanese issuer on a US listing: cross-listed, Asia / Emerging Markets.
 *   * ASML   — Dutch issuer on a US listing: cross-listed, Europe / Europe.
 *   * 7203.T — Japanese issuer on a Japanese listing: Asia / Pacific (NOT Emerging).
 *   * SPY    — an ETF: no domicile at all, so country falls back to the listing.
 *   * BTC-USD— crypto: no geography whatsoever.
 *
 * `yahoo_symbol` is kept equal to `analysis_symbol` on purpose — the Symbol column
 * renders its own "via …" annotation when they differ, which would collide with
 * the Country column's cross-listing annotation in text assertions.
 */
export const FIXTURE_GRID = [
  {
    execution_id: 1, isin: 'US0378331005', analysis_id: 1,
    yahoo_symbol: 'AAPL', analysis_symbol: 'AAPL', name: 'Apple Inc.',
    exchange: 'NasdaqGS', currency: 'USD', asset_class: 'equity', sector: 'Technology',
    listing_country: 'United States', domicile_country: 'United States',
    country: 'United States', continent: 'North America', msci_region: 'North America',
    med_adv_eur: 9_000_000_000, status: 'ok',
  },
  {
    execution_id: 2, isin: 'US8740391003', analysis_id: 2,
    yahoo_symbol: 'TSM', analysis_symbol: 'TSM', name: 'Taiwan Semiconductor Manufacturing',
    exchange: 'NYSE', currency: 'USD', asset_class: 'equity', sector: 'Technology',
    listing_country: 'United States', domicile_country: 'Taiwan',
    country: 'Taiwan', continent: 'Asia', msci_region: 'Emerging Markets',
    med_adv_eur: 3_000_000_000, status: 'ok',
  },
  {
    execution_id: 3, isin: 'USN070592100', analysis_id: 3,
    yahoo_symbol: 'ASML', analysis_symbol: 'ASML', name: 'ASML Holding N.V.',
    exchange: 'NasdaqGS', currency: 'USD', asset_class: 'equity', sector: 'Technology',
    listing_country: 'United States', domicile_country: 'Netherlands',
    country: 'Netherlands', continent: 'Europe', msci_region: 'Europe',
    med_adv_eur: 1_500_000_000, status: 'ok',
  },
  {
    execution_id: 4, isin: 'JP3633400001', analysis_id: 4,
    yahoo_symbol: '7203.T', analysis_symbol: '7203.T', name: 'Toyota Motor Corporation',
    exchange: 'Tokyo', currency: 'JPY', asset_class: 'equity', sector: 'Consumer Cyclical',
    listing_country: 'Japan', domicile_country: 'Japan',
    country: 'Japan', continent: 'Asia', msci_region: 'Pacific',
    med_adv_eur: 800_000_000, status: 'ok',
  },
  {
    execution_id: 5, isin: 'US78462F1030', analysis_id: 5,
    yahoo_symbol: 'SPY', analysis_symbol: 'SPY', name: 'SPDR S&P 500 ETF Trust',
    exchange: 'NYSEArca', currency: 'USD', asset_class: 'etf', sector: 'etf',
    listing_country: 'United States', domicile_country: null,
    country: 'United States', continent: 'North America', msci_region: 'North America',
    med_adv_eur: 500_000_000, status: 'ok',
  },
  {
    execution_id: 6, isin: 'XF000BTC0017', analysis_id: 6,
    yahoo_symbol: 'BTC-USD', analysis_symbol: 'BTC-USD', name: 'Bitcoin USD',
    exchange: 'Cboe US', currency: 'USD', asset_class: 'crypto', sector: 'crypto',
    listing_country: 'United States', domicile_country: null,
    country: null, continent: null, msci_region: null,
    med_adv_eur: 100_000_000, status: 'ok',
  },
];

/** Stubs every endpoint /asset-pipeline touches on mount. */
export async function mockAssetPipeline(page: Page) {
  // Catch-all FIRST so the specific routes below win (Playwright dispatches the
  // most recently registered matching handler). Keeps stray calls — the usage
  // badge's SSE stream, the catalog — from hanging against an unreachable host.
  await page.route('**/api/**', async (route) => {
    await route.fulfill({ status: 200, contentType: 'application/json', body: '{}' });
  });

  await page.route('**/api/asset-pipeline/grid', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ rows: FIXTURE_GRID }),
    });
  });

  await page.route('**/api/asset-pipeline/universes', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ universes: [] }),
    });
  });
}

/** The filter dropdowns, by their `title`, in the order they must render. */
export const FILTER_TITLES = {
  product: 'Leonteq productType',
  class: 'yfinance asset class',
  sector: 'yfinance sector',
  country: 'Issuer domicile (falls back to the listing venue)',
  continent: 'Geographic continent',
  region: 'MSCI ACWI region',
} as const;
