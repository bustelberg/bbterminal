import type { Page } from '@playwright/test';

/**
 * Mocks the read-side API surface /backtest hits on mount. Doesn't
 * touch the SSE backtest stream (`POST /api/momentum/backtest`) —
 * smoke tests here verify the page renders + the config panel is
 * interactive, not the run flow itself. Flow-level coverage can land
 * later as separate specs once we have an SSE mocking pattern.
 */
export async function mockBacktestPageReads(page: Page) {
  await page.route('**/api/momentum/signals', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        signals: [
          { code: 'mom_12_1', label: '12-1 momentum', group: 'price', default_weight: 1.0 },
          { code: 'vol_20d_vs_60d', label: '20d vs 60d volume', group: 'volume', default_weight: 1.0 },
        ],
        categories: ['price', 'volume'],
      }),
    });
  });

  await page.route('**/api/universe-templates', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([
        {
          template_key: 'ACWI',
          label: 'ACWI',
          earliest_date: '2002-01-01',
          earliest_captured_month: '2002-01',
          latest_captured_month: '2026-05',
          months_captured: 293,
          latest_membership_count: 2700,
        },
      ]),
    });
  });

  await page.route('**/api/momentum/backtests', async (route) => {
    if (route.request().method() !== 'GET') {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([]),
    });
  });

  await page.route('**/api/momentum/current-picks', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([]),
    });
  });

  await page.route('**/api/data/latest-price-date', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({ latest_date: '2026-05-28' }),
    });
  });

  await page.route('**/api/exchange-fees', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([]),
    });
  });

  await page.route('**/api/benchmarks', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([]),
    });
  });
}
