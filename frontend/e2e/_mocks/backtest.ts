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
        // Shape must match `SignalDef` (app/components/momentum/types.ts):
        // the weight map is keyed on `key`, so a `code` field here would
        // render the sliders against `weights[undefined]`.
        signals: [
          { key: 'mom_12_1', label: '12-1 momentum', description: '12-month return skipping the last month.', group: 'price', default_weight: 1.0 },
          { key: 'vol_20d_vs_60d', label: '20d vs 60d volume', description: '20-day vs 60-day average volume ratio.', group: 'volume', default_weight: 1.0 },
        ],
        categories: ['price', 'volume'],
      }),
    });
  });

  // The /backtest universe picker now lists ONLY frozen static snapshots
  // (live templates are excluded for reproducibility). Source: GET
  // /api/static-universes — shape per _frozen_summary (template_key carries
  // the label sent as index_universe; frozen_at set).
  await page.route('**/api/static-universes', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify([
        {
          template_key: 'ACWI (as of 2026-05)',
          label: 'ACWI (as of 2026-05)',
          description: 'Frozen ACWI snapshot',
          earliest_date: '2026-05-01',
          universe_id: 9001,
          months_captured: 1,
          earliest_captured_month: '2026-05',
          latest_captured_month: '2026-05',
          latest_membership_count: 2700,
          last_refreshed_at: '2026-05-28T00:00:00Z',
          frozen_at: '2026-05-28T00:00:00Z',
          frozen_from: 'ACWI',
        },
      ]),
    });
  });

  // Still mocked though no longer the picker source — harmless if some
  // shared hook fetches it; keeps the route from hitting the network.
  await page.route('**/api/universe-templates', async (route) => {
    await route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify([]) });
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
