import type { Page } from '@playwright/test';

/**
 * Fixture rows used by /companies tests. Mirrors the live
 * `GET /api/companies` response shape closely enough for the
 * `CompanyManager` table to render — extra computed columns
 * (delisted_at, out_of_scope_*, etc.) are left undefined; the
 * component treats them as null and renders without badges.
 */
export const FIXTURE_COMPANIES = [
  {
    company_id: 1,
    company_name: 'Apple Inc.',
    gurufocus_ticker: 'AAPL',
    gurufocus_exchange: 'NAS',
    country: 'United States',
    universes: ['ACWI'],
  },
  {
    company_id: 2,
    company_name: 'Microsoft Corporation',
    gurufocus_ticker: 'MSFT',
    gurufocus_exchange: 'NAS',
    country: 'United States',
    universes: ['ACWI'],
  },
  {
    company_id: 3,
    company_name: 'ASML Holding',
    gurufocus_ticker: 'ASML',
    gurufocus_exchange: 'XAMS',
    country: 'Netherlands',
    universes: ['ACWI', 'LEONTEQ'],
  },
];

/**
 * Stubs every endpoint the /companies page fetches on mount. Tests can
 * still override individual routes afterwards (e.g. to test error
 * states) — Playwright dispatches the most recently registered handler
 * that matches.
 */
export async function mockCompanies(page: Page) {
  await page.route('**/api/companies', async (route) => {
    if (route.request().method() !== 'GET') {
      await route.continue();
      return;
    }
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify(FIXTURE_COMPANIES),
    });
  });

  await page.route('**/api/companies/memberships', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        memberships: {
          '1': ['ACWI'],
          '2': ['ACWI'],
          '3': ['ACWI', 'LEONTEQ'],
        },
      }),
    });
  });
}
