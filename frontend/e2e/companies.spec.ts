import { expect, test } from '@playwright/test';

import { mockAuth } from './_mocks/auth';
import { FIXTURE_COMPANIES, mockCompanies } from './_mocks/companies';

/**
 * /companies — the simplest authenticated page in the app. Tests the
 * search-filter contract: typing in the box narrows the rendered rows
 * down to matches against ticker / name / exchange. This is the
 * regression check we'd want anytime the table-rendering or filter
 * logic gets refactored.
 */
test.describe('/companies', () => {
  test.beforeEach(async ({ page }) => {
    await mockAuth(page);
    await mockCompanies(page);
  });

  test('renders all fixture rows on load', async ({ page }) => {
    await page.goto('/companies');

    // Wait for the table to populate. Each row's ticker shows up as
    // monospaced text — we use it as the "row is rendered" signal.
    for (const c of FIXTURE_COMPANIES) {
      await expect(page.getByText(c.gurufocus_ticker, { exact: true })).toBeVisible();
    }
  });

  test('search box narrows visible rows to matches', async ({ page }) => {
    await page.goto('/companies');

    // Wait for initial load so we know all rows are present first.
    await expect(page.getByText('AAPL', { exact: true })).toBeVisible();

    const search = page.getByPlaceholder('Search name, ticker, exchange...');
    await search.fill('ASML');

    // Only ASML stays. AAPL + MSFT should drop out.
    await expect(page.getByText('ASML', { exact: true })).toBeVisible();
    await expect(page.getByText('AAPL', { exact: true })).not.toBeVisible();
    await expect(page.getByText('MSFT', { exact: true })).not.toBeVisible();

    // Clearing the box brings everything back.
    await search.clear();
    await expect(page.getByText('AAPL', { exact: true })).toBeVisible();
    await expect(page.getByText('MSFT', { exact: true })).toBeVisible();
    await expect(page.getByText('ASML', { exact: true })).toBeVisible();
  });

  test('search by company name also works', async ({ page }) => {
    await page.goto('/companies');

    const search = page.getByPlaceholder('Search name, ticker, exchange...');
    await search.fill('Microsoft');

    await expect(page.getByText('MSFT', { exact: true })).toBeVisible();
    await expect(page.getByText('AAPL', { exact: true })).not.toBeVisible();
  });
});
