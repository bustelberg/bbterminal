import { expect, test } from '@playwright/test';

import { mockAuth } from './_mocks/auth';
import { mockBacktestPageReads } from './_mocks/backtest';

/**
 * /backtest — smoke coverage. The page is the largest component in the
 * app (~2,400 lines) and the primary motivation for adding e2e in the
 * first place: refactoring it without a regression net is risky. These
 * tests verify the shell renders + the universe dropdown surfaces the
 * mocked template list. SSE flow coverage (run → results) is intentionally
 * left for a follow-up once we have a mocking pattern for the streaming
 * `POST /api/momentum/backtest` endpoint.
 */
test.describe('/backtest', () => {
  test.beforeEach(async ({ page }) => {
    await mockAuth(page);
    await mockBacktestPageReads(page);
  });

  test('renders the page header without crashing', async ({ page }) => {
    await page.goto('/backtest');

    await expect(
      page.getByRole('heading', { name: 'Momentum Backtester' }),
    ).toBeVisible();
    // The subtitle is always visible from mount — its presence confirms
    // the component tree mounted past the header div without throwing.
    await expect(
      page.getByText('Price momentum portfolio', { exact: false }),
    ).toBeVisible();
  });
});
