import { expect, test } from '@playwright/test';

import { mockAuth } from './_mocks/auth';
import { mockBacktestPageReads } from './_mocks/backtest';

/**
 * /backtest — smoke coverage. The page is the largest component in the
 * app (~2,300 lines) and the primary motivation for adding e2e in the
 * first place: refactoring it without a regression net is risky. These
 * tests verify the shell renders, the config panel surfaces its core
 * controls, and the selection-mode → control-state wiring works — the
 * exact behaviour an upcoming `<BacktestConfigPanel>` extraction must
 * preserve. SSE flow coverage (run → results) is intentionally left for a
 * follow-up once we have a mocking pattern for the streaming
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

  test('config panel renders its core controls', async ({ page }) => {
    await page.goto('/backtest');

    // Date range — two `<input type="month">` (Start + End).
    await expect(page.locator('input[type="month"]')).toHaveCount(2);

    // The Strategy selector — uniquely identified by its sector_etf
    // option, so it survives surrounding-markup churn.
    await expect(
      page.locator('select:has(option[value="sector_etf"])'),
    ).toBeVisible();

    // Signal-weight sliders render the mocked signal defs, grouped under
    // their category headings.
    await expect(page.getByRole('heading', { name: 'Price Momentum' })).toBeVisible();
    await expect(page.getByRole('heading', { name: 'Volume Confirmation' })).toBeVisible();
    await expect(page.getByText('12-1 momentum')).toBeVisible();
    await expect(page.getByText('20d vs 60d volume')).toBeVisible();

    // The run action.
    await expect(page.getByRole('button', { name: /Run variants/ })).toBeVisible();
  });

  test('selection mode drives control state', async ({ page }) => {
    await page.goto('/backtest');

    const strategy = page.locator('select:has(option[value="sector_etf"])');

    // Momentum (default): the momentum signal sliders are shown.
    await expect(page.getByText('12-1 momentum')).toBeVisible();

    // Random baseline: the momentum-only signal sliders disappear.
    await strategy.selectOption('random');
    await expect(page.getByText('12-1 momentum')).toBeHidden();

    // Back to momentum restores them.
    await strategy.selectOption('momentum');
    await expect(page.getByText('12-1 momentum')).toBeVisible();
  });

  test('variants panel renders the cross-product axes', async ({ page }) => {
    await page.goto('/backtest');

    await expect(page.getByRole('heading', { name: 'Variants' })).toBeVisible();
    await expect(page.getByText('Permutations')).toBeVisible();
    // The cross-product axis columns ("Strategy" is omitted — it collides
    // with the mode-select label above).
    await expect(page.getByText('Frequency')).toBeVisible();
    await expect(page.getByText('Grouping')).toBeVisible();
  });
});
