import { expect, test } from '@playwright/test';

/**
 * /login is one of the few routes the proxy.ts middleware lets through
 * without an authenticated session, so it doubles as a smoke test for
 * the whole Playwright pipeline — if this fails, the dev server didn't
 * boot or the bypass env var isn't taking effect.
 */
test.describe('/login', () => {
  test('renders the sign-in form', async ({ page }) => {
    await page.goto('/login');

    await expect(page.getByRole('heading', { name: 'BBTerminal' })).toBeVisible();
    await expect(page.getByText('Sign in to your account')).toBeVisible();
    await expect(page.getByPlaceholder('you@bustelberg.nl')).toBeVisible();
    await expect(page.getByPlaceholder('••••••••')).toBeVisible();
    await expect(page.getByRole('button', { name: 'Sign in' })).toBeVisible();
  });

  test('flips to "Request access" mode when the toggle is clicked', async ({ page }) => {
    await page.goto('/login');

    await page.getByRole('button', { name: 'Request access' }).click();

    await expect(page.getByText('Request access')).toBeVisible();
    // The submit button label flips with the mode.
    await expect(page.getByRole('button', { name: 'Sign in' }).last()).toBeVisible();
  });
});
