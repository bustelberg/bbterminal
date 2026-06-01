import type { Page } from '@playwright/test';

/**
 * Mocks Supabase Auth so client-side hooks like `useEffectiveRole` see
 * a logged-in admin. The server-side middleware bypass is handled by
 * `E2E_BYPASS_AUTH=1` (see `playwright.config.ts`). Tests that need
 * admin-only UI controls (add / edit / delete buttons on /companies,
 * for example) should call `mockAuth(page)` before navigating.
 *
 * Pass `role: 'user'` to test the non-admin variant of a page.
 */
export async function mockAuth(
  page: Page,
  { role = 'admin' as 'admin' | 'user' } = {},
) {
  // Supabase Auth `getUser()` is a POST to /auth/v1/user with a bearer
  // header. Returning a minimal user object with the requested role is
  // enough to satisfy the client-side hooks.
  await page.route('**/auth/v1/user', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        id: '00000000-0000-0000-0000-000000000001',
        email: 'e2e@bustelberg.nl',
        app_metadata: { role },
        user_metadata: {},
        aud: 'authenticated',
        role: 'authenticated',
      }),
    });
  });

  // The Supabase client also calls /auth/v1/token for session refresh
  // on first load — stub it so it doesn't hang against the unreachable
  // mock host.
  await page.route('**/auth/v1/token**', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'application/json',
      body: JSON.stringify({
        access_token: 'e2e-mock-token',
        refresh_token: 'e2e-mock-refresh',
        expires_in: 3600,
        token_type: 'bearer',
        user: { id: '00000000-0000-0000-0000-000000000001' },
      }),
    });
  });
}
