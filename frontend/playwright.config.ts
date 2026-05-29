import { defineConfig, devices } from '@playwright/test';

/**
 * Playwright config for the BBTerminal frontend's UI regression suite.
 *
 * Strategy:
 *   - All tests run against a Next.js dev server that Playwright manages
 *     (boots on demand for CI, reuses an already-running one for local dev).
 *   - `E2E_BYPASS_AUTH=1` short-circuits `proxy.ts`'s Supabase session
 *     check so we don't need a real local Supabase to render authenticated
 *     pages. Tests then mock `/api/*` responses via `page.route()`.
 *   - Chromium-only by default — Firefox + WebKit add a lot of time for
 *     comparatively little extra coverage on a single-target webapp.
 *     Add projects below when a real browser regression demands it.
 */
export default defineConfig({
  testDir: './e2e',
  fullyParallel: true,
  forbidOnly: !!process.env.CI,
  retries: process.env.CI ? 2 : 0,
  workers: process.env.CI ? 1 : undefined,
  reporter: process.env.CI ? [['github'], ['list']] : 'list',
  timeout: 30_000,
  expect: { timeout: 5_000 },

  use: {
    baseURL: 'http://127.0.0.1:3100',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: 'retain-on-failure',
  },

  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],

  // Run against the production build per Next.js's official Playwright
  // guidance — `next dev` enforces a per-project singleton in Next 16,
  // so it can't coexist with the developer's running dev server. Port
  // 3100 keeps it out of the way of dev (3000). The first boot pays
  // a ~30s build cost; subsequent runs reuse the `.next` cache for ~10s.
  // `reuseExistingServer` skips the boot when one is already up (handy
  // when iterating on a single test).
  webServer: {
    command: 'npm run build && npx next start --port 3100',
    url: 'http://127.0.0.1:3100',
    reuseExistingServer: !process.env.CI,
    timeout: 180_000,
    env: {
      E2E_BYPASS_AUTH: '1',
      // The Supabase + API URLs still have to be defined because the
      // build / runtime import modules that read them. Pointing both
      // at unreachable hosts is fine — tests intercept every call via
      // page.route() before it leaves the browser.
      NEXT_PUBLIC_SUPABASE_URL: 'http://127.0.0.1:54399',
      NEXT_PUBLIC_SUPABASE_ANON_KEY: 'e2e-mock-anon-key',
      NEXT_PUBLIC_API_URL: 'http://127.0.0.1:8099',
    },
  },
});
