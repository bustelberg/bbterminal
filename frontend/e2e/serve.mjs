// A persistent production server for FAST local e2e — build once, then leave it running.
//
// Playwright's `webServer` rebuilds (`next build`, ~35s) on every fresh `npm run e2e`. Its
// `reuseExistingServer` skips that boot entirely when a server is already up on :3100 — but
// Playwright tears down any server IT starts, so the reuse only helps if YOU keep one alive.
// That is what this is: run it once in its own terminal, then iterate with
//
//     npm run e2e -- portfolios.spec.ts --grep "book-vs-strategy"   # ~2s, no rebuild
//
// It bakes in the exact env the config's webServer uses, so the persistent server behaves
// identically: E2E_BYPASS_AUTH (read at runtime by proxy.ts) + the mock NEXT_PUBLIC_* URLs
// (inlined at BUILD time — which is why they must be set for `next build`, not just `next start`;
// tests intercept every call via page.route() before it leaves the browser).
//
// A plain node launcher, deliberately: no `cross-env` dep, and inline `X=y cmd` env prefixes are
// bash-only and break on Windows PowerShell. Rebuild (restart this) after changing app code; for
// test-only edits nothing here needs to re-run.
import { spawnSync } from 'node:child_process';

const env = {
  ...process.env,
  E2E_BYPASS_AUTH: '1',
  NEXT_PUBLIC_SUPABASE_URL: 'http://127.0.0.1:54399',
  NEXT_PUBLIC_SUPABASE_ANON_KEY: 'e2e-mock-anon-key',
  NEXT_PUBLIC_API_URL: 'http://127.0.0.1:8099',
};

const next = (args) => spawnSync('next', args, { stdio: 'inherit', shell: true, env });

console.log('[e2e:serve] building…');
if (next(['build']).status !== 0) process.exit(1);
console.log('[e2e:serve] serving on http://127.0.0.1:3100 — leave this running; e2e will reuse it.');
next(['start', '--port', '3100']);
