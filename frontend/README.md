# BBTerminal — Frontend

Next.js 16 (App Router) UI for the BBTerminal financial data terminal.

For full setup instructions (Supabase, backend, env vars), see the [project root README](../README.md#local-development).

## Quick start

```bash
npm install
npm run dev
```

Frontend runs on `http://localhost:3000`. Requires `frontend/.env.local` (see [local environment variables](../README.md#environment-variables)) and the backend running on `http://localhost:8000`.

## Conventions

- All pages and components are client components (`'use client'`).
- This is **Next.js 16** — APIs and conventions may differ from older versions. See `AGENTS.md` and `node_modules/next/dist/docs/` before relying on training-data knowledge.
- Stores in `lib/stores/` use a lightweight `createStore` reactive pattern; SSE-driven flows (backtest, ingest, broker scan) live there so they survive page navigation.
- See the [UI design system](../docs/ui-design-system.md).

## Useful commands

```bash
npm run dev        # Dev server (Turbopack)
npm run build      # Production build
npm run lint       # ESLint
npx tsc --noEmit   # Type check
npm test           # Unit tests (vitest)
npm run e2e        # E2E tests (Playwright — runs a production build)
```
