/**
 * WHICH tiles the home page has, and in what order. No copy, no hooks, NO `'use client'`.
 *
 * ⚠⚠ THE MISSING DIRECTIVE IS THE WHOLE REASON THIS FILE EXISTS, AND THE BUG IT FIXES IS A RUNTIME
 * ONE. `HOME_TILE_ORDER` lived in `homeCopy.ts`, which is `'use client'` because it exports a hook.
 * `app/page.tsx` is a SERVER component — it reads the session and the `view_as` cookie — and when a
 * server module imports from a client one, the bundler hands back a CLIENT REFERENCE PROXY rather
 * than the module's values. Component exports survive that; a plain array does not. The result was
 *
 *     TypeError: …homeCopy.ts [app-rsc] (ecmascript).HOME_TILE_ORDER.filter is not a function
 *
 * on the first render of the home page, with `tsc` clean and every unit test green — vitest imports
 * modules directly and never applies the RSC boundary transform, so nothing in the suite can see
 * this. The rule it enforces: a server component may import DATA only from a module with no
 * `'use client'` directive.
 *
 * ⚠ `homeCopy.ts` IMPORTS THE KEY FROM HERE, so the copy map and the render order still cannot
 * drift — `Record<HomeTileKey, …>` is what makes a missing translation a compile error, and that is
 * unchanged by the split.
 */
export type HomeTileKey =
  | '/management-dashboard'
  | '/earnings'
  | '/schedule'
  | '/backtest'
  | '/universe'
  | '/longequity-universe'
  | '/sp500'
  | '/acwi'
  | '/leonteq'
  | '/fx-rates'
  | '/airs-portfolio'
  | '/request_gurufocus'
  | '/benchmarks';

/**
 * Display order, and the ONE list of what exists.
 *
 * ⚠ SEPARATE FROM THE COPY MAP ON PURPOSE: the order is a layout decision and belongs to neither
 * language, and the server filters this array by role before any copy is read. Every entry must
 * appear in `HomeCopy['tiles']` — `HomeTileKey` is what enforces that in both directions.
 */
export const HOME_TILE_ORDER: readonly HomeTileKey[] = [
  '/management-dashboard',
  '/earnings',
  '/schedule',
  '/backtest',
  '/universe',
  '/longequity-universe',
  '/sp500',
  '/acwi',
  '/leonteq',
  '/fx-rates',
  '/airs-portfolio',
  '/request_gurufocus',
  '/benchmarks',
];
