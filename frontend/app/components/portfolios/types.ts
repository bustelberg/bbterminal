/** Shared types for the /portfolios surfaces.
 *
 * `Basket` lived in `PerformanceModal.tsx` until that modal was removed as dead code (2026-08-03).
 * It has nothing to do with performance — every Analyse/Fundamental entry point passes one — so
 * it now sits with the other per-area type modules (`earnings/types.ts`, `momentum/types.ts`,
 * `universe/types.ts`) rather than in whichever component happened to declare it first.
 */

/** A group of holdings to aggregate as a value-weighted basket, instead of a single ISIN.
 *  `name` is carried only for progress display (the backend ignores it). */
export type Basket = {
  holdings: { isin: string; weight: number; name?: string }[];
  label: string;
};
