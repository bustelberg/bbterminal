/** The risk profiles an AIRS product line is offered at.
 *
 * Shared by the /portfolios table and the correlation matrix beneath it, so the two panels on one
 * page cannot offer different profiles, label the same one differently, or disagree about which
 * models are in it.
 *
 * ⚠ THE CLASSIFICATION IS THE BACKEND'S, AND MUST STAY THERE. `_airs_portfolio_variant` decides
 * which profile a model has, off AIRS's own name; this file only filters on string equality.
 * The rule is not portable: "bep offensief" CONTAINS "offensief", so a naive reimplementation
 * puts Beperkt Offensief models in the Offensief filter — and does it for only ONE of the five
 * (the four spelled BEPOF/Bepoff/BEPOFF/BEOFF survive), which is exactly the sort of bug a
 * reasonable test misses.
 *
 * ⚠ THESE STRINGS MUST MATCH `_airs_portfolio_variant.VARIANTS` EXACTLY. A profile spelled
 * differently here matches nothing and renders an empty result that reads as "we own none of
 * those" rather than as a typo.
 */
export type Variant = 'all' | 'Offensief' | 'Beperkt Offensief' | 'Neutraal' | 'Defensief';

/** Most-to-least offensive — the order these are understood in, not alphabetical. */
export const VARIANT_FILTERS: { key: Variant; label: string }[] = [
  { key: 'all', label: 'All' },
  { key: 'Offensief', label: 'Offensief' },
  { key: 'Beperkt Offensief', label: 'Bep. offensief' },
  { key: 'Neutraal', label: 'Neutraal' },
  { key: 'Defensief', label: 'Defensief' },
];
