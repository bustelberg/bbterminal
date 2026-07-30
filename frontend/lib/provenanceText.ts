/**
 * Text helpers for the provenance card, split out of `provenance.tsx` so they can be unit-tested
 * (this repo is unit tests only — see CLAUDE.md; a `.tsx` with JSX is not the place for a pure
 * string function).
 */

/**
 * ⚠ The card supplies the sentence's final period, so a `how` carrying its own renders "..".
 * Stripped centrally rather than at each of the ~40 call sites, where a stray period is invisible
 * in the source and only shows up on screen.
 */
export const trimStop = (s: string) => s.replace(/\.\s*$/, '');
