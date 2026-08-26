/**
 * ONE BADGE LOOK FOR EVERYTHING INSIDE AN ⓘ CARD.
 *
 * ⚠⚠ IT EXISTS BECAUSE THERE WERE THREE. The provenance card set its date as bare `font-mono`
 * beside a `rounded-full` freshness pill, and the new live-value badge arrived with a third
 * treatment of its own — three ways of saying "this bit is data, not prose", inside one family of
 * tooltips a reader moves between by hovering. A badge's whole job is to be recognised without
 * being read; three looks means it has to be read.
 *
 * ⚠ THE CHROME IS SHARED, THE TYPOGRAPHY IS NOT — and that division is the point rather than a
 * compromise. Tint, border and radius are what make two things look like the same KIND of object,
 * so those are here. Size and face belong to the role: a status pill is a fixed short label set
 * small, a value flows inline in a sentence and must sit on that sentence's baseline at its size.
 * Forcing the value badge to the pill's `text-[10px]` would shrink a date below the prose around
 * it, which reads as a rendering fault rather than as emphasis.
 *
 * ⚠ TOKENS ONLY, and the ⚠⚠ from the design system applies: `overlay` may never be used bare —
 * these use `neutral-*` at low alpha, which is the same wash the pill already had.
 */

/** The default: a value or a status that carries no verdict. */
export const BADGE_NEUTRAL = 'bg-neutral-500/10 border border-neutral-700/40';

/** The one warned state. ⚠ SAME TWO COLOURS THE PROVENANCE ICON HAS, driven by the same boolean —
 *  a third tone is how the pill and the icon came to disagree in the first place. */
export const BADGE_WARN = 'bg-warn-500/15 border border-warn-500/40 text-warn-600';

/** A short status label — fixed size, fully rounded. */
export const BADGE_PILL = 'px-1.5 py-px rounded-full text-[10px] font-medium whitespace-nowrap';

/**
 * A live value inside running prose — inherits the sentence's size, keeps the data face.
 *
 * ⚠ `rounded-full` LIKE THE PILL, deliberately, even though some values are long. A date range
 * sets as a lozenge and that is fine; two radii one line apart is what makes two badges read as
 * two unrelated conventions.
 */
export const BADGE_VALUE = 'inline-block px-1.5 rounded-full font-mono tabular-nums';
