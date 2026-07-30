/** THE info icon. One definition, imported everywhere — there is no second correct one.
 *
 * ⚠ THIS EXISTS BECAUSE THERE WERE FOUR. `InfoTip`, a second `InfoTip` under `universe/`,
 * `ApiUsageBadge` and `Provenance` each carried their own copy of the class string, and they had
 * drifted into two visibly different icons: a grey OUTLINED circle at `w-4`/`text-[10px]` and a
 * filled ACCENT circle at `w-3.5`/`text-[9px]`. Both appeared on the same screen, so the same
 * affordance read as two different controls.
 *
 * ⚠ APPEARANCE ONLY — NO MARGIN. Spacing belongs to the call site (`ml-1` beside a number,
 * nothing inside a flex row with its own gap). Folding a margin in here would make every future
 * caller either accept the wrong spacing or override it, and an overridden shared class is how
 * the next fork starts.
 *
 * ⚠ AND IT MUST PIN EVERY INHERITED TEXT PROPERTY, OR THE CELL AROUND IT RESTYLES THE GLYPH.
 * The icon's content is the LITERAL CHARACTER `i`, so it is text, and text inherits. Everything
 * here was specified except the properties that shape a character — and each omission produced
 * the same bug in a different place, twice, on the same page:
 *
 *   `font-family`     the ⓘ beside the portfolio NAME (`text-fg`) and the one beside its position
 *                     COUNT (`text-right font-mono`) are the identical component with the
 *                     identical class string, and rendered with different letterforms and
 *                     different baselines — one table row, two icons.
 *   `text-transform`  the attribution table's `<thead>` row is `uppercase tracking-wide`, so the
 *                     `i` came out as a wide-tracked capital `I` — while the WARN variant's `!`
 *                     is unaffected by case, making even the two states disagree with each other.
 *
 * That is the "one affordance reading as two controls" failure this file was created to end,
 * arriving through inheritance rather than through a forked copy — which is worse, because there
 * is no duplicated string for a reviewer or the guard test to find. A shared appearance class
 * that leaves a visual property unset has not actually shared that property.
 */

/** Shape, size, colour, weight, and every text property that would otherwise be inherited.
 *  Identical everywhere the reader sees an "i" — including inside a `font-mono` cell or an
 *  `uppercase tracking-wide` table header, which is why these are stated, not left open. */
export const INFO_ICON =
  'inline-flex items-center justify-center w-3.5 h-3.5 rounded-full align-middle '
  + 'font-mono text-[9px] font-semibold leading-none normal-case tracking-normal '
  + 'cursor-help transition-colors '
  + 'bg-accent-500/10 text-accent-500 hover:bg-accent-500/20';

/** The same icon carrying a warning — a `!` on stale data. ⚠ Same geometry, same font AND same
 *  text properties as `INFO_ICON` on purpose: it must read as the SAME control in a different
 *  state, not as another control. Only the hue changes. */
export const INFO_ICON_WARN =
  'inline-flex items-center justify-center w-3.5 h-3.5 rounded-full align-middle '
  + 'font-mono text-[9px] font-semibold leading-none normal-case tracking-normal '
  + 'cursor-help transition-colors '
  + 'bg-warn-500/20 text-warn-600 hover:bg-warn-500/30';
