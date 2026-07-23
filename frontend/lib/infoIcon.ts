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
 */

/** Shape, size, colour, weight. Identical everywhere the reader sees an "i". */
export const INFO_ICON =
  'inline-flex items-center justify-center w-3.5 h-3.5 rounded-full align-middle '
  + 'text-[9px] font-semibold leading-none cursor-help transition-colors '
  + 'bg-accent-500/10 text-accent-500 hover:bg-accent-500/20';

/** The same icon carrying a warning — a `!` on stale data. ⚠ Same geometry as `INFO_ICON` on
 *  purpose: it must read as the SAME control in a different state, not as another control. Only
 *  the hue changes. */
export const INFO_ICON_WARN =
  'inline-flex items-center justify-center w-3.5 h-3.5 rounded-full align-middle '
  + 'text-[9px] font-semibold leading-none cursor-help transition-colors '
  + 'bg-warn-500/20 text-warn-600 hover:bg-warn-500/30';
