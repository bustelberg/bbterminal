/**
 * The small uppercase badge a cell wears when it holds a STATE instead of a value.
 *
 * ⚠⚠ ONE COMPONENT, BECAUSE THE SAME WORD MUST LOOK THE SAME EVERYWHERE. `UNSUBSCRIBED` means one
 * thing in this app — GuruFocus lists this instrument only on exchanges outside our subscription,
 * so the data is unobtainable rather than missing — and it appears on the /asset-pipeline grid, on
 * /companies and in the fundamentals drill-downs. Three hand-rolled spans is three chances for the
 * same fact to render as three different things, at which point the reader has to learn each table
 * separately instead of learning the vocabulary once.
 *
 * ⚠ A BADGE IS FOR AN ANSWER, NOT A BLANK. Each one names a reason a cell cannot hold a number, and
 * every one of them is a dead end already paid for — so it must not read as an invitation to
 * retry. The `title` carries the full explanation; the label is the two words you can scan a
 * column for.
 */

/** The tones, so a caller picks from the vocabulary rather than inventing a colour.
 *
 * ⚠ `warn` vs `warnSoft` IS A REAL DISTINCTION, not two shades of the same idea: `warn` is
 * "unobtainable — stop asking" (UNSUBSCRIBED, NOT EQUITY's louder cousins) and `warnSoft` is "a
 * gap we could still close" (NO DATA). `muted`/`faint` are for the states that are simply facts
 * about the instrument and carry no urgency at all. */
export const BADGE_TONE = {
  warn: 'bg-warn-500/15 text-warn-300 border-warn-500/25',
  warnSoft: 'bg-warn-500/10 text-warn-300/80 border-warn-500/20',
  muted: 'bg-overlay/[0.06] text-fg-muted border-neutral-700',
  faint: 'bg-overlay/[0.06] text-fg-faint border-neutral-800',
} as const;

export type BadgeTone = (typeof BADGE_TONE)[keyof typeof BADGE_TONE];

export function StateBadge({ label, tone, title }: {
  label: string;
  tone: BadgeTone | string;
  /**
   * Why the cell holds this instead of a number, as a native `title`.
   *
   * ⚠ OPTIONAL ONLY BECAUSE `InfoTip` EXISTS — never because a badge may go unexplained. A caller
   * that wraps this in an `InfoTip` must NOT also pass a title: the browser would sit on the
   * native one for a second or two and then show a second tooltip over the instant one. Omit it
   * there, pass it everywhere else.
   */
  title?: string;
}) {
  return (
    <span title={title}
      className={`text-[10px] uppercase tracking-wider font-semibold px-1 py-0.5 rounded border
                  ${title ? 'cursor-help ' : ''}${tone}`}>
      {label}
    </span>
  );
}
