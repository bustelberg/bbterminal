'use client';

/**
 * The per-card Daily switch — used by the two YIELD cards and nothing else.
 *
 * ⚠ WHY IT IS NOT A THIRD OPTION ON THE TAB'S CADENCE CONTROL. A yield is the only shape on the
 * Long Equity tab with a daily input: its denominator is a price (or a price × share count), which
 * moves every trading day. The other ten cards are pure accounting — revenue, margins, debt
 * ratios, cash conversion — and have no daily figure at all, so a tab-wide "Daily" would leave ten
 * charts blank and look broken rather than unavailable.
 *
 * Off, the card follows the tab (Annual / Quarterly). On, it overrides for this card only.
 */
export default function DailyToggle({ on, onChange, note }: {
  on: boolean;
  onChange: (v: boolean) => void;
  /** What daily means for THIS card — the numerator that stays flat and the leg that moves. */
  note: string;
}) {
  return (
    <button type="button" onClick={() => onChange(!on)} aria-pressed={on} title={note}
      className={`cursor-pointer shrink-0 text-[10px] px-1.5 py-0.5 rounded border transition-colors ${
        on
          ? 'bg-accent-600 text-white border-transparent'
          : 'border-neutral-800/40 text-fg-subtle hover:bg-overlay/5 hover:text-accent-300'}`}>
      Daily
    </button>
  );
}
