/**
 * The two-arrow reload glyph, spinning while a refresh runs.
 *
 * ⚠ AN INLINE SVG, NOT THE `↻` CHARACTER — the glyph renders at a different weight and baseline in
 * every font that has it, and is simply missing in some, so a text arrow is a control that looks
 * different on each machine.
 *
 * ⚠ IT LIVES HERE RATHER THAN IN `PortfolioOverviewPanel` BECAUSE THE MODAL NEEDS IT TOO, and that
 * panel already imports the modal — importing back would be a cycle. One glyph, so the refresh in
 * the Analyse modal's header and the one on the row it was opened from are visibly the same
 * control, which is the whole point of putting it in both places.
 */
export function RefreshIcon({ spinning, size = 14 }: { spinning?: boolean; size?: number }) {
  return (
    <svg viewBox="0 0 24 24" width={size} height={size} fill="none" stroke="currentColor"
      strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"
      className={spinning ? 'animate-spin' : ''}>
      <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8" />
      <path d="M21 3v5h-5" />
      <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16" />
      <path d="M3 21v-5h5" />
    </svg>
  );
}
