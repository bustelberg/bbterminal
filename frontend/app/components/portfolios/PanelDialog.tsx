'use client';

/**
 * A panel from the Analyse modal, raised into the middle of the screen.
 *
 * ⚠⚠ IT EXISTS BECAUSE THE BUTTON AND ITS RESULT WERE AT OPPOSITE ENDS OF THE MODAL. Risk and
 * Attribution are opened from controls beside the allocation bars, at the very TOP, and rendered
 * into a slot BELOW the composition charts — a screen or more further down. Pressing a button and
 * having nothing visibly happen is indistinguishable from a button that does not work, and the
 * reader who does scroll has lost the thing they were comparing against on the way.
 *
 * ⚠ NOT USED FOR `BucketDetailPanel`, deliberately. That one is opened by clicking a BAR in a
 * chart, so the chart is its context and appearing directly beneath the bar you clicked is the
 * correct behaviour — it is already next to what opened it. The rule is about the DISTANCE between
 * a control and its result, not about the panel being important.
 *
 * ⚠ MOUNTED INSIDE THE ANALYSE MODAL'S CONTENT BOX, never beside it — the same rule the Fundamental
 * and Owner-earnings modals already follow, and for the same reason: the Analyse modal's backdrop
 * closes it on click, so a nested backdrop mounted as its SIBLING would bubble a dismissal straight
 * through and close both. The content box stops propagation, so from in there the two dismiss
 * independently, and `fixed inset-0` still escapes the box's LAYOUT while staying inside its event
 * tree.
 */
import { useEffect } from 'react';

export default function PanelDialog({ onClose, children, labelledBy }: {
  onClose: () => void;
  children: React.ReactNode;
  /** id of the heading inside `children`, when it has one. */
  labelledBy?: string;
}) {
  /**
   * ⚠⚠ ESCAPE CLOSES THIS ONE, NOT THE MODAL BEHIND IT — and that is a bug fix, not a nicety.
   * `PortfolioAnalysisModal` listens for Escape on `window` and calls its own `onClose`, so every
   * nested dialog in this folder currently dismisses the ENTIRE analysis when you press Escape to
   * dismiss the dialog. Twelve of them do it (`MarginInputsModal`, `HoldingsRevenueModal`,
   * `OwnerEarningsModal`, `FundamentalsModal`, …): none registers a handler, so the parent's is the
   * only one that runs. This shell does not add a thirteenth.
   *
   * ⚠ CAPTURE PHASE, WHICH IS THE ENTIRE MECHANISM. Both listeners sit on `window`; a keydown is
   * dispatched at the focused element and bubbles up, so a capture listener here runs BEFORE the
   * parent's bubble listener and `stopPropagation` stops the event ever reaching it. A bubble-phase
   * listener would be a coin flip on registration order.
   */
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      e.stopPropagation();
      onClose();
    };
    window.addEventListener('keydown', onKey, true);
    return () => window.removeEventListener('keydown', onKey, true);
  }, [onClose]);

  return (
    <div className="fixed inset-0 z-[60] flex items-center justify-center bg-scrim/60 p-4 sm:p-6"
      onClick={onClose} role="presentation">
      {/**
        * ⚠⚠ A FIXED BOX, AND THE CONTENT SCROLLS INSIDE IT — never a box that grows to its content.
        * Sized to the content, this dialog resized on every switch between its two views, on every
        * change of the attribution axis, and twice more on each load (a one-line "Computing…"
        * collapsed it to a strip, then it snapped open when the payload landed). Everything the
        * reader had their eye on moved each time, including the control they had just clicked.
        *
        * ⚠ AND IT IS SMALLER THAN THE ANALYSE MODAL BEHIND IT (80vw/80vh), not larger. A nested
        * dialog that overhangs its parent reads as a new screen rather than as something opened
        * from the one underneath, and there is no longer any edge of the parent visible to dismiss
        * back to.
        *
        * ⚠ `min-h-0` IS LOAD-BEARING ON EVERY FLEX CHILD BELOW THIS. A flex item's default
        * `min-height:auto` refuses to shrink below its content, so a child that declares
        * `overflow-auto` still stretches the box instead of scrolling — the fixed height silently
        * stops being fixed, and only for the tallest content, which is the case nobody tests.
        */}
      {/* ⚠ `w-[76vw]`, NOT `w-full max-w-6xl`. This backdrop is `fixed inset-0`, so it spans the
          VIEWPORT rather than the modal behind it — and `w-full` there resolves against the
          viewport too. On a 1200px screen that is 1152px of dialog inside a 960px parent: wider
          than the thing it opened from, which is the one shape the sizing above exists to avoid.
          A viewport-relative width keeps the claim true at every size. */}
      <div className="w-[76vw] h-[76vh] flex flex-col min-h-0"
        onClick={(e) => e.stopPropagation()} role="dialog" aria-modal="true"
        aria-labelledby={labelledBy}>
        {children}
      </div>
    </div>
  );
}
