'use client';

import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { INFO_ICON } from '../../lib/infoIcon';

/**
 * Small "i" icon that reveals a tooltip on hover. Tooltip is positioned
 * with `position: fixed` and clamped into the viewport so it can't be
 * clipped by overflow:hidden ancestors (the bane of inline tooltips).
 *
 * Renders the tooltip ABOVE the icon (translateY(-100%)). Pass `text`
 * for the body content; `\n` is preserved as a line break and `\n\n`
 * reads as a paragraph break (the `whitespace-pre-line` style on the
 * inner span keeps newlines without preserving other whitespace
 * collapsing).
 *
 * Originated in EarningsDashboard; lifted here so any future "help
 * icon next to a label" usage can drop it in.
 */
export default function InfoTip({ text, content, children }: {
  text?: string;
  /** Rich JSX body — rendered instead of `text` when given, so a caller can style a structured
   *  card (labels, pills, dividers) while still using this component's viewport-clamped positioning. */
  content?: React.ReactNode;
  /**
   * Optional TRIGGER. Without it you get the "i" icon (every existing call site). With it, the
   * children ARE the trigger — hover the thing itself, no icon needed.
   *
   * This exists because the native `title=` attribute is unusable for anything a reader needs:
   * the browser sits on it for ~1-2 SECONDS before showing it, and that delay is not
   * configurable. A tooltip that arrives after the reader has given up explaining a column to
   * themselves — wrongly — is worse than no tooltip. This one appears on hover, immediately.
   */
  children?: React.ReactNode;
}) {
  const [show, setShow] = useState(false);
  // PINNED = clicked open so the reader can SELECT the text (source, formula) and copy it. Hover
  // still previews; a click sticks it, and another click / a click outside / the × closes it. A
  // hover tooltip is `pointer-events-none` (it must not eat the pointer over the value); a pinned
  // one is interactive and selectable.
  const [pinned, setPinned] = useState(false);
  const visible = show || pinned;
  // Off-screen initial position; useLayoutEffect snaps the tooltip to
  // its real position after measuring the rendered size, before the
  // browser paints — so the user never sees the off-screen frame.
  const [pos, setPos] = useState<{ top: number; left: number }>({
    top: -9999,
    left: -9999,
  });
  const iconRef = useRef<HTMLSpanElement>(null);
  const tooltipRef = useRef<HTMLSpanElement>(null);

  const margin = 8;

  // Position the tooltip AFTER it renders, using its actual measured
  // size. This is the only way to keep it on-screen when the content
  // height varies wildly (e.g., a 3-paragraph "why empty" disclosure
  // vs. a one-line metric definition). Strategy:
  //
  //   1. Try above the icon — preferred so the tooltip doesn't cover
  //      the value cell the user is hovering near.
  //   2. If it would overflow above, place below.
  //   3. If neither fits fully, clamp to viewport edges and accept
  //      the `max-h-[80vh] overflow-hidden` cap on the tooltip span.
  //
  // Runs synchronously before paint, so position changes don't flash.
  useLayoutEffect(() => {
    if (!visible || !tooltipRef.current || !iconRef.current) return;
    const tipRect = tooltipRef.current.getBoundingClientRect();
    const iconRect = iconRef.current.getBoundingClientRect();
    const vh = window.innerHeight;
    const vw = window.innerWidth;

    // Horizontal: center on icon, clamped to viewport width.
    const cx = iconRect.left + iconRect.width / 2;
    const desiredLeft = cx - tipRect.width / 2;
    const maxLeft = vw - margin - tipRect.width;
    const left = Math.max(margin, Math.min(desiredLeft, maxLeft));

    // Vertical: above → below → clamp.
    const above = iconRect.top - 8 - tipRect.height;
    const below = iconRect.bottom + 8;
    let top: number;
    if (above >= margin) {
      top = above;
    } else if (below + tipRect.height <= vh - margin) {
      top = below;
    } else {
      // Last resort — tooltip is taller than either side's space.
      // Pin to whichever edge gives more space; the inner span's
      // max-h-[80vh] truncates the content.
      const spaceAbove = iconRect.top - margin;
      const spaceBelow = vh - iconRect.bottom - margin;
      top = spaceAbove >= spaceBelow ? margin : Math.max(margin, vh - margin - tipRect.height);
    }

    if (top !== pos.top || left !== pos.left) {
      setPos({ top, left });
    }
    // pos is intentionally excluded — we only want this to run when
    // visibility or content changes. Re-running on pos updates would
    // be infinite-loopy.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [visible, text, content]);

  // While pinned, a click anywhere OUTSIDE the trigger and the tooltip dismisses it — the usual
  // popover affordance. A click inside the tooltip (selecting text) must not, so both refs are
  // excluded; the tooltip also stops its own mousedown from bubbling, so a badge inside a
  // clickable table row can be interacted with without toggling the row.
  useEffect(() => {
    if (!pinned) return;
    const onDown = (e: MouseEvent) => {
      const t = e.target as Node;
      if (!tooltipRef.current?.contains(t) && !iconRef.current?.contains(t)) setPinned(false);
    };
    document.addEventListener('mousedown', onDown);
    return () => document.removeEventListener('mousedown', onDown);
  }, [pinned]);

  return (
    <span className="relative cursor-help" onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}>
      <span
        ref={iconRef}
        // Click toggles PINNED. stopPropagation so a badge sitting in a clickable table row
        // (expand-on-click) does not also toggle the row. Un-pinning also clears the hover state so
        // a second click closes it immediately rather than lingering on hover.
        onClick={(e) => {
          e.stopPropagation();
          if (pinned) { setPinned(false); setShow(false); } else setPinned(true);
        }}
        className={children
          ? undefined
          : INFO_ICON}
      >
        {children ?? 'i'}
      </span>
      {visible && (
        <span
          ref={tooltipRef}
          // A click/mousedown inside must not bubble to a row handler, and (when pinned) it must
          // not trigger the click-outside dismiss either.
          onClick={(e) => e.stopPropagation()}
          onMouseDown={(e) => e.stopPropagation()}
          // `max-h-[80vh]` + `overflow-hidden` keep the tooltip inside the viewport. It is
          // `pointer-events-none` on HOVER (so it can't eat the pointer over the value) and
          // interactive + `select-text` when PINNED (so the source/formula can be selected & copied).
          // ⚠ `normal-case` + `text-left` + `tracking-normal` + `font-normal` are RESETS, not
          // styling. The tooltip renders inside its trigger, so it inherits whatever the trigger
          // sits in — a table header carries `uppercase tracking-wide text-right`, which once
          // rendered the whole explanation SHOUTED IN CAPS, right-aligned. Inheritance did it.
          className={`fixed w-72 max-h-[80vh] overflow-hidden px-3 py-2 bg-popover border rounded-lg text-xs text-fg-soft leading-relaxed z-[9999] shadow-xl whitespace-pre-line normal-case text-left tracking-normal font-normal ${
            pinned
              ? 'pointer-events-auto cursor-auto select-text border-accent-500/50'
              : 'pointer-events-none border-neutral-700'}`}
          style={{ top: pos.top, left: pos.left }}
        >
          {pinned && (
            <button
              type="button"
              onClick={() => { setPinned(false); setShow(false); }}
              className="absolute top-1 right-1.5 text-fg-faint hover:text-fg text-sm leading-none"
              aria-label="close"
            >
              ×
            </button>
          )}
          {content ?? text}
        </span>
      )}
    </span>
  );
}
