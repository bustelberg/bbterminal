'use client';

import { useLayoutEffect, useRef, useState } from 'react';

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
export default function InfoTip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
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
    if (!show || !tooltipRef.current || !iconRef.current) return;
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
  }, [show, text]);

  return (
    <span className="relative cursor-help" onMouseEnter={() => setShow(true)} onMouseLeave={() => setShow(false)}>
      <span ref={iconRef} className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-gray-600 text-gray-500 text-[10px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors">i</span>
      {show && (
        <span
          ref={tooltipRef}
          // `max-h-[80vh]` + `overflow-hidden` keep the tooltip inside
          // the viewport when neither above nor below has room for the
          // full content. `pointer-events-none` means the user can't
          // scroll inside it; in that case the most important content
          // (whyEmpty paragraph) is below the metric definition, so
          // ideally we'd reverse the order — left as a future tweak.
          className="fixed w-72 max-h-[80vh] overflow-hidden px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-xs text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none whitespace-pre-line"
          style={{ top: pos.top, left: pos.left }}
        >
          {text}
        </span>
      )}
    </span>
  );
}
