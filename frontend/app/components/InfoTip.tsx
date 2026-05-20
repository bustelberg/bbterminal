'use client';

import { useRef, useState } from 'react';

/**
 * Small "i" icon that reveals a tooltip on hover. Tooltip is positioned
 * with `position: fixed` and clamped into the viewport so it can't be
 * clipped by overflow:hidden ancestors (the bane of inline tooltips).
 *
 * Renders the tooltip ABOVE the icon (translateY(-100%)). Pass `text`
 * for the body content; multiline strings render as a single block —
 * if you need richer markup use a children prop instead (not exposed
 * today; can be added when the first caller needs it).
 *
 * Originated in EarningsDashboard; lifted here so any future "help
 * icon next to a label" usage can drop it in.
 */
export default function InfoTip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number }>({ top: 0, left: 0 });
  const iconRef = useRef<HTMLSpanElement>(null);

  const tipWidth = 224; // w-56
  const margin = 8;

  // Compute the tooltip's top-left corner directly so we don't have to
  // reason about how a transform interacts with the clamp. Tooltip is
  // shown ABOVE the icon (translateY(-100%) handles the vertical shift,
  // since the rendered height isn't known until after layout); horizontal
  // position is the icon's center minus half the tooltip's width, then
  // clamped into the viewport.
  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const desiredLeft = rect.left + rect.width / 2 - tipWidth / 2;
      const maxLeft = window.innerWidth - margin - tipWidth;
      const clampedLeft = Math.max(margin, Math.min(desiredLeft, maxLeft));
      setPos({ top: rect.top - 8, left: clampedLeft });
    }
    setShow(true);
  };

  return (
    <span className="relative cursor-help" onMouseEnter={handleEnter} onMouseLeave={() => setShow(false)}>
      <span ref={iconRef} className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-gray-600 text-gray-500 text-[10px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors">i</span>
      {show && (
        <span
          className="fixed w-56 px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-xs text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, transform: 'translateY(-100%)' }}
        >
          {text}
        </span>
      )}
    </span>
  );
}
