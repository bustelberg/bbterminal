'use client';

import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';

/** Small "i" affordance that pops a tooltip below itself.
 *
 * The tooltip is rendered via a portal to <body> so it escapes any
 * `contain: paint` / `content-visibility` ancestor (e.g. CollapsibleCard's
 * body). Without the portal, those containment styles re-target
 * position:fixed descendants to the contained element instead of the
 * viewport, and the tooltip lands off-screen.
 *
 * Closes on any scroll so the fixed-positioned tooltip doesn't float
 * over the sticky header after its anchor row has scrolled away.
 */
export default function CellInfoTip({ children }: { children: React.ReactNode }) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState({ top: 0, left: 0 });
  const [mounted, setMounted] = useState(false);
  const iconRef = useRef<HTMLSpanElement>(null);
  const tipWidth = 220;
  const margin = 8;

  // createPortal needs `document` which is undefined during SSR — defer
  // portal rendering until after first client-side mount. This IS the
  // canonical "am I client-side?" pattern; React 19's set-state-in-effect
  // lint flags it but the alternatives (useSyncExternalStore boilerplate,
  // or an impure useState initializer that touches `window`) are worse.
  // eslint-disable-next-line react-hooks/set-state-in-effect
  useEffect(() => setMounted(true), []);

  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const clampedLeft = Math.max(
        margin + tipWidth / 2,
        Math.min(centerX, window.innerWidth - margin - tipWidth / 2),
      );
      setPos({ top: rect.bottom + 6, left: clampedLeft });
    }
    setShow(true);
  };

  useEffect(() => {
    if (!show) return;
    const close = () => setShow(false);
    window.addEventListener('scroll', close, true);
    return () => window.removeEventListener('scroll', close, true);
  }, [show]);

  return (
    <span
      className="inline-block align-middle"
      onMouseEnter={handleEnter}
      onMouseLeave={() => setShow(false)}
    >
      <span
        ref={iconRef}
        className="inline-flex items-center justify-center w-3 h-3 ml-1 rounded-full border border-gray-700 text-gray-500 text-[8px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors cursor-help align-middle"
      >
        i
      </span>
      {show && mounted && createPortal(
        <span
          className="fixed px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-[11px] text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, width: tipWidth, transform: 'translate(-50%, 0)' }}
        >
          {children}
        </span>,
        document.body,
      )}
    </span>
  );
}
