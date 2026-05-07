'use client';

import { useState, type ReactNode } from 'react';

type Props = {
  /** Header label. Use a string for the simple case, or pass a node when
   * the title needs additional inline elements (e.g. a "log scale" toggle). */
  title: ReactNode;
  /** Right-aligned annotation in the header bar (sweep status, sector
   * count, anything advisory). Optional — omit for a left-only header. */
  rightSlot?: ReactNode;
  /** Initial collapsed state. Defaults to expanded. */
  defaultCollapsed?: boolean;
  /** Body wrapper class — defaults to no padding so the caller can control
   * spacing precisely (some bodies want full-bleed tables, others want
   * `px-5 pb-5`). */
  bodyClassName?: string;
  children: ReactNode;
};

/** Card chrome shared by every data panel on /momentum. The header is a
 * `role=button` div (not a real `<button>`) so consumers can put nested
 * interactive elements — buttons, dropdowns, toggles — into rightSlot
 * without violating "no button inside button". Click anywhere on the
 * header toggles the body; nested interactives need `e.stopPropagation()`
 * on their own click handlers to opt out of that toggle. A chevron
 * indicates state. Mirrors the look that started in SectorTimelineChart
 * so every card collapses the same way. */
export default function CollapsibleCard({
  title,
  rightSlot,
  defaultCollapsed = false,
  bodyClassName,
  children,
}: Props) {
  const [collapsed, setCollapsed] = useState(defaultCollapsed);
  const toggle = () => setCollapsed((c) => !c);
  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
      <div
        role="button"
        tabIndex={0}
        aria-expanded={!collapsed}
        className="w-full flex items-center justify-between px-5 py-3 text-left hover:bg-white/[0.02] transition-colors cursor-pointer select-none"
        onClick={toggle}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            toggle();
          }
        }}
      >
        <h3 className="text-white text-sm font-medium flex items-center gap-2 min-w-0">
          <Chevron collapsed={collapsed} />
          {title}
        </h3>
        {rightSlot != null && (
          <div className="text-[11px] text-gray-500 ml-3 flex items-center gap-2 flex-wrap">{rightSlot}</div>
        )}
      </div>
      {!collapsed && (
        // `content-visibility: auto` lets the browser skip layout + paint
        // for off-screen card bodies during scroll. With 10 variant runs
        // and a wide-universe daily backtest the page can host tens of
        // thousands of DOM nodes; this turns scrolling from janky into
        // smooth without changing any visual output. `contain-intrinsic-size`
        // gives the browser a placeholder height so the scrollbar doesn't
        // jitter as cards realize.
        <div
          className={bodyClassName}
          style={{ contentVisibility: 'auto', containIntrinsicSize: '600px' }}
        >
          {children}
        </div>
      )}
    </div>
  );
}

function Chevron({ collapsed }: { collapsed: boolean }) {
  return (
    <svg
      width="10"
      height="10"
      viewBox="0 0 10 10"
      className={`text-gray-500 transition-transform ${collapsed ? '' : 'rotate-90'}`}
      fill="currentColor"
      aria-hidden="true"
    >
      <path d="M3 1.5 L7 5 L3 8.5 Z" />
    </svg>
  );
}
