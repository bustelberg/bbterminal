'use client';

/**
 * Animated three-dot loading indicator. CSS-only — no JS tick, no
 * re-renders, no React state. Drop in anywhere you'd have written
 * "Loading…" so the user sees motion instead of dead text.
 *
 * Usage:
 *   <LoadingDots />                    →  · · ·   (bare dots, default)
 *   <LoadingDots label="Loading" />    →  Loading · · ·
 *   <LoadingDots label="Refreshing" className="text-gray-400" />
 *
 * Animation: each dot fades from 20% → 100% → 20% on a 1.4 s cycle,
 * staggered by 0.16 s. The keyframes live in this file's inline
 * <style jsx> so the component is fully self-contained.
 */
export default function LoadingDots({
  label,
  className,
}: {
  label?: string;
  className?: string;
}) {
  return (
    <span className={`inline-flex items-baseline gap-1 ${className ?? ''}`}>
      {label && <span>{label}</span>}
      <span className="inline-flex gap-[3px] ml-[2px]" aria-hidden="true">
        <span className="bb-loading-dot" />
        <span className="bb-loading-dot" style={{ animationDelay: '160ms' }} />
        <span className="bb-loading-dot" style={{ animationDelay: '320ms' }} />
      </span>
      <span className="sr-only">{label ? `${label}…` : 'Loading…'}</span>
      <style jsx>{`
        .bb-loading-dot {
          width: 4px;
          height: 4px;
          border-radius: 9999px;
          background: currentColor;
          opacity: 0.25;
          animation: bb-loading-bounce 1.4s ease-in-out infinite;
          align-self: center;
        }
        @keyframes bb-loading-bounce {
          0%, 80%, 100% { opacity: 0.2; }
          40% { opacity: 1; }
        }
      `}</style>
    </span>
  );
}
