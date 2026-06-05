import { useEffect, useState } from 'react';

/**
 * A periodically-updating `Date.now()` for relative-time displays
 * ("in 6h", "elapsed 12s", …). Replaces the hand-rolled
 * `setInterval(() => setNow(Date.now()), …)` effect that several
 * components each re-implemented (with their own lazy-init / purity
 * workarounds).
 *
 *   const now = useNow();          // ticks every 1s
 *   const now = useNow(15000);     // ticks every 15s
 *   const now = useNow(1000, isRunning);  // only ticks while enabled
 *
 * `enabled=false` stops the interval (and the re-renders) entirely —
 * pass a condition for tickers that only matter while something is
 * active. The returned value still reflects the last tick.
 */
export function useNow(intervalMs = 1000, enabled = true): number {
  // Lazy initializer so the impure Date.now() runs once at mount, not on
  // every render (react-hooks/purity).
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!enabled) return;
    const id = window.setInterval(() => setNow(Date.now()), intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs, enabled]);
  return now;
}
