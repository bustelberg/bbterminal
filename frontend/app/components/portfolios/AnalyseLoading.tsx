'use client';

/**
 * WHAT THE ANALYSE MODAL SHOWS WHILE ITS ONE REQUEST IS IN FLIGHT.
 *
 * ⚠⚠ IT REPLACES A SKELETON, WHICH WAS THE WRONG ANSWER (2026-09-03, on request, in those words).
 * The skeleton drew placeholder blocks in the shape of the layout to come — allocation bars, the
 * scorecard chips, a table of grey rows — and the shape it promised is not the point of the wait:
 * this modal paints nothing until one request returns, so the reader is not watching a page
 * assemble, they are waiting for an answer. Big text and a moving ellipsis says the one true thing
 * (it is working) without dressing the wait up as progress.
 *
 * ⚠ THE DOTS ARE THE WHOLE MECHANISM AND THEY ARE HONEST. They cycle on a timer, so they move
 * whether the request is one second from returning or thirty — which is exactly what they claim.
 * A bar or a percentage would have to be animated against a guess: the endpoint answers once, and
 * its per-phase `timings_ms` arrive WITH the payload, i.e. after the wait is over.
 *
 * ⚠ THE LABEL'S OWN ELLIPSIS IS STRIPPED. Both translations end in one ("Loading composition…",
 * "Samenstelling laden…"), and left in place it would sit beside the animated one as four dots
 * that never move followed by three that do.
 */
import { useEffect, useState } from 'react';

/** How long each state of the ellipsis holds. Slow enough to read as breathing, not flicker. */
const TICK_MS = 400;

export default function AnalyseLoading({ label }: {
  /** "Loading composition…", translated by the caller. Its trailing ellipsis is dropped. */
  label: string;
}) {
  const [n, setN] = useState(1);
  useEffect(() => {
    const t = setInterval(() => setN((v) => (v % 3) + 1), TICK_MS);
    return () => clearInterval(t);
  }, []);

  return (
    <p className="py-20 text-center text-2xl font-semibold text-fg-subtle">
      {label.replace(/[.…\s]+$/, '')}
      {/* ⚠ THE WIDTH IS RESERVED FOR ALL THREE DOTS. Rendered inline, the text would shift left and
          right three times a second as the ellipsis grew and shrank — a heading that will not hold
          still is harder to sit in front of than one that does nothing at all. `inline-block` with
          a fixed width and left alignment lets the dots change inside a box that does not. */}
      <span className="inline-block w-[1.5em] text-left">{'.'.repeat(n)}</span>
    </p>
  );
}
