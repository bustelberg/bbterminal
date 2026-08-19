/**
 * Reload a view WHILE a long job rewrites what it shows — throttled, and one at a time.
 *
 * ⚠⚠ THE POINT IS THE SINGLE-FLIGHT, NOT THE THROTTLE. "Refresh all" on /management-dashboard
 * rewrites 44 account rows over minutes, and the panel used to repaint only when the whole job
 * resolved. Firing the reload on every progress line instead is worse than doing nothing: each one
 * reads the WHOLE table, they overlap, and the last to RETURN wins regardless of which was newest —
 * so the table can visibly go backwards while the scan goes forwards. One in flight, ever.
 *
 * ⚠ AND A COALESCED TAIL. A request that arrives while one is running must not queue a second, or a
 * burst of forty lines becomes forty sequential reads finishing long after the job. It sets a flag;
 * exactly one follow-up runs when the current read lands.
 *
 * ⚠ IT ADVANCES ONLY ON A HIGHER `done`. Plenty of progress lines are pure narration and carry no
 * count, or repeat one; reloading on those spends a whole-table read to render the same rows.
 * A LOWER count is ignored too — out-of-order frames must not walk the trigger backwards.
 */
export type LiveReload = {
  /** Feed it each progress line's `done`. Fires at most one reload per `everyMs`. */
  onProgress: (done?: number) => void;
  /** For tests: whether a reload is in flight right now. */
  busy: () => boolean;
};

export function createLiveReload(
  reload: () => Promise<unknown>,
  everyMs = 4000,
  /** Injected so a test can control time without faking the whole clock. */
  now: () => number = () => Date.now(),
  /** Injected for the same reason — the coalesced tail is scheduled, not immediate. */
  schedule: (fn: () => void, ms: number) => void = (fn, ms) => { setTimeout(fn, ms); },
): LiveReload {
  // ⚠ `-Infinity`, NOT 0. With a clock that starts near zero (a monotonic one, or a test) the very
  // FIRST advance would fall inside the throttle window and be deferred — the reload that matters
  // most, because it is the one that proves the feature works at all. Caught by
  // `reloads on the first advance`.
  let last = -Infinity;
  let busy = false;
  let again = false;
  let seen = -1;
  // ⚠ ONE PENDING TIMER, EVER. Both deferral paths below (throttled, and coalesced-after-a-run)
  // funnel through `runSoon`; without the guard a burst of forty progress lines would schedule
  // forty timers and the throttle would buy nothing.
  let scheduled = false;

  const runSoon = (ms: number) => {
    if (scheduled || busy) return;
    scheduled = true;
    schedule(() => { scheduled = false; run(); }, Math.max(0, ms));
  };

  const run = () => {
    busy = true;
    last = now();
    void reload()
      // ⚠ A FAILED RELOAD IS NOT A FAILED JOB. The scan is still writing rows; a read that could
      // not be served must not stop the next one from trying, and must never reject upward into
      // the progress handler (see `watchJob`'s note on listeners that throw).
      .catch(() => undefined)
      .then(() => {
        busy = false;
        if (again) {
          again = false;
          runSoon(everyMs);
        }
      });
  };

  return {
    onProgress(done?: number) {
      if (typeof done !== 'number' || done <= seen) return;
      seen = done;
      if (busy) { again = true; return; }
      // ⚠ THE THROTTLED PATH MUST SCHEDULE, NOT JUST FLAG. It used to set `again` and return — but
      // `again` is only consumed when a RUNNING reload finishes, and here none is running. The
      // advance was simply dropped, so a scan that reported its remaining progress inside one
      // window never repainted again. Caught by `throttles: a second advance inside the window`.
      if (now() - last < everyMs) { runSoon(everyMs - (now() - last)); return; }
      run();
    },
    busy: () => busy,
  };
}
