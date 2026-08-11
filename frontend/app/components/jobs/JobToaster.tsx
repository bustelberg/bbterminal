'use client';

import { useEffect, useState } from 'react';

import { useIsAdmin } from '../../../lib/hooks/useEffectiveRole';
import {
  attachRunningJobs, cancelJob, dismissJob, jobsStore, LINGER_SECONDS, type JobToast,
} from '../../../lib/stores/jobs';

/**
 * THE BOTTOM-RIGHT PROGRESS STACK.
 *
 * ⚠ MOUNTED IN THE ROOT LAYOUT, NOT IN A PANEL, and that is the entire point. A toaster inside the
 * page that started the job unmounts on the first route change and takes the progress with it,
 * while the server carries on working — which is the invisible-but-running state this layer was
 * built to end. Here it outlives every page.
 *
 * ⚠ IT RENDERS NOTHING WHEN THERE ARE NO JOBS. It sits in the layout of every route, so it must
 * cost nothing to have around: no wrapper, no fixed element, no stacking context.
 */
export default function JobToaster() {
  const jobs = jobsStore.use((s) => s.jobs);
  const isAdmin = useIsAdmin();

  // ⚠ ADMIN ONLY, BECAUSE `/api/jobs` IS. Every job that exists spends GuruFocus quota, so the
  // gate holds the whole namespace to admins — asking as a user would 403 on every page load.
  useEffect(() => { if (isAdmin) void attachRunningJobs(); }, [isAdmin]);

  if (!jobs.length) return null;

  return (
    // `pointer-events-none` on the stack, restored per card: the column spans a corner of the
    // viewport and would otherwise swallow clicks on whatever sits underneath it.
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 w-80 max-w-[calc(100vw-2rem)]
                    pointer-events-none">
      {/* ⚠ KEYED ON `id:status`, SO GOING STALE REMOUNTS THE CARD. That is what lets the countdown
          be a `useState` initialiser instead of an effect — the card that appears when a job
          finishes is a new card, already armed with the right number of seconds. */}
      {jobs.map((j) => <JobCard key={`${j.id}:${j.status}`} job={j} />)}
    </div>
  );
}

const TONE: Record<string, { bar: string; text: string; label: string }> = {
  running: { bar: 'bg-accent-500', text: 'text-fg-soft', label: 'text-fg-muted' },
  done: { bar: 'bg-pos-500', text: 'text-pos-400', label: 'text-fg-muted' },
  failed: { bar: 'bg-neg-500', text: 'text-neg-400', label: 'text-fg-muted' },
  // ⚠ CANCELLED IS NOT A FAILURE. It did what it was told; colouring it red beside a real error
  // teaches the reader to ignore both.
  cancelled: { bar: 'bg-warn-500', text: 'text-warn-400', label: 'text-fg-muted' },
};

/** Long enough for the fade to be seen as a fade; short enough not to feel like a hang. Must match
 *  the `duration-` class on the card, or the row is removed mid-animation. */
const FADE_MS = 300;

/** How often the stale countdown updates. ⚠ IT IS ALSO THE BAR'S TRANSITION LENGTH — the two are
 *  the same 100ms on purpose, so the bar is never more than one tick behind the number it is drawn
 *  from. A longer transition than the tick is exactly what left the bar unfinished at zero. */
const TICK_MS = 100;

function JobCard({ job }: { job: JobToast }) {
  const tone = TONE[job.status] ?? TONE.running;
  const running = job.status === 'running';
  // ⚠ INDETERMINATE UNTIL A TOTAL ARRIVES. `done/0` is NaN, and a bar that reads 100% before the
  // first step is worse than one that reads nothing.
  const pct = job.total > 0 ? Math.min(100, (job.done / job.total) * 100) : null;

  /**
   * THE STALE COUNTDOWN.
   *
   * ⚠ REAL STATE, NOT DERIVED. Almost everything else in this app is computed during render
   * precisely so it cannot drift — but this is a clock, and a clock has nowhere to be derived
   * from. What keeps it honest is that it only ever counts DOWN from a constant chosen by status.
   *
   * ⚠ A CHAIN OF ONE-SECOND TIMEOUTS, NOT AN INTERVAL, and that is what makes the hover pause
   * exact: the effect simply does not schedule the next tick while the pointer is over the card,
   * so hovering freezes the number rather than letting a background interval keep firing and drop
   * the card the moment you look away.
   */
  const linger = LINGER_SECONDS[job.status] ?? 0;
  /**
   * ⚠⚠ COUNTED IN MILLISECONDS, NOT SECONDS, AND THAT IS A BUG FIX RATHER THAN A REFINEMENT.
   *
   * It ticked once a second and the bar was a CSS `transition-[width] duration-1000`, so the bar
   * was always animating TOWARDS the value the number had already reached — a full tick behind.
   * When the number hit 0 the bar was still ~17% full and had a second of travel left, while the
   * card's fade takes 300ms: the toast disappeared with the countdown bar visibly unfinished.
   *
   * The two now read the same value. The seconds on screen are `ceil(ms/1000)` and the bar is
   * `ms/total`, so they cannot disagree — at zero the bar is empty because it IS zero, not because
   * an animation was given long enough to get there.
   *
   * ⚠ ARMED AT MOUNT, NOT IN AN EFFECT — the parent keys this card on `id:status`, so a job going
   * stale remounts it and the initialiser runs with the right value. Arming in an effect meant a
   * synchronous setState in the effect body (a cascading render) plus a guard to stop every
   * unrelated re-render restarting the countdown.
   */
  const [ms, setMs] = useState<number | null>(linger ? linger * 1000 : null);
  const [hover, setHover] = useState(false);
  // Derived, not stored: "at zero" and "fading" are the same fact, and holding it twice is how the
  // two come to disagree.
  const leaving = ms !== null && ms <= 0;
  const secondsLeft = ms === null ? null : Math.ceil(ms / 1000);

  // ⚠ THE INTERVAL DOES NOT DEPEND ON `ms`, or it would be torn down and rebuilt on every tick —
  // which resets the browser's timer each time and makes the countdown run slow. It decrements by
  // the tick it was scheduled for; a throttled background tab therefore stretches the countdown
  // rather than expiring the toast while nobody is looking, which is the behaviour we want.
  useEffect(() => {
    if (!linger || hover) return;
    const id = window.setInterval(
      () => setMs((m) => (m === null ? m : Math.max(0, m - TICK_MS))), TICK_MS);
    return () => window.clearInterval(id);
  }, [linger, hover]);

  useEffect(() => {
    if (!leaving) return;
    // The card is already fading (see `leaving`); this only removes it once the transition has had
    // time to run. Removing immediately would make it vanish rather than fade.
    const t = window.setTimeout(() => dismissJob(job.id), FADE_MS);
    return () => window.clearTimeout(t);
  }, [leaving, job.id]);

  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      className={`pointer-events-auto bg-card border border-neutral-800/40 rounded-xl
                  shadow-lg px-3 py-2.5 space-y-1.5
                  transition-all duration-300 ease-out
                  ${leaving ? 'opacity-0 translate-x-4' : 'opacity-100 translate-x-0'}`}>
      <div className="flex items-baseline gap-2">
        <span className="text-xs font-medium text-fg-strong truncate flex-1" title={job.title}>
          {job.title}
        </span>
        {/* ⚠ WHAT IT COST, AND ONLY WHEN IT COST SOMETHING. GuruFocus calls come out of a finite
            monthly quota, unlike our own database reads — so a reader deciding whether to press
            again deserves to see the meter. Zero is hidden rather than shown as "0 calls": a
            refusal and a cache hit both legitimately spend nothing, and a 0 on every one of those
            cards trains the eye to skip the number on the cards where it matters. */}
        {job.apiCalls > 0 && (
          <span className="text-[11px] font-mono px-1 rounded bg-overlay/10 text-fg-muted
                           shrink-0"
            title={`${job.apiCalls} GuruFocus API call${job.apiCalls === 1 ? '' : 's'} spent from `
              + 'this month’s quota. Cached feeds and refusals cost nothing and are not counted.'}>
            {job.apiCalls} call{job.apiCalls === 1 ? '' : 's'}
          </span>
        )}
        <span className={`text-[11px] font-mono ${tone.label}`}>
          {/* The press is acknowledged the moment it happens, even though the worker stops at its
              next safe point a few seconds later. */}
          {job.cancelRequested && running ? 'cancelling…'
            : running ? (pct === null ? 'working' : `${Math.round(pct)}%`)
              // ⚠ THE COUNTDOWN IS SHOWN, NOT JUST RUN. A card that disappears on an invisible
              // timer reads as a bug the first time you watch it happen; a number ticking down
              // says it was always going to. `paused` on hover explains why it stopped.
              : `${job.status}${secondsLeft === null ? ''
                : hover ? ' · paused' : ` · ${secondsLeft}s`}`}
        </span>
      </div>

      {/* Running: the work's progress. Stale: the countdown draining, so the bar keeps meaning
          "time left in this card" rather than freezing at a full 100% that says nothing.
          ⚠ THE STALE WIDTH IS READ FROM `ms`, THE SAME VALUE THE SECONDS ARE — the bar reaches
          empty because it is empty, not because an animation was given long enough to get there.
          The transition is one TICK, so it smooths the 100ms steps without ever lagging behind. */}
      <div className="h-1 rounded bg-overlay/10 overflow-hidden">
        <div className={`h-full ${tone.bar} ${hover ? '' : 'transition-[width] ease-linear'}`}
          style={{
            transitionDuration: `${TICK_MS}ms`,
            width: running
              ? (pct === null ? '35%' : `${pct}%`)
              : `${linger && ms !== null ? (ms / (linger * 1000)) * 100 : 100}%`,
          }} />
      </div>

      {/* ⚠ ONE LINE, TRUNCATED, WITH THE WHOLE THING ON THE TITLE. Detail belongs in the console —
          a toast that grows to fit its message reflows the stack under the reader's cursor.
          ⚠ THE HOVER CARRIES BOTH READINGS. `summary` is what the reader wanted to know ("loaded
          FY2010–FY2025"); `message` is the last progress line, which for a finished ingest is the
          per-feed breakdown ("statements 36,378 · estimates 164"). That breakdown is what you need
          the moment one feed comes back empty, and nothing else on screen would tell you which. */}
      <p className={`text-[12px] truncate ${tone.text}`}
        title={[job.summary, job.message].filter(Boolean).join('\n') || undefined}>
        {job.summary || job.message}
      </p>

      <div className="flex justify-end gap-2">
        {running && !job.cancelRequested && (
          <button type="button" onClick={() => void cancelJob(job.id)}
            title="Stop this job at its next safe point. Whatever has already been written stays
written — it is not rolled back — and re-running picks up where it left off."
            className="cursor-pointer text-[11px] px-2 py-0.5 rounded border border-neutral-700
                       text-fg-subtle hover:text-warn-300 hover:border-warn-500/50
                       transition-colors">
            Cancel
          </button>
        )}
        {!running && (
          <button type="button" onClick={() => dismissJob(job.id)}
            className="cursor-pointer text-[11px] px-2 py-0.5 rounded border border-neutral-700
                       text-fg-subtle hover:bg-overlay/5 transition-colors">
            Dismiss
          </button>
        )}
      </div>
    </div>
  );
}
