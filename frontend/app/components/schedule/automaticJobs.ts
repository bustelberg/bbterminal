/**
 * THE AUTOMATIC-JOBS OVERVIEW — pure helpers, so the rules are testable without a DOM.
 *
 * Everything a reader has to be able to tell apart lives here rather than inside JSX, because the
 * whole value of this page is that six states which look alike stay apart:
 *
 *   missing  — declared and NOT registered. The failure the page exists for.
 *   error    — the last run failed, or started and never finished.
 *   overdue  — it ran, but too long ago for its own cadence.
 *   unknown  — ⚠ WE CANNOT TELL. Never rendered as ok and never as a failure.
 *   off      — opt-in and correctly not enabled here.
 *   running  — in flight right now.
 *   ok       — ran, recently, and finished.
 */

export type JobRow = {
  id: string;
  label: string;
  fills: string;
  cadence: string;
  note?: string | null;
  optional_env?: string | null;
  registered: boolean;
  next_run_at?: string | null;
  last_run_at?: string | null;
  last_status?: string | null;
  last_age_hours?: number | null;
  max_age_hours?: number | null;
  last_detail?: string | null;
  last_summary?: Record<string, unknown> | null;
  observable: boolean;
  /** Whether this job has a body the Run-now endpoint can dispatch to. False for the queue worker
   *  and the two pipeline jobs (which own a richer Run-now in their own panel) — the button is
   *  absent rather than present-and-404ing. */
  runnable: boolean;
  status: string;
  reason: string;
};

export type JobsPayload = {
  jobs: JobRow[];
  summary: { counts: Record<string, number>; worst: string; total: number };
  scheduler_running: boolean;
  disable_scheduler?: string;
  checked_at: string;
  history_error?: string;
};

/**
 * ⚠ THE TONE IS PART OF THE CLAIM, so it is decided once here rather than per cell.
 *
 * ⚠⚠ `unknown` IS NEUTRAL INK, NOT AMBER AND NOT GREEN. Amber reads as "something is wrong" and
 * green as "checked and fine"; the honest rendering of "we have no evidence either way" is the
 * absence of a verdict. This is the same rule the rest of the app follows for an unpriceable
 * holding — `n/a`, never `0%`.
 */
export const JOB_TONE: Record<string, { dot: string; text: string; label: string }> = {
  missing: { dot: 'bg-neg-500', text: 'text-neg-400', label: 'not registered' },
  error: { dot: 'bg-neg-500', text: 'text-neg-400', label: 'failed' },
  overdue: { dot: 'bg-warn-500', text: 'text-warn-400', label: 'overdue' },
  // ⚠ AMBER, NOT RED. The process was restarted mid-run (a deploy, an OOM, or `uvicorn --reload`)
  // — the work did not finish, but nothing is broken and the fix is to run it again. Red here
  // means every local restart paints a fault, and a reader who learns to discount red rows will
  // discount the real one too.
  interrupted: { dot: 'bg-warn-500', text: 'text-warn-400', label: 'interrupted' },
  // ⚠⚠ AMBER AND ITS OWN WORD, NOT `overdue`. Both mean the work has not happened, but this one
  // KNOWS WHY — the row was written by an observer (the misfire listener, or the boot-time gap
  // scan) and carries the fire time it belongs to and whether the process was busy or absent.
  // Folded into `overdue` the reader loses the one distinction that decides where to look: overdue
  // is inferred from silence, missed is recorded evidence. The reason is in the expanded row.
  missed: { dot: 'bg-warn-500', text: 'text-warn-400', label: 'never ran' },
  unknown: { dot: 'bg-neutral-500', text: 'text-fg-muted', label: 'unknown' },
  off: { dot: 'bg-neutral-600', text: 'text-fg-faint', label: 'off' },
  running: { dot: 'bg-accent-500', text: 'text-accent-400', label: 'running' },
  ok: { dot: 'bg-pos-500', text: 'text-pos-400', label: 'ok' },
};

/**
 * "3 days ago" / "just now" — the age a reader actually reasons in.
 *
 * ⚠ AN ABSENT AGE IS `—`, NEVER "0h ago". A job that has never been recorded has no age, and a
 * zero there reads as "ran this second" — the exact inversion of what it means.
 */
export function ago(hours: number | null | undefined): string {
  if (hours == null) return '—';
  if (hours < 1) return `${Math.max(0, Math.round(hours * 60))}m ago`;
  if (hours < 48) return `${hours.toFixed(1)}h ago`;
  return `${(hours / 24).toFixed(1)}d ago`;
}

/** A timestamp as `Wed 13 Aug 05:00`, or `—`. Local time: the reader's question is "has it fired
 *  today", which they ask in their own clock, and every one of these has its timezone in `cadence`
 *  already. */
export function stamp(iso: string | null | undefined): string {
  if (!iso) return '—';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return '—';
  return d.toLocaleString(undefined, {
    weekday: 'short', day: '2-digit', month: 'short', hour: '2-digit', minute: '2-digit',
  });
}

/**
 * The one line under the page title.
 *
 * ⚠⚠ THE SCHEDULER BEING DOWN IS NOT ONE ROW'S PROBLEM, IT IS EVERY ROW'S, and saying it once at
 * the top is the difference between a reader diagnosing it in a second and reading eight identical
 * red rows looking for a pattern. `DISABLE_SCHEDULER` is named explicitly because on a replica this
 * is the CORRECT state and the page must not be read as an outage.
 */
export function headline(p: JobsPayload): { text: string; tone: string } {
  if (!p.scheduler_running) {
    return {
      text: p.disable_scheduler
        ? `No scheduler in this process — DISABLE_SCHEDULER=${p.disable_scheduler}. `
          + 'Correct on a replica; on the primary it means nothing automatic is running.'
        : 'No scheduler is running in this process — nothing automatic will fire here.',
      tone: 'text-warn-300',
    };
  }
  const c = p.summary.counts;
  const bad = (c.missing ?? 0) + (c.error ?? 0);
  if (bad) return { text: `${bad} job(s) need attention.`, tone: 'text-neg-300' };
  // ⚠ INTERRUPTED SITS WITH OVERDUE, NOT WITH "need attention". Both mean the same thing to the
  // reader — work that has not happened — and neither is a fault to debug. Naming them separately
  // in the headline would imply two different problems.
  const late = (c.overdue ?? 0) + (c.interrupted ?? 0);
  if (late) {
    return {
      text: c.interrupted
        ? `${late} job(s) did not complete — re-run them.`
        : `${late} job(s) overdue.`,
      tone: 'text-warn-300',
    };
  }
  if (c.unknown) {
    return {
      text: `${c.unknown} job(s) leave no record we can read — their state is unknown, not ok.`,
      tone: 'text-fg-muted',
    };
  }
  return { text: 'Every declared job is registered and current.', tone: 'text-pos-400' };
}

/** `{synced: 12, errors: 0}` → `synced 12 · errors 0`. The summary is free-form per job, so it is
 *  rendered generically rather than with per-job knowledge the page would have to keep in step. */
export function summaryLine(s: Record<string, unknown> | null | undefined): string {
  if (!s) return '';
  return Object.entries(s)
    .filter(([, v]) => v != null && !(Array.isArray(v) && !v.length))
    .map(([k, v]) => `${k.replace(/_/g, ' ')} ${Array.isArray(v) ? v.join(', ') : String(v)}`)
    .join(' · ');
}
