import { apiFetch } from '../apiFetch';
import { API_URL } from '../apiUrl';
import { traceError } from '../debugTrace';
import { createStore } from '../store';
import { runSSE } from '../stream';

/**
 * BACKGROUND JOBS, AND THE TOASTS THAT REPORT THEM.
 *
 * ⚠ MODULE-SCOPED ON PURPOSE — see `lib/store.ts`'s own note. The state lives outside React's
 * tree, so a job keeps reporting when the panel that started it unmounts and when the reader
 * navigates to another page. A toast owned by the component that launched it would vanish with
 * the route change while the work carried on invisibly, which is the failure this whole layer
 * exists to remove.
 *
 * The server is the authority on a job's state (`backend/jobs.py`); this holds only what is
 * needed to draw it, plus `dismissed`, which is a purely local "I have read this".
 */

export type JobStatus = 'running' | 'done' | 'failed' | 'cancelled';

export type JobToast = {
  id: string;
  /** What the reader pressed — a company name, not the ISIN the server keys on. */
  title: string;
  status: JobStatus;
  done: number;
  total: number;
  /** The latest progress line. One line: anything with detail goes to the console. */
  message: string;
  summary: string | null;
  /** Metered external calls spent (GuruFocus quota today). ⚠ 0 RENDERS NOTHING rather than "0
   *  calls" — a refusal and a cache hit both legitimately cost nothing, and a zero on every such
   *  card trains the eye to skip the number on the cards where it matters. */
  apiCalls: number;
  /** ⚠ NOT `status === 'cancelled'`. Cancellation is cooperative, so this is true from the moment
   *  the button is pressed while the job is still finishing its current feed. The card reads
   *  "cancelling…" in that window; without it the button would look inert for several seconds. */
  cancelRequested: boolean;
  dismissed: boolean;
};

type JobsState = { jobs: JobToast[] };

export const jobsStore = createStore<JobsState>({ jobs: [] });

/**
 * How long a finished card lingers before it fades, in seconds — the countdown the card shows.
 *
 * ⚠ THE THREE ARE NOT THE SAME NUMBER, because they are not equally worth reading. A success is
 * confirmation of something you asked for and already expected; a cancellation is worth a glance
 * to see how far it got; a failure is the only one carrying information you did not have, and it
 * points at the console for the rest. Giving them one duration would either rush the failure off
 * screen or leave successes piling up in the corner.
 *
 * ⚠ THE COUNTDOWN IS VISIBLE AND PAUSES ON HOVER (see `JobCard`). A toast that vanishes while
 * being read is worse than one that never appeared — you know you missed something and cannot get
 * it back.
 */
export const LINGER_SECONDS: Record<JobStatus, number> = {
  running: 0,        // not stale yet — no countdown
  done: 6,
  cancelled: 8,
  failed: 12,
};

const upsert = (id: string, patch: Partial<JobToast>) => {
  jobsStore.set((s) => {
    const i = s.jobs.findIndex((j) => j.id === id);
    if (i < 0) return {};
    const next = s.jobs.slice();
    next[i] = { ...next[i], ...patch };
    return { jobs: next };
  });
};

export const dismissJob = (id: string) =>
  jobsStore.set((s) => ({ jobs: s.jobs.filter((j) => j.id !== id) }));

/** Ask the server to stop. ⚠ It halts at its next safe point, not immediately — the card shows
 *  `cancelRequested` straight away so the press is acknowledged, and `status` follows when the
 *  worker actually stops. */
export async function cancelJob(id: string) {
  upsert(id, { cancelRequested: true, message: 'cancelling…' });
  try {
    const r = await apiFetch(`${API_URL}/api/jobs/${encodeURIComponent(id)}/cancel`,
      { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
  } catch (e) {
    traceError('jobs', `could not cancel job ${id}`, e);
    upsert(id, { message: 'cancel failed — see the console' });
  }
}

const isTerminal = (s: JobStatus) => s !== 'running';

/**
 * Follow one job's stream to its end.
 *
 * `after` lets a re-attach ask for the events it missed, so a page reload shows the run's history
 * rather than joining mid-sentence.
 */
export function watchJob(id: string, title: string, after = 0): Promise<JobToast> {
  jobsStore.set((s) => (s.jobs.some((j) => j.id === id) ? {} : {
    jobs: [...s.jobs, {
      id, title, status: 'running' as JobStatus, done: 0, total: 0,
      message: 'starting…', summary: null, apiCalls: 0, cancelRequested: false, dismissed: false,
    }],
  }));

  return new Promise<JobToast>((resolve) => {
    // ⚠ NO AUTO-DISMISS TIMER HERE. The card owns the countdown, because only the card knows
    // whether the reader is hovering it — a timer started from this side would fire regardless and
    // yank the toast out from under the cursor. This just reports the outcome.
    const finish = () => {
      const job = jobsStore.get().jobs.find((j) => j.id === id);
      resolve(job ?? {
        id, title, status: 'failed', done: 0, total: 0, message: '', summary: null, apiCalls: 0,
        cancelRequested: false, dismissed: false,
      });
    };

    void runSSE(
      `${API_URL}/api/jobs/${encodeURIComponent(id)}/stream?after=${after}`,
      { method: 'GET' },
      (raw) => {
        const e = raw as {
          type?: string; kind?: string; message?: string; status?: JobStatus;
          done?: number; total?: number; summary?: string | null; cancel_requested?: boolean;
          api_calls?: number;
        };
        if (e.type === 'job') {
          upsert(id, {
            status: e.status ?? 'running',
            done: e.done ?? 0,
            total: e.total ?? 0,
            summary: e.summary ?? null,
            apiCalls: e.api_calls ?? 0,
            cancelRequested: !!e.cancel_requested,
          });
          return;
        }
        // A progress line. `done`/`total` ride along on the ones that have them; the others are
        // pure narration and must not reset the bar to zero.
        upsert(id, {
          message: e.message ?? '',
          ...(typeof e.done === 'number' ? { done: e.done } : {}),
          ...(typeof e.total === 'number' ? { total: e.total } : {}),
        });
      },
    ).then(finish).catch((err) => {
      traceError('jobs', `the stream for job ${id} failed`, err);
      upsert(id, { status: 'failed', message: 'lost the progress stream — see the console' });
      finish();
    });
  });
}

/**
 * Start a job and follow it.
 *
 * `startUrl` is the endpoint that OWNS the work and returns `{job_id}` — there is deliberately no
 * generic "run this kind" starter, on the server or here.
 *
 * Returns the id plus a promise that settles when the job reaches a terminal state, so a caller
 * can refresh its own view afterwards without knowing anything about toasts.
 */
export async function startJob(
  startUrl: string, title: string, init?: RequestInit,
): Promise<{ id: string; done: Promise<JobToast>; body: Record<string, unknown> }> {
  // ⚠ `init` IS OPTIONAL AND MERGED AFTER `method`, so a caller can add a JSON body (the basket
  // fill posts its holdings) without being able to turn this into a GET by accident.
  const r = await apiFetch(startUrl, { ...init, method: 'POST' });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  const b = (await r.json()) as { job_id: string };
  // ⚠ THE START RESPONSE IS HANDED BACK, NOT JUST THE HANDLE. An endpoint often knows something
  // at start-up that the job itself never reports — the portfolio fill returns how many holdings
  // it could reach and why the rest it could not, which the caller has to render BEFORE the first
  // progress line arrives. Discarding it forced a second request for data we already had.
  // Additive: every existing caller destructures `{ id, done }` and is untouched.
  return { id: b.job_id, done: watchJob(b.job_id, title), body: b as Record<string, unknown> };
}

/**
 * Re-attach to whatever is already running on the server.
 *
 * ⚠ THIS IS THE PAYOFF OF PUTTING A JOB IN THE MIDDLE. Without it a reload leaves the work running
 * with nothing on screen to say so — the exact state the old thread-and-queue endpoints left you
 * in, except now it is recoverable.
 */
export async function attachRunningJobs() {
  try {
    const r = await apiFetch(`${API_URL}/api/jobs`);
    if (!r.ok) return;                        // 403 for a non-admin is an answer, not an error
    const rows = (await r.json()) as {
      id: string; label: string; status: JobStatus;
    }[];
    for (const j of rows) {
      if (j.status !== 'running') continue;   // finished ones are history, not a toast
      if (jobsStore.get().jobs.some((x) => x.id === j.id)) continue;
      void watchJob(j.id, j.label);
    }
  } catch (e) {
    traceError('jobs', 'could not list running jobs', e);
  }
}

export { isTerminal };
