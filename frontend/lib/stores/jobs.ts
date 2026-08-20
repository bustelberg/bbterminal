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
  /**
   * The server's identity for this piece of work — `fundamentals.index` + `ACWI`.
   *
   * ⚠ IT IS HERE SO A CONTROL CAN FIND ITS OWN RUN AGAIN. A button knows it has a job in flight
   * only from its own React state, so reopening a modal or reloading the page brings it back
   * offering to START one while the work is still going. `(kind, label)` is what the server
   * de-duplicates on (`jobs.start`), so it is also the only key a component can match on to adopt
   * the run instead of launching a second.
   *
   * Empty until the first `job` frame arrives; `attachRunningJobs` fills it immediately.
   */
  kind: string;
  label: string;
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

/**
 * Ask the server to stop. ⚠ It halts at its next safe point, not immediately — the card shows
 * `cancelRequested` straight away so the press is acknowledged, and `status` follows when the
 * worker actually stops.
 *
 * ⚠ RETURNS WHETHER THE REQUEST LANDED, because a caller that puts its own button into a
 * "Cancelling…" state has to be able to take it back out again. Swallowing the failure here left
 * such a button disabled and lying for the rest of the run — the request never arrived, the job
 * carried on, and the only control that could stop it had turned itself off. Additive: every
 * existing call site ignores the value and is unchanged.
 */
export async function cancelJob(id: string): Promise<boolean> {
  upsert(id, { cancelRequested: true, message: 'cancelling…' });
  // ⚠⚠ LOCAL JOBS FIRST, AND THIS IS WHY THERE IS STILL ONE CANCEL PATH. `JobToaster`'s Cancel
  // button calls this function and nothing else; a second kind of job with a second kind of cancel
  // would mean teaching that button which sort it is looking at, at which point every future
  // control has to know too. A job that runs in this tab has no `/api/jobs/{id}` to POST to — the
  // POST would 404 and the card would sit at "cancelling…" for ever — so it is aborted here.
  const abort = localCancels.get(id);
  if (abort) { abort(); return true; }
  try {
    const r = await apiFetch(`${API_URL}/api/jobs/${encodeURIComponent(id)}/cancel`,
      { method: 'POST' });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    return true;
  } catch (e) {
    traceError('jobs', `could not cancel job ${id}`, e);
    upsert(id, { cancelRequested: false, message: 'cancel failed — see the console' });
    return false;
  }
}

const isTerminal = (s: JobStatus) => s !== 'running';

/** Abort handles for jobs with no server side, keyed by toast id — see `startLocalJob`. */
const localCancels = new Map<string, () => void>();

/**
 * Report a piece of work THIS TAB is doing on the shared toast stack.
 *
 * ⚠⚠ NOT EVERY CANCELLABLE THING IS A SERVER JOB. `startJob` needs an endpoint that owns the work
 * and hands back a `job_id`; re-reading a cached GET has no such endpoint and does not deserve
 * one. Without this, a control like the Deep Valuation tab's share-price refresh had two bad
 * options: paint its own private spinner — a second progress vocabulary the reader has to learn,
 * and the exact thing the job layer was built to delete — or invent a backend job for a fetch.
 *
 * ⚠ CANCELLATION IS AN `AbortController`, AND IT IS REAL. `cancelJob` finds this handle before it
 * reaches for the network, so the toaster's own Cancel button works on these with no change to it.
 *
 * ⚠ IT DIES WITH THE TAB, AND THAT IS THE ONE THING A SERVER JOB DOES BETTER. A route change or a
 * reload takes the work with it — there is nothing to re-attach to (`attachRunningJobs` lists the
 * server's jobs, and this is not one). Use it only for work short enough that losing it costs
 * nothing; anything that outlives a page view belongs on the server.
 *
 * `run` returns the card's summary line. Throwing marks the card failed with the message.
 */
export function startLocalJob(
  title: string, kind: string, run: (signal: AbortSignal) => Promise<string | void>,
): string {
  const id = `local:${kind}:${
    typeof crypto !== 'undefined' && 'randomUUID' in crypto ? crypto.randomUUID() : `${performance.now()}`}`;
  const ctrl = new AbortController();
  localCancels.set(id, () => ctrl.abort());
  jobsStore.set((st) => ({
    jobs: [...st.jobs, {
      id, title, kind, label: title, status: 'running' as JobStatus, done: 0, total: 0,
      message: '', summary: null, apiCalls: 0, cancelRequested: false, dismissed: false,
    }],
  }));
  void (async () => {
    try {
      const summary = await run(ctrl.signal);
      // ⚠ THE SIGNAL, NOT THE ERROR, DECIDES. A cancelled fetch can resolve rather than throw
      // (a cacheable read is shared, so aborting one caller does not stop the request), and a card
      // that went green on a run the reader stopped is worse than one that never reported.
      upsert(id, ctrl.signal.aborted
        ? { status: 'cancelled', summary: 'cancelled', message: '' }
        : { status: 'done', summary: summary || null, message: '' });
    } catch (e) {
      upsert(id, ctrl.signal.aborted
        ? { status: 'cancelled', summary: 'cancelled', message: '' }
        : {
          status: 'failed',
          summary: e instanceof Error ? e.message : String(e),
          message: 'see the console',
        });
      if (!ctrl.signal.aborted) traceError('jobs', `local job ${kind} failed`, e);
    } finally {
      localCancels.delete(id);
    }
  })();
  return id;
}

/**
 * Follow one job's stream to its end.
 *
 * `after` lets a re-attach ask for the events it missed, so a page reload shows the run's history
 * rather than joining mid-sentence.
 */
/**
 * `onProgress` — called for every progress line, so a caller can refresh what it shows AS the job
 * runs rather than only when it ends.
 *
 * ⚠ IT IS NOT THE TOAST'S JOB. The toast already narrates; this exists for callers whose SCREEN is
 * the thing the job changes — "Refresh all" rewrites 44 account rows over several minutes, and
 * repainting them only at the end means staring at figures you have already replaced. Optional, so
 * every existing caller is untouched.
 *
 * ⚠ IT MUST NOT THROW. It runs inside the stream handler; an exception here would kill the
 * progress stream and the toast with it, turning a cosmetic nicety into a lost job. Wrapped below.
 */
export function watchJob(id: string, title: string, after = 0,
                         seed?: { kind?: string; label?: string },
                         onProgress?: (e: { done?: number; total?: number; message?: string })
                           => void): Promise<JobToast> {
  jobsStore.set((s) => (s.jobs.some((j) => j.id === id) ? {} : {
    jobs: [...s.jobs, {
      id, title, kind: seed?.kind ?? '', label: seed?.label ?? '',
      status: 'running' as JobStatus, done: 0, total: 0,
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
        id, title, kind: seed?.kind ?? '', label: seed?.label ?? '',
        status: 'failed', done: 0, total: 0, message: '', summary: null, apiCalls: 0,
        cancelRequested: false, dismissed: false,
      });
    };

    void runSSE(
      `${API_URL}/api/jobs/${encodeURIComponent(id)}/stream?after=${after}`,
      { method: 'GET' },
      (raw) => {
        const e = raw as {
          type?: string; kind?: string; label?: string; message?: string; status?: JobStatus;
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
            // ⚠ ONLY FROM THE `job` FRAME. A progress EVENT also carries a `kind` and it means
            // something entirely different there (`progress` / `skip` / `error`) — writing that
            // into the identity would make every control lose track of its own run one line in.
            ...(e.kind ? { kind: e.kind } : {}),
            ...(e.label ? { label: e.label } : {}),
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
        // ⚠ AFTER the toast, and swallowed. The toast is the job's own record and must land even
        // if a listener misbehaves; see the note on `onProgress`.
        if (onProgress) {
          try {
            onProgress({ done: e.done, total: e.total, message: e.message });
          } catch (err) {
            traceError('jobs', `an onProgress listener for ${id} threw`, err);
          }
        }
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
  /** Per-progress-line callback — see `watchJob`. For callers whose screen the job rewrites. */
  onProgress?: (e: { done?: number; total?: number; message?: string }) => void,
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
  return { id: b.job_id, done: watchJob(b.job_id, title, 0, undefined, onProgress),
    body: b as Record<string, unknown> };
}

/**
 * ⚠ THE ONE CALL THAT MAKES RUNNING WORK VISIBLE MUST NOT BE A SINGLE SHOT.
 *
 * `attachRunningJobs` fires once, from a `useEffect` in the root layout, the moment the role
 * resolves — so a backend that is unreachable for that one second costs the reader every toast
 * until they reload the whole tab, while the server carries on working. And "unreachable for one
 * second" is the normal case, not the exotic one: a `uvicorn --reload` restart on a file save, a
 * Railway redeploy, a laptop waking up. All of them surface as `TypeError: Failed to fetch` — the
 * request left, the connection died, no status was ever returned.
 *
 * Bounded and backing off, because a backend that is genuinely down must not be polled forever:
 * ~13s of trying, then it gives up and says so in the console.
 */
const ATTACH_RETRY_MS = [1_000, 3_000, 9_000];

const _sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

/** ⚠ THE RETRY MADE `attachRunningJobs` RE-ENTRANT. It used to be over in one round trip, so a
 *  second call could not overlap the first; now one can still be sleeping when the effect refires
 *  (the admin flag resolving, a "view as user" toggle), and two loops would each adopt the same
 *  jobs in the window before `watchJob` puts them in the store. */
let _attaching = false;

/**
 * Re-attach to whatever is already running on the server.
 *
 * ⚠ THIS IS THE PAYOFF OF PUTTING A JOB IN THE MIDDLE. Without it a reload leaves the work running
 * with nothing on screen to say so — the exact state the old thread-and-queue endpoints left you
 * in, except now it is recoverable.
 */
export async function attachRunningJobs() {
  if (_attaching) return;                     // one attach at a time — see `_attaching`
  _attaching = true;
  try {
    for (let attempt = 0; ; attempt++) {
      try {
        const r = await apiFetch(`${API_URL}/api/jobs`);
        if (!r.ok) {
          // ⚠ 403 FOR A NON-ADMIN IS AN ANSWER, NOT AN ERROR — and so is a 404. A 5xx is not: it
          // is the server still coming up, which is the one non-answer worth asking again for.
          if (r.status >= 500 && attempt < ATTACH_RETRY_MS.length) {
            await _sleep(ATTACH_RETRY_MS[attempt]);
            continue;
          }
          return;
        }
        const rows = (await r.json()) as {
          id: string; kind: string; label: string; status: JobStatus;
        }[];
        for (const j of rows) {
          if (j.status !== 'running') continue;   // finished ones are history, not a toast
          if (jobsStore.get().jobs.some((x) => x.id === j.id)) continue;
          void watchJob(j.id, j.label, 0, { kind: j.kind, label: j.label });
        }
        return;
      } catch (e) {
        if (attempt >= ATTACH_RETRY_MS.length) {
          traceError('jobs', 'could not list running jobs', e);
          return;
        }
        await _sleep(ATTACH_RETRY_MS[attempt]);
      }
    }
  } finally {
    _attaching = false;
  }
}

export { isTerminal };
