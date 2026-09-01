'use client';

import { useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { traceError } from '../../../lib/debugTrace';
import { usePollingFetch } from '../../../lib/hooks/usePollingFetch';
import { cancelJob, startJob } from '../../../lib/stores/jobs';
import InfoTip from '../InfoTip';
import { RefreshIcon } from '../portfolios/RefreshIcon';
import {
  ago, headline, JOB_TONE, stamp, summaryLine, type JobRow, type JobsPayload,
} from './automaticJobs';
import { JOB_PANELS } from './jobPanels';
import { usePipelineActivity, type PipelineCtx } from './usePipelineActivity';

/**
 * EVERY JOB THAT IS SUPPOSED TO RUN BY ITSELF, AND WHETHER IT DID.
 *
 * ⚠⚠ IT ANSWERS "IS ANYTHING MISSING", WHICH NO OTHER SURFACE COULD. A run history only shows jobs
 * that ran, and the scheduler's own list only shows jobs that registered — so a job that silently
 * stopped being registered appears in NEITHER: it has no runs to be absent from, and the list it
 * is missing from is the one being read. The declaration in `backend/scheduled_jobs.py` is what
 * makes an absence visible, and this renders the disagreement.
 *
 * ⚠ IT REPLACED THE "Smart pipeline activity" CARD (2026-08-13). That card was a second list of
 * jobs with its own countdowns, read from the same `list_scheduled_jobs()` — two clocks for one
 * fire time. Its panels now live behind the rows they belong to (`jobPanels.JOB_PANELS`).
 *
 * ⚠ ON /schedule RATHER THAN A NEW /admin PAGE, deliberately. This page already IS the job page;
 * splitting "jobs" across two routes means every future reader has to know which half to open.
 *
 * ⚠ NOT AN SSE STREAM. Nothing here changes second to second — the interesting transitions are
 * hours apart — so it polls slowly and stays a plain read. Adding a stream would be a second live
 * surface to keep alive for information that is, by construction, daily.
 */
export default function AutomaticJobsCard() {
  /**
   * ⚠ CALLED ONCE, HERE, AND HANDED TO EVERY PANEL. The panels are drawn from one SSE stream (with
   * six polling fallbacks); a panel that fetched for itself would open another consumer for every
   * row a reader expands. It runs whether or not a row is open because the stream is what the
   * panels' live status is made of — the same cost the retired activity card always paid.
   */
  const pipeline = usePipelineActivity();
  /**
   * ⚠ THE MANUAL REFRESH IS A CHANGE OF URL, because `usePollingFetch` keys its effect on the url
   * and exposes no reload. A distinct param is what re-runs it — and it is the honest shape here
   * anyway, since the reader pressing Refresh wants a NEW read, not a replay of the cached one.
   */
  const [nonce, setNonce] = useState(0);
  // 60s: the fastest anything here can legitimately change is a job starting, and re-reading eight
  // rows every few seconds costs more than it tells anyone. The hook also pauses while the tab is
  // hidden, so a /schedule left open in a background tab makes no requests at all.
  const { data, error } = usePollingFetch<JobsPayload>(
    `${API_URL}/api/admin/scheduled-jobs${nonce ? `?_=${nonce}` : ''}`, 60_000);
  const busy = false;
  const refresh = () => setNonce(Date.now());

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl">
      <div className="px-4 py-3 border-b border-neutral-800/40 flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-fg-strong flex items-center gap-1.5">
            Automatic jobs
            <InfoTip text={'Every job declared in backend/scheduled_jobs.py, beside what the '
              + 'scheduler in THIS process is actually holding and when each last ran. A job that '
              + 'is declared but not registered is the failure this table exists to show: it '
              + 'appears in no run history, because it has no runs.'} />
          </h2>
          {data && (
            <p className={`text-[12px] mt-0.5 ${headline(data).tone}`}>{headline(data).text}</p>
          )}
          {/* ⚠ LOSING THE HISTORY MUST NOT LOOK LIKE LOSING THE JOBS. The registered/declared half
              needs no database at all, so it still renders — and says which half is missing. */}
          {data?.history_error && (
            <p className="text-[12px] text-warn-300 mt-0.5">
              ⚠ Run history unavailable ({data.history_error}) — the “last run” column is blank for
              every job, which is not the same as never having run.
            </p>
          )}
        </div>
        <button type="button" onClick={refresh} disabled={busy}
          title="Re-read the declaration, the scheduler and the run history."
          className="cursor-pointer shrink-0 inline-flex items-center gap-1.5 text-xs px-3 py-1.5
                     rounded-lg border border-neutral-700 bg-page text-fg-soft transition-colors
                     hover:text-accent-300 hover:border-accent-500/50 disabled:opacity-50">
          <RefreshIcon spinning={busy} size={12} />
          Refresh
        </button>
      </div>

      {error && !data && (
        <p className="px-4 py-3 text-xs text-neg-300">Could not load the jobs: {error}</p>
      )}
      {!data && !error && <p className="px-4 py-6 text-xs text-fg-subtle">Loading…</p>}

      {data && (
        <div className="overflow-auto">
          <table className="w-full text-[12px]">
            <thead className="bg-card">
              <tr className="text-fg-faint text-[11px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 text-left font-medium">Job</th>
                <th className="px-3 py-1.5 text-left font-medium">Fills</th>
                <th className="px-3 py-1.5 text-left font-medium">Cadence</th>
                <th className="px-3 py-1.5 text-left font-medium whitespace-nowrap">Last run</th>
                <th className="px-3 py-1.5 text-left font-medium whitespace-nowrap">Next run</th>
                <th className="px-3 py-1.5 text-left font-medium">State</th>
                {/* The per-job panel marker.
                    ⚠ THIS COMMENT SITS ABOVE THE TAG, NEVER AFTER IT ON THE SAME LINE. JSX drops a
                    whitespace-only LINE, but spaces between two expressions on ONE line survive as
                    a text node — and a text node is illegal inside a `tr`, which React reports as a
                    hydration error rather than as the typo it is. */}
                <th className="px-3 py-1.5 text-right font-medium" />
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {data.jobs.map((j) => (
                <Row key={j.id} j={j} panel={JOB_PANELS[j.id]} ctx={pipeline} />
              ))}
            </tbody>
          </table>
          <p className="px-4 py-2 text-[11px] text-fg-faint border-t border-neutral-800/40">
            {/* ⚠ THE SCOPE IS NAMED. The scheduler is in-process by design (one instance,
                DISABLE_SCHEDULER=1 on any replica), so “registered” and “next run” describe the
                container that served this request — not the fleet. A reader who assumes otherwise
                would take a correct replica for a broken primary. */}
            Registration and next-run times are read from the scheduler in the process that served
            this request. Checked {stamp(data.checked_at)}.
          </p>
        </div>
      )}
    </section>
  );
}

/**
 * RUN THIS JOB NOW, AND STOP IT — one button, reporting into the shared toast stack.
 *
 * ⚠ THE SAME BODY THE SCHEDULER RUNS. The endpoint dispatches to `scheduler.JOB_BODIES`, so this
 * cannot come to mean something different from what the nightly tick does.
 *
 * ⚠⚠ CANCEL IS COOPERATIVE AND ITS LATENCY DIFFERS PER JOB, WHICH THE TOOLTIP STATES. The AIRS scan
 * stops between ACCOUNTS (four reports are stored as a unit); the drift probe between COMPANIES;
 * the FX and size jobs are seconds long and have no useful boundary at all. Promising
 * "immediately" on a scraper mid-download is the decorative Cancel this app has already removed
 * once — the honest version says where it will stop.
 *
 * ⚠ ABSENT WHEN THE JOB HAS NO BODY (`runnable`), never rendered-and-failing: the queue worker has
 * nothing worth triggering, and the two pipeline jobs own a richer Run-now inside their panel.
 */
function RunControl({ j }: { j: JobRow }) {
  const [jobId, setJobId] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  if (!j.runnable) return null;

  const run = async () => {
    setBusy(true);
    try {
      const { id, done } = await startJob(
        `${API_URL}/api/admin/scheduled-jobs/${encodeURIComponent(j.id)}/run`, j.label);
      setJobId(id);
      await done;
    } catch (e) {
      traceError('jobs', `could not start ${j.id}`, e);
    } finally {
      // ⚠ CLEARED ON ANY OUTCOME — done, failed OR cancelled. Leaving the id set would strand the
      // row on "Cancel" for a job that has already stopped.
      setBusy(false);
      setJobId(null);
    }
  };

  // ⚠ ONE BUTTON, TWO ACTIONS, FLIPPING ON THE PRESS — the same rule as the portfolio refresh:
  // keyed on the job id it would wait a round-trip, and for that window the control would read
  // "Run" over work already running, which is how a second job gets started by accident.
  const stopping = busy && jobId === null;
  return (
    <button type="button"
      onClick={(e) => {
        e.stopPropagation();
        if (busy && jobId) void cancelJob(jobId);
        else if (!busy) void run();
      }}
      disabled={stopping}
      title={busy
        ? 'Stop this run. Cancellation is cooperative — it lands at the job’s next safe boundary '
          + '(between accounts for the AIRS scan, between companies for the drift probe); the short '
          + 'jobs have none and will simply finish. Everything already written is kept.'
        : 'Run this job now, with the same body the scheduler runs. Progress appears in the toast '
          + 'at the bottom right.'}
      className={`cursor-pointer text-[11px] px-2 py-0.5 rounded border transition-colors
                  disabled:opacity-50 disabled:cursor-wait ${busy
        ? 'border-warn-500/40 text-warn-400 hover:bg-warn-500/10'
        : 'border-neutral-800/40 text-fg-subtle hover:text-accent-300 hover:border-accent-500/50'}`}>
      {stopping ? 'Starting…' : busy ? '✕ Cancel' : 'Run now'}
    </button>
  );
}

function Row({ j, panel, ctx }: {
  j: JobRow;
  /** This job's detail, from `JOB_PANELS`. Absent for most jobs — see the registry. */
  panel?: (ctx: PipelineCtx) => React.ReactNode;
  ctx: PipelineCtx;
}) {
  const [open, setOpen] = useState(false);
  const tone = JOB_TONE[j.status] ?? JOB_TONE.unknown;
  const summary = summaryLine(j.last_summary);
  const expandable = Boolean(j.note || j.reason || j.last_detail || summary || panel);
  return (
    <>
      <tr onClick={() => expandable && setOpen(!open)}
        className={`transition-colors ${expandable ? 'cursor-pointer hover:bg-overlay/[0.02]' : ''}
                    ${open ? 'bg-accent-500/[0.06]' : ''}`}>
        <td className="px-3 py-1.5 text-fg">
          <span className={`inline-block w-3 text-[10px] ${open ? 'text-accent-400' : 'text-fg-faint'}`}>
            {expandable ? (open ? '▾' : '▸') : ''}
          </span>
          {j.label}
        </td>
        <td className="px-3 py-1.5 text-fg-muted">{j.fills || '—'}</td>
        <td className="px-3 py-1.5 text-fg-muted whitespace-nowrap">{j.cadence}</td>
        <td className="px-3 py-1.5 whitespace-nowrap font-mono text-fg-subtle"
          title={j.last_run_at ? stamp(j.last_run_at) : undefined}>
          {/* ⚠ THE AGE, NOT THE TIMESTAMP, IS THE READABLE FORM — "3.1d ago" answers "is this late"
              in one glance where a date makes the reader do the arithmetic. The exact time is on
              the hover, which is where a precise value belongs. */}
          {ago(j.last_age_hours)}
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap font-mono text-fg-subtle">
          {/* ⚠ A REGISTERED JOB WITH NO NEXT RUN IS PAUSED — it is in the list and will never fire
              again. A bare "—" beside a green dot would read as "nothing scheduled yet". */}
          {j.registered && !j.next_run_at
            ? <span className="text-warn-400" title="Registered but PAUSED — it has no next fire time.">paused</span>
            : stamp(j.next_run_at)}
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap">
          <span className={`inline-flex items-center gap-1.5 ${tone.text}`}>
            <span className={`inline-block w-1.5 h-1.5 rounded-full ${tone.dot}`} />
            {tone.label}
          </span>
        </td>
        <td className="px-3 py-1.5 text-right whitespace-nowrap">
          {/* ⚠ NO "Details" BUTTON — the panel lives INSIDE the row (step 3), so the row's own ▸ is
              the disclosure and a second control opening the same thing two cells apart would be
              two ways to do one thing. */}
          <RunControl j={j} />
        </td>
      </tr>
      {open && (
        <tr className="bg-inset/60">
          <td colSpan={7} className="px-3 py-2 space-y-1">
            {j.reason && <p className={`text-[12px] ${tone.text}`}>{j.reason}</p>}
            {j.last_detail && (
              <p className="text-[12px] text-fg-soft font-mono">{j.last_detail}</p>
            )}
            {summary && <p className="text-[12px] text-fg-muted font-mono">{summary}</p>}
            {/* ⚠ SAID ON THE ROW, NOT ONLY IN THE HEADER COUNT. "Unknown" is the one state a reader
                will assume they misread, so the row itself explains that the silence is ours. */}
            {!j.observable && (
              <p className="text-[12px] text-fg-faint">
                This job records nothing durable, so “did it run” has no answer here — only the
                deploy logs know.
              </p>
            )}
            {j.note && <p className="text-[12px] text-fg-faint">{j.note}</p>}
            <p className="text-[11px] text-fg-faint font-mono">
              id {j.id}
              {j.optional_env ? ` · opt-in via ${j.optional_env}` : ''}
              {j.max_age_hours ? ` · overdue after ${(j.max_age_hours / 24).toFixed(1)}d` : ''}
            </p>
            {/* ⚠ THE DOMAIN, BELOW THE GENERIC. Everything above is what the table knows about
                every job; this is what only THIS job can say — what it will touch, what it last
                touched, and its controls. Rendered from the registry, so the table needs to know
                nothing about it. */}
            {panel && <div className="pt-2 space-y-3">{panel(ctx)}</div>}
          </td>
        </tr>
      )}
    </>
  );
}
