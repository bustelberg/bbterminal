'use client';

import { useState } from 'react';
import { API_URL } from '../../../lib/apiUrl';
import { traceError } from '../../../lib/debugTrace';
import { usePollingFetch } from '../../../lib/hooks/usePollingFetch';
import { jobsStore, startJob } from '../../../lib/stores/jobs';
import InfoTip from '../InfoTip';
import {
  ago, headline, jobFailure, jobReport, jobTiming, JOB_TONE, stamp, summaryLine, type JobRow, type JobsPayload,
} from './automaticJobs';
import { JOB_PANELS, ROW_ACTIONS } from './jobPanels';
import { usePipelineActivity, type PipelineCtx } from './usePipelineActivity';
import ScheduleActionButton from './ScheduleActionButton';

/**
 * Every job that is supposed to run by itself, and whether it did.
 *
 *  It answers "IS ANYTHING MISSING", WHICH NO OTHER SURFACE COULD. A run history only shows jobs
 * that ran, and the scheduler's own list only shows jobs that registered — so a job that silently
 * stopped being registered appears in NEITHER: it has no runs to be absent from, and the list it
 * is missing from is the one being read. The declaration in `backend/scheduled_jobs.py` is what
 * makes an absence visible, and this renders the disagreement.
 *
 *  It replaced the "Smart pipeline activity" CARD (2026-08-13). That card was a second list of
 * jobs with its own countdowns, read from the same `list_scheduled_jobs()` — two clocks for one
 * fire time. Its panels now live behind the rows they belong to (`jobPanels.JOB_PANELS`).
 *
 *  ON /schedule RATHER THAN A NEW /admin PAGE, deliberately. This page already IS the job page;
 * splitting "jobs" across two routes means every future reader has to know which half to open.
 *
 *  Not an SSE stream. Nothing here changes second to second — the interesting transitions are
 * hours apart — so it polls slowly and stays a plain read. Adding a stream would be a second live
 * surface to keep alive for information that is, by construction, daily.
 */
export default function AutomaticJobsCard() {
  /**
   *  Called once, here, and handed to every panel. The panels are drawn from one SSE stream (with
   * six polling fallbacks); a panel that fetched for itself would open another consumer for every
   * row a reader expands. It runs whether or not a row is open because the stream is what the
   * panels' live status is made of — the same cost the retired activity card always paid.
   */
  const pipeline = usePipelineActivity();
  /**
   *  The manual refresh is a change of URL, because `usePollingFetch` keys its effect on the url
   * and exposes no reload. A distinct param is what re-runs it — and it is the honest shape here
   * anyway, since the reader pressing Refresh wants a NEW read, not a replay of the cached one.
   */
  const [nonce, setNonce] = useState(0);
  // 60s: the fastest anything here can legitimately change is a job starting, and re-reading eight
  // rows every few seconds costs more than it tells anyone. The hook also pauses while the tab is
  // hidden, so a /schedule left open in a background tab makes no requests at all.
  const { data, error, loading } = usePollingFetch<JobsPayload>(
    `${API_URL}/api/admin/scheduled-jobs${nonce ? `?_=${nonce}` : ''}`, 60_000);
  const refresh = () => setNonce(Date.now());
  const failures = data?.jobs.map(jobFailure).filter((failure) => failure !== null) ?? [];

  return (
    <section className="bg-card border border-neutral-800/40 rounded-xl">
      <div className="px-4 py-3 border-b border-neutral-800/40 flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <h2 className="text-sm font-semibold text-fg-strong flex items-center gap-1.5">
            Automatic jobs
            {/*  THE ⓘ IS WHERE THE EXPLANATION LIVES, and it is the only place on this card that
                explains anything. The table's job is to be scannable; the reasoning is one hover
                away rather than a paragraph every reader passes over daily. */}
            <InfoTip text={'What is declared, what the scheduler in THIS process is holding, and '
              + 'when each last ran. A declared-but-not-registered job is what this exists to '
              + 'show — it appears in no run history, because it has no runs.'} />
          </h2>
          {data && (
            <p className={`text-[12px] mt-0.5 ${headline(data).tone}`}>{headline(data).text}</p>
          )}
          {/*  LOSING THE HISTORY MUST NOT LOOK LIKE LOSING THE JOBS. The registered/declared half
              needs no database at all, so it still renders — and says which half is missing. */}
          {data?.history_error && (
            <p className="text-[12px] text-warn-300 mt-0.5">
               Run history unavailable ({data.history_error}) — the “last run” column is blank for
              every job, which is not the same as never having run.
            </p>
          )}
        </div>
        <div className="flex shrink-0 flex-wrap items-center justify-end gap-2">
          <CopyJsonButton label="Copy all failed JSON"
            payload={failures.length ? { checked_at: data?.checked_at, failures } : null} />
          <ScheduleActionButton onClick={refresh} disabled={loading} aria-busy={loading}
            title="Re-read the declaration, the scheduler and the run history.">
            {loading ? 'Refreshing…' : 'Refresh'}
          </ScheduleActionButton>
        </div>
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
                     THIS COMMENT SITS ABOVE THE TAG, NEVER AFTER IT ON THE SAME LINE. JSX drops a
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
            {/*  THE SCOPE IS STILL NAMED, IN ONE CLAUSE. The scheduler is in-process by design
                (one instance, DISABLE_SCHEDULER=1 on any replica), so “registered” and “next run”
                describe the container that served this request, not the fleet — a reader who
                assumes otherwise takes a correct replica for a broken primary. That is the whole
                fact; the two sentences it used to take were the same fact said twice. */}
            Read from this container’s scheduler · checked {stamp(data.checked_at)}.
          </p>
        </div>
      )}
    </section>
  );
}

function CopyJsonButton({ label, payload, title }: { label: string; payload: object | null; title?: string }) {
  const [feedback, setFeedback] = useState('');

  async function copy() {
    setFeedback('Copying…');
    try {
      await navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
      setFeedback('Copied');
    } catch {
      setFeedback('Copy failed');
    }
  }

  return (
    <ScheduleActionButton disabled={!payload || feedback === 'Copying…'} title={title} aria-label={label}
      onClick={(event) => { event.stopPropagation(); void copy(); }}>
      {feedback || label}
    </ScheduleActionButton>
  );
}

/**
 * Run this job now, reporting into the shared toast stack where it can be cancelled.
 *
 *  The same body the scheduler runs. The endpoint dispatches to `scheduler.JOB_BODIES`, so this
 * cannot come to mean something different from what the nightly tick does.
 *
 *  Cancel is cooperative and its latency differs per job, which the tooltip states. The AIRS scan
 * stops between ACCOUNTS (four reports are stored as a unit); the drift probe between COMPANIES;
 * the FX and size jobs are seconds long and have no useful boundary at all. Promising
 * "immediately" on a scraper mid-download is the decorative Cancel this app has already removed
 * once — the honest version says where it will stop.
 *
 *  Absent when the job has no body (`runnable`), never rendered-and-failing: the queue worker has
 * nothing worth triggering, and the two pipeline jobs own a richer Run-now inside their panel.
 */
function RunControl({ j }: { j: JobRow }) {
  const [pending, setPending] = useState(false);
  const running = jobsStore.use((state) => state.jobs.some(
    (job) => job.kind === `scheduled.${j.id}` && job.status === 'running'));
  const busy = pending || running;
  if (!j.runnable) return null;

  const run = async () => {
    if (busy) return;
    setPending(true);
    try {
      const { done } = await startJob(
        `${API_URL}/api/admin/scheduled-jobs/${encodeURIComponent(j.id)}/run`, j.label);
      await done;
    } catch (e) {
      traceError('jobs', `could not start ${j.id}`, e);
    } finally {
      setPending(false);
    }
  };

  return (
    <ScheduleActionButton
      onClick={(e) => {
        e.stopPropagation();
        void run();
      }}
      disabled={busy}
      aria-busy={busy}
      title={busy
        ? 'This job is running. Follow progress or cancel it in the job toast.'
        : 'Run this job now, with the same body the scheduler runs. Progress appears in the toast '
          + 'at the bottom right.'}>
      {busy ? 'Running…' : 'Run now'}
    </ScheduleActionButton>
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
  const failure = jobFailure(j);
  const report = jobReport(j);
  const timing = jobTiming(j);
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
          {/*  THE AGE, NOT THE TIMESTAMP, IS THE READABLE FORM — "3.1d ago" answers "is this late"
              in one glance where a date makes the reader do the arithmetic. The exact time is on
              the hover, which is where a precise value belongs. */}
          {ago(j.last_age_hours)}
        </td>
        <td className="px-3 py-1.5 whitespace-nowrap font-mono text-fg-subtle">
          {/*  A REGISTERED JOB WITH NO NEXT RUN IS PAUSED — it is in the list and will never fire
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
          {/*  NO "Details" BUTTON — the panel lives INSIDE the row (step 3), so the row's own ▸ is
              the disclosure and a second control opening the same thing two cells apart would be
              two ways to do one thing.
               THE REGISTRY WINS WHERE IT HAS AN ENTRY, and the two are mutually exclusive by
              construction: `ROW_ACTIONS` exists precisely for the rows `runnable` is false on, so
              `RunControl` would render nothing there anyway. Falling back rather than rendering
              both keeps one run control per row. */}
          <div className="flex items-center justify-end gap-2">
            {report && <CopyJsonButton label="Copy JSON" title={`Copy run report for ${j.label}`} payload={report} />}
            {ROW_ACTIONS[j.id]?.(ctx) ?? <RunControl j={j} />}
          </div>
        </td>
      </tr>
      {open && (
        <tr className="bg-inset/60">
          <td colSpan={7} className="px-3 py-2 space-y-1">
            {j.reason && <p className={`text-[12px] ${tone.text}`}>{j.reason}</p>}
            {failure ? (
              <div className="text-[12px] text-fg-soft">
                <p>{failure.detail}</p>
                {failure.causes.length > 0 && (
                  <ul className="list-disc pl-4">
                    {failure.causes.map(({ message, affected, affected_count }) => (
                      <li key={message}>
                        {message}
                        {affected && ` (${affected.join(', ')}${affected_count > affected.length ? ` +${affected_count - affected.length} more` : ''})`}
                      </li>
                    ))}
                    {failure.additional_causes && <li>{failure.additional_causes} more causes in the full receipt</li>}
                  </ul>
                )}
              </div>
            ) : j.last_detail && (
              <p className="text-[12px] text-fg-soft font-mono">{j.last_detail}</p>
            )}
            {timing && 'duration_ms' in timing && typeof timing.duration_ms === 'number' && (
              <p className="text-[12px] text-fg-muted font-mono">Duration {timing.duration_ms.toLocaleString()} ms</p>
            )}
            {summary && <p className="text-[12px] text-fg-muted font-mono">{summary}</p>}
            {j.last_summary && Object.values(j.last_summary).some(value => value !== null && typeof value === 'object') && (
              <details className="text-xs text-fg-muted">
                <summary className="cursor-pointer py-1">Source dates, coverage and per-portfolio results</summary>
                <pre className="mt-2 max-h-96 overflow-auto whitespace-pre-wrap rounded-lg bg-page p-3 text-[11px]">{JSON.stringify(j.last_summary, null, 2)}</pre>
              </details>
            )}
            {/*  SAID ON THE ROW, NOT ONLY IN THE HEADER COUNT. "Unknown" is the one state a reader
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
            {/*  THE DOMAIN, BELOW THE GENERIC. Everything above is what the table knows about
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
