/**
 * Pipeline-timeline derivation for `/schedule`.
 *
 * `runToTimelineProps` maps a raw `ingest_run` row to the per-step
 * state / progress-pct the `ProgressTimeline` component renders. Pure
 * function — no React, no I/O — so it's unit-testable and shared by both
 * the live pipeline strip and `ScheduleRunDetail`.
 */
import { type StepDef, type StepState } from '../ProgressTimeline';
import type { IngestRun } from './types';

export const PIPELINE_STEPS: StepDef[] = [
  { key: 'acquisition', label: 'Source acquisition' },
  { key: 'templates', label: 'Template universe refresh' },
  { key: 'prune', label: 'Orphan company prune' },
  { key: 'dedupe', label: 'Duplicate company merge' },
  { key: 'prices', label: 'Price + volume refresh' },
  { key: 'momentum', label: 'Momentum compute' },
];

export function runToTimelineProps(run: IngestRun): {
  state: Record<string, StepState>;
  pct: number;
  running: boolean;
  doneSummary: string | null;
  errorMessage: string | null;
  totalElapsedMs: number | null;
} {
  const finished = run.status === 'ok' || run.status === 'error';
  const phase = run.current_phase;
  const state: Record<string, StepState> = {
    acquisition: { status: 'pending' },
    templates: { status: 'pending' },
    prune: { status: 'pending' },
    dedupe: { status: 'pending' },
    prices: { status: 'pending' },
    momentum: { status: 'pending' },
  };

  const liveMessage = run.current_message ?? undefined;

  // Phase 0 — Acquisition. There's no per-run summary column on
  // `ingest_run` for acquisition results — current_message carries the
  // status line. Once we move past this phase we mark it done.
  if (phase === 'templates' || phase === 'prune' || phase === 'dedupe' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.acquisition = { status: 'done', message: 'sources acquired' };
  } else if (phase === 'acquisition') {
    state.acquisition = { status: 'in_progress', message: liveMessage ?? 'probing upstream sources…' };
  }

  // Phase 1 — Templates
  const templates = run.templates_summary ?? [];
  const tplErr = templates.filter((t) => t.error).length;
  const tplOk = templates.length - tplErr;
  if (templates.length > 0 && (phase === 'prune' || phase === 'dedupe' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished)) {
    // Aggregate diff across templates for the inline message.
    const totAdd = templates.reduce((a, t) => a + (t.additions_count || 0), 0);
    const totRem = templates.reduce((a, t) => a + (t.removals_count || 0), 0);
    const totRen = templates.reduce((a, t) => a + (t.renames_count || 0), 0);
    state.templates = {
      status: tplErr > 0 && tplOk === 0 ? 'error' : 'done',
      message: `${tplOk}/${templates.length} ok · +${totAdd} / −${totRem}${totRen > 0 ? ` / r${totRen}` : ''}`,
    };
  } else if (phase === 'templates') {
    state.templates = { status: 'in_progress', message: liveMessage ?? 'reconstructing template universes…' };
  } else if (finished) {
    state.templates = { status: 'error', message: 'failed' };
  }

  // Phase 2 — Prune (no per-run summary column; current_message
  // carries the count line. Once we move past prune the step is done
  // unless the phase errored, which would land in error_summary).
  if (phase === 'dedupe' || phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.prune = { status: 'done', message: 'orphan companies pruned' };
  } else if (phase === 'prune') {
    state.prune = { status: 'in_progress', message: liveMessage ?? 'pruning orphan companies…' };
  }

  // Phase 2.5 — Dedupe (no per-run summary column; current_message
  // carries the merge counts).
  if (phase === 'prices' || phase === 'momentum' || phase === 'done' || finished) {
    state.dedupe = { status: 'done', message: 'duplicates merged' };
  } else if (phase === 'dedupe') {
    state.dedupe = { status: 'in_progress', message: liveMessage ?? 'merging duplicate companies…' };
  }

  // Phase 3 — Prices
  if (run.companies_processed > 0 && (phase === 'momentum' || phase === 'done' || finished)) {
    const denominator = run.companies_total ? ` of ${run.companies_total}` : '';
    state.prices = {
      status: 'done',
      message: `${run.companies_processed}${denominator} processed · ${run.prices_refreshed}p / ${run.volumes_refreshed}v · ${run.forbidden_count} forbidden`,
    };
  } else if (phase === 'prices') {
    let msg = liveMessage;
    if (!msg) {
      const denom = run.companies_total ? ` of ${run.companies_total}` : '';
      msg = run.companies_processed > 0
        ? `${run.companies_processed}${denom} processed…`
        : `starting${denom}…`;
    }
    state.prices = { status: 'in_progress', message: msg };
  } else if (finished) {
    state.prices = { status: 'error', message: 'failed' };
  }

  // Phase 4 — Momentum
  const mom = run.momentum_summary ?? [];
  const successCount = mom.filter((m) => m.status === 'ok').length;
  const errorCount = mom.filter((m) => m.status === 'error').length;
  if (mom.length > 0 && (phase === 'done' || finished)) {
    const parts = [`${successCount} ok`];
    if (errorCount > 0) parts.push(`${errorCount} failed`);
    state.momentum = {
      status: errorCount > 0 && successCount === 0 ? 'error' : 'done',
      message: `${parts.join(' · ')} of ${mom.length} strateg${mom.length === 1 ? 'y' : 'ies'}`,
    };
  } else if (phase === 'momentum') {
    state.momentum = { status: 'in_progress', message: liveMessage ?? 'computing holdings…' };
  } else if (finished) {
    // Pipeline finished without producing any momentum results — usually
    // means no scheduled strategies are enabled.
    state.momentum = { status: 'done', message: 'skipped (no scheduled strategies)' };
  }

  let score = 0;
  for (const s of Object.values(state)) {
    if (s.status === 'done') score += 1;
    else if (s.status === 'in_progress') score += 0.5;
    else if (s.status === 'error') score += 1;
  }
  const pct = Math.round((score / PIPELINE_STEPS.length) * 100);

  let elapsedMs: number | null = null;
  try {
    const startMs = Date.parse(run.started_at);
    const endMs = run.finished_at ? Date.parse(run.finished_at) : Date.now();
    elapsedMs = Math.max(0, endMs - startMs);
  } catch {
    elapsedMs = null;
  }

  return {
    state,
    pct,
    running: run.status === 'running',
    doneSummary: run.status === 'ok' && phase === 'done' ? 'Pipeline complete' : null,
    errorMessage: run.status === 'error' && run.error_summary ? run.error_summary : null,
    totalElapsedMs: elapsedMs,
  };
}
