'use client';

/**
 * Tail a pipeline run's STEP TRANSCRIPT into the browser console.
 *
 * The /schedule card shows one line — the run's `current_message`, throttled to
 * ~1/s and overwritten on every write. That is a status, and it is the right
 * amount of detail for a card. It cannot answer the questions you actually have
 * while a rebalance is running: which companies did it refresh, what did each
 * one return, which computation is it in, and what did each strategy end up
 * holding. Those are thousands of lines, and they belong in the console
 * (full diagnostic to the console, one short line to the UI).
 *
 * The transcript is read with a CURSOR (`?after=<seq>`), so a slow tab or a
 * dropped request resumes exactly where it left off instead of skipping the
 * lines it missed — for a log, a silent gap is the one unacceptable failure.
 * The server buffers in memory and says how many entries it had to drop
 * (`dropped`) and whether our cursor fell off the back of the ring (`gap`);
 * both are reported here rather than papered over.
 */

import { API_URL } from '../../../lib/apiUrl';
import { apiFetch } from '../../../lib/apiFetch';
import { watchRun } from '../../../lib/watchRun';

export type RunLogEntry = {
  seq: number;
  at: string;
  level: string;
  phase: string | null;
  message: string;
};

export type RunLogPage = {
  entries?: RunLogEntry[];
  next?: number;
  latest?: number;
  dropped?: number;
  gap?: number;
  more?: boolean;
};

/** Poll interval while the run is live. Fast enough that per-company lines feel
 * live, slow enough that a 20-minute month-end refresh is ~2k requests, not 60k. */
const POLL_MS = 600;

/** `2026-08-02T09:14:22.517+00:00` → `09:14:22`. Falls back to the raw string
 * rather than inventing a time we don't have. */
export function formatLogTime(at: string): string {
  const m = /T(\d{2}:\d{2}:\d{2})/.exec(at || '');
  return m ? m[1] : (at || '');
}

/** One console line: `09:14:22 [rebalance #412] prices · NAS:AAPL — prices +2…`.
 * Pure, so the format is unit-testable without a run. */
export function formatRunLogLine(entry: RunLogEntry, label: string): string {
  const phase = entry.phase ? `${entry.phase} · ` : '';
  return `${formatLogTime(entry.at)} [${label}] ${phase}${entry.message}`;
}

/** `console.log` / `warn` / `error` by level. The level is a COLOUR, not a
 * story — the message carries the meaning. */
function emit(entry: RunLogEntry, label: string): void {
  const line = formatRunLogLine(entry, label);
  if (entry.level === 'error') console.error(line);
  else if (entry.level === 'warn') console.warn(line);
  else console.log(line);
}

async function fetchPage(runId: number, after: number, signal?: AbortSignal): Promise<RunLogPage | null> {
  try {
    const r = await apiFetch(`${API_URL}/api/ingest/runs/${runId}/log?after=${after}`, { signal });
    if (!r.ok) return null;
    return (await r.json()) as RunLogPage;
  } catch {
    // A dropped poll is not a lost line — the cursor hasn't moved, so the next
    // poll re-asks for the same range.
    return null;
  }
}

/**
 * Stream `runId`'s transcript to the console until the run reaches a terminal
 * status, then drain whatever landed after that. `label` prefixes every line
 * (e.g. `rebalance #412`).
 *
 * Resolves when the run is finished and the log is drained. Never throws — a
 * console tail must not be able to break the button that started the run.
 */
export async function tailRunToConsole(
  runId: number,
  label: string,
  signal?: AbortSignal,
): Promise<void> {
  let cursor = 0;
  let finished = false;
  let finalStatus = 'unknown';
  const started = Date.now();

  console.log(`▶ [${label}] run started — streaming step log (${API_URL}/api/ingest/runs/${runId}/log)`);

  // The run ROW tells us when to stop; the log tells us what happened. They are
  // separate streams on purpose: the row is durable and terminal-statused, the
  // transcript is in-memory and append-only.
  const watching = watchRun(runId, (row) => {
    const s = String(row.status ?? '');
    if (s && s !== 'running') { finalStatus = s; finished = true; }
  }, signal).catch(() => null);

  const drain = async (): Promise<void> => {
    // `more` means the page was capped — keep pulling before sleeping, or a
    // burst (1,400 companies) would trickle out at one page per poll.
    for (;;) {
      if (signal?.aborted) return;
      const page = await fetchPage(runId, cursor, signal);
      if (!page) return;
      if (page.gap) {
        console.warn(`⚠ [${label}] ${page.gap} step(s) were dropped from the server's ring buffer — the transcript below has a gap`);
      }
      for (const e of page.entries ?? []) emit(e, label);
      cursor = page.next ?? cursor;
      if (!page.more) return;
    }
  };

  while (!finished) {
    if (signal?.aborted) return;
    await drain();
    if (finished) break;
    await new Promise((res) => setTimeout(res, POLL_MS));
  }
  await drain();          // trailing lines written between the last poll and the finish
  await watching;

  const secs = ((Date.now() - started) / 1000).toFixed(1);
  const line = `■ [${label}] run ${finalStatus} after ${secs}s — ${cursor} step(s) logged`;
  if (finalStatus === 'error') console.error(line);
  else console.log(line);
}
