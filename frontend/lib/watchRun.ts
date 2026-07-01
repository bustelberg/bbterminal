import { API_URL } from './apiUrl';
import { runSSE } from './stream';

export type RunRow = Record<string, unknown> & { status?: string };

/**
 * Stream one `ingest_run` to completion over SSE (`/api/ingest/runs/{id}/stream`),
 * invoking `onUpdate` on each change. Resolves with the final row when the run
 * reaches a terminal status (the server closes the stream) — replacing the
 * hand-rolled 2s `setInterval` poll the transient "watch this job" UIs used.
 *
 * Best-effort: on a stream error it resolves with the last row seen (or null) so
 * callers can reconcile with a follow-up reload rather than hang. Pass a signal
 * to cancel (e.g. on unmount).
 */
export async function watchRun(
  runId: number,
  onUpdate: (row: RunRow) => void,
  signal?: AbortSignal,
): Promise<RunRow | null> {
  let last: RunRow | null = null;
  try {
    await runSSE(
      `${API_URL}/api/ingest/runs/${runId}/stream`,
      { method: 'GET' },
      (evt) => {
        const f = evt as { topic?: string; payload?: RunRow };
        if (f?.topic === 'run' && f.payload) {
          last = f.payload;
          onUpdate(f.payload);
        }
      },
      signal,
    );
  } catch {
    // Stream failed — surface the last row we saw; the caller reloads to reconcile.
  }
  return last;
}
