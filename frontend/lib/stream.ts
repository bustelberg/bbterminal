import { apiFetch } from './apiFetch';

// Consumes an SSE stream from a fetch response body and invokes onEvent for
// each parsed `data: ...` line. Throws on non-2xx or network errors so callers
// can decide how to surface them.
export async function runSSE(
  url: string,
  init: RequestInit,
  onEvent: (data: unknown) => void,
  signal?: AbortSignal,
): Promise<void> {
  const resp = await apiFetch(url, { ...init, signal });
  if (!resp.ok || !resp.body) {
    throw new Error(`HTTP ${resp.status}`);
  }
  const reader = resp.body.getReader();
  // The fetch `signal` aborts the initial request but doesn't propagate
  // to an active reader on every browser/version — Cancel would fire,
  // the outer fetch resolved, and the reader kept consuming whatever the
  // server kept sending until the TCP socket closed. Wire abort →
  // reader.cancel() so subsequent reads return done=true immediately,
  // AND poll signal.aborted at the top of the loop so we break even if
  // a chunk arrived between the abort signal and the next read.
  const onAbort = () => { reader.cancel().catch(() => {}); };
  if (signal) {
    if (signal.aborted) {
      reader.cancel().catch(() => {});
    } else {
      signal.addEventListener('abort', onAbort);
    }
  }

  try {
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      if (signal?.aborted) break;
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() ?? '';
      for (const line of lines) {
        if (!line.startsWith('data: ')) continue;
        try {
          onEvent(JSON.parse(line.slice(6)));
        } catch {
          // ignore malformed events
        }
      }
    }
  } finally {
    if (signal) signal.removeEventListener('abort', onAbort);
  }
}
