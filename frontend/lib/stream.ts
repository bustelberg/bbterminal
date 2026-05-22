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
  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
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
}
