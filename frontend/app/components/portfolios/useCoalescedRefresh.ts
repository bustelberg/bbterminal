'use client';

import { useEffect, useRef } from 'react';

/** Progress can arrive faster than a read completes. Finish that read, then fetch the latest state. */
export function useCoalescedRefresh(identity: string, revision: number, enabled: boolean,
  load: (signal: AbortSignal) => Promise<void>) {
  const refresh = useRef<(() => void) | null>(null);
  useEffect(() => {
    if (!enabled) { refresh.current = null; return; }
    const controller = new AbortController();
    let running = false, dirty = false;
    const request = async () => {
      dirty = true;
      if (running) return;
      running = true;
      try {
        while (dirty && !controller.signal.aborted) {
          dirty = false;
          await load(controller.signal);
        }
      } finally { running = false; }
    };
    refresh.current = () => { void request(); };
    // The revision effect below starts the initial read as well.
    return () => { refresh.current = null; controller.abort(); };
    // identity contains every input to load; progress alone must never abort an in-flight read.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [identity, enabled]);
  useEffect(() => { refresh.current?.(); }, [identity, enabled, revision]);
}

/** Retry transient transport/server failures, but never retry authentication or invalid inputs. */
export async function retrySeriesRead<T>(read: () => Promise<T>, signal: AbortSignal): Promise<T> {
  for (let attempt = 0; ; attempt++) {
    try { return await read(); }
    catch (error) {
      const status = (error as { status?: number })?.status;
      if (signal.aborted || attempt >= 3 || !(error instanceof TypeError || (status != null && (status >= 500 || status === 429)))) throw error;
      await new Promise<void>((resolve, reject) => {
        const abort = () => { clearTimeout(timer); reject(signal.reason); };
        const timer = setTimeout(() => { signal.removeEventListener('abort', abort); resolve(); }, [300, 900, 1800][attempt]);
        signal.addEventListener('abort', abort, { once: true });
      });
    }
  }
}
