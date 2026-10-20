'use client';

import { useRef, useState } from 'react';
import { startJob, startLocalJob } from '../../../lib/stores/jobs';
import { API_URL } from '../../../lib/apiUrl';

/** Serialize full account refreshes: AIRS uses one authenticated browser session. */
export function useForcePriceChecks() {
  const running = useRef(false);
  const [progress, setProgress] = useState<{ done: number; total: number } | null>(null);
  const run = async (accounts: { id: string; name: string }[], title: string, onComplete: () => Promise<void>) => {
    if (running.current || !accounts.length) return;
    running.current = true;
    setProgress({ done: 0, total: accounts.length });
    try {
      const { done } = await startJob(`${API_URL}/api/airs/vermogen/price-checks/job`, title,
        { headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ accounts: accounts.map((account) => account.id) }) });
      await done;
      await onComplete();
    } catch (error) {
      startLocalJob(title, 'airs.price-checks.error', async () => { throw error; }, { cancellable: false });
    } finally {
      running.current = false;
      setProgress(null);
    }
  };
  return { progress, run };
}
