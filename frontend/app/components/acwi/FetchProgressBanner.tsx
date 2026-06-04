'use client';

import { acwiFetchStore } from '../../../lib/stores/acwi';
import ProgressTimeline from '../ProgressTimeline';

/** Live progress bar + persistent summary for the announcement-detail SSE
 * stream. Reads the module-scoped `acwiFetchStore` directly so it reflects
 * the stream regardless of which view kicked it off (and survives nav). */
export default function FetchProgressBanner() {
  const fetchProgress = acwiFetchStore.use((s) => s.progress);
  const fetching = acwiFetchStore.use((s) => s.fetching);
  const fetchSummary = acwiFetchStore.use((s) => s.summary);

  return (
    <>
      {/* Fetch progress bar */}
      {fetchProgress && (
        <ProgressTimeline
          steps={[]}
          log={[
            `Fetching announcement details: ${fetchProgress.message}`,
            ...(fetchProgress.errors > 0 ? [`${fetchProgress.errors} error${fetchProgress.errors !== 1 ? 's' : ''} so far`] : []),
          ]}
          pct={fetchProgress.total > 0 ? fetchProgress.pct : 0}
          running={fetching}
          defaultLogOpen
          title="Fetch progress"
        />
      )}

      {/* Fetch summary (persists after completion) */}
      {fetchSummary && !fetching && (
        <div className={`${fetchSummary.errors > 0 ? 'bg-amber-500/10 border-amber-500/20' : 'bg-emerald-500/10 border-emerald-500/20'} border rounded-lg px-4 py-3 text-sm space-y-2`}>
          <div className="flex items-center justify-between">
            <span className={fetchSummary.errors > 0 ? 'text-amber-400' : 'text-emerald-400'}>
              {fetchSummary.errors > 0 ? '⚠' : '✓'} {fetchSummary.message}
            </span>
            <button
              onClick={() => acwiFetchStore.set({ summary: null })}
              className="text-gray-500 hover:text-gray-300 text-xs"
            >
              dismiss
            </button>
          </div>
          {fetchSummary.errorList.length > 0 && (
            <details className="text-xs">
              <summary className="text-rose-400 cursor-pointer hover:text-rose-300">
                Show {fetchSummary.errorList.length} failed announcement{fetchSummary.errorList.length !== 1 ? 's' : ''}
              </summary>
              <div className="mt-2 space-y-1 max-h-40 overflow-y-auto">
                {fetchSummary.errorList.map((e, i) => (
                  <div key={i} className="flex gap-2 text-gray-400">
                    <span className="text-rose-400 shrink-0">✗</span>
                    <span className="truncate">{e.title}</span>
                    <span className="text-gray-600 shrink-0">— {e.error}</span>
                  </div>
                ))}
              </div>
            </details>
          )}
        </div>
      )}
    </>
  );
}
