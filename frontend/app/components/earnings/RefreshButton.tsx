'use client';

/** The indigo "Refresh X" button used to kick off a per-source data refresh
 * (financials / indicators / earnings). Shows "Refreshing..." while busy
 * and disables itself until the SSE stream completes. */
export default function RefreshButton({
  label,
  running,
  onClick,
}: {
  label: string;
  running: boolean;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      disabled={running}
      className="px-3 py-1.5 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 text-white transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
    >
      {running ? 'Refreshing...' : label}
    </button>
  );
}
