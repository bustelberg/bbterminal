export default function Loading() {
  return <div role="status" aria-label="Loading page" className="space-y-6 p-6 motion-safe:animate-pulse">
    <span className="sr-only">Loading page…</span>
    <div aria-hidden="true" className="h-7 w-48 rounded bg-overlay/10" />
    <div aria-hidden="true" className="grid gap-4 sm:grid-cols-3">
      {[0, 1, 2].map(key => <div key={key} className="h-24 rounded-xl border border-neutral-800/40 bg-card" />)}
    </div>
    <div aria-hidden="true" className="h-64 rounded-xl border border-neutral-800/40 bg-card" />
  </div>;
}
