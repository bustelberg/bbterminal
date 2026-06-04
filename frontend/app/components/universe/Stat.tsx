'use client';

/** One label/value cell in the universe-card stat grid. */
export default function Stat({ label, value, mono }: { label: string; value: string | number; mono?: boolean }) {
  return (
    <div>
      <div className="text-gray-500 text-[10px] uppercase tracking-wider">{label}</div>
      <div className={`text-gray-200 text-sm mt-0.5 ${mono ? 'font-mono text-xs' : ''}`}>{value}</div>
    </div>
  );
}
