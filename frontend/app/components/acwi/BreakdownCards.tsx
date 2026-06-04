'use client';

type Breakdown = [string, { count: number; weight: number }][];

/** Side-by-side sector + top-country weight breakdowns of the live iShares
 * holdings. Pure presentational — both arrays are pre-sorted by weight in
 * `useAcwiData`. */
export default function BreakdownCards({
  sectorBreakdown,
  countryBreakdown,
}: {
  sectorBreakdown: Breakdown;
  countryBreakdown: Breakdown;
}) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
      {/* Sector breakdown */}
      <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
        <h2 className="text-sm font-medium text-fg-soft mb-3">Sector Breakdown</h2>
        <div className="space-y-1.5">
          {sectorBreakdown.map(([sector, { count: c, weight }]) => (
            <div key={sector} className="flex items-center gap-3 text-sm">
              <div className="flex-1 text-fg truncate">{sector}</div>
              <div className="text-fg-muted font-mono text-xs w-12 text-right">{c}</div>
              <div className="w-24">
                <div className="h-1.5 rounded-full bg-neutral-800 overflow-hidden">
                  <div
                    className="h-full rounded-full bg-accent-500"
                    style={{ width: `${Math.min(weight * 3, 100)}%` }}
                  />
                </div>
              </div>
              <div className="text-fg-soft font-mono text-xs w-16 text-right">{weight.toFixed(2)}%</div>
            </div>
          ))}
        </div>
      </div>

      {/* Country breakdown (top 15) */}
      <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
        <h2 className="text-sm font-medium text-fg-soft mb-3">
          Top Countries <span className="text-fg-subtle font-normal">({countryBreakdown.length} total)</span>
        </h2>
        <div className="space-y-1.5">
          {countryBreakdown.slice(0, 15).map(([country, { count: c, weight }]) => (
            <div key={country} className="flex items-center gap-3 text-sm">
              <div className="flex-1 text-fg truncate">{country}</div>
              <div className="text-fg-muted font-mono text-xs w-12 text-right">{c}</div>
              <div className="w-24">
                <div className="h-1.5 rounded-full bg-neutral-800 overflow-hidden">
                  <div
                    className="h-full rounded-full bg-accent-500"
                    style={{ width: `${Math.min(weight * 1.5, 100)}%` }}
                  />
                </div>
              </div>
              <div className="text-fg-soft font-mono text-xs w-16 text-right">{weight.toFixed(2)}%</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
