'use client';

import InfoTip from './InfoTip';
import type { CriterionDef } from './types';

/** Reference card listing the quality criteria (1 point each). Hidden until
 * the criteria list has loaded. */
export default function CriteriaCard({ criteria }: { criteria: CriterionDef[] }) {
  if (criteria.length === 0) return null;
  return (
    <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4">
      <h2 className="text-white text-sm font-medium mb-3">Quality Criteria (score 1 point each, need &gt;= 1 to qualify)</h2>
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-2">
        {criteria.map(c => (
          <div key={c.key} className="text-xs text-gray-400 flex items-center gap-1.5">
            <div className="w-1.5 h-1.5 rounded-full bg-indigo-500/60 shrink-0" />
            {c.label}
            <span className="text-gray-600 font-mono text-[10px]">{c.min_years ?? 1}y</span>
            {c.description && <InfoTip text={c.description} />}
          </div>
        ))}
      </div>
    </div>
  );
}
