'use client';

import { useEffect, useState } from 'react';
import type { AssetGridRow } from '../../lib/types/api';
import AssetDualChart from './AssetDualChart';

/** Modal fragment: side-by-side price+volume charts for one asset — native
 * (left) and EUR-converted (right), with a log/linear price toggle. */
export default function AssetChartModal({ row, onClose }: { row: AssetGridRow; onClose: () => void }) {
  const [scale, setScale] = useState<'log' | 'linear'>('log');

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const span = row.price_from ? `${row.price_from} → ${row.price_to}` : null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-scrim/60"
      onClick={onClose} role="dialog" aria-modal="true">
      <div className="bg-card border border-neutral-800/40 rounded-xl shadow-xl w-[80vw] h-[80vh] overflow-auto p-4"
        onClick={(e) => e.stopPropagation()}>
        <div className="flex items-start justify-between gap-3 mb-3">
          <div className="min-w-0">
            <div className="flex items-baseline gap-2 flex-wrap">
              <span className="text-base font-mono font-semibold text-fg-strong">{row.analysis_symbol ?? row.yahoo_symbol ?? row.isin}</span>
              {row.name && <span className="text-sm text-fg-soft truncate">{row.name}</span>}
            </div>
            <div className="text-[11px] text-fg-faint mt-0.5 font-mono">
              {span ? `${span} · ` : ''}{(row.bars ?? 0).toLocaleString()} bars
              {' · '}
              <button type="button" onClick={() => setScale((s) => (s === 'log' ? 'linear' : 'log'))}
                className="text-accent-400 hover:underline">
                switch to {scale === 'log' ? 'linear' : 'log'}
              </button>
            </div>
          </div>
          <button type="button" onClick={onClose} aria-label="Close"
            className="text-fg-faint hover:text-fg-strong text-xl leading-none px-1 -mt-1">×</button>
        </div>

        {row.analysis_id != null
          ? <AssetDualChart analysisId={row.analysis_id} scale={scale} />
          : <div className="bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-sm text-neg-300">No stored price/volume data for <b>{row.isin}</b>.</div>}
      </div>
    </div>
  );
}
