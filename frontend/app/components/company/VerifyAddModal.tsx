'use client';

import Spinner from '../Spinner';
import { guruFocusUrl } from '../../../lib/gurufocusUrl';
import type { PendingAdd } from './types';

/** Confirmation modal shown before a company is POSTed. Forces the user to
 * eyeball the GuruFocus URL — a wrong ticker/exchange pair means no price
 * data will ever load for the row. */
export default function VerifyAddModal({
  pendingAdd,
  confirming,
  onConfirm,
  onCancel,
}: {
  pendingAdd: PendingAdd;
  confirming: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}) {
  const url = guruFocusUrl(pendingAdd.gurufocus_ticker, pendingAdd.gurufocus_exchange);
  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60">
      <div className="bg-card border border-neutral-800/60 rounded-xl p-6 max-w-lg w-full mx-4 shadow-xl">
        <h2 className="text-base font-semibold text-fg-strong mb-2">Verify GuruFocus listing</h2>
        <p className="text-sm text-fg-muted leading-relaxed mb-4">
          Open the URL below to confirm it points to the right company. The
          ticker and exchange combination must match GuruFocus exactly,
          otherwise no price data will be available for this company.
        </p>
        <div className="bg-page border border-neutral-800/60 rounded-lg p-3 mb-4 space-y-1.5 text-sm">
          <div className="flex justify-between gap-4">
            <span className="text-fg-subtle">Name</span>
            <span className="text-fg font-medium text-right">{pendingAdd.company_name}</span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-fg-subtle">Ticker</span>
            <span className="font-mono text-fg">{pendingAdd.gurufocus_ticker}</span>
          </div>
          <div className="flex justify-between gap-4">
            <span className="text-fg-subtle">Exchange</span>
            <span className="font-mono text-fg">{pendingAdd.gurufocus_exchange}</span>
          </div>
          <div className="pt-2 mt-2 border-t border-neutral-800/60">
            <a
              href={url}
              target="_blank"
              rel="noopener noreferrer"
              className="text-accent-400 hover:text-accent-300 hover:underline break-all transition-colors"
            >
              {url}
            </a>
          </div>
        </div>
        <p className="text-sm text-fg-soft mb-4">
          Is this the company you mean to add?
        </p>
        <div className="flex justify-end gap-2">
          <button
            onClick={onCancel}
            disabled={confirming}
            className="px-4 py-2 rounded-lg text-sm font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors disabled:opacity-50"
          >
            Cancel
          </button>
          <button
            onClick={onConfirm}
            disabled={confirming}
            className="px-4 py-2 rounded-lg text-sm font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50 inline-flex items-center gap-2"
          >
            {confirming && <Spinner size={14} className="h-3.5 w-3.5 text-fg-strong" />}
            {confirming ? 'Adding…' : 'Yes, add this company'}
          </button>
        </div>
      </div>
    </div>
  );
}
