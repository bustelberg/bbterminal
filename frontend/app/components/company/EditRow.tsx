'use client';

import { useState } from 'react';
import Spinner from '../Spinner';
import { inputCls } from './styles';
import type { Company } from './types';

// ─── Inline edit row ──────────────────────────────────────────────────────────

export default function EditRow({
  company,
  exchangeOptions,
  onSave,
  onCancel,
}: {
  company: Company;
  exchangeOptions: string[];
  onSave: (updated: Partial<Company>) => Promise<void>;
  onCancel: () => void;
}) {
  const [name, setName] = useState(company.company_name ?? '');
  const [ticker, setTicker] = useState(company.gurufocus_ticker);
  const [exchange, setExchange] = useState(company.gurufocus_exchange);
  const [saving, setSaving] = useState(false);

  async function handleSave() {
    setSaving(true);
    await onSave({ company_name: name, gurufocus_ticker: ticker, gurufocus_exchange: exchange });
    setSaving(false);
  }

  return (
    <tr className="border-b border-neutral-800/30 bg-accent-500/5">
      <td className="px-4 py-2 text-sm text-fg-subtle">{company.company_id}</td>
      <td className="px-3 py-2"><input value={name} onChange={(e) => setName(e.target.value)} className={inputCls} /></td>
      <td className="px-3 py-2"><input value={ticker} onChange={(e) => setTicker(e.target.value)} className={inputCls} /></td>
      <td className="px-3 py-2">
        <input list="edit-exchange" value={exchange} onChange={(e) => setExchange(e.target.value)} className={inputCls} />
        <datalist id="edit-exchange">{exchangeOptions.map((o) => <option key={o} value={o} />)}</datalist>
      </td>
      <td className="px-3 py-2 text-fg-muted font-mono text-xs">{company.isin ?? '—'}</td>
      <td className="px-3 py-2 text-fg-muted">{company.country ?? '—'}</td>
      <td className="px-3 py-2 text-fg-faint text-xs">—</td>
      <td className="px-3 py-2">
        <div className="flex gap-1.5">
          <button onClick={handleSave} disabled={saving} className="px-3 py-1.5 rounded-lg text-xs font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong disabled:opacity-50 transition-colors inline-flex items-center gap-1.5">
            {saving && <Spinner size={12} className="h-3 w-3 text-fg-strong" />}
            {saving ? 'Saving…' : 'Save'}
          </button>
          <button onClick={onCancel} className="px-3 py-1.5 rounded-lg text-xs font-medium text-fg-muted hover:text-fg-strong hover:bg-overlay/5 transition-colors">
            Cancel
          </button>
        </div>
      </td>
    </tr>
  );
}
