'use client';

import { useState } from 'react';
import ProgressTimeline, { type StepState } from '../ProgressTimeline';
import { apiFetch } from '../../../lib/apiFetch';
import { API_URL } from '../../../lib/apiUrl';
import { DERIVE_STEPS, deepCloneConfig } from './filterConfig';
import type {
  DerivedCriterionSpec,
  FilterConfig,
  FilterConfigEntry,
  Preview,
  UniverseRow,
} from './types';

type TightenPanelProps = {
  base: UniverseRow;
  specs: DerivedCriterionSpec[];
  defaults: FilterConfig;
  onClose: () => void;
  onCreated: () => void;
};

/** Inline editor to derive a tighter universe from a base one: edit the
 * per-metric thresholds, dry-run a `Preview`, then stream the create via
 * SSE into the `ProgressTimeline`. */
export default function TightenPanel({ base, specs, defaults, onClose, onCreated }: TightenPanelProps) {
  const [config, setConfig] = useState<FilterConfig>(() => deepCloneConfig(defaults));
  const [label, setLabel] = useState(`${base.label} (tight)`);
  const [description, setDescription] = useState('');
  const [previewing, setPreviewing] = useState(false);
  const [creating, setCreating] = useState(false);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [errMsg, setErrMsg] = useState<string | null>(null);
  const [stepMap, setStepMap] = useState<Record<string, StepState>>({});
  const [detailLog, setDetailLog] = useState<string[]>([]);
  const [doneSummary, setDoneSummary] = useState<string | null>(null);

  const updateEntry = (key: string, patch: Partial<FilterConfigEntry>) => {
    setConfig(prev => ({ ...prev, [key]: { ...prev[key], ...patch } }));
    setPreview(null);
  };

  const updateComponent = (key: string, code: string, value: number) => {
    setConfig(prev => {
      const entry = prev[key] ?? { enabled: false, components: {} };
      return {
        ...prev,
        [key]: {
          ...entry,
          components: { ...(entry.components ?? {}), [code]: value },
        },
      };
    });
    setPreview(null);
  };

  const runPreview = async () => {
    setPreviewing(true);
    setErrMsg(null);
    try {
      const r = await apiFetch(`${API_URL}/api/universe/derive/preview`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ base_universe_id: base.universe_id, filter_config: config }),
      });
      if (!r.ok) throw new Error(await r.text());
      setPreview(await r.json());
    } catch (e) {
      setErrMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setPreviewing(false);
    }
  };

  const create = async () => {
    if (!label.trim()) {
      setErrMsg('Label is required');
      return;
    }
    setCreating(true);
    setErrMsg(null);
    setStepMap({});
    setDetailLog([]);
    setDoneSummary(null);
    try {
      const resp = await apiFetch(`${API_URL}/api/universe/derive`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          base_universe_id: base.universe_id,
          label: label.trim(),
          description: description.trim() || null,
          filter_config: config,
        }),
      });
      if (!resp.ok || !resp.body) throw new Error(`HTTP ${resp.status}`);

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = '';
      let succeeded = false;
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const chunks = buf.split('\n\n');
        buf = chunks.pop() ?? '';
        for (const chunk of chunks) {
          for (const line of chunk.split('\n')) {
            if (!line.startsWith('data:')) continue;
            const payload = line.slice(5).trim();
            if (!payload) continue;
            let j: { type?: string; step?: string; status?: string; message?: string; data?: unknown };
            try { j = JSON.parse(payload); } catch { continue; }
            if (j.type === 'progress' && j.step) {
              const status = (j.status as StepState['status']) ?? 'in_progress';
              setStepMap(prev => ({
                ...prev,
                [j.step as string]: { status, message: j.message ?? '' },
              }));
              setDetailLog(prev => [...prev, `${j.step}: ${j.message ?? ''}`]);
            } else if (j.type === 'done') {
              succeeded = true;
              setDoneSummary(j.message ?? 'Done.');
            } else if (j.type === 'error') {
              setErrMsg(j.message ?? 'Failed');
            }
          }
        }
      }
      if (succeeded) {
        setTimeout(() => onCreated(), 800);
      }
    } catch (e) {
      setErrMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setCreating(false);
    }
  };

  return (
    <div className="ml-6 bg-card-alt border border-accent-900/40 rounded-xl px-5 py-4 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="text-fg-strong text-sm font-medium">Tighten {base.label}</h3>
        <button onClick={onClose} className="text-fg-subtle hover:text-fg-soft text-xs">Close</button>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <div>
          <label className="text-fg-subtle text-[11px] uppercase tracking-wider">New universe label</label>
          <input
            value={label}
            onChange={e => setLabel(e.target.value)}
            className="mt-1 w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
          />
        </div>
        <div>
          <label className="text-fg-subtle text-[11px] uppercase tracking-wider">Description (optional)</label>
          <input
            value={description}
            onChange={e => setDescription(e.target.value)}
            className="mt-1 w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
          />
        </div>
      </div>

      <div className="space-y-2">
        {specs.map(spec => {
          const entry = config[spec.key] ?? { enabled: false };
          return (
            <div key={spec.key} className="bg-page border border-neutral-800 rounded-lg px-3 py-2">
              <label className="flex items-center gap-2 text-sm text-fg cursor-pointer">
                <input
                  type="checkbox"
                  checked={!!entry.enabled}
                  onChange={e => updateEntry(spec.key, { enabled: e.target.checked })}
                  className="accent-accent-500"
                />
                <span className="font-medium">{spec.label}</span>
                {!spec.components && spec.op && (
                  <span className="text-fg-subtle text-xs font-mono">{spec.op}</span>
                )}
              </label>
              {entry.enabled && (
                <div className="mt-2 pl-6 space-y-2">
                  {spec.components ? (
                    spec.components.map(c => {
                      const v = entry.components?.[c.code] ?? c.default;
                      return (
                        <div key={c.code} className="flex items-center gap-2 text-xs">
                          <span className="text-fg-muted flex-1">{c.label}</span>
                          <input
                            type="number"
                            step="0.5"
                            value={v}
                            onChange={e => updateComponent(spec.key, c.code, parseFloat(e.target.value))}
                            className="w-24 bg-page border border-neutral-700 rounded px-2 py-1 text-right font-mono text-fg-strong focus:border-accent-500 outline-none"
                          />
                          <span className="text-fg-subtle w-4">%</span>
                        </div>
                      );
                    })
                  ) : (
                    <div className="flex items-center gap-2 text-xs">
                      <span className="text-fg-muted flex-1">Threshold</span>
                      <input
                        type="number"
                        step="0.5"
                        value={entry.threshold ?? spec.default_threshold}
                        onChange={e => updateEntry(spec.key, { threshold: parseFloat(e.target.value) })}
                        className="w-24 bg-page border border-neutral-700 rounded px-2 py-1 text-right font-mono text-fg-strong focus:border-accent-500 outline-none"
                      />
                      <span className="text-fg-subtle w-4">%</span>
                    </div>
                  )}
                </div>
              )}
            </div>
          );
        })}
      </div>

      <div className="flex items-center gap-2 flex-wrap">
        <button
          onClick={runPreview}
          disabled={previewing}
          className="px-3 py-1.5 rounded-lg text-xs font-medium bg-neutral-700 hover:bg-neutral-600 text-fg-strong transition-colors disabled:opacity-50"
        >
          {previewing ? 'Previewing...' : 'Preview'}
        </button>
        <button
          onClick={create}
          disabled={creating}
          className="px-3 py-1.5 rounded-lg text-xs font-medium bg-accent-600 hover:bg-accent-500 text-fg-strong transition-colors disabled:opacity-50"
        >
          {creating ? 'Creating...' : 'Save derived universe'}
        </button>
        {errMsg && <span className="text-neg-400 text-xs">{errMsg}</span>}
      </div>

      {preview && (
        <div className="bg-page border border-neutral-800 rounded-lg p-3 space-y-2">
          <div className="text-xs text-fg-muted">
            <span className="font-mono text-fg-strong">{preview.passed_rows.toLocaleString()}</span>
            {' / '}
            <span className="font-mono">{preview.base_rows.toLocaleString()}</span>
            {' rows pass'}
            {preview.missing_metrics > 0 && (
              <span className="text-warn-400">
                {' '} · {preview.missing_metrics.toLocaleString()} excluded for missing metrics
              </span>
            )}
          </div>
          {preview.monthly_counts.length > 0 && (
            <details>
              <summary className="text-fg-subtle text-xs cursor-pointer hover:text-fg-soft">
                Monthly counts ({preview.monthly_counts.length} months)
              </summary>
              <div className="mt-2 max-h-40 overflow-auto text-xs font-mono text-fg-muted grid grid-cols-2 md:grid-cols-3 gap-x-4 gap-y-1">
                {preview.monthly_counts.map(m => (
                  <div key={m.month} className="flex justify-between">
                    <span>{m.month}</span>
                    <span className="text-fg-subtle">{m.count}{m.base_count != null && ` / ${m.base_count}`}</span>
                  </div>
                ))}
              </div>
            </details>
          )}
        </div>
      )}

      <ProgressTimeline
        steps={DERIVE_STEPS}
        state={stepMap}
        log={detailLog}
        doneSummary={doneSummary}
        running={creating}
      />
    </div>
  );
}
