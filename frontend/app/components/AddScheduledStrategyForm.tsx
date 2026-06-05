'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import LoadingDots from './LoadingDots';
import { useMomentumSignals, useUniverseTemplates, type SignalDef } from '../../lib/hooks/apiData';
import { API_URL } from '../../lib/apiUrl';

// API_URL imported from lib/apiUrl above — single source of truth.

const FREQUENCIES: { value: string; label: string; description: string }[] = [
  { value: 'daily', label: 'Daily', description: 'Runs every weekly tick (we only have Monday-close data today; effectively weekly).' },
  { value: 'weekly', label: 'Weekly', description: 'Rebalances on every Tuesday tick (Monday close).' },
  { value: 'monthly', label: 'Monthly', description: 'Rebalances on the Tuesday after the 1st of each new month.' },
  { value: 'bimonthly', label: 'Bi-monthly', description: 'Rebalances every 2 months, on the Tuesday after that month\'s 1st.' },
  { value: 'quarterly', label: 'Quarterly', description: 'Rebalances every 3 months, on the Tuesday after that month\'s 1st.' },
];

type SelectionMode = 'momentum' | 'random' | 'all';

const SELECTION_MODES: { value: SelectionMode; label: string }[] = [
  { value: 'momentum', label: 'Momentum' },
  { value: 'random', label: 'Random (baseline)' },
  { value: 'all', label: 'All universe (index proxy)' },
];

const STRATEGY_LABELS: Record<SelectionMode, string> = {
  momentum: 'Momentum',
  random: 'Random',
  all: 'All universe',
};

const FREQUENCY_LABELS: Record<string, string> = {
  daily: 'Daily',
  weekly: 'Weekly',
  monthly: 'Monthly',
  bimonthly: 'Bi-monthly',
  quarterly: 'Quarterly',
};

type UniverseSummary = {
  template_key: string;
  label: string;
  earliest_date: string;
  earliest_captured_month: string | null;
  latest_captured_month: string | null;
};

// (Universe + signal caches now live in `lib/hooks/apiData.ts` and are
// shared with MomentumBacktester via the same module-level TTL cache.)

/** Inline "Add scheduled strategy" form. Multi-select on universes /
 * strategies / frequencies; submitting creates one schedule entry per
 * Cartesian-product permutation. Each entry's `config` carries the
 * shared strategy parameters (top-N pair, min price score, signal +
 * category weights, max companies) — those apply to every permutation. */
export default function AddScheduledStrategyForm({
  onAdded,
  onCancel,
}: {
  onAdded: () => Promise<void> | void;
  onCancel: () => void;
}) {
  // Data sources — shared cached hooks. Both read synchronously from
  // the module-level cache when it's warm, so the form renders fully
  // populated on first paint when the user has seen this data before.
  const { data: _utData, loading: universesLoading, error: _utError } = useUniverseTemplates();
  const { data: _sigData } = useMomentumSignals();
  const universes = (_utData ?? []) as UniverseSummary[];

  const [selectedUniverses, setSelectedUniverses] = useState<string[]>(
    () => universes.map((u) => u.template_key),
  );
  const [selectedModes, setSelectedModes] = useState<SelectionMode[]>(['momentum']);
  const [selectedFrequencies, setSelectedFrequencies] = useState<string[]>(['weekly']);

  const [topNSectors, setTopNSectors] = useState(4);
  const [topNPerSector, setTopNPerSector] = useState(6);
  const [minPriceScore, setMinPriceScore] = useState<string>('');
  const [maxCompanies, setMaxCompanies] = useState(0);
  const [signalDefs, setSignalDefs] = useState<SignalDef[]>(
    () => _sigData?.signals ?? [],
  );
  const [weights, setWeights] = useState<Record<string, number>>(() => {
    if (!_sigData) return {};
    const w: Record<string, number> = {};
    _sigData.signals.forEach((s) => { w[s.key] = s.default_weight; });
    return w;
  });
  const [categories, setCategories] = useState<string[]>(
    () => _sigData?.categories ?? [],
  );
  const [categoryWeights, setCategoryWeights] = useState<Record<string, number>>(() => {
    if (!_sigData) return {};
    const cw: Record<string, number> = {};
    _sigData.categories.forEach((cat) => { cw[cat] = 50; });
    return cw;
  });

  // Name input only applies when there's exactly one permutation. With
  // multi-select, each entry's name is auto-generated from its tuple.
  const [name, setName] = useState('');
  const [nameTouched, setNameTouched] = useState(false);

  const [saving, setSaving] = useState(false);
  const [savingProgress, setSavingProgress] = useState<{ done: number; total: number } | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Reflect universe-list errors into the form's error banner.
  useEffect(() => {
    if (_utError) setError(`Failed to load universes: ${_utError}`);
  }, [_utError]);

  // Default-select all universes when the data lands. Skipped if the
  // user has already touched the selection (non-empty + not equal to
  // the previous default).
  useEffect(() => {
    if (!_utData) return;
    setSelectedUniverses((prev) => (prev.length === 0 ? _utData.map((u) => u.template_key) : prev));
  }, [_utData]);

  // Populate signal/category defaults when the signals fetch lands.
  useEffect(() => {
    if (!_sigData) return;
    setSignalDefs(_sigData.signals);
    setWeights((prev) => {
      if (Object.keys(prev).length > 0) return prev;
      const w: Record<string, number> = {};
      _sigData.signals.forEach((s) => (w[s.key] = s.default_weight));
      return w;
    });
    setCategories(_sigData.categories);
    setCategoryWeights((prev) => {
      if (Object.keys(prev).length > 0) return prev;
      const cw: Record<string, number> = {};
      _sigData.categories.forEach((c) => (cw[c] = 50));
      return cw;
    });
  }, [_sigData]);

  // ── Permutation math ────────────────────────────────────────────
  const permutations = useMemo(() => {
    const out: Array<{ universe: string; mode: SelectionMode; frequency: string }> = [];
    for (const u of selectedUniverses) {
      for (const m of selectedModes) {
        for (const f of selectedFrequencies) {
          out.push({ universe: u, mode: m, frequency: f });
        }
      }
    }
    return out;
  }, [selectedUniverses, selectedModes, selectedFrequencies]);

  const permutationCount = permutations.length;
  const isSinglePermutation = permutationCount === 1;

  // What strategy modes are in the active selection? Some param
  // sections only apply to certain modes (min price score → momentum,
  // top-N pair → not 'all'). With multi-select we render a param if
  // ANY chosen strategy needs it.
  const needsTopNPair = selectedModes.some((m) => m !== 'all');
  const needsMinPriceScore = selectedModes.includes('momentum');
  const needsSignalWeights = selectedModes.includes('momentum');

  // ── Name derivation ─────────────────────────────────────────────
  /** Compose a name for a single permutation. */
  const nameForPermutation = useCallback(
    (perm: { universe: string; mode: SelectionMode; frequency: string }): string => {
      const parts: string[] = [
        perm.universe,
        STRATEGY_LABELS[perm.mode],
        FREQUENCY_LABELS[perm.frequency] ?? perm.frequency,
      ];
      if (perm.mode !== 'all') {
        parts.push(`${topNSectors}×${topNPerSector}`);
      }
      if (perm.mode === 'momentum' && minPriceScore.trim() !== '') {
        parts.push(`price≥${minPriceScore.trim()}`);
      }
      return parts.join(' · ');
    },
    [topNSectors, topNPerSector, minPriceScore],
  );

  const suggestedName = useMemo(() => {
    if (!isSinglePermutation) return '';
    return nameForPermutation(permutations[0]);
  }, [isSinglePermutation, permutations, nameForPermutation]);

  useEffect(() => {
    if (!isSinglePermutation) {
      // Multi-select: reset the touched flag so a future drop back to
      // single-select re-enables auto-fill.
      setNameTouched(false);
      setName('');
      return;
    }
    if (nameTouched) return;
    setName(suggestedName);
  }, [isSinglePermutation, suggestedName, nameTouched]);

  const groupedSignals = useMemo(() => {
    const groups: Record<string, SignalDef[]> = {};
    for (const s of signalDefs) {
      const g = s.group ?? 'price';
      if (!groups[g]) groups[g] = [];
      groups[g].push(s);
    }
    return groups;
  }, [signalDefs]);

  // ── Submit ──────────────────────────────────────────────────────
  const buildConfig = useCallback(
    (universe: string, mode: SelectionMode): Record<string, unknown> => {
      const config: Record<string, unknown> = {
        selection_mode: mode,
        index_universe: universe,
        universe_label: null,
        max_companies: maxCompanies,
        strategy_type: 'long_only',
        rebalance_frequency: 'monthly',
      };
      if (mode !== 'all') {
        config.top_n_sectors = topNSectors;
        config.top_n_per_sector = topNPerSector;
      }
      if (mode === 'momentum') {
        const ps = minPriceScore.trim();
        config.min_price_score = ps === '' ? null : Number(ps);
        config.signal_weights = weights;
        config.category_weights = categoryWeights;
      }
      return config;
    },
    [topNSectors, topNPerSector, minPriceScore, maxCompanies, weights, categoryWeights],
  );

  const handleSubmit = useCallback(async () => {
    setError(null);
    if (permutationCount === 0) {
      setError('Pick at least one universe, strategy, and frequency.');
      return;
    }
    if (isSinglePermutation && !name.trim()) {
      setError('Name is required (or clear the field to use the auto-suggestion).');
      return;
    }

    setSaving(true);
    setSavingProgress({ done: 0, total: permutationCount });
    let failedAt: { perm: typeof permutations[number]; err: string } | null = null;
    try {
      for (let i = 0; i < permutations.length; i++) {
        const perm = permutations[i];
        const entryName = isSinglePermutation
          ? name.trim()
          : nameForPermutation(perm);
        const config = buildConfig(perm.universe, perm.mode);
        try {
          const r = await apiFetch(`${API_URL}/api/scheduled-strategies`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ name: entryName, frequency: perm.frequency, config }),
          });
          if (!r.ok) {
            const body = await r.text().catch(() => '');
            failedAt = { perm, err: `${r.status} ${body.slice(0, 240)}` };
            break;
          }
        } catch (e) {
          failedAt = { perm, err: e instanceof Error ? e.message : String(e) };
          break;
        }
        setSavingProgress({ done: i + 1, total: permutationCount });
      }
      if (failedAt) {
        setError(
          `Added ${savingProgress?.done ?? 0} of ${permutationCount}, then failed on ` +
          `${nameForPermutation(failedAt.perm)}: ${failedAt.err}`,
        );
        // Refresh the parent list so it shows the entries that DID land.
        await onAdded();
        return;
      }
      await onAdded();
    } finally {
      setSaving(false);
      setSavingProgress(null);
    }
  }, [permutations, permutationCount, isSinglePermutation, name, nameForPermutation, buildConfig, onAdded, savingProgress?.done]);

  const toggle = (list: string[], setList: (xs: string[]) => void, value: string, on: boolean) => {
    if (on) {
      if (!list.includes(value)) setList([...list, value]);
    } else {
      setList(list.filter((v) => v !== value));
    }
  };

  return (
    <div className="px-5 py-4 border-b border-neutral-800/40 bg-page space-y-5">
      <div className="text-[10px] uppercase tracking-wider text-fg-subtle">
        New scheduled strategy
        <span className="ml-2 normal-case tracking-normal text-fg-faint">
          (multi-select to create one entry per universe × strategy × frequency permutation)
        </span>
      </div>

      {/* Identity + scheduling */}
      <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
        <div>
          <label className="text-fg-subtle text-xs block mb-1">
            Universe{selectedUniverses.length === 1 ? '' : 's'}
            <span className="text-fg-faint ml-1">({selectedUniverses.length})</span>
          </label>
          <div className="bg-card border border-neutral-700 rounded-lg px-3 py-2 space-y-1 max-h-40 overflow-y-auto">
            {universesLoading ? (
              <div className="text-xs text-fg-subtle"><LoadingDots label="Loading" /></div>
            ) : universes.length === 0 ? (
              <div className="text-xs text-fg-muted">
                No template universes refreshed yet.
              </div>
            ) : universes.map((u) => {
              const checked = selectedUniverses.includes(u.template_key);
              return (
                <label key={u.template_key} className="flex items-center gap-2 text-xs cursor-pointer">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => toggle(selectedUniverses, setSelectedUniverses, u.template_key, e.target.checked)}
                    className="accent-accent-500"
                  />
                  <span className={checked ? 'text-fg' : 'text-fg-muted'}>{u.label}</span>
                  <span className="text-fg-faint font-mono text-[10px]">
                    {u.earliest_date.slice(0, 7)} – {u.latest_captured_month?.slice(0, 7)}
                  </span>
                </label>
              );
            })}
          </div>
        </div>
        <div>
          <label className="text-fg-subtle text-xs block mb-1">
            Strateg{selectedModes.length === 1 ? 'y' : 'ies'}
            <span className="text-fg-faint ml-1">({selectedModes.length})</span>
          </label>
          <div className="bg-card border border-neutral-700 rounded-lg px-3 py-2 space-y-1">
            {SELECTION_MODES.map((m) => {
              const checked = selectedModes.includes(m.value);
              return (
                <label key={m.value} className="flex items-center gap-2 text-xs cursor-pointer">
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => toggle(selectedModes, setSelectedModes as (xs: string[]) => void, m.value, e.target.checked)}
                    className="accent-accent-500"
                  />
                  <span className={checked ? 'text-fg' : 'text-fg-muted'}>{m.label}</span>
                </label>
              );
            })}
          </div>
        </div>
        <div>
          <label className="text-fg-subtle text-xs block mb-1">
            Frequenc{selectedFrequencies.length === 1 ? 'y' : 'ies'}
            <span className="text-fg-faint ml-1">({selectedFrequencies.length})</span>
          </label>
          <div className="bg-card border border-neutral-700 rounded-lg px-3 py-2 space-y-1">
            {FREQUENCIES.map((f) => {
              const checked = selectedFrequencies.includes(f.value);
              return (
                <label key={f.value} className="flex items-center gap-2 text-xs cursor-pointer" title={f.description}>
                  <input
                    type="checkbox"
                    checked={checked}
                    onChange={(e) => toggle(selectedFrequencies, setSelectedFrequencies, f.value, e.target.checked)}
                    className="accent-accent-500"
                  />
                  <span className={checked ? 'text-fg' : 'text-fg-muted'}>{f.label}</span>
                </label>
              );
            })}
          </div>
        </div>
        <div>
          <label className="text-fg-subtle text-xs block mb-1">
            Name
            {!isSinglePermutation && (
              <span className="text-fg-faint ml-1">(auto-named per entry)</span>
            )}
          </label>
          <input
            type="text"
            value={isSinglePermutation ? name : ''}
            onChange={(e) => {
              const next = e.target.value;
              setName(next);
              setNameTouched(next.trim() !== '');
            }}
            placeholder={isSinglePermutation ? (suggestedName || 'e.g. ACWI weekly momentum') : 'auto-generated per permutation'}
            disabled={!isSinglePermutation}
            className="w-full bg-card border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none disabled:opacity-50 disabled:cursor-not-allowed"
          />
          {isSinglePermutation && nameTouched && suggestedName && (
            <button
              type="button"
              onClick={() => { setNameTouched(false); setName(suggestedName); }}
              className="mt-1 text-[10px] text-accent-400 hover:text-accent-300 transition-colors"
              title="Reset to the auto-generated name"
            >
              ↻ reset to suggestion
            </button>
          )}
        </div>
      </div>

      {/* Strategy parameters — conditional per the union of selected modes. */}
      <div className="pt-3 border-t border-neutral-800/40">
        <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-3">
          Strategy parameters
          <span className="text-fg-faint normal-case tracking-normal ml-2">
            (shared across every permutation)
          </span>
        </div>

        {needsTopNPair && (
          <div className="flex flex-wrap items-end gap-6 mb-5">
            <div>
              <label className="text-fg-subtle text-xs block mb-1">Top Sectors</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topNSectors}
                onChange={(e) => setTopNSectors(Number(e.target.value))}
                className="w-16 bg-card border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono text-center focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-fg-subtle text-xs block mb-1">Per Sector</label>
              <input
                type="number"
                min={1}
                max={20}
                value={topNPerSector}
                onChange={(e) => setTopNPerSector(Number(e.target.value))}
                className="w-16 bg-card border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono text-center focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
              />
            </div>
            <div>
              <label className="text-fg-subtle text-xs block mb-1">Max Companies</label>
              <input
                type="number"
                min={0}
                max={500}
                value={maxCompanies}
                onChange={(e) => setMaxCompanies(Number(e.target.value))}
                className="w-20 bg-card border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono text-center focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                title="0 = no cap"
              />
              <span className="text-fg-faint text-xs ml-1">0 = all</span>
            </div>
            {needsMinPriceScore && (
              <div>
                <label className="text-fg-subtle text-xs block mb-1">Min Price Score</label>
                <input
                  type="number"
                  min={0}
                  max={100}
                  step={1}
                  placeholder="off"
                  value={minPriceScore}
                  onChange={(e) => setMinPriceScore(e.target.value)}
                  className="w-20 bg-card border border-neutral-700 rounded-lg px-3 py-2 text-fg-strong text-sm font-mono text-center focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
                  title="Optional 0-100 floor on each candidate's price-category score. Empty = no filter. (Applies only to momentum permutations.)"
                />
                <span className="text-fg-faint text-xs ml-1">{minPriceScore.trim() === '' ? 'off' : '>'}</span>
              </div>
            )}
          </div>
        )}

        {needsSignalWeights && signalDefs.length > 0 && (
          <div className="space-y-4">
            {(['price', 'volume'] as const).map((group) => {
              const groupSignals = groupedSignals[group] || [];
              if (groupSignals.length === 0) return null;
              return (
                <div key={group}>
                  <h3 className="text-fg-muted text-xs font-medium mb-2.5 uppercase tracking-wider">
                    {group === 'price' ? 'Price Momentum' : 'Volume Confirmation'}
                  </h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-2.5">
                    {groupSignals.map((s) => (
                      <div key={s.key} className="flex items-center gap-3">
                        <span className="text-fg-soft text-xs font-medium w-36 shrink-0 truncate" title={s.description}>
                          {s.label}
                        </span>
                        <input
                          type="range"
                          min={0}
                          max={10}
                          step={1}
                          value={weights[s.key] ?? 0}
                          onChange={(e) => setWeights((prev) => ({ ...prev, [s.key]: Number(e.target.value) }))}
                          className="flex-1 h-1 accent-accent-500 cursor-pointer"
                        />
                        <span className="text-fg-subtle text-xs w-5 text-right font-mono shrink-0">{weights[s.key] ?? 0}</span>
                      </div>
                    ))}
                  </div>
                </div>
              );
            })}
            {categories.length > 1 && (
              <div>
                <h3 className="text-fg-muted text-xs font-medium mb-2.5 uppercase tracking-wider">Category Weights</h3>
                <div className="flex items-center gap-6 flex-wrap">
                  {categories.map((cat) => (
                    <div key={cat} className="flex items-center gap-2">
                      <span className="text-fg-soft text-xs font-medium w-28">
                        {cat === 'price' ? 'Price Momentum' : cat === 'volume' ? 'Volume Confirmation' : cat}
                      </span>
                      <input
                        type="range"
                        min={0}
                        max={100}
                        step={5}
                        value={categoryWeights[cat] ?? 50}
                        onChange={(e) => setCategoryWeights((prev) => ({ ...prev, [cat]: Number(e.target.value) }))}
                        className="w-32 h-1 accent-accent-500 cursor-pointer"
                      />
                      <span className="text-fg-subtle text-xs w-8 text-right font-mono">{categoryWeights[cat] ?? 50}%</span>
                    </div>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Permutation preview */}
      {permutationCount > 1 && (
        <div className="pt-3 border-t border-neutral-800/40">
          <div className="text-[10px] uppercase tracking-wider text-fg-subtle mb-2">
            Will create {permutationCount} entries
          </div>
          <div className="bg-card border border-neutral-800/40 rounded-lg px-3 py-2 max-h-40 overflow-y-auto space-y-0.5">
            {permutations.map((p, i) => (
              <div key={i} className="text-[11px] text-fg-soft font-mono">
                {nameForPermutation(p)}
              </div>
            ))}
          </div>
        </div>
      )}

      {error && <div className="text-xs text-neg-300">{error}</div>}

      <div className="flex items-center gap-3 pt-2">
        <button
          type="button"
          onClick={() => void handleSubmit()}
          disabled={saving || permutationCount === 0}
          className="text-xs px-3 py-1.5 rounded-lg bg-accent-600 hover:bg-accent-500 disabled:opacity-50 disabled:cursor-not-allowed text-fg-strong transition-colors"
        >
          {saving
            ? (savingProgress
                ? `Adding ${savingProgress.done + 1} of ${savingProgress.total}…`
                : 'Adding…')
            : permutationCount > 1
              ? `Add ${permutationCount} to schedule`
              : 'Add to schedule'}
        </button>
        <button
          type="button"
          onClick={onCancel}
          disabled={saving}
          className="text-xs px-3 py-1.5 rounded-lg text-fg-muted hover:bg-overlay/5 transition-colors"
        >
          Cancel
        </button>
      </div>
    </div>
  );
}
