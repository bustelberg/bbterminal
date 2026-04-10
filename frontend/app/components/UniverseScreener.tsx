'use client';

import { useState, useRef, useEffect, useCallback } from 'react';

const API_URL = process.env.NEXT_PUBLIC_API_URL ?? 'http://localhost:8000';

type CriterionDef = { key: string; label: string; description?: string; min_years?: number };
type CompanyResult = {
  company_id: number;
  ticker: string;
  exchange: string;
  company_name: string;
  sector: string;
  country: string;
  scores: Record<string, number>;
  details: Record<string, string>;
  total_score: number;
  passes: boolean;
};
type MonthEntry = { month: string; total: number; passing: number };
type LabelEntry = {
  label: string;
  start_month: string;
  end_month: string;
  month_count: number;
  avg_passing: number;
};

export default function UniverseScreener() {
  const [criteria, setCriteria] = useState<CriterionDef[]>([]);
  const [universeLabel, setUniverseLabel] = useState('default');
  const [startMonth, setStartMonth] = useState('2016-01');
  const [endMonth, setEndMonth] = useState('2026-04');
  const [running, setRunning] = useState(false);
  const [logs, setLogs] = useState<string[]>([]);
  const [labels, setLabels] = useState<LabelEntry[]>([]);
  const [selectedLabel, setSelectedLabel] = useState<string | null>(null);
  const [storedMonths, setStoredMonths] = useState<MonthEntry[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
  const [monthResults, setMonthResults] = useState<CompanyResult[]>([]);
  const [loadingMonth, setLoadingMonth] = useState(false);
  const [sortKey, setSortKey] = useState<string>('total_score');
  const [sortAsc, setSortAsc] = useState(false);
  const [filterPass, setFilterPass] = useState<'all' | 'pass' | 'fail'>('all');
  const logEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    fetch(`${API_URL}/api/universe/criteria`)
      .then(r => r.json())
      .then(setCriteria)
      .catch(() => {});
    loadLabels();
  }, []);

  useEffect(() => {
    if (logEndRef.current) {
      const el = logEndRef.current;
      el.parentElement!.scrollTop = el.parentElement!.scrollHeight;
    }
  }, [logs]);

  const loadLabels = () => {
    fetch(`${API_URL}/api/universe/labels`)
      .then(r => r.json())
      .then((data: LabelEntry[]) => {
        setLabels(data);
        // Auto-select first label if none selected
        if (data.length > 0 && !selectedLabel) {
          selectLabel(data[0].label);
        }
      })
      .catch(() => {});
  };

  const loadMonthsForLabel = (label: string) => {
    fetch(`${API_URL}/api/universe/months?label=${encodeURIComponent(label)}`)
      .then(r => r.json())
      .then(setStoredMonths)
      .catch(() => {});
  };

  const selectLabel = (label: string) => {
    setSelectedLabel(label);
    setSelectedMonth(null);
    setMonthResults([]);
    loadMonthsForLabel(label);
  };

  const runBuild = useCallback(() => {
    if (!universeLabel.trim()) return;
    setRunning(true);
    setLogs([]);

    fetch(`${API_URL}/api/universe/build`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ start_month: startMonth, end_month: endMonth, label: universeLabel.trim() }),
    }).then(res => {
      const reader = res.body!.getReader();
      const decoder = new TextDecoder();
      let buffer = '';

      function read() {
        reader.read().then(({ done, value }) => {
          if (done) { setRunning(false); loadLabels(); return; }
          buffer += decoder.decode(value, { stream: true });
          const lines = buffer.split('\n');
          buffer = lines.pop() || '';
          for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            try {
              const evt = JSON.parse(line.slice(6));
              if (evt.type === 'progress') {
                setLogs(prev => [...prev, evt.message]);
              } else if (evt.type === 'progress_update') {
                setLogs(prev => {
                  const copy = [...prev];
                  if (copy.length > 0) copy[copy.length - 1] = evt.message;
                  else copy.push(evt.message);
                  return copy;
                });
              } else if (evt.type === 'done') {
                setLogs(prev => [...prev, evt.message]);
                setRunning(false);
                const builtLabel = universeLabel.trim();
                loadLabels();
                setSelectedLabel(builtLabel);
                loadMonthsForLabel(builtLabel);
              }
            } catch {}
          }
          read();
        });
      }
      read();
    }).catch(() => { setRunning(false); });
  }, [startMonth, endMonth, universeLabel]);

  const selectMonth = (month: string) => {
    if (!selectedLabel) return;
    setSelectedMonth(month);
    setLoadingMonth(true);
    setMonthResults([]);
    setFilterPass('all');
    fetch(`${API_URL}/api/universe/months/${month}?label=${encodeURIComponent(selectedLabel)}`)
      .then(r => r.json())
      .then(data => { setMonthResults(data); setLoadingMonth(false); })
      .catch(() => setLoadingMonth(false));
  };

  const deleteMonth = (month: string, e: React.MouseEvent) => {
    e.stopPropagation();
    if (!selectedLabel) return;
    fetch(`${API_URL}/api/universe/months/${month}?label=${encodeURIComponent(selectedLabel)}`, { method: 'DELETE' })
      .then(() => {
        loadMonthsForLabel(selectedLabel);
        if (selectedMonth === month) { setSelectedMonth(null); setMonthResults([]); }
      });
  };

  const deleteLabel = (label: string) => {
    if (!confirm(`Delete universe "${label}" and all its months?`)) return;
    fetch(`${API_URL}/api/universe/labels/${encodeURIComponent(label)}`, { method: 'DELETE' })
      .then(() => {
        if (selectedLabel === label) {
          setSelectedLabel(null);
          setStoredMonths([]);
          setSelectedMonth(null);
          setMonthResults([]);
        }
        loadLabels();
      });
  };

  const handleSort = (key: string) => {
    if (sortKey === key) setSortAsc(!sortAsc);
    else { setSortKey(key); setSortAsc(false); }
  };

  const filtered = monthResults.filter(r =>
    filterPass === 'all' ? true : filterPass === 'pass' ? r.passes : !r.passes
  );

  const sorted = [...filtered].sort((a, b) => {
    let va: number | string = 0, vb: number | string = 0;
    if (sortKey === 'total_score') { va = a.total_score; vb = b.total_score; }
    else if (sortKey === 'ticker') { va = a.ticker; vb = b.ticker; }
    else if (sortKey === 'company_name') { va = a.company_name; vb = b.company_name; }
    else if (sortKey === 'sector') { va = a.sector || ''; vb = b.sector || ''; }
    else { va = a.scores[sortKey] ?? -1; vb = b.scores[sortKey] ?? -1; }
    if (va < vb) return sortAsc ? -1 : 1;
    if (va > vb) return sortAsc ? 1 : -1;
    return 0;
  });

  const SortHeader = ({ k, label, className, title }: { k: string; label: string; className?: string; title?: string }) => (
    <th
      className={`px-3 py-2.5 text-left text-xs font-medium text-gray-400 cursor-pointer hover:text-white select-none ${className || ''}`}
      onClick={() => handleSort(k)}
      title={title}
    >
      {label} {sortKey === k ? (sortAsc ? '\u25b2' : '\u25bc') : ''}
    </th>
  );

  const passingCount = monthResults.filter(r => r.passes).length;
  const failingCount = monthResults.filter(r => !r.passes).length;

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="shrink-0 px-8 py-5 border-b border-gray-800/40">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-white text-lg font-semibold">Universe Screener</h1>
            <p className="text-gray-500 text-sm mt-0.5">
              Build labeled monthly universes from LongEquity quality criteria
            </p>
          </div>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-2">
              <label className="text-gray-400 text-xs">Label</label>
              <input
                type="text"
                value={universeLabel}
                onChange={e => setUniverseLabel(e.target.value)}
                placeholder="e.g. longequity-v1"
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-white focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none w-40"
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-gray-400 text-xs">From</label>
              <input
                type="month"
                value={startMonth}
                onChange={e => setStartMonth(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <div className="flex items-center gap-2">
              <label className="text-gray-400 text-xs">To</label>
              <input
                type="month"
                value={endMonth}
                onChange={e => setEndMonth(e.target.value)}
                className="bg-[#0f1117] border border-gray-700 rounded-lg px-3 py-1.5 text-sm text-white font-mono focus:border-indigo-500 focus:ring-1 focus:ring-indigo-500/30 outline-none"
              />
            </div>
            <button
              onClick={runBuild}
              disabled={running || !universeLabel.trim()}
              className="px-4 py-2 rounded-lg text-sm font-medium bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 text-white transition-colors"
            >
              {running ? 'Building...' : 'Build Universes'}
            </button>
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto px-8 py-5 space-y-5">
        {/* Criteria Reference */}
        {criteria.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 px-5 py-4">
            <h2 className="text-white text-sm font-medium mb-3">{"Quality Criteria (score 1 point each, need >= 1 to qualify)"}</h2>
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
        )}

        {/* Log Panel */}
        {logs.length > 0 && (
          <div className="bg-[#0b0d13] border border-gray-800/40 rounded-lg overflow-hidden">
            <div className="px-3 py-1.5 border-b border-gray-800/40 flex items-center gap-2">
              {running
                ? <div className="w-1.5 h-1.5 rounded-full bg-indigo-400 animate-pulse" />
                : <div className="w-1.5 h-1.5 rounded-full bg-emerald-400" />}
              <span className="text-gray-500 text-xs font-medium">{running ? 'Building Universes' : 'Build Complete'}</span>
              {!running && (
                <button onClick={() => setLogs([])} className="ml-auto text-gray-500 hover:text-gray-300 transition-colors" aria-label="Close">
                  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 20 20" fill="currentColor" className="w-3.5 h-3.5">
                    <path d="M6.28 5.22a.75.75 0 00-1.06 1.06L8.94 10l-3.72 3.72a.75.75 0 101.06 1.06L10 11.06l3.72 3.72a.75.75 0 101.06-1.06L11.06 10l3.72-3.72a.75.75 0 00-1.06-1.06L10 8.94 6.28 5.22z" />
                  </svg>
                </button>
              )}
            </div>
            <div className="max-h-48 overflow-y-auto p-3 font-mono text-xs">
              {logs.map((l, i) => (
                <div key={i} className="text-gray-400">{l}</div>
              ))}
              <div ref={logEndRef} />
            </div>
          </div>
        )}

        {/* Stored Labels */}
        {labels.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
            <div className="px-5 py-3 border-b border-gray-800/40">
              <h2 className="text-white text-sm font-medium">Stored Universes</h2>
            </div>
            <div className="divide-y divide-gray-800/20">
              {labels.map(l => (
                <div
                  key={l.label}
                  onClick={() => selectLabel(l.label)}
                  className={`px-5 py-3 flex items-center justify-between cursor-pointer transition-colors ${
                    selectedLabel === l.label ? 'bg-indigo-600/10' : 'hover:bg-white/[0.02]'
                  }`}
                >
                  <div className="flex items-center gap-4">
                    <span className={`text-sm font-medium ${selectedLabel === l.label ? 'text-indigo-400' : 'text-white'}`}>
                      {l.label}
                    </span>
                    <span className="text-xs text-gray-500 font-mono">
                      {l.start_month} to {l.end_month}
                    </span>
                    <span className="text-xs text-gray-500">
                      {l.month_count} months
                    </span>
                    <span className="text-xs text-emerald-400 font-mono">
                      ~{l.avg_passing} passing/mo
                    </span>
                  </div>
                  <button
                    onClick={e => { e.stopPropagation(); deleteLabel(l.label); }}
                    className="text-gray-600 hover:text-rose-400 transition-colors text-xs"
                  >
                    Delete
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Months Grid for Selected Label */}
        {selectedLabel && storedMonths.length > 0 && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
            <div className="px-5 py-3 border-b border-gray-800/40">
              <h2 className="text-white text-sm font-medium">
                {selectedLabel} — {storedMonths.length} months
              </h2>
            </div>
            <div className="grid grid-cols-6 sm:grid-cols-8 lg:grid-cols-12 gap-1 p-3">
              {storedMonths.map(m => (
                <button
                  key={m.month}
                  onClick={() => selectMonth(m.month)}
                  className={`group relative px-2 py-2 rounded-lg text-xs font-mono transition-colors ${
                    selectedMonth === m.month
                      ? 'bg-indigo-600 text-white'
                      : 'bg-[#0f1117] text-gray-300 hover:bg-white/5'
                  }`}
                >
                  <div>{m.month}</div>
                  <div className={`text-[10px] ${selectedMonth === m.month ? 'text-indigo-200' : 'text-emerald-400'}`}>
                    {m.passing}/{m.total}
                  </div>
                  <button
                    onClick={e => deleteMonth(m.month, e)}
                    className="absolute -top-1 -right-1 w-4 h-4 rounded-full bg-rose-500/80 text-white text-[10px] leading-none flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-rose-500"
                  >
                    x
                  </button>
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Month Detail */}
        {selectedMonth && (
          <div className="bg-[#151821] rounded-xl border border-gray-800/40 overflow-hidden">
            <div className="px-5 py-3 border-b border-gray-800/40 flex items-center justify-between">
              <div className="flex items-center gap-4">
                <h2 className="text-white text-sm font-medium">
                  {selectedMonth}
                </h2>
                {monthResults.length > 0 && (
                  <div className="flex items-center gap-3 text-xs">
                    <span className="text-emerald-400 font-mono">{passingCount} pass</span>
                    <span className="text-gray-500">|</span>
                    <span className="text-rose-400 font-mono">{failingCount} fail</span>
                    <span className="text-gray-500">|</span>
                    <span className="text-gray-400 font-mono">{monthResults.length} total</span>
                  </div>
                )}
              </div>
              <div className="flex items-center gap-2">
                {(['all', 'pass', 'fail'] as const).map(f => (
                  <button
                    key={f}
                    onClick={() => setFilterPass(f)}
                    className={`px-3 py-1 rounded text-xs font-medium transition-colors ${
                      filterPass === f
                        ? 'bg-indigo-600 text-white'
                        : 'text-gray-400 hover:text-white hover:bg-white/5'
                    }`}
                  >
                    {f === 'all' ? 'All' : f === 'pass' ? 'Passing' : 'Failing'}
                  </button>
                ))}
              </div>
            </div>
            {loadingMonth ? (
              <div className="p-8 text-center text-gray-500 text-sm">Loading...</div>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b border-gray-800/40">
                      <SortHeader k="ticker" label="Ticker" />
                      <SortHeader k="company_name" label="Company" />
                      <SortHeader k="sector" label="Sector" />
                      <SortHeader k="total_score" label="Score" />
                      {criteria.map(c => (
                        <SortHeader key={c.key} k={c.key} label={c.label.split(' ')[0]} className="text-center" title={c.description} />
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {sorted.map(r => (
                      <tr key={r.company_id} className="border-b border-gray-800/20 hover:bg-white/[0.02]">
                        <td className="px-3 py-2.5 text-white font-mono text-xs">{r.ticker}</td>
                        <td className="px-3 py-2.5 text-gray-300 text-xs max-w-[200px] truncate">{r.company_name}</td>
                        <td className="px-3 py-2.5 text-gray-400 text-xs">{r.sector}</td>
                        <td className="px-3 py-2.5 font-mono">
                          <span className={`font-semibold ${r.total_score >= 4 ? 'text-emerald-400' : r.total_score >= 1 ? 'text-amber-400' : 'text-rose-400'}`}>
                            {r.total_score}/7
                          </span>
                        </td>
                        {criteria.map(c => {
                          const score = r.scores[c.key];
                          const detail = r.details?.[c.key] || '';
                          return (
                            <td key={c.key} className="px-3 py-2.5 text-center" title={detail}>
                              {score === undefined ? (
                                <span className="text-gray-600">-</span>
                              ) : score === 1 ? (
                                <span className="text-emerald-400">Y</span>
                              ) : (
                                <span className="text-gray-600">N</span>
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function InfoTip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  const [pos, setPos] = useState<{ top: number; left: number }>({ top: 0, left: 0 });
  const iconRef = useRef<HTMLSpanElement>(null);

  const tipWidth = 224;
  const margin = 8;

  const handleEnter = () => {
    if (iconRef.current) {
      const rect = iconRef.current.getBoundingClientRect();
      const centerX = rect.left + rect.width / 2;
      const clampedLeft = Math.max(margin + tipWidth / 2, Math.min(centerX, window.innerWidth - margin - tipWidth / 2));
      setPos({ top: rect.bottom + 8, left: clampedLeft });
    }
    setShow(true);
  };

  return (
    <span className="relative cursor-help" onMouseEnter={handleEnter} onMouseLeave={() => setShow(false)}>
      <span ref={iconRef} className="inline-flex items-center justify-center w-4 h-4 rounded-full border border-gray-600 text-gray-500 text-[10px] leading-none hover:border-indigo-400 hover:text-indigo-400 transition-colors">i</span>
      {show && (
        <span
          className="fixed w-56 px-3 py-2 bg-[#1e2130] border border-gray-700 rounded-lg text-xs text-gray-300 leading-relaxed z-[9999] shadow-xl pointer-events-none"
          style={{ top: pos.top, left: pos.left, transform: 'translate(-50%, 0)' }}
        >
          {text}
        </span>
      )}
    </span>
  );
}
