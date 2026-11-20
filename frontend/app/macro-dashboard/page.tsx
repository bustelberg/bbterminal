'use client';

import { useEffect, useRef, useState } from 'react';
import { CartesianGrid, Line, LineChart, ReferenceLine, ResponsiveContainer, Tooltip, XAxis, YAxis } from 'recharts';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { components } from '../../lib/api-types';

type Indicator = components['schemas']['Indicator'];
type Snapshot = components['schemas']['Snapshot'];
type Reading = components['schemas']['OverviewReading'];

const sections = [
  { title: 'Growth & jobs', question: 'Is growth momentum improving or is the labor market weakening?',
    keys: ['oecd-cli', 'europe-oecd-cli', 'us-unemployment'] },
  { title: 'Inflation', question: 'Is underlying inflation easing or picking up?',
    keys: ['core-cpi', 'ea-core-hicp'] },
  { title: 'Rates & stress', question: 'Are financing costs and financial stress rising?',
    keys: ['us-10y', 'de-10y', 'high-yield', 'nfci', 'vix'] },
  { title: 'FX & valuation', question: 'How is the euro moving, and how expensive are U.S. equities?',
    keys: ['eur-usd', 'buffett'] },
];

const chartGuide: Record<string, string> = {
  'oecd-cli': 'This index flags turning points in U.S. growth, so a fall from 101 to 100.5 suggests momentum is cooling while the reading is still above its trend reference of 100.',
  'europe-oecd-cli': 'This index tracks growth momentum across France, Germany, Italy, and the UK, so a rise from 99 to 99.5 suggests momentum is improving while still below the trend reference of 100.',
  'us-unemployment': 'This is the share of the U.S. labor force that is unemployed, so a move from 4.1% to 4.4% is a rise of 0.30 percentage points.',
  'core-cpi': 'This measures the annual change in U.S. consumer prices excluding food and energy, so 2.50% means those prices are 2.50% higher than in the same month last year.',
  'ea-core-hicp': 'This measures the annual change in euro-area consumer prices excluding energy, food, alcohol, and tobacco, so a fall from 2.40% to 2.20% means prices are still rising year over year, just more slowly.',
  'us-10y': 'This is the annualized market yield on U.S. government debt at a 10-year maturity, so a rise from 4.50% to 4.75% means the benchmark yield increased by 25 basis points.',
  'de-10y': 'This chart shows the monthly average yield on German 10-year government debt, so a move from 2.50% to 2.75% is an increase of 25 basis points.',
  'high-yield': 'This shows the extra yield investors demand for below-investment-grade U.S. corporate bonds over comparable Treasuries, so 350 bp means a 3.50 percentage point premium.',
  nfci: 'This index measures U.S. financial conditions relative to their historical average of zero, so a rise from −0.60 to −0.48 means conditions tightened while remaining looser than average.',
  vix: 'This index reflects expected S&P 500 volatility over the next 30 days on an annualized scale, so a rise from 16 to 25 means options are pricing in larger swings.',
  buffett: 'This ratio compares U.S. public equity market value with annualized nominal GDP, so a reading of 2.50× means the market is valued at two and a half times annual output.',
  'eur-usd': 'This is the number of U.S. dollars one euro buys, so a rise from 1.08 to 1.12 means the euro strengthened against the dollar.',
};

const chartReferences: Record<string, { value: number; label: string }> = {
  'oecd-cli': { value: 100, label: 'Long-term trend (100)' },
  'europe-oecd-cli': { value: 100, label: 'Long-term trend (100)' },
  nfci: { value: 0, label: 'Historical average (0)' },
};

function readingUnit(key: string) {
  if (['us-unemployment', 'core-cpi', 'ea-core-hicp', 'us-10y', 'de-10y'].includes(key)) return '%';
  if (key === 'high-yield') return ' bp';
  if (key === 'buffett') return '×';
  return '';
}

function readingChange(key: string, latest: number, previous: number) {
  const delta = latest - previous;
  const unit = readingUnit(key);
  if (key === 'us-10y' || key === 'de-10y') return { value: delta * 100, unit: ' bp' };
  return { value: delta, unit: unit === '%' ? ' pp' : unit || (key === 'eur-usd' ? '' : ' pts') };
}
const ranges = ['All', '10Y', '5Y', '1Y', 'YTD'] as const;
type Range = typeof ranges[number];
type RowState = { busy?: boolean; snapshot?: Snapshot; error?: string; range?: Range };
const endpoint = `${API_URL}/api/macro-dashboard/indicators`;
const format = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const signedFormat = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2, maximumFractionDigits: 2, signDisplay: 'exceptZero',
});
const observedDate = new Intl.DateTimeFormat('en-GB', {
  day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
});
const control = 'rounded-md border border-neutral-800/50 px-2 py-1 text-xs hover:bg-overlay/5 disabled:opacity-50';
const currentYear = new Intl.DateTimeFormat('en', { timeZone: 'Europe/Amsterdam', year: 'numeric' });

function rangeStart(range: Range, latest: string): string {
  if (range === 'All') return '';
  if (range === 'YTD') return `${currentYear.format(new Date())}-01-01`;
  const year = Number(latest.slice(0, 4)) - Number.parseInt(range, 10);
  const monthAndDay = latest.slice(4);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  return `${year}${monthAndDay === '-02-29' && !leapYear ? '-02-28' : monthAndDay}`;
}

async function getJson<T>(url: string, method = 'GET'): Promise<T> {
  const response = await apiFetch(url, { method, cache: 'no-store', noReadCache: true });
  const body = await response.json();
  if (!response.ok) throw new Error(typeof body.detail === 'string' ? body.detail : `HTTP ${response.status}`);
  return body as T;
}

export default function MacroDashboard() {
  const [indicators, setIndicators] = useState<Indicator[] | null>(null);
  const [readings, setReadings] = useState<Reading[]>([]);
  const [overviewError, setOverviewError] = useState('');
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const modalRef = useRef<HTMLDialogElement>(null);
  const [error, setError] = useState('');
  const [states, setStates] = useState<Record<string, RowState>>({});

  useEffect(() => {
    let active = true;
    getJson<Indicator[]>(endpoint).then(rows => {
      if (active) setIndicators(rows);
    }).catch(cause => {
      if (active) setError(cause instanceof Error ? cause.message : String(cause));
    });
    getJson<Reading[]>(`${endpoint}/overview`).then(rows => {
      if (active) setReadings(rows);
    }).catch(() => {
      if (active) setOverviewError('Saved readings could not be loaded. You can still open individual charts.');
    });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    const dialog = modalRef.current;
    if (selectedKey && dialog && !dialog.open) dialog.showModal();
  }, [selectedKey]);

  const update = (key: string, state: RowState) =>
    setStates(previous => ({ ...previous, [key]: { ...previous[key], ...state } }));

  async function load(key: string, fetchFresh: boolean) {
    setSelectedKey(key);
    update(key, { busy: true, error: '' });
    try {
      const snapshot = await getJson<Snapshot>(`${endpoint}/${key}${fetchFresh ? '/fetch' : ''}`, fetchFresh ? 'POST' : 'GET');
      update(key, { snapshot });
      if (fetchFresh) {
        try {
          setReadings(await getJson<Reading[]>(`${endpoint}/overview`));
          setOverviewError('');
        } catch {
          setOverviewError('The series was saved, but the overview could not reload.');
        }
      }
    } catch (cause) {
      update(key, { error: cause instanceof Error ? cause.message : String(cause) });
    } finally {
      update(key, { busy: false });
    }
  }

  const indicator = indicators?.find(row => row.key === selectedKey);
  const reference = indicator ? chartReferences[indicator.key] : undefined;
  const state = selectedKey ? states[selectedKey] ?? {} : {};
  const points = state.snapshot?.points ?? [];
  const latest = points.at(-1);
  const range = state.range ?? 'All';
  const start = latest ? rangeStart(range, latest.date) : '';
  const visiblePoints = points.filter(point => point.date >= start);

  return <main className="mx-auto w-full max-w-5xl space-y-5 p-4 text-fg md:p-8">
    <h1 className="text-2xl font-semibold tracking-tight text-fg-strong">Macro Dashboard</h1>
    {error && <p role="alert" className="text-sm text-neg-300">{error}</p>}
    {!indicators && !error && <p role="status" className="text-sm text-fg-muted">Loading indicators…</p>}
    {overviewError && <p role="alert" className="text-sm text-neg-300">{overviewError}</p>}
    {indicators && sections.map(section => <section key={section.title} className="space-y-2">
      <h2 className="text-base font-semibold text-fg-strong">{section.title}</h2>
      <p className="text-xs text-fg-muted">{section.question}</p>
      <div className="overflow-x-auto rounded-xl border border-neutral-800/50 bg-elevated/40">
        <table className="w-full min-w-[34rem] table-fixed text-sm" aria-label={section.title}>
          <colgroup><col className="w-[38%]" /><col className="w-[19%]" /><col className="w-[14%]" />
            <col className="w-[14%]" /><col className="w-[15%]" /></colgroup>
          <thead className="border-b border-neutral-800/40 bg-elevated text-xs text-fg-muted"><tr>
            <th scope="col" className="px-3 py-2 text-left">Indicator</th>
            <th scope="col" className="px-3 py-2 text-right">1-month change</th>
            <th scope="col" className="px-3 py-2 text-left">1Y trend</th>
            <th scope="col" className="px-3 py-2 text-right">Latest</th>
            <th scope="col" className="px-3 py-2 text-right">Observed</th>
          </tr></thead>
          <tbody className="divide-y divide-neutral-800/30">
            {section.keys.map(key => {
              const item = indicators.find(row => row.key === key);
              if (!item) return null;
              const reading = readings.find(row => row.key === key);
              const change = reading?.comparison
                ? readingChange(key, reading.latest.value, reading.comparison.value) : null;
              return <tr key={key} onClick={() => void load(key, false)}
                className="cursor-pointer hover:bg-overlay/5 focus-within:bg-overlay/5">
                <th scope="row" className="px-3 py-3 text-left font-medium">
                  <button type="button" aria-haspopup="dialog" onClick={event => {
                    event.stopPropagation();
                    void load(key, false);
                  }}
                    className="text-left text-accent-300 hover:underline">{item.name}</button>
                </th>
                <td className={`px-3 py-3 text-right tabular-nums ${!change || Math.abs(change.value) < 0.005
                  ? 'text-fg-muted' : change.value > 0 ? 'text-pos-400' : 'text-neg-400'}`} title={reading?.comparison
                  ? `Compared with ${observedDate.format(new Date(reading.comparison.date)).toLowerCase()}`
                  : item.frequency === 'Quarterly' ? 'Monthly observations are unavailable.' : undefined}>
                  {change ? `${signedFormat.format(change.value)}${change.unit}` : '—'}
                </td>
                <td className="px-3 py-3">
                  {reading && reading.trend.length > 1 ? <div className="h-8 min-w-0" aria-hidden="true">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={reading.trend}>
                        <YAxis hide domain={['dataMin', 'dataMax']} />
                        <Line type="linear" dataKey="value" stroke="#38bdf8" strokeWidth={1.5}
                          dot={false} isAnimationActive={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div> : <span className="text-fg-muted">—</span>}
                </td>
                <td className="px-3 py-3 text-right tabular-nums">
                  {reading ? `${format.format(reading.latest.value)}${readingUnit(key)}` : '—'}
                </td>
                <td className="px-3 py-3 text-right text-xs text-fg-muted tabular-nums">
                  {reading ? observedDate.format(new Date(reading.latest.date)).toLowerCase() : '—'}
                </td>
              </tr>;
            })}
          </tbody>
        </table>
      </div>
    </section>)}
    {indicator && <dialog ref={modalRef} onClose={() => setSelectedKey(null)}
      onClick={event => {
        if (event.target !== event.currentTarget) return;
        const bounds = event.currentTarget.getBoundingClientRect();
        if (event.clientX < bounds.left || event.clientX > bounds.right
          || event.clientY < bounds.top || event.clientY > bounds.bottom) {
          event.currentTarget.close();
        }
      }}
      aria-labelledby="macro-indicator-title"
      className="m-auto max-h-[94dvh] w-[calc(100%_-_1.5rem)] max-w-[96rem] space-y-4 overflow-x-hidden overflow-y-auto rounded-xl border border-neutral-800/50 bg-elevated p-5 text-fg backdrop:bg-black/60 sm:w-[calc(100%_-_3rem)] sm:p-6">
      <header className="flex flex-wrap items-center justify-between gap-3">
        <h2 id="macro-indicator-title" className="text-base font-semibold">{indicator.name}</h2>
        <div className="flex gap-2">
          <button type="button" className={control} disabled={state.busy} onClick={() => void load(indicator.key, true)}>Fetch</button>
          <button type="button" className={control} onClick={() => modalRef.current?.close()}>Close</button>
        </div>
      </header>
      <div className="flex flex-wrap items-baseline gap-3 text-xs text-fg-muted">
        {latest && <span className="font-medium text-fg">{format.format(latest.value)} · {latest.date}</span>}
        {visiblePoints.length > 0 && <span>{visiblePoints[0].date} – {visiblePoints.at(-1)?.date}</span>}
        <span>{indicator.frequency}</span>
        {state.snapshot?.fetched_at && <span title={state.snapshot.fetched_at}>
          Last checked {observedDate.format(new Date(state.snapshot.fetched_at)).toLowerCase()}
        </span>}
        {indicator.sources.map((source, index) => <a key={source} className="underline"
          href={source} target="_blank" rel="noreferrer">{source.endsWith('?format=csvfile')
            ? 'OECD data (CSV)' : index === 0 ? 'Source' : 'GDP source'}</a>)}
      </div>
      <p className="text-sm text-fg-muted">{chartGuide[indicator.key] ?? indicator.method}</p>
      {state.error && <p role="alert" className="text-xs text-neg-300">{state.error}</p>}
      {state.busy && <p role="status" className="text-xs text-fg-muted">Loading series…</p>}
      {!state.busy && !state.error && !latest && <p className="text-xs text-fg-muted">No saved history. Select Fetch to download it.</p>}
      {latest && <div role="group" aria-label="Chart date range" className="flex flex-wrap gap-2">
        {ranges.map(option => <button key={option} type="button"
          aria-pressed={range === option}
          className={`${control} ${range === option ? 'bg-accent-500/15 text-accent-300' : 'text-fg-muted'}`}
          onClick={() => update(indicator.key, { range: option })}>{option}</button>)}
      </div>}
      {latest && !visiblePoints.length && <p className="text-xs text-fg-muted">No observations in this range.</p>}
      {visiblePoints.length > 0 && <div className="h-[clamp(15rem,65dvh,55rem)] min-w-0 overflow-hidden" aria-label={`Historical chart for ${indicator.name}`}>
        <ResponsiveContainer width="100%" height="100%">
          <LineChart data={visiblePoints.map(point => ({ ...point, time: Date.parse(point.date) }))}>
            <CartesianGrid stroke="currentColor" opacity={0.12} vertical={false} />
            <XAxis dataKey="time" type="number" domain={['dataMin', 'dataMax']}
              tickFormatter={time => new Date(time).toISOString().slice(0, range === 'YTD' || range === '1Y' ? 7 : 4)}
              minTickGap={24} fontSize={11} />
            <YAxis domain={['auto', 'auto']} tickFormatter={value => format.format(value)} fontSize={11} width={58} />
            {reference && <ReferenceLine y={reference.value} ifOverflow="extendDomain"
              stroke="#94a3b8" strokeDasharray="5 4"
              label={{ value: reference.label, position: 'insideTopRight', fill: '#94a3b8', fontSize: 12 }} />}
            <Tooltip allowEscapeViewBox={{ x: false, y: false }} isAnimationActive={false}
              contentStyle={{ maxWidth: '14rem', whiteSpace: 'normal', overflowWrap: 'anywhere' }}
              itemStyle={{ whiteSpace: 'normal' }}
              labelFormatter={time => new Date(time).toISOString().slice(0, 10)}
              formatter={value => format.format(Number(value))} />
            <Line type="linear" dataKey="value" name={indicator.name} stroke="#38bdf8"
              strokeWidth={2} dot={visiblePoints.length < 15} isAnimationActive={false} />
          </LineChart>
        </ResponsiveContainer>
      </div>}
    </dialog>}
  </main>;
}
