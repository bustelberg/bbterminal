'use client';

import { FormEvent, useState } from 'react';
import { API_URL } from '../../lib/apiUrl';
import { apiFetch } from '../../lib/apiFetch';

type ResearchSection = {
  key: string;
  label: string;
  endpoint: string;
  status_code: number | null;
  ok: boolean;
  error: string | null;
  data: unknown;
};

type ResearchResponse = {
  symbol: string;
  fetched_at: string;
  guru_focus_requests: number;
  sections: ResearchSection[];
};

function payloadSize(data: unknown): string {
  if (Array.isArray(data)) return `${data.length} items`;
  if (data !== null && typeof data === 'object') {
    return `${Object.keys(data as Record<string, unknown>).length} top-level fields`;
  }
  return data === null ? 'null' : typeof data;
}

async function responseError(response: Response): Promise<string> {
  try {
    const body = await response.json() as { detail?: unknown };
    if (typeof body.detail === 'string') return body.detail;
  } catch {
    // The HTTP status below is still useful when an intermediary returned HTML or an empty body.
  }
  return `${response.status} ${response.statusText}`.trim();
}

export default function ResearchPage() {
  const [symbol, setSymbol] = useState('NVDA');
  const [result, setResult] = useState<ResearchResponse | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function load(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const cleaned = symbol.trim().toUpperCase();
    if (!cleaned || loading) return;

    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const response = await apiFetch(
        `${API_URL}/api/admin/gurufocus-research?symbol=${encodeURIComponent(cleaned)}`,
        { noReadCache: true },
      );
      if (!response.ok) throw new Error(await responseError(response));
      setResult(await response.json() as ResearchResponse);
      setSymbol(cleaned);
    } catch (caught) {
      setError(caught instanceof Error ? caught.message : String(caught));
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="mx-auto w-full max-w-7xl px-4 py-8 sm:px-6">
      <h1 className="text-2xl font-semibold text-fg">GuruFocus research data</h1>
      <p className="mt-2 max-w-4xl text-sm text-fg-muted">
        A deliberately plain view of the complete vendor responses for one company. Nothing below
        is renamed, combined or reformatted. This is an admin-only diagnostic page.
      </p>

      <form onSubmit={load} className="mt-6 flex max-w-xl items-end gap-3">
        <label className="min-w-0 flex-1 text-sm text-fg-muted">
          GuruFocus symbol
          <input
            value={symbol}
            onChange={(event) => setSymbol(event.target.value)}
            spellCheck={false}
            autoCapitalize="characters"
            placeholder="NVDA or XAMS:ASML"
            className="mt-1 block w-full rounded-md border border-neutral-700 bg-panel px-3 py-2 font-mono text-sm text-fg outline-none focus:border-blue-500"
          />
        </label>
        <button
          type="submit"
          disabled={loading || !symbol.trim()}
          className="rounded-md bg-blue-600 px-4 py-2 text-sm font-medium text-white disabled:cursor-not-allowed disabled:opacity-50"
        >
          {loading ? 'Loading 14 endpoints…' : 'Load company'}
        </button>
      </form>

      <p className="mt-3 text-xs text-fg-subtle">
        Each load makes 14 GuruFocus requests and can take a little while. It includes current
        estimates, key-ratio forecasts (including FCF), historical estimate/actual comparisons,
        forward P/E history, and ten actual-plus-current-forecast metric series.
      </p>

      {error && (
        <div className="mt-6 rounded-md border border-red-700/50 bg-red-950/30 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {result && (
        <section className="mt-8">
          <div className="mb-4 flex flex-wrap items-baseline gap-x-4 gap-y-1">
            <h2 className="text-xl font-semibold text-fg">{result.symbol}</h2>
            <span className="text-xs text-fg-subtle">
              fetched {new Date(result.fetched_at).toLocaleString()} · {result.guru_focus_requests}
              {' '}GuruFocus requests
            </span>
          </div>

          <div className="space-y-3">
            {result.sections.map((section, index) => (
              <details
                key={section.key}
                open={index < 4}
                className="overflow-hidden rounded-md border border-neutral-800 bg-panel"
              >
                <summary className="cursor-pointer px-4 py-3 text-sm text-fg marker:text-fg-subtle">
                  <span className="font-medium">{section.label}</span>
                  <span className="ml-2 font-mono text-xs text-fg-subtle">
                    /{section.endpoint}
                  </span>
                  <span className={`ml-2 text-xs ${section.ok ? 'text-emerald-400' : 'text-red-400'}`}>
                    {section.ok ? payloadSize(section.data) : `failed${section.status_code ? ` (${section.status_code})` : ''}`}
                  </span>
                </summary>
                {section.ok ? (
                  <pre className="max-h-[70vh] overflow-auto border-t border-neutral-800 bg-black/20 p-4 font-mono text-xs leading-5 text-fg-muted">
                    {JSON.stringify(section.data, null, 2)}
                  </pre>
                ) : (
                  <div className="border-t border-neutral-800 px-4 py-3 font-mono text-xs text-red-300">
                    {section.error ?? 'GuruFocus returned no data.'}
                  </div>
                )}
              </details>
            ))}
          </div>
        </section>
      )}
    </main>
  );
}
