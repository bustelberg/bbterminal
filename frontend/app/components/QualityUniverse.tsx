'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';

import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import { type Lang, useLang } from '../../lib/i18n';
import LoadingDots from './LoadingDots';

const TEMPLATE_KEY = 'QUALITY';

type SourceCompany = {
  ticker: string;
  yahoo_ticker: string | null;
  company_name: string;
  sector: string | null;
  country: string | null;
  sources: string[];
};

const SECTOR_EN: Record<string, string> = {
  'financiële waarden': 'Financials',
  industrie: 'Industrials',
  materialen: 'Materials',
  'luxe-consumentengoederen': 'Consumer Discretionary',
  nutsbedrijven: 'Utilities',
  gezondheidszorg: 'Health Care',
  energie: 'Energy',
  vastgoed: 'Real Estate',
  communicatie: 'Communication Services',
  'basis-consumentengoederen': 'Consumer Staples',
  it: 'Information Technology',
};

function sectorLabel(sector: string | null, lang: Lang): string {
  if (!sector) return '-';
  return lang === 'en' ? (SECTOR_EN[sector.toLocaleLowerCase('nl-NL')] ?? sector) : sector;
}

export default function QualityUniverse() {
  const [lang] = useLang();
  const [companies, setCompanies] = useState<SourceCompany[]>([]);
  const [asOfDate, setAsOfDate] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [search, setSearch] = useState('');
  const [error, setError] = useState<string | null>(null);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const response = await apiFetch(`${API_URL}/api/universe-templates/${TEMPLATE_KEY}/source-companies`);
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      const data = await response.json() as { companies?: SourceCompany[]; as_of_date?: string };
      setCompanies(data.companies ?? []);
      setAsOfDate(data.as_of_date ?? null);
      setError(null);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : String(cause));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const filtered = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) return companies;
    return companies.filter((member) => [member.ticker, member.yahoo_ticker, member.company_name, member.country, member.sector, sectorLabel(member.sector, lang)]
      .some((value) => value?.toLowerCase().includes(query)));
  }, [companies, lang, search]);

  return (
    <section className="space-y-4">
      <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-4 flex items-start justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-sm font-medium text-fg-strong">Quality universe</h2>
          <p className="text-xs text-fg-subtle mt-1 max-w-3xl">
            Combined list from the Global Compounders database and iShares MSCI World Quality Factor ETF holdings.
          </p>
          {asOfDate && (
            <p className="text-xs text-fg-muted mt-2">
              {companies.length.toLocaleString()} companies / iShares source snapshot {asOfDate}
            </p>
          )}
        </div>
      </div>

      {error && <div className="text-xs text-neg-300 bg-neg-500/10 border border-neg-500/20 rounded-lg px-4 py-3">{error}</div>}

      {loading ? (
        <div className="bg-card rounded-xl border border-neutral-800/40 px-5 py-8 text-sm text-fg-subtle"><LoadingDots label="Loading Quality universe" /></div>
      ) : (
        <div className="bg-card rounded-xl border border-neutral-800/40 overflow-hidden">
          <div className="px-5 py-3 border-b border-neutral-800/40 flex items-center justify-between gap-3 flex-wrap">
            <input
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder="Search Yahoo ticker, company, country or sector"
              className="w-full max-w-md bg-page border border-neutral-700 rounded-lg px-3 py-1.5 text-sm text-fg outline-none focus:border-accent-500"
            />
            <span className="text-xs text-fg-subtle">{filtered.length.toLocaleString()} companies</span>
          </div>
          <div className="max-h-[620px] overflow-auto">
            <table className="w-full text-sm">
              <thead className="sticky top-0 bg-card text-xs text-fg-subtle border-b border-neutral-800/40">
                <tr><th className="text-left font-medium px-5 py-3">Yahoo ticker</th><th className="text-left font-medium px-3 py-3">Source ticker</th><th className="text-left font-medium px-3 py-3">Company</th><th className="text-left font-medium px-3 py-3">Country</th><th className="text-left font-medium px-3 py-3">Sources</th><th className="text-left font-medium px-5 py-3">Sector</th></tr>
              </thead>
              <tbody>
                {filtered.map((member) => (
                  <tr key={member.ticker} className="border-b border-neutral-800/30 hover:bg-overlay/[0.02]">
                    <td className="px-5 py-2.5 font-medium">
                      {member.yahoo_ticker ? (
                        <a
                          href={`https://finance.yahoo.com/quote/${encodeURIComponent(member.yahoo_ticker)}`}
                          target="_blank"
                          rel="noreferrer"
                          className="text-accent-400 hover:text-accent-300 hover:underline"
                          aria-label={`Open ${member.yahoo_ticker} on Yahoo Finance`}
                        >
                          {member.yahoo_ticker}
                        </a>
                      ) : '-'}
                    </td>
                    <td className="px-3 py-2.5 text-fg-muted">{member.ticker}</td>
                    <td className="px-3 py-2.5 text-fg-soft">{member.company_name}</td>
                    <td className="px-3 py-2.5 text-fg-muted">{member.country ?? '-'}</td>
                    <td className="px-3 py-2.5 text-fg-muted">{member.sources.map((source) => source === 'compounders' ? 'Compounders' : 'iShares Quality').join(' / ')}</td>
                    <td className="px-5 py-2.5 text-fg-muted">{sectorLabel(member.sector, lang)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </section>
  );
}
