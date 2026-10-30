import type { ReactNode } from 'react';
import { useLang, type Lang } from '../../../lib/i18n';
import { dayOf } from './asOfLine';

/** Short prose first; real observations and dates underneath, never a guessed date. */
export default function SimpleExplanation({ children, details }: { children: ReactNode; details?: ReactNode }) {
  return <div className="max-w-sm space-y-2 text-xs leading-relaxed"><p>{children}</p>
    {details && <div className="space-y-1 text-fg-muted">{details}</div>}
  </div>;
}

export function infoFacts(lang: Lang) {
  const nl = lang === 'nl';
  const date = (value?: string | null) => {
    const day = dayOf(value);
    const parsed = day ? new Date(`${day}T12:00:00Z`) : null;
    if (!parsed || Number.isNaN(parsed.getTime()) || parsed.toISOString().slice(0, 10) !== day)
      return nl ? 'datum onbekend' : 'date unavailable';
    return parsed.toLocaleDateString(nl ? 'nl-NL' : 'en-GB', {
      day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC',
    });
  };
  const window = (from?: string | null, to?: string | null) => `${date(from)} – ${date(to)}`;
  return {
    date, window,
    dated: (source: string, asOf?: string | null) => `${source} · ${date(asOf)}`,
    weights: (asOf?: string | null) => `${nl ? 'AIRS-gewichten' : 'AIRS weights'} · ${date(asOf)}`,
    sample: (from?: string | null, to?: string | null, count?: number | null, frequency?: string | null) =>
      `${window(from, to)} · ${count ?? '—'} ${frequency === 'weekly'
        ? nl ? 'weekrendementen' : 'weekly returns' : frequency === 'daily'
          ? nl ? 'dagrendementen' : 'daily returns' : nl ? 'rendementen' : 'returns'}`,
    pairs: (measured: number | undefined, possible: number, minimum: number | undefined) => nl
      ? `${measured ?? '—'} van ${possible} paren · minimaal ${minimum ?? '—'} gezamenlijke rendementen per paar`
      : `${measured ?? '—'} of ${possible} pairs · at least ${minimum ?? '—'} shared returns per pair`,
    worst: (from?: string | null, to?: string | null, value?: number | null) =>
      `${nl ? 'Grootste daling' : 'Worst fall'}: ${window(from, to)} · ${value == null ? '—' : value.toFixed(2) + '%'}`,
    snapshot: (benchmark: string, asOf?: string | null) => `${benchmark} ${nl ? 'momentopname' : 'snapshot'} · ${date(asOf)}`,
    coverage: (percentage: number, benchmark: string) => nl ? `${percentage.toFixed(2)}% van het ${benchmark}-gewicht geprijsd` : `${percentage.toFixed(2)}% of ${benchmark} weight priced`,
    overlap: (overlap?: number | null, active?: number | null) =>
      `${overlap == null ? '—' : overlap.toFixed(2) + '%'} overlap + ${active == null ? '—' : active.toFixed(2) + '%'} active share = 100%`,
  };
}

export function useInfoFacts() {
  const [lang] = useLang();
  return infoFacts(lang);
}
