'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { apiFetch } from '../../lib/apiFetch';
import { API_URL } from '../../lib/apiUrl';
import type { AirsAccountModelLink, AirsAccountModelLinks } from '../../lib/types/api';

/**
 * Which MODEL is each AIRS ACCOUNT running?
 *
 * WHY THIS TABLE HAS TO EXIST AT ALL
 *   The ISINs and the money are on opposite sides of a wall. A model (`Overzicht
 *   Modelportefeuilles`) has weights + ISINCode and AIRS values none of it. An account
 *   (`Front-Office`) has real returns and NO ISIN — only a fund name. Measured: of 58 models
 *   with a composition and 31 AIRS-valued accounts, the overlap is ZERO. Pairing them is the
 *   only bridge, and neither side carries a key for it.
 *
 * ⚠ THE HOLDINGS CANNOT IDENTIFY THE MODEL — the obvious matcher is the useless one.
 *   BUS_FTS_Bepoff_AFS, BUS_FTS_DEF_AFS and BUS_FTS_NEU_AFS hold the IDENTICAL 27 ISINs (27 of
 *   27, all three pairs); BUS_FTS_OFF_AFS's 25 are a subset of each. One strategy, four risk
 *   weightings, one instrument list. Comparing contents scores all four 100.
 *
 * ⚠ SO THE NAME IS THE ONLY DISCRIMINATOR, AND IT IS FOUR CONVENTIONS AND A TYPO:
 *     AITopSelectie OFF DYN     <-> AITopSelectie OFF FX          suffix swapped
 *     BUS_MTS_OFF_AFS_DYN       <-> BUS_MTS_OFF_AFS               suffix appended
 *     BUS_FTS_OFF_DYN           <-> BUS_FTS_OFF_AFS               suffix REPLACED
 *     BUS_BM_AAN_kw_EUR_2026_d  <-> BUS_BM_AAND_kw_EUR_2026       the word itself mangled
 *     VTopSelectie OFF DY       <-> VTopSelectie OFF FX           missing its N
 *   Hence: the guess matches an exact stem or refuses, and a human decides. The failure that
 *   matters is not a missing link — it is a confident wrong one, because nobody re-checks a
 *   match that looks right and the wrong risk profile holds nearly the same names.
 */

const pct = (v: number | null | undefined) =>
  v == null ? '—' : `${v >= 0 ? '+' : ''}${v.toFixed(2)}%`;
const tone = (v: number | null | undefined) =>
  v == null ? 'text-fg-faint' : v >= 0 ? 'text-pos-400' : 'text-neg-400';

/** manual = a human decided (wins always). guess = recomputed each read. none = we refuse. */
function SourceBadge({ a }: { a: AirsAccountModelLink }) {
  if (a.source === 'manual') {
    return (
      <span className="px-1.5 py-0.5 rounded bg-accent-500/15 text-accent-400 text-[10px]"
        title="A human decided this. It always wins over the guess, and it never goes stale into a wrong number — only out of date.">
        manual
      </span>
    );
  }
  if (a.source === 'guess') {
    return (
      <span className="px-1.5 py-0.5 rounded bg-warn-500/15 text-warn-400 text-[10px]"
        title={`Not confirmed by anyone — ${a.reason ?? ''}. The stem matched exactly, which is why it cannot have confused two risk profiles, but AIRS's naming has five conventions and only four are known here.`}>
        guess
      </span>
    );
  }
  return (
    <span className="px-1.5 py-0.5 rounded bg-overlay/10 text-fg-faint text-[10px]"
      title={a.reason ?? 'No fixed portfolio matched.'}>
      —
    </span>
  );
}

export default function AccountModelLinkPanel() {
  const [data, setData] = useState<AirsAccountModelLinks | null>(null);
  const [err, setErr] = useState<string | null>(null);
  const [saving, setSaving] = useState<string | null>(null);
  const [onlyUnresolved, setOnlyUnresolved] = useState(false);

  const load = useCallback(async () => {
    try {
      const r = await apiFetch(`${API_URL}/api/airs/account-model-links`);
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setData(await r.json());
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  /** '' = forget the decision (guess speaks again); 'none' = explicitly not a model. Those are
   *  different facts and the API keeps them apart — DELETE vs a stored NULL. */
  const choose = async (portefeuille: string, value: string) => {
    setSaving(portefeuille);
    try {
      const r = value === ''
        ? await apiFetch(`${API_URL}/api/airs/account-model-links/${encodeURIComponent(portefeuille)}`,
          { method: 'DELETE' })
        : await apiFetch(`${API_URL}/api/airs/account-model-links/${encodeURIComponent(portefeuille)}`, {
          method: 'PUT',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ model_portfolio_id: value === 'none' ? null : Number(value) }),
        });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      await load();
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e));
    } finally {
      setSaving(null);
    }
  };

  const rows = useMemo(() => {
    if (!data) return [];
    return onlyUnresolved ? data.accounts.filter((a) => a.source !== 'manual') : data.accounts;
  }, [data, onlyUnresolved]);

  const counts = useMemo(() => {
    const a = data?.accounts ?? [];
    return {
      total: a.length,
      manual: a.filter((x) => x.source === 'manual').length,
      guess: a.filter((x) => x.source === 'guess').length,
      none: a.filter((x) => x.source === 'none').length,
    };
  }, [data]);

  return (
    <div className="bg-card rounded-xl border border-neutral-800/40 p-5">
      <div className="flex items-baseline justify-between gap-4 flex-wrap">
        <div>
          <h2 className="text-base font-semibold text-fg-strong">Dynamic → Fixed</h2>
          <p className="text-xs text-fg-muted mt-1 max-w-3xl"
            title="Fixed portfolios carry the ISINs and AIRS values none of them; Dynamic ones carry the values and no ISIN. The pairing is the only link between them, and it cannot be derived: the risk variants of a strategy hold the same instruments at different weights, so only the name distinguishes them.">
            Which Fixed portfolio each Dynamic portfolio runs. Only the name links them.
          </p>
        </div>
        {data && (
          <div className="flex items-center gap-3 text-xs text-fg-subtle whitespace-nowrap">
            <span>{counts.manual} confirmed · {counts.guess} guessed · {counts.none} no match</span>
            <label className="flex items-center gap-1.5 cursor-pointer">
              <input type="checkbox" checked={onlyUnresolved}
                onChange={(e) => setOnlyUnresolved(e.target.checked)} />
              Needs a decision
            </label>
          </div>
        )}
      </div>

      {err && (
        <div className="mt-3 bg-neg-500/10 border border-neg-500/20 rounded-lg px-3 py-2 text-xs text-neg-400">
          {err}
        </div>
      )}
      {!data && !err && <p className="mt-3 text-xs text-fg-faint">Loading…</p>}

      {data && (
        <div className="overflow-auto rounded-lg border border-neutral-800/40 max-h-[60vh] mt-4">
          <table className="w-full text-xs">
            <thead className="bg-card sticky top-0 z-10">
              <tr className="text-fg-faint text-[10px] uppercase tracking-wide border-b border-neutral-800/40">
                <th className="px-3 py-1.5 font-medium text-left">Dynamic portfolio</th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="AIRS's own cumulatief_rendement for the year — every month compounded, flow-aware.">
                  YTD
                </th>
                <th className="px-3 py-1.5 font-medium text-left">Runs fixed portfolio</th>
                <th className="px-3 py-1.5 font-medium text-right"
                  title="Positions in that fixed portfolio. A dynamic portfolio normally holds this + 1 — the extra is the cash line (Effectenrekening), which has no ISIN.">
                  ISINs
                </th>
                <th className="px-3 py-1.5 font-medium text-left">Source</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-neutral-800/20">
              {rows.map((a) => (
                <tr key={a.portefeuille} className="hover:bg-overlay/[0.02]">
                  <td className="px-3 py-1.5 font-mono text-fg whitespace-nowrap">{a.portefeuille}</td>
                  <td className={`px-3 py-1.5 text-right font-mono ${tone(a.ytd_pct)}`}>{pct(a.ytd_pct)}</td>
                  <td className="px-3 py-1.5">
                    <select
                      className="bg-page border border-neutral-700 rounded-lg px-2 py-1 text-xs w-full max-w-[22rem] focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30"
                      disabled={saving === a.portefeuille}
                      /* A guessed value shows as selected but is NOT stored — picking it is what
                         confirms it, which is the whole point of the badge beside it. */
                      value={a.source === 'manual' && a.model_portfolio_id == null
                        ? 'none'
                        : (a.model_portfolio_id != null ? String(a.model_portfolio_id) : '')}
                      onChange={(e) => void choose(a.portefeuille, e.target.value)}
                    >
                      <option value="">{a.source === 'guess' ? '— accept guess —' : '— none selected —'}</option>
                      <option value="none">Not a fixed portfolio (benchmark / test)</option>
                      {data.models.map((m) => (
                        <option key={m.id} value={m.id}>{m.name} ({m.positions})</option>
                      ))}
                    </select>
                  </td>
                  <td className="px-3 py-1.5 text-right font-mono text-fg-subtle">
                    {a.model_positions ?? '—'}
                  </td>
                  <td className="px-3 py-1.5"><SourceBadge a={a} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {data && (
        <p className="text-[11px] text-fg-faint mt-3 leading-relaxed"
          title="The guess strips the venue suffix and requires an exact match on the remainder, so it cannot confuse two risk profiles — BUS_FTS_DEF and BUS_FTS_NEU are different strings. Where AIRS uses an unknown convention (BUS_BM_AAND… → BUS_BM_AAN…_d) it offers nothing rather than a near match.">
          A <span className="text-warn-400">guess</span>{' '}is an exact match on the name, venue
          suffix removed; unknown conventions are left blank. A selection is stored and takes
          precedence.
        </p>
      )}
    </div>
  );
}
