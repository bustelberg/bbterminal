'use client';

import { LANGS, LANG_LABEL, type Lang } from '../../lib/i18n';

/**
 * EN | NL, as a joined segmented bar.
 *
 * ⚠ SEGMENTED, NOT TWO PILLS — the shape carries the meaning. A joined bar says "exactly one of
 * these", which is what a language is; separate pills say "any of these", the shape the `Tables`
 * tab's row and window filters use because those genuinely combine. Borrowing the wrong one would
 * promise that EN and NL can both be on.
 *
 * Same markup as the benchmark picker inside that tab, deliberately: two controls that mean "pick
 * one" and look different are two controls a reader has to learn separately.
 */
export default function LangSwitch({ lang, onChange, title }: {
  lang: Lang; onChange: (l: Lang) => void; title?: string;
}) {
  return (
    <div className="inline-flex rounded-lg border border-neutral-700 overflow-hidden text-[11px]"
      title={title}>
      {LANGS.map((l) => (
        <button key={l} type="button" onClick={() => onChange(l)} aria-pressed={lang === l}
          lang={l}
          className={`cursor-pointer px-2.5 py-0.5 font-medium transition-colors ${
            lang === l ? 'bg-accent-600 text-white' : 'text-fg-muted hover:bg-overlay/5'}`}>
          {LANG_LABEL[l]}
        </button>
      ))}
    </div>
  );
}
