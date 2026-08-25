'use client';

/**
 * REAL MATHEMATICAL TYPESETTING FOR THE ⓘ CARDS — KaTeX, not Unicode in a mono font.
 *
 * ⚠⚠ THE UNICODE VERSION WAS UNREADABLE AND NO AMOUNT OF CSS WAS GOING TO FIX IT. `½ · Σ |wᵖ − wᵇ|`
 * is not a formula, it is a row of glyphs that resemble one: the summation has no limits, the
 * superscripts are baseline-shifted characters rather than real scripts, the fraction is a single
 * codepoint that cannot grow, and a mono font gives `Σ` the same advance width as a comma. Set it
 * properly and the same expression reads at a glance.
 *
 * ⚠ `displayMode`, NOT INLINE. These stand alone in their own block, so the summation gets its
 * limits above and below and fractions get full height — which is the entire visual difference
 * between `\sum_{i=1}^{N}` rendered inline and rendered as display math.
 *
 * ⚠ `throwOnError: false` — A BAD FORMULA MUST NOT BLANK THE CARD. KaTeX renders the offending
 * source in red instead of throwing, so a typo in one tooltip degrades to a visible mistake in that
 * tooltip rather than an error boundary swallowing the panel around it.
 *
 * ⚠ `trust: false` (the default, stated) blocks `\href` and `\includegraphics`. Every string that
 * reaches here is a constant in our own copy tables, never user input — but `dangerouslySetInnerHTML`
 * on anything is worth being explicit about, and the day one of these becomes interpolated the
 * guard is already in place.
 *
 * ⚠ THE STYLESHEET IS IMPORTED HERE, beside the only thing that uses it. Next's App Router allows
 * an external package's CSS in any colocated component (`node_modules/next/dist/docs/01-app/
 * 01-getting-started/11-css.md` — "Stylesheets published by external packages can be imported
 * anywhere in the app directory"), so it does not have to be hoisted into the root layout where a
 * reader would have no idea what needed it.
 */
import { useMemo } from 'react';
import katex from 'katex';
import 'katex/dist/katex.min.css';

export default function Formula({ tex, className = '' }: {
  tex: string;
  className?: string;
}) {
  const html = useMemo(
    () => katex.renderToString(tex, { displayMode: true, throwOnError: false, trust: false }),
    [tex],
  );
  return (
    // ⚠ `overflow-x-auto` ON THE WRAPPER, not `break-words`: a formula cannot be broken at an
    // arbitrary operator the way a sentence can be broken at a space. A long one scrolls; the
    // alternative is `\sum` on one line and its summand on the next, which is worse than either.
    //
    // ⚠ `text-fg-strong` AND NOT A KATEX COLOUR OVERRIDE. KaTeX draws its rules (fraction bars,
    // radicals) with `currentColor`, so setting the colour here carries them along; recolouring
    // `.katex` internals would leave the bars behind at the old ink.
    <span className={`block overflow-x-auto text-fg-strong ${className}`}
      // eslint-disable-next-line react/no-danger
      dangerouslySetInnerHTML={{ __html: html }} />
  );
}
