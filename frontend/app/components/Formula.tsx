'use client';

import type { ReactNode } from 'react';

/**
 * React 19 renders MathML fine at runtime — the DOM is the DOM — but `@types/react` does not
 * declare the MathML intrinsics, so TSX rejects `<math>` and friends. Declared here, narrowly:
 * only the five elements this file actually emits, so an unsupported tag stays a type error
 * rather than becoming a silently-unrendered element.
 */
declare module 'react' {
  // JSX intrinsics live in a `namespace` by TypeScript's design — there is no ES-module form of
  // this augmentation, so the lint rule cannot be satisfied, only acknowledged.
  // eslint-disable-next-line @typescript-eslint/no-namespace
  namespace JSX {
    interface IntrinsicElements {
      math: { display?: 'inline' | 'block'; className?: string; children?: ReactNode };
      mrow: { children?: ReactNode };
      mi: { children?: ReactNode };
      mo: { stretchy?: 'true' | 'false'; children?: ReactNode };
      msub: { children?: ReactNode };
      mover: { accent?: 'true' | 'false'; children?: ReactNode };
    }
  }
}

/**
 * Math typesetting with NATIVE MathML — no library, no bundle, no fonts.
 *
 * WHY NOT KaTeX
 *     KaTeX is the obvious answer and it is the wrong one here. It costs ~70KB of JS plus ~130KB
 *     of Computer Modern woff2, and Computer Modern is a *serif* face that does not belong beside
 *     Geist — every expression would announce itself as a foreign object pasted into the page. For
 *     four static expressions that is a lot of weight to carry in order to look worse.
 *
 *     MathML is native in every browser we target (Chrome/Edge 109+, Firefox, Safari all ship
 *     MathML Core). It inherits our font stack and our colour tokens, so an expression looks like
 *     it was set in the same page it lives on — because it was.
 *
 * WHAT IT BUYS OVER HAND-ROLLED `<sub>` (which is what this replaces)
 *     * `<mi>` renders a VARIABLE, and a variable is italic. That is not decoration: italic is how
 *       maths distinguishes a variable `w` from a label "w". Hand-rolled markup silently loses it.
 *     * `<mo>` gets real operator spacing. A bare `×` between two spans is glued to its operands.
 *     * `<mover accent>` draws a TRUE overbar. The alternative is `R̄` — a combining macron, which
 *       lands in a different place in every font and sometimes lands on nothing at all.
 *     * A screen reader announces it as an equation instead of spelling out "R subscript B".
 *
 * THE ESCAPE HATCH
 *     Every call site goes through this component, so if the app ever needs fractions, sums or
 *     matrices at a scale MathML makes painful, KaTeX drops in HERE and nothing else changes.
 */

/** A variable — italic, by the convention MathML applies for free. `sub` is its subscript. */
export function V({ name, sub }: { name: string; sub?: string }) {
  if (!sub) return <mi>{name}</mi>;
  return (
    <msub>
      <mi>{name}</mi>
      <mi>{sub}</mi>
    </msub>
  );
}

/** A variable under a bar — the benchmark's TOTAL, as opposed to its return in one bucket. */
export function VBar({ name, sub }: { name: string; sub?: string }) {
  const barred = (
    <mover accent="true">
      <mi>{name}</mi>
      <mo>&#x2015;</mo>
    </mover>
  );
  if (!sub) return barred;
  return (
    <msub>
      {barred}
      <mi>{sub}</mi>
    </msub>
  );
}

/** An operator. `−` and `×` are the real characters, not a hyphen and a letter x. */
export function Op({ children }: { children: string }) {
  return <mo>{children}</mo>;
}

export function Paren({ children }: { children: ReactNode }) {
  return (
    <>
      <mo stretchy="false">(</mo>
      {children}
      <mo stretchy="false">)</mo>
    </>
  );
}

/**
 * An inline expression. `display: inline` keeps it on the text baseline rather than centring it
 * as a display block.
 */
export default function Formula({ children, className = '' }: {
  children: ReactNode;
  className?: string;
}) {
  return (
    // `font-[inherit]` is the whole point: the expression is set in the page's own type, not in a
    // library's. MathML italicises the <mi>s itself.
    <math display="inline" className={`font-[inherit] ${className}`}>
      <mrow>{children}</mrow>
    </math>
  );
}
