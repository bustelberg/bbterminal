'use client';

/**
 * WHICH PART OF A TOOLTIP IS THIS BOOK'S NUMBER, AND WHICH PART IS THE SENTENCE AROUND IT.
 *
 * ⚠⚠ A CARD IS READ BY SCANNING, NOT BY READING. Every ⓘ here mixes a fixed explanation with live
 * values — dates, counts, weights, a benchmark's name — and set in one continuous face they are
 * indistinguishable: the reader has to parse the whole sentence to find the one figure they came
 * for, and worse, has no way to tell a number that came from their book from a number that is part
 * of the explanation. `T − 1` is prose; `T = 261` is data. Badging the live half makes the card
 * answerable at a glance, and makes "is this figure current?" a question the eye can ask.
 *
 * ⚠⚠ THE COPY TABLE STAYS PLAIN STRINGS, WHICH IS THE WHOLE REASON FOR THE SENTINEL. The obvious
 * alternative is copy functions returning JSX — but `riskCopy` is a `.ts` module whose entire
 * guarantee is that `nl` is typed as `RiskCopy`, so a missing translation is a COMPILE error.
 * Turning ~660 strings into nodes would trade that guarantee, and every translator's job, for a
 * styling concern. Instead a copy function wraps its own interpolations in {@link v}, and one
 * renderer splits on the marks.
 *
 * ⚠ THE MARKS ARE CONTROL CHARACTERS, chosen because they cannot occur in a company name, a
 * benchmark label or a date — and because if this renderer is ever bypassed they are INVISIBLE
 * rather than turning up as stray brackets in the UI. A visible delimiter like «…» would eventually
 * appear in real data and split a badge in half.
 */
import { Fragment } from 'react';
import { BADGE_NEUTRAL, BADGE_VALUE } from './badgeChrome';

// ⚠ BUILT AT RUNTIME RATHER THAN TYPED AS LITERALS. A raw invisible character in a source
// file survives nothing well — editors strip it, `grep` output becomes unreadable, and this
// repo has already been bitten by an encoding round-trip mangling source (see the
// PowerShell/cp1252 note in the project docs). This form is plain ASCII and says exactly
// which codepoints are meant.
//
// ⚠⚠ U+2060/U+2061, NOT THE C0 CONTROLS THIS FIRST USED, AND THE DIFFERENCE IS THE FAILURE
// MODE. `v()` is rendered only by DynamicText; a marked string sent anywhere else — a button
// label, a table footer, a bare <p> — reaches the DOM with its marks intact. As C0 controls
// that painted a TOFU BOX beside every count on the panel, which is exactly what a reader
// reported. These two are Cf (format) characters that no font draws and no browser shows, so
// the same mistake now degrades to "the value is not badged" — still wrong, but invisible
// and harmless rather than a rendering fault on screen.
//
// ⚠ WORD JOINER rather than a zero-width SPACE: U+200B offers a line-break opportunity, so a
// badge boundary could split a date across two lines. These two create no break at all.
const OPEN = String.fromCharCode(0x2060);   // WORD JOINER
const CLOSE = String.fromCharCode(0x2061);  // FUNCTION APPLICATION

/**
 * Mark one interpolated value as dynamic. `` `${v(count)} companies held` ``
 *
 * ⚠⚠ ONLY IN A STRING THAT REACHES {@link DynamicText} — which in practice means an `AspectCard`
 * field or a legend row, nothing else. A marked string used as a BUTTON LABEL, a table cell or a
 * bare `<p>` is not badged, because nothing on that path splits it; the marks simply travel into
 * the DOM. That shipped once, on the filter chips, the table footer and the two notes under the
 * tiles. It is now invisible rather than ugly (see the sentinel note above) but it is still a
 * badge the reader was promised and did not get, so check the destination before adding one.
 *
 * ⚠ IT STRIPS THE MARKS OUT OF ITS OWN INPUT. A value is data — a portfolio name someone typed, a
 * label from a vendor — and data carrying a sentinel would close a badge early and leave the rest
 * of the sentence inside one. Cheap to prevent, and impossible to notice once shipped.
 */
export function v(value: string | number | null | undefined): string {
  const s = value == null ? '' : String(value);
  return OPEN + s.split(OPEN).join('').split(CLOSE).join('') + CLOSE;
}

/**
 * Split a marked string into its static and dynamic runs.
 *
 * ⚠ EXPORTED FOR ITS OWN TEST. The rendering is trivial; the splitting is where an unbalanced or
 * empty mark would quietly eat text, so that is the part worth asserting on directly.
 *
 * ⚠ AN UNCLOSED MARK YIELDS PLAIN TEXT, NEVER A SWALLOWED TAIL. The failure mode of a naive
 * `split(OPEN)` pairing is that everything after a stray open becomes one enormous badge — or
 * disappears entirely. Text always survives here; at worst it is not badged.
 *
 * ⚠⚠ AND A STRAY **CLOSE** IS DROPPED TOO, WHICH IT WAS NOT. Only the unbalanced OPEN was handled:
 * a CLOSE with no opener fell through every static branch and was pushed out INSIDE the text, so
 * an invisible U+2061 landed in rendered prose — measured as `'⁡orphan close'` where
 * `'orphan close'` was expected. Invisible is the whole problem: it copies, it lands in a title
 * attribute, it can break a `===` against the same sentence built without marks, and nothing on
 * screen ever shows it. EVERY run is now stripped — a dynamic one can carry a stray OPEN
 * (`v(v('x'))`, or a double open), which ends up inside the badge rather than in the prose but is
 * just as invisible and just as copyable.
 */
export function splitValues(text: string): { text: string; dynamic: boolean }[] {
  const out: { text: string; dynamic: boolean }[] = [];
  /** ⚠ AND IT SKIPS AN EMPTY RUN. Stripping can empty a segment (`splitValues(CLOSE)`), and an
   *  empty static part is a node that renders nothing under a key that suggests it does. */
  const pushStatic = (s: string) => {
    const plain = s.split(OPEN).join('').split(CLOSE).join('');
    if (plain) out.push({ text: plain, dynamic: false });
  };
  let rest = text;
  while (rest) {
    const open = rest.indexOf(OPEN);
    if (open < 0) { pushStatic(rest); break; }
    const close = rest.indexOf(CLOSE, open + 1);
    if (close < 0) {
      // Unbalanced: the mark itself is dropped, everything else survives as static text.
      pushStatic(rest.slice(0, open) + rest.slice(open + 1));
      break;
    }
    if (open > 0) pushStatic(rest.slice(0, open));
    // ⚠ STRIPPED TOO: the run ends at the first CLOSE, so it cannot hold one of those — but a
    // second OPEN inside it (a double open, or `v()` around an already-marked value) survives
    // into the badge, invisibly. See the ⚠⚠ above.
    const inner = rest.slice(open + 1, close).split(OPEN).join('');
    // ⚠ AN EMPTY VALUE PRODUCES NO BADGE. `v('')` is a real case — a null date formatted away —
    // and an empty chip is a floating grey rectangle that reads as a rendering fault.
    if (inner) out.push({ text: inner, dynamic: true });
    rest = rest.slice(close + 1);
  }
  return out;
}

/** Does this string carry any marked value? Lets a caller skip the splitter entirely. */
export const hasValues = (text: string) => text.includes(OPEN);

/**
 * One live value, set apart from the sentence around it.
 *
 * ⚠ THE CHROME COMES FROM `badgeChrome`, SHARED WITH THE PROVENANCE CARD'S FRESHNESS PILL, so a
 * value and a status inside the same tooltip read as one convention rather than two.
 *
 * ⚠ MONO AND `tabular-nums`, BECAUSE ALMOST EVERY ONE OF THESE IS A NUMBER OR A DATE — the same
 * face the tiles and tables use for data, so a figure in a tooltip looks like the figure it
 * explains rather than like a word. A name lands here too (a benchmark, a book) and reads fine.
 *
 */
export function ValueBadge({ children }: { children: React.ReactNode }) {
  return (
    <span className={`${BADGE_VALUE} ${BADGE_NEUTRAL} text-fg-strong`}>{children}</span>
  );
}

/**
 * Prose with its live values badged.
 *
 * ⚠ IT IS A PASS-THROUGH FOR AN UNMARKED STRING, so adoption is per-string rather than
 * all-at-once: copy that has not been converted renders exactly as it did before.
 */
export default function DynamicText({ text }: { text: string }) {
  if (!hasValues(text)) return <>{text}</>;
  return (
    <>
      {splitValues(text).map((part, i) => (
        <Fragment key={i}>
          {part.dynamic ? <ValueBadge>{part.text}</ValueBadge> : part.text}
        </Fragment>
      ))}
    </>
  );
}
