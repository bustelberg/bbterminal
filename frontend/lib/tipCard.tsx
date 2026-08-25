import Formula from './formula';

/** THE tooltip card. One shell, every info icon.
 *
 * ⚠ THIS EXISTS BECAUSE THERE WERE TWO. A number's provenance rendered as a designed card
 * (a micro-label, a bold source, a rule, then labelled fields) while every other tooltip rendered
 * as a bare paragraph. Same icon, same gesture, two different objects — so the structured one read
 * as a feature of certain cells rather than as the way this app explains itself.
 *
 * ⚠ THE SHELL IS SHARED; THE FIELDS ARE NOT, AND MUST NOT BE. A definition has no Source, no When
 * and no How: it explains a CONCEPT, not the origin of a number. Rendering "Source: —" over a
 * definition would fabricate provenance for something that never had any, which is the exact
 * failure the provenance card was built to prevent. So both cards share the chrome — label, title,
 * rule, body — and each fills it with what it actually knows.
 */

/** A short micro-label above the card title. Mirrors the provenance card's "SOURCE". */
export function TipCard({ label, labelSuffix, title, subtitle, children }: {
  label: string;
  labelSuffix?: React.ReactNode;
  title?: React.ReactNode;
  subtitle?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-2 min-w-[13rem]">
      <div>
        <div className="flex items-center gap-1.5 mb-0.5">
          <span className="text-[10px] uppercase tracking-wider text-fg-faint">{label}</span>
          {labelSuffix}
        </div>
        {title && <div className="text-fg-strong font-semibold leading-tight">{title}</div>}
        {subtitle && <div className="text-fg-muted text-[12px] mt-0.5">{subtitle}</div>}
      </div>
      <div className="border-t border-neutral-800/40 pt-2 space-y-1.5">{children}</div>
    </div>
  );
}

/** A short label column ("WHERE", "WHEN", "HOW") beside its value — the card's two-column grid.
 *  Shared by the provenance card and the generic {@link AspectCard} so every field row lines up
 *  the same way, whatever fills it. */
export function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <div className="flex items-baseline gap-2">
      <span className="text-[10px] uppercase tracking-wider text-fg-faint w-11 shrink-0 pt-px">{label}</span>
      <span className="min-w-0 text-[12px]">{children}</span>
    </div>
  );
}

/**
 * THE ARITHMETIC, WORKED, WITH THIS SCREEN'S OWN NUMBERS IN IT — see `portfolios/workedFormula`.
 *
 * ⚠⚠ IT IS TYPESET, NOT WRITTEN OUT IN UNICODE (2026-08-22). `½ · Σ |wᵖ − wᵇ|` in a mono face is
 * not a formula, it is a row of glyphs that resemble one — a summation with no limits, superscripts
 * that are baseline-shifted characters rather than real scripts, a fraction that is one codepoint
 * and cannot grow, and `Σ` given the same advance width as a comma. Reported, correctly, as "still
 * very hard to see". KaTeX sets the same expression properly; see `lib/formula`.
 *
 * ⚠ IT IS A BLOCK, NOT A `Field`. `Field`'s value column is about 14rem next to its label, and
 * display maths reflowed through that is unreadable in exactly the way that makes a reader stop
 * checking.
 *
 * ⚠ AND IT SITS DIRECTLY UNDER `where`, which is where the formula is described in words. Prose,
 * then the maths: the two have to be adjacent or the reader is comparing a description at the top
 * of a card with an expression at the bottom.
 */
function Worked({ text }: { text: string }) {
  // ⚠⚠ ONE TYPESET EXPRESSION, NOT TWO BLOCKS. `withWorked` joins the symbolic half and the
  // substituted half with `\\[4pt]` — a LaTeX line break — so KaTeX sets them as one display,
  // aligned and in one face. The previous version split on a blank line and styled each half
  // differently, which is what made them read as two unrelated objects: mono digits under
  // pseudo-mathematical Unicode.
  return (
    <span className="block rounded-md border border-neutral-800/40 bg-overlay/[0.05] px-3 py-2.5">
      <Formula tex={text} />
    </span>
  );
}

export function AspectCard({ what, where, when, how, worked }: {
  what: React.ReactNode;
  where?: React.ReactNode;
  when?: React.ReactNode;
  how?: React.ReactNode;
  /** The same formula as `where`, with real numbers substituted. See {@link Worked}. */
  worked?: string;
}) {
  return (
    <TipCard label="What" title={what}>
      {where != null && where !== '' && <Field label="Where">{where}</Field>}
      {worked ? <Worked text={worked} /> : null}
      {when != null && when !== '' && <Field label="When">{when}</Field>}
      {how != null && how !== '' && <Field label="How">{how}</Field>}
    </TipCard>
  );
}

/** Longest a leading fragment may be and still be a TITLE rather than the start of a sentence. */
const MAX_TITLE_LEN = 48;

/**
 * Split "Term — explanation" into a card title and its body.
 *
 * ⚠ IT IS DELIBERATELY CONSERVATIVE, BECAUSE A WRONG SPLIT IS WORSE THAN NO SPLIT. Promoting the
 * first clause of a sentence to a bold heading leaves a body that begins mid-thought — the tooltip
 * still renders, still looks designed, and reads as gibberish. So a fragment must be SHORT and
 * must not already contain sentence punctuation; anything else keeps the whole text as the body,
 * which is always safe.
 *
 * A useful side effect: where a title IS found, the em dash that separated it becomes structure
 * instead of punctuation, so the card loses a long dash rather than printing one.
 */
export function splitTipTitle(text: string): { title?: string; body: string } {
  const at = text.indexOf(' — ');
  if (at < 0 || at > MAX_TITLE_LEN) return { body: text };
  const head = text.slice(0, at).trim();
  // A fragment carrying its own sentence punctuation is prose, not a heading.
  if (!head || /[.;:!?]/.test(head)) return { body: text };
  const body = text.slice(at + 3).trim();
  return body ? { title: head, body } : { body: text };
}

/** A plain explanatory tooltip, in the same shell the provenance card uses. */
export function AboutCard({ text }: { text: string }) {
  const { title, body } = splitTipTitle(text);
  return (
    <TipCard label="About" title={title}>
      {/* `whitespace-pre-line` is load-bearing: call sites use \n\n for paragraph breaks and the
          old bare-paragraph rendering honoured them. */}
      <span className="block text-[12px] text-fg-soft leading-relaxed whitespace-pre-line">
        {body}
      </span>
    </TipCard>
  );
}
