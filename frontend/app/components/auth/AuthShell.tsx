'use client'

import Image from 'next/image'

/**
 * The frame every signed-out page sits in: login, `/auth/confirm`, `/set-password`.
 *
 * ⚠⚠ IT EXISTS BECAUSE THESE THREE PAGES WERE THE LAST DARK-THEME SURFACES IN THE APP. They were
 * written before "Azure Blanc" and never converted, so they painted on `bg-scrim` (#0c1118, a
 * near-black) with `text-fg-strong` (#11161d, the LIGHT theme's darkest ink) — ink and ground four
 * points of luminance apart. ⚠ `bg-scrim` survives only where it is a BACKDROP and always with an
 * alpha (the mobile drawer's `bg-scrim/60`, a raw-payload block's `bg-scrim/30`); bare, as a page
 * ground, it is the same mistake the `bg-overlay` note in `CLAUDE.md` already records.
 *
 * ⚠ THE POINT OF ONE COMPONENT IS THAT THE THREE PAGES CANNOT DRIFT AGAIN. They are seen in
 * sequence — request a link, confirm it, choose a password — so a card that changes width, radius
 * or type between steps reads as three different products. Pages own their FIELDS and their
 * SENTENCES; the frame, the mark, the heading scale and the footnote live here.
 */
export default function AuthShell({
  title,
  subtitle,
  children,
  footer,
}: {
  title: string
  /** One line under the heading. Optional — the error card has nothing useful to say here. */
  subtitle?: React.ReactNode
  children: React.ReactNode
  /** Small print under the card, outside it — a mode switch or a reassurance. */
  footer?: React.ReactNode
}) {
  return (
    <div className="relative min-h-screen w-full overflow-hidden bg-page flex items-center justify-center p-6">
      {/* Two soft accent washes. Purely atmospheric, so `aria-hidden` and `pointer-events-none`:
          a decorative div that swallows clicks over a form is the classic version of this bug. */}
      <div aria-hidden className="pointer-events-none absolute inset-0">
        <div className="absolute -top-40 left-1/2 h-[26rem] w-[26rem] -translate-x-1/2 rounded-full bg-accent-300/25 blur-3xl" />
        <div className="absolute -bottom-48 -right-24 h-[24rem] w-[24rem] rounded-full bg-accent-200/40 blur-3xl" />
      </div>

      <div className="relative w-full max-w-sm">
        <div className="flex flex-col items-center gap-3 mb-6">
          <Image
            src="/logo.jpg"
            alt=""
            width={44}
            height={44}
            priority
            className="rounded-xl shadow-sm ring-1 ring-neutral-800/20"
          />
          <span className="text-base font-semibold tracking-tight text-fg-strong">BBTerminal</span>
        </div>

        <div className="bg-card border border-neutral-800/30 rounded-2xl shadow-lg shadow-accent-900/[0.06] p-7 sm:p-8">
          <h1 className="text-lg font-semibold tracking-tight text-fg-strong">{title}</h1>
          {subtitle && <p className="mt-1.5 text-sm text-fg-subtle leading-relaxed">{subtitle}</p>}
          <div className="mt-6">{children}</div>
        </div>

        {footer && (
          <div className="mt-5 text-center text-xs text-fg-faint leading-relaxed">{footer}</div>
        )}
      </div>
    </div>
  )
}

/** One text/password field, so the three pages cannot style their inputs differently. */
export const authFieldClass =
  'w-full bg-inset border border-neutral-700 rounded-lg px-3.5 py-2.5 text-sm text-fg-strong '
  + 'placeholder-fg-faint transition-colors '
  + 'focus:outline-none focus:border-accent-500 focus:ring-2 focus:ring-accent-500/25 focus:bg-card'

export const authLabelClass = 'block text-xs font-medium text-fg-muted mb-1.5'

/**
 * The primary action.
 *
 * ⚠ `text-white`, NOT `text-fg-strong`. `accent-600` is the button FILL in this theme and
 * `fg-strong` is its darkest INK — the pairing the palette exists to prevent, and it was on the
 * login button.
 */
export const authButtonClass =
  'w-full bg-accent-600 hover:bg-accent-500 active:bg-accent-900 disabled:opacity-50 '
  + 'disabled:cursor-not-allowed text-white text-sm font-medium rounded-lg px-4 py-2.5 '
  + 'shadow-sm transition-colors'

/** The way back to /login from a dead end — secondary, so it never competes with a real submit. */
export const authSecondaryButtonClass =
  'block w-full text-center bg-card hover:bg-overlay/[0.04] border border-neutral-700 '
  + 'text-fg text-sm font-medium rounded-lg px-4 py-2.5 transition-colors'

/**
 * An error or a confirmation, as a tinted block rather than a line of coloured text.
 *
 * ⚠ THE SENTENCES HERE ARE LONG BY DESIGN — every one of them names what to DO next
 * (`lib/authError.ts`), and a two-line instruction set in 12px red on white reads as a validation
 * complaint rather than an answer. A block with its own ground is what makes it look like a reply.
 *
 * ⚠ `role="alert"` on the error only. An info block is the expected outcome of pressing the button
 * the person just pressed; announcing it as an alert interrupts a screen reader to state the
 * obvious.
 */
export function AuthNotice({ kind, children }: { kind: 'error' | 'info', children: React.ReactNode }) {
  const error = kind === 'error'
  return (
    <div
      role={error ? 'alert' : undefined}
      className={`rounded-lg border px-3.5 py-3 text-xs leading-relaxed ${
        error
          ? 'bg-neg-100 border-neg-200 text-neg-400'
          : 'bg-accent-200/50 border-accent-300/60 text-accent-400'
      }`}
    >
      {children}
    </div>
  )
}
