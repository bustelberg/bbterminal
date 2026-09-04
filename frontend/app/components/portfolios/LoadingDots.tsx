'use client';

/**
 * A CELL THAT IS STILL WAITING — one dot, two, three, round again.
 *
 * ⚠⚠ IT MOVES BECAUSE A MOTIONLESS `…` DOES NOT SAY WHICH ABSENCE IT IS (2026-09-03, on request:
 * "the dots in this table should be dynamic when loading"). The `Tables` tab prints `—` for
 * "measured, and there is no answer" and `…` for "not measured yet" — a dash and an ellipsis
 * sitting motionless in the same column, indistinguishable at a glance, on exactly the screen
 * where the difference decides whether you go looking for a bug. Movement is the whole signal;
 * nothing else in the row animates.
 *
 * ⚠⚠ AND THE DOTS EXISTED ALREADY, IN THE ONE FILE NOBODY SEES. `CagrTable.tsx` grew a timer-driven
 * `Dots` for this same request while the table that actually renders — `TablesTab` — kept its
 * static ellipsis; `CagrTable`'s default export is imported nowhere (only its `CAGR_BENCHMARKS`
 * constant is). So the fix had been written and shipped to a dead component. One implementation
 * now, in a file whose name says what it is.
 *
 * ⚠ THE ANIMATION IS CSS (`.loading-dots` in `globals.css`), NOT A `setInterval`. A loading table
 * holds up to seventy of these: a timer each is seventy React re-renders a second, and they drift
 * out of phase because each starts when its own cell mounted — a column of dots blinking at
 * random reads as a glitch, not as progress.
 *
 * ⚠ `aria-hidden`, the same as every `.loading-bar` caller. Seventy live regions announcing
 * "loading" is worse for a screen reader than the silence it replaces; the panel says once that it
 * is loading.
 *
 * ⚠⚠ IT MUST ONLY BE RENDERED WHILE SOMETHING IS ACTUALLY COMING. Dots that keep moving after a
 * fetch has FAILED, or over a value that will never exist (a bank has no gross profit line, so
 * that row's two sides share no year), promise an arrival — which is a worse lie than the static
 * ellipsis it replaced. Every call site gates on its own row's payloads plus the absence of an
 * error; see `TablesTab.arriving`.
 */
export default function LoadingDots() {
  return (
    <span className="loading-dots" aria-hidden>
      <span>.</span>
      <span>.</span>
      <span>.</span>
    </span>
  );
}
