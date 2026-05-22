/**
 * Two tiny hooks that consolidate the "close-on-outside-click" /
 * "close-on-Escape" dropdown boilerplate that was previously inlined
 * in 6+ components (each ~10 lines of identical useEffect + document
 * listener + cleanup).
 *
 * Both are no-ops while `enabled` is false, so they can be called
 * unconditionally at the top of a component and only "activate" when
 * the open-state is true (matching the original pattern's behavior).
 */
import { type RefObject, useEffect } from 'react';

/**
 * Calls `handler` whenever a mousedown happens outside `ref`'s element.
 * Pass the dropdown-open state as `enabled` so the listener is only
 * attached while the dropdown is visible.
 *
 *   const ref = useRef<HTMLDivElement>(null);
 *   const [open, setOpen] = useState(false);
 *   useClickOutside(ref, () => setOpen(false), open);
 */
export function useClickOutside<T extends HTMLElement>(
  ref: RefObject<T | null>,
  handler: () => void,
  enabled: boolean = true,
): void {
  useEffect(() => {
    if (!enabled) return;
    const onMouseDown = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        handler();
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [ref, handler, enabled]);
}

/**
 * Calls `handler` when the user presses Escape. Same `enabled` pattern
 * as `useClickOutside` so it can be paired with it on the same
 * dropdown for the full "close on outside click or Escape" UX.
 */
export function useEscapeKey(handler: () => void, enabled: boolean = true): void {
  useEffect(() => {
    if (!enabled) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') handler();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, [handler, enabled]);
}
