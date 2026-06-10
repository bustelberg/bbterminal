import { createStore } from './store';

export type DialogKind = 'alert' | 'confirm' | 'prompt' | 'loading';

export type DialogRequest = {
  id: number;
  kind: DialogKind;
  title?: string;
  message: string;
  // confirm
  confirmLabel?: string;
  cancelLabel?: string;
  destructive?: boolean;
  // prompt
  defaultValue?: string;
  placeholder?: string;
  // When set on a prompt/confirm, confirming does NOT close the modal — it
  // swaps the SAME modal to a spinner showing this message (and resolves the
  // promise with the value). The caller then runs its async work and finishes
  // with `dialog.alert(...)`, which swaps the spinner to the result. This
  // strings prompt → loading → result into one continuous popup (no flicker).
  chainLoading?: string;
};

type DialogState = {
  open: DialogRequest | null;
};

const store = createStore<DialogState>({ open: null });
export const dialogStore = store;

let nextId = 1;
let resolver: ((value: unknown) => void) | null = null;

function present<T>(req: Omit<DialogRequest, 'id'>): Promise<T> {
  // If something is already open, resolve the previous request as cancelled.
  if (resolver) {
    const r = resolver;
    resolver = null;
    r(req.kind === 'prompt' ? null : false);
  }
  const id = nextId++;
  store.set({ open: { id, ...req } });
  return new Promise<T>((resolve) => {
    resolver = resolve as (v: unknown) => void;
  });
}

export function resolveOpen(value: unknown) {
  const r = resolver;
  resolver = null;
  store.set({ open: null });
  if (r) r(value);
}

/** Resolve the open prompt/confirm's promise with `value` but, instead of
 * closing, swap the SAME modal to a non-dismissable loading spinner. The
 * caller continues and finishes with `dialog.alert(...)` (which swaps the
 * spinner to the result). Used by DialogHost when a request has `chainLoading`. */
export function chainToLoading(value: unknown, message: string, title?: string) {
  const r = resolver;
  resolver = null;
  const id = nextId++;
  store.set({ open: { id, kind: 'loading', message, title } });
  if (r) r(value);
}

export const dialog = {
  alert: (
    message: string,
    opts?: { title?: string; confirmLabel?: string },
  ): Promise<void> =>
    present<void>({
      kind: 'alert',
      message,
      title: opts?.title,
      confirmLabel: opts?.confirmLabel ?? 'OK',
    }),

  confirm: (
    message: string,
    opts?: {
      title?: string;
      confirmLabel?: string;
      cancelLabel?: string;
      destructive?: boolean;
      chainLoading?: string;
    },
  ): Promise<boolean> =>
    present<boolean>({
      kind: 'confirm',
      message,
      title: opts?.title,
      confirmLabel: opts?.confirmLabel ?? 'OK',
      cancelLabel: opts?.cancelLabel ?? 'Cancel',
      destructive: opts?.destructive,
      chainLoading: opts?.chainLoading,
    }),

  prompt: (
    message: string,
    opts?: {
      title?: string;
      defaultValue?: string;
      placeholder?: string;
      confirmLabel?: string;
      cancelLabel?: string;
      chainLoading?: string;
    },
  ): Promise<string | null> =>
    present<string | null>({
      kind: 'prompt',
      message,
      title: opts?.title,
      defaultValue: opts?.defaultValue,
      placeholder: opts?.placeholder,
      confirmLabel: opts?.confirmLabel ?? 'OK',
      cancelLabel: opts?.cancelLabel ?? 'Cancel',
      chainLoading: opts?.chainLoading,
    }),

  /** Force-close whatever is open (e.g. to dismiss a chained loading spinner
   * when the caller bails after the prompt). Resolves any pending promise as
   * cancelled so awaiters don't hang. */
  close: () => {
    const r = resolver;
    resolver = null;
    store.set({ open: null });
    if (r) r(null);
  },
};
