import { createStore } from './store';

export type DialogKind = 'alert' | 'confirm' | 'prompt';

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
    },
  ): Promise<boolean> =>
    present<boolean>({
      kind: 'confirm',
      message,
      title: opts?.title,
      confirmLabel: opts?.confirmLabel ?? 'OK',
      cancelLabel: opts?.cancelLabel ?? 'Cancel',
      destructive: opts?.destructive,
    }),

  prompt: (
    message: string,
    opts?: {
      title?: string;
      defaultValue?: string;
      placeholder?: string;
      confirmLabel?: string;
      cancelLabel?: string;
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
    }),
};
