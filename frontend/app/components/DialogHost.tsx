'use client';

import { useEffect, useRef, useState } from 'react';
import { dialogStore, resolveOpen } from '../../lib/dialog';

export default function DialogHost() {
  const open = dialogStore.use((s) => s.open);
  const [value, setValue] = useState('');
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (open?.kind === 'prompt') {
      setValue(open.defaultValue ?? '');
      // Focus and select after render
      requestAnimationFrame(() => {
        inputRef.current?.focus();
        inputRef.current?.select();
      });
    }
  }, [open]);

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        e.preventDefault();
        cancel();
      } else if (e.key === 'Enter' && open.kind !== 'prompt') {
        e.preventDefault();
        confirm();
      }
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [open]);

  if (!open) return null;

  function cancel() {
    if (!open) return;
    if (open.kind === 'alert') resolveOpen(undefined);
    else if (open.kind === 'confirm') resolveOpen(false);
    else resolveOpen(null);
  }

  function confirm() {
    if (!open) return;
    if (open.kind === 'alert') resolveOpen(undefined);
    else if (open.kind === 'confirm') resolveOpen(true);
    else resolveOpen(value);
  }

  const confirmClass = open.destructive
    ? 'bg-neg-600 hover:bg-neg-500 text-fg-strong'
    : 'bg-accent-600 hover:bg-accent-500 text-fg-strong';

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-scrim/60 backdrop-blur-sm"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) cancel();
      }}
    >
      <div className="w-full max-w-md mx-4 bg-card border border-neutral-800/60 rounded-xl shadow-2xl">
        <div className="px-5 pt-4 pb-3">
          {open.title && (
            <div className="text-sm font-semibold text-fg-strong mb-1">{open.title}</div>
          )}
          <div className="text-sm text-fg-soft whitespace-pre-wrap">{open.message}</div>
          {open.kind === 'prompt' && (
            <input
              ref={inputRef}
              value={value}
              onChange={(e) => setValue(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === 'Enter') {
                  e.preventDefault();
                  confirm();
                }
              }}
              placeholder={open.placeholder}
              className="mt-3 w-full bg-page border border-neutral-700 rounded-lg px-3 py-2 text-sm text-fg-strong placeholder-fg-subtle focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none"
            />
          )}
        </div>
        <div className="px-5 py-3 border-t border-neutral-800/60 flex items-center justify-end gap-2">
          {open.kind !== 'alert' && (
            <button
              onClick={cancel}
              className="px-3 py-1.5 rounded-lg text-sm text-fg-soft hover:bg-overlay/5 transition-colors"
            >
              {open.cancelLabel ?? 'Cancel'}
            </button>
          )}
          <button
            onClick={confirm}
            className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-colors ${confirmClass}`}
          >
            {open.confirmLabel ?? 'OK'}
          </button>
        </div>
      </div>
    </div>
  );
}
