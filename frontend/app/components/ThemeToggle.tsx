'use client';

import { useSyncExternalStore } from 'react';

// Runtime light/dark switch. Dark is the default (no attribute); light is
// `data-theme="light"` on <html>. The no-FOUC script in app/layout.tsx applies
// the stored choice before paint; this button just flips + persists it. The
// whole palette lives in app/globals.css — nothing here knows colours.
//
// The DOM (`data-theme` attribute) is the source of truth, read via
// useSyncExternalStore so the server snapshot ('dark') and the post-hydration
// client value reconcile without a mismatch warning, and so a change in
// another tab re-renders this one.
type Theme = 'dark' | 'light';
const STORAGE_KEY = 'bb-theme';
const listeners = new Set<() => void>();

function currentTheme(): Theme {
  if (typeof document === 'undefined') return 'dark';
  return document.documentElement.getAttribute('data-theme') === 'light' ? 'light' : 'dark';
}

function setTheme(next: Theme) {
  const el = document.documentElement;
  if (next === 'light') el.setAttribute('data-theme', 'light');
  else el.removeAttribute('data-theme');
  try { localStorage.setItem(STORAGE_KEY, next); } catch { /* private mode */ }
  listeners.forEach((l) => l());
}

function subscribe(cb: () => void) {
  listeners.add(cb);
  window.addEventListener('storage', cb); // reflect changes from other tabs
  return () => { listeners.delete(cb); window.removeEventListener('storage', cb); };
}

export default function ThemeToggle() {
  const theme = useSyncExternalStore<Theme>(subscribe, currentTheme, () => 'dark');
  const next: Theme = theme === 'dark' ? 'light' : 'dark';

  return (
    <button
      type="button"
      onClick={() => setTheme(next)}
      className="w-full px-3 py-2.5 rounded-lg text-sm font-medium text-fg-subtle hover:text-fg-strong hover:bg-overlay/5 transition-colors text-left flex items-center gap-2"
      title={`Switch to ${next} theme`}
    >
      {/* Icon for the theme you'd switch TO. */}
      <span aria-hidden className="shrink-0 text-fg-muted">
        {theme === 'light' ? (
          // moon
          <svg className="w-4 h-4" viewBox="0 0 20 20" fill="currentColor"><path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z" /></svg>
        ) : (
          // sun
          <svg className="w-4 h-4" viewBox="0 0 20 20" fill="currentColor"><path fillRule="evenodd" d="M10 2a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm4 8a4 4 0 11-8 0 4 4 0 018 0zm-.464 4.95l.707.707a1 1 0 001.414-1.414l-.707-.707a1 1 0 00-1.414 1.414zm2.12-10.607a1 1 0 010 1.414l-.706.707a1 1 0 11-1.414-1.414l.707-.707a1 1 0 011.414 0zM17 11a1 1 0 100-2h-1a1 1 0 100 2h1zm-7 4a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM5.05 6.464A1 1 0 106.465 5.05l-.708-.707a1 1 0 00-1.414 1.414l.707.707zm1.414 8.486l-.707.707a1 1 0 01-1.414-1.414l.707-.707a1 1 0 011.414 1.414zM4 11a1 1 0 100-2H3a1 1 0 000 2h1z" clipRule="evenodd" /></svg>
        )}
      </span>
      {theme === 'light' ? 'Dark theme' : 'Light theme'}
    </button>
  );
}
