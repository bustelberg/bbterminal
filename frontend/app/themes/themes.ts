/**
 * Throwaway theme-preview definitions for the /themes picker. Each theme is
 * just a bag of CSS custom properties (`--t-*`) that the shared `Showcase`
 * component reads via `var(--t-…)` inline styles — so adding a candidate
 * theme is a one-object edit here, no component changes.
 *
 * These pages are NOT wired into the real app theme (that lives in
 * `globals.css`'s `@theme` block). They exist purely to compare directions
 * side by side; delete `app/themes/` once a direction is chosen.
 */
export type Theme = {
  slug: string;
  name: string;
  tagline: string;
  /** Optional font stack applied to the whole preview. */
  font?: string;
  /** Maps directly to `style` on the preview root — the `--t-*` tokens the
   * Showcase renders against. */
  vars: Record<string, string>;
};

const GEIST_SANS = "'Geist', ui-sans-serif, system-ui, -apple-system, sans-serif";
const GEIST_MONO = "'Geist Mono', ui-monospace, SFMono-Regular, Menlo, monospace";

export const THEMES: Theme[] = [
  {
    slug: 'midnight-terminal',
    name: 'Midnight Terminal',
    tagline: 'Near-black trading desk with an amber accent and tight, monospaced numerals. Maximum data density.',
    font: GEIST_MONO,
    vars: {
      '--t-bg': '#07090d',
      '--t-sidebar': '#0b0e14',
      '--t-card': '#10141c',
      '--t-card-alt': '#0d111a',
      '--t-elevated': '#171c27',
      '--t-inset': '#0a0d13',
      '--t-border': '#222a36',
      '--t-border-strong': '#333d4d',
      '--t-fg': '#e8eaef',
      '--t-fg-muted': '#99a3b3',
      '--t-fg-subtle': '#606c7d',
      '--t-accent': '#ffa033',
      '--t-accent-2': '#34d399',
      '--t-accent-fg': '#0a0d13',
      '--t-accent-soft': 'rgba(255,160,51,0.14)',
      '--t-pos': '#31d07f',
      '--t-neg': '#ff5c5c',
      '--t-warn': '#ffd23f',
      '--t-radius': '3px',
      '--t-shadow': '0 1px 0 rgba(255,255,255,0.03)',
    },
  },
  {
    slug: 'paper-light',
    name: 'Paper Light',
    tagline: 'Warm off-white institutional report. Ink-on-paper text, restrained blue accent, soft card shadows.',
    font: GEIST_SANS,
    vars: {
      '--t-bg': '#f4f2ec',
      '--t-sidebar': '#fbfaf7',
      '--t-card': '#ffffff',
      '--t-card-alt': '#faf9f5',
      '--t-elevated': '#ffffff',
      '--t-inset': '#efece4',
      '--t-border': '#e4e1d7',
      '--t-border-strong': '#d3cfc2',
      '--t-fg': '#20242e',
      '--t-fg-muted': '#586072',
      '--t-fg-subtle': '#8b93a3',
      '--t-accent': '#2563eb',
      '--t-accent-2': '#0e9f6e',
      '--t-accent-fg': '#ffffff',
      '--t-accent-soft': 'rgba(37,99,235,0.10)',
      '--t-pos': '#0f8a4f',
      '--t-neg': '#c92f2f',
      '--t-warn': '#b07a0c',
      '--t-radius': '10px',
      '--t-shadow': '0 1px 2px rgba(20,24,40,0.06), 0 6px 16px rgba(20,24,40,0.05)',
    },
  },
  {
    slug: 'nordic-frost',
    name: 'Nordic Frost',
    tagline: 'Calm desaturated slate (Nord palette). Frost-cyan accent, muted greens/reds — easy on the eyes for long sessions.',
    font: GEIST_SANS,
    vars: {
      '--t-bg': '#2e3440',
      '--t-sidebar': '#2b303b',
      '--t-card': '#3b4252',
      '--t-card-alt': '#373e4d',
      '--t-elevated': '#434c5e',
      '--t-inset': '#353b48',
      '--t-border': '#4c566a',
      '--t-border-strong': '#5c6883',
      '--t-fg': '#eceff4',
      '--t-fg-muted': '#cad2de',
      '--t-fg-subtle': '#9aa4b8',
      '--t-accent': '#88c0d0',
      '--t-accent-2': '#81a1c1',
      '--t-accent-fg': '#2e3440',
      '--t-accent-soft': 'rgba(136,192,208,0.16)',
      '--t-pos': '#a3be8c',
      '--t-neg': '#bf616a',
      '--t-warn': '#ebcb8b',
      '--t-radius': '10px',
      '--t-shadow': '0 2px 10px rgba(0,0,0,0.22)',
    },
  },
  {
    slug: 'neon-synth',
    name: 'Neon Synth',
    tagline: 'Deep violet night with magenta→cyan neon, glowing edges and large radii. Bold and high-contrast.',
    font: GEIST_SANS,
    vars: {
      '--t-bg': '#0c0a1d',
      '--t-sidebar': '#120e2b',
      '--t-card': '#181235',
      '--t-card-alt': '#15102f',
      '--t-elevated': '#211748',
      '--t-inset': '#100c26',
      '--t-border': '#312a5c',
      '--t-border-strong': '#4a3f86',
      '--t-fg': '#efeaff',
      '--t-fg-muted': '#b6acdf',
      '--t-fg-subtle': '#7e74b2',
      '--t-accent': '#ff3bd4',
      '--t-accent-2': '#2af0ff',
      '--t-accent-fg': '#11061f',
      '--t-accent-soft': 'rgba(255,59,212,0.16)',
      '--t-pos': '#45f0a0',
      '--t-neg': '#ff4d6d',
      '--t-warn': '#ffd84d',
      '--t-radius': '14px',
      '--t-shadow': '0 0 0 1px rgba(255,59,212,0.10), 0 10px 36px rgba(124,58,237,0.28)',
    },
  },
];

export function themeBySlug(slug: string): Theme | undefined {
  return THEMES.find((t) => t.slug === slug);
}
