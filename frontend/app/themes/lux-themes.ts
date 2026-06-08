/**
 * "Web3 / luxury / futuristic" theme candidates — gradient meshes, frosted
 * glass surfaces, hairline borders, glow. Richer token set than the solid
 * `themes.ts` (adds `--t-mesh`, `--t-glass`, `--t-hairline`, `--t-grad`, …);
 * rendered by the glass-tuned `ShowcaseLux` component.
 *
 * Throwaway scaffolding for the /themes picker — delete `app/themes/` once a
 * direction is chosen.
 */
export type LuxTheme = {
  slug: string;
  name: string;
  tagline: string;
  light?: boolean;
  font?: string;
  vars: Record<string, string>;
};

const SANS = "'Geist', ui-sans-serif, system-ui, -apple-system, sans-serif";

export const LUX_THEMES: LuxTheme[] = [
  {
    slug: 'aurora-glass',
    name: 'Aurora Glass',
    tagline: 'Frosted glass over a violet→cyan aurora. Hairline borders, soft glow, big radii — the modern web3 dashboard look.',
    font: SANS,
    vars: {
      '--t-bg': '#070a16',
      '--t-mesh':
        'radial-gradient(60% 50% at 18% -5%, rgba(124,58,237,0.32), transparent 70%), radial-gradient(50% 50% at 105% 8%, rgba(34,211,238,0.22), transparent 70%), radial-gradient(70% 60% at 50% 115%, rgba(56,189,248,0.18), transparent 70%)',
      '--t-glass': 'rgba(255,255,255,0.045)',
      '--t-glass-strong': 'rgba(255,255,255,0.08)',
      '--t-inset': 'rgba(255,255,255,0.03)',
      '--t-hairline': 'rgba(255,255,255,0.10)',
      '--t-hairline-strong': 'rgba(255,255,255,0.18)',
      '--t-fg': '#eef1ff',
      '--t-fg-muted': 'rgba(238,241,255,0.66)',
      '--t-fg-subtle': 'rgba(238,241,255,0.40)',
      '--t-accent': '#a78bfa',
      '--t-accent-fg': '#0a0a1a',
      '--t-grad': 'linear-gradient(120deg, #a78bfa 0%, #38bdf8 55%, #22d3ee 100%)',
      '--t-grad-soft': 'linear-gradient(120deg, rgba(167,139,250,0.20), rgba(34,211,238,0.14))',
      '--t-pos': '#34e0a1',
      '--t-neg': '#ff6b81',
      '--t-warn': '#ffd166',
      '--t-radius': '18px',
      '--t-glow': '0 0 0 1px rgba(255,255,255,0.05), 0 24px 70px rgba(124,58,237,0.28)',
    },
  },
  {
    slug: 'obsidian-gold',
    name: 'Obsidian Gold',
    tagline: 'True-black luxury. Champagne-gold gradient accents on ultra-thin gold hairlines, deep shadow, restrained glow. Private-bank energy.',
    font: SANS,
    vars: {
      '--t-bg': '#0a0a0a',
      '--t-mesh':
        'radial-gradient(50% 40% at 12% -5%, rgba(212,175,55,0.16), transparent 70%), radial-gradient(45% 45% at 105% 105%, rgba(212,175,55,0.10), transparent 70%)',
      '--t-glass': 'rgba(255,255,255,0.028)',
      '--t-glass-strong': 'rgba(255,255,255,0.05)',
      '--t-inset': 'rgba(255,255,255,0.02)',
      '--t-hairline': 'rgba(212,175,55,0.22)',
      '--t-hairline-strong': 'rgba(212,175,55,0.42)',
      '--t-fg': '#f5f1e6',
      '--t-fg-muted': 'rgba(245,241,230,0.58)',
      '--t-fg-subtle': 'rgba(245,241,230,0.36)',
      '--t-accent': '#d4af37',
      '--t-accent-fg': '#0a0a0a',
      '--t-grad': 'linear-gradient(120deg, #f6e7a8 0%, #d4af37 48%, #b8860b 100%)',
      '--t-grad-soft': 'linear-gradient(120deg, rgba(212,175,55,0.18), rgba(184,134,11,0.10))',
      '--t-pos': '#7ad7a4',
      '--t-neg': '#e9657b',
      '--t-warn': '#e0b84a',
      '--t-radius': '13px',
      '--t-glow': '0 0 0 1px rgba(212,175,55,0.16), 0 22px 60px rgba(0,0,0,0.65)',
    },
  },
  {
    slug: 'iridescent-holo',
    name: 'Iridescent Holo',
    tagline: 'Holographic plum night — magenta→indigo→cyan gradients, glassy panels, oversized radii and vivid glow. Maximal futuristic.',
    font: SANS,
    vars: {
      '--t-bg': '#0b0716',
      '--t-mesh':
        'radial-gradient(55% 45% at 8% -5%, rgba(255,0,170,0.24), transparent 70%), radial-gradient(45% 45% at 95% 6%, rgba(0,229,255,0.20), transparent 70%), radial-gradient(70% 65% at 50% 118%, rgba(124,58,237,0.24), transparent 70%)',
      '--t-glass': 'rgba(255,255,255,0.05)',
      '--t-glass-strong': 'rgba(255,255,255,0.09)',
      '--t-inset': 'rgba(255,255,255,0.03)',
      '--t-hairline': 'rgba(255,255,255,0.12)',
      '--t-hairline-strong': 'rgba(255,255,255,0.22)',
      '--t-fg': '#f3eaff',
      '--t-fg-muted': 'rgba(243,234,255,0.66)',
      '--t-fg-subtle': 'rgba(243,234,255,0.42)',
      '--t-accent': '#d946ef',
      '--t-accent-fg': '#14001f',
      '--t-grad': 'linear-gradient(120deg, #ff5cf4 0%, #9b6bff 45%, #22d3ee 100%)',
      '--t-grad-soft': 'linear-gradient(120deg, rgba(255,92,244,0.18), rgba(34,211,238,0.14))',
      '--t-pos': '#4dffc3',
      '--t-neg': '#ff5c8a',
      '--t-warn': '#ffe066',
      '--t-radius': '22px',
      '--t-glow': '0 0 0 1px rgba(255,255,255,0.08), 0 26px 70px rgba(217,70,239,0.30)',
    },
  },
  {
    slug: 'platinum-mist',
    name: 'Platinum Mist',
    tagline: 'Light-luxury counterpoint — pale platinum glass over a soft indigo→teal mist, fine grey hairlines, airy radii. Sleek and bright.',
    light: true,
    font: SANS,
    vars: {
      '--t-bg': '#eef1f6',
      '--t-mesh':
        'radial-gradient(60% 50% at 0% -5%, rgba(99,102,241,0.12), transparent 70%), radial-gradient(50% 50% at 100% 0%, rgba(236,72,153,0.08), transparent 70%), radial-gradient(60% 60% at 50% 120%, rgba(20,184,166,0.10), transparent 70%)',
      '--t-glass': 'rgba(255,255,255,0.55)',
      '--t-glass-strong': 'rgba(255,255,255,0.80)',
      '--t-inset': 'rgba(20,24,48,0.04)',
      '--t-hairline': 'rgba(20,24,48,0.10)',
      '--t-hairline-strong': 'rgba(20,24,48,0.18)',
      '--t-fg': '#1a1f2e',
      '--t-fg-muted': 'rgba(26,31,46,0.62)',
      '--t-fg-subtle': 'rgba(26,31,46,0.42)',
      '--t-accent': '#6366f1',
      '--t-accent-fg': '#ffffff',
      '--t-grad': 'linear-gradient(120deg, #818cf8 0%, #22d3ee 60%, #34d399 100%)',
      '--t-grad-soft': 'linear-gradient(120deg, rgba(99,102,241,0.14), rgba(34,211,238,0.10))',
      '--t-pos': '#0f9d58',
      '--t-neg': '#d23f3f',
      '--t-warn': '#b8860b',
      '--t-radius': '18px',
      '--t-glow': '0 1px 0 rgba(255,255,255,0.7) inset, 0 22px 54px rgba(31,41,80,0.12)',
    },
  },
];

export function luxBySlug(slug: string): LuxTheme | undefined {
  return LUX_THEMES.find((t) => t.slug === slug);
}
