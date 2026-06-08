/**
 * "Flagship / $100k" theme candidates — the classy dark direction taken to
 * full production polish: animated conic-gradient borders, film-grain + glow
 * layered backgrounds, gradient display type, sheen-swept CTAs. Richer token
 * set (adds the conic stops `--t-c1/2/3`); rendered by `ShowcasePrestige`.
 *
 * Throwaway /themes scaffolding — delete `app/themes/` once decided.
 */
export type PrestigeTheme = {
  slug: string;
  name: string;
  tagline: string;
  font?: string;
  vars: Record<string, string>;
};

const SANS = "'Geist', ui-sans-serif, system-ui, -apple-system, sans-serif";

export const PRESTIGE_THEMES: PrestigeTheme[] = [
  {
    slug: 'obsidian-atelier',
    name: 'Obsidian Atelier',
    tagline: 'Obsidian black with a slowly-rotating sapphire→platinum conic edge, film grain, drifting glow and gradient display type. The full couture treatment.',
    font: SANS,
    vars: {
      '--t-bg': '#08090c',
      '--t-card': '#0f1116',
      '--t-card-2': '#13161f',
      '--t-elevated': '#16191f',
      '--t-inset': '#0b0c10',
      '--t-divider': 'rgba(255,255,255,0.07)',
      '--t-fg': '#f4f5f8',
      '--t-fg-muted': 'rgba(244,245,248,0.62)',
      '--t-fg-subtle': 'rgba(244,245,248,0.40)',
      '--t-accent': '#9db8ff',
      '--t-accent-fg': '#08090c',
      '--t-gold': '#d9c089',
      '--t-pos': '#4fd1a0',
      '--t-neg': '#ff6b81',
      '--t-warn': '#e8c879',
      '--t-c1': '#6f8cff',
      '--t-c2': '#eaeefb',
      '--t-c3': '#2b3550',
      '--t-glow1': 'rgba(111,140,255,0.22)',
      '--t-glow2': 'rgba(217,192,137,0.10)',
      '--t-grad': 'linear-gradient(120deg, #b9c6e6 0%, #9db8ff 50%, #eaeefb 100%)',
      '--t-radius': '16px',
    },
  },
  {
    slug: 'bleu-royale',
    name: 'Bleu Royale',
    tagline: 'Deep royal midnight blue with a rotating sapphire→silver conic edge and champagne micro-accents. Rich, calm, expensive — a private-office terminal.',
    font: SANS,
    vars: {
      '--t-bg': '#060c1c',
      '--t-card': '#0c1631',
      '--t-card-2': '#0f1c3d',
      '--t-elevated': '#122146',
      '--t-inset': '#0a1228',
      '--t-divider': 'rgba(255,255,255,0.08)',
      '--t-fg': '#eaf0fb',
      '--t-fg-muted': 'rgba(234,240,251,0.64)',
      '--t-fg-subtle': 'rgba(234,240,251,0.42)',
      '--t-accent': '#7aa2ff',
      '--t-accent-fg': '#060c1c',
      '--t-gold': '#e3c98a',
      '--t-pos': '#3fe0a1',
      '--t-neg': '#ff6b81',
      '--t-warn': '#ffd166',
      '--t-c1': '#3b82f6',
      '--t-c2': '#cfe0ff',
      '--t-c3': '#16245a',
      '--t-glow1': 'rgba(59,130,246,0.26)',
      '--t-glow2': 'rgba(227,201,138,0.10)',
      '--t-grad': 'linear-gradient(120deg, #9db8ff 0%, #5b8cff 50%, #cfe0ff 100%)',
      '--t-radius': '18px',
    },
  },
];

export function prestigeBySlug(slug: string): PrestigeTheme | undefined {
  return PRESTIGE_THEMES.find((t) => t.slug === slug);
}
