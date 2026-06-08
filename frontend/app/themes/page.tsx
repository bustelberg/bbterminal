import Link from 'next/link';
import { THEMES } from './themes';
import { LUX_THEMES } from './lux-themes';

/** Theme picker index — swatch + link to each full-screen preview, grouped
 * into the solid "institutional" set and the gradient/glass "web3 / luxury"
 * set. Throwaway scaffolding for choosing a direction; delete `app/themes/`
 * once decided. Renders inside the current app theme. */
export default function ThemesIndex() {
  return (
    <div className="px-8 py-8 max-w-5xl">
      <div className="mb-8">
        <h1 className="text-2xl font-semibold text-fg-strong mb-2">Theme previews</h1>
        <p className="text-sm text-fg-muted leading-relaxed max-w-2xl">
          Each theme is a full mock dashboard (chrome, KPIs, a gains/losses
          table, charts, buttons, inputs, badges, alerts, typography) so you can
          judge every surface. Open one to see it edge-to-edge.
        </p>
      </div>

      <Section
        title="Solid · institutional"
        sub="Opaque surfaces, crisp data density."
        items={THEMES.map((t) => ({ slug: t.slug, name: t.name, tagline: t.tagline, vars: t.vars }))}
      />

      <div className="mt-10">
        <Section
          title="Gradient · web3 / luxury"
          sub="Frosted glass, gradient accents, hairline borders, glow."
          items={LUX_THEMES.map((t) => ({ slug: t.slug, name: t.name, tagline: t.tagline, vars: t.vars, gradient: true }))}
        />
      </div>
    </div>
  );
}

type Item = { slug: string; name: string; tagline: string; vars: Record<string, string>; gradient?: boolean };

function Section({ title, sub, items }: { title: string; sub: string; items: Item[] }) {
  return (
    <section>
      <div className="flex items-baseline gap-3 mb-3">
        <h2 className="text-xs uppercase tracking-wider text-fg-soft font-medium">{title}</h2>
        <span className="text-xs text-fg-subtle">{sub}</span>
      </div>
      <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
        {items.map((t) => (
          <Link
            key={t.slug}
            href={`/themes/${t.slug}`}
            className="group block rounded-xl border border-neutral-800/40 overflow-hidden hover:border-accent-500/40 transition-colors"
          >
            <div
              className="flex h-20 items-center gap-2 px-4"
              style={{
                background: t.vars['--t-bg'],
                backgroundImage: t.gradient ? t.vars['--t-mesh'] : undefined,
              }}
            >
              <span
                className="inline-block h-8 w-8 rounded-lg"
                style={{ backgroundImage: t.gradient ? t.vars['--t-grad'] : undefined, background: t.gradient ? undefined : t.vars['--t-accent'] }}
              />
              <Swatch c={t.vars['--t-pos']} />
              <Swatch c={t.vars['--t-neg']} />
              <Swatch c={t.vars['--t-warn']} />
              <div
                className="ml-auto h-10 w-24 rounded-lg"
                style={{
                  background: t.gradient ? (t.vars['--t-glass'] ?? 'transparent') : t.vars['--t-card'],
                  border: `1px solid ${t.gradient ? t.vars['--t-hairline'] : t.vars['--t-border']}`,
                }}
              />
            </div>
            <div className="p-4 bg-card">
              <div className="flex items-center justify-between mb-1">
                <h3 className="text-base font-semibold text-fg-strong group-hover:text-accent-400 transition-colors">{t.name}</h3>
                <span className="text-xs text-accent-400 shrink-0 ml-3">Open preview →</span>
              </div>
              <p className="text-sm text-fg-muted leading-relaxed">{t.tagline}</p>
            </div>
          </Link>
        ))}
      </div>
    </section>
  );
}

function Swatch({ c }: { c: string }) {
  return <span className="inline-block h-7 w-7 rounded-md" style={{ background: c }} />;
}
