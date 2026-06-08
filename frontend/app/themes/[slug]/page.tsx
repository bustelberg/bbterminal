import { notFound } from 'next/navigation';
import { THEMES, themeBySlug } from '../themes';
import { BORDER_THEMES, borderBySlug } from '../border-themes';
import { PRESTIGE_THEMES, prestigeBySlug } from '../prestige-themes';
import Showcase from '../Showcase';
import ShowcaseBorder from '../ShowcaseBorder';
import ShowcasePrestige from '../ShowcasePrestige';

/** Full-screen preview of one theme — solid (`Showcase`), gradient-border
 * (`ShowcaseBorder`), or flagship (`ShowcasePrestige`) depending on which set
 * the slug belongs to. */
export default async function ThemePreviewPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const prestige = prestigeBySlug(slug);
  if (prestige) return <ShowcasePrestige theme={prestige} />;
  const solid = themeBySlug(slug);
  if (solid) return <Showcase theme={solid} />;
  const border = borderBySlug(slug);
  if (border) return <ShowcaseBorder theme={border} />;
  notFound();
}

// Pre-render every known slug across all sets.
export function generateStaticParams() {
  return [...THEMES, ...BORDER_THEMES, ...PRESTIGE_THEMES].map((t) => ({ slug: t.slug }));
}
