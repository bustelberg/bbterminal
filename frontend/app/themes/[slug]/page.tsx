import { notFound } from 'next/navigation';
import { THEMES, themeBySlug } from '../themes';
import { LUX_THEMES, luxBySlug } from '../lux-themes';
import Showcase from '../Showcase';
import ShowcaseLux from '../ShowcaseLux';

/** Full-screen preview of one theme — solid (`Showcase`) or glass/gradient
 * (`ShowcaseLux`) depending on which set the slug belongs to. */
export default async function ThemePreviewPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = await params;
  const solid = themeBySlug(slug);
  if (solid) return <Showcase theme={solid} />;
  const lux = luxBySlug(slug);
  if (lux) return <ShowcaseLux theme={lux} />;
  notFound();
}

// Pre-render every known slug across both sets.
export function generateStaticParams() {
  return [...THEMES, ...LUX_THEMES].map((t) => ({ slug: t.slug }));
}
