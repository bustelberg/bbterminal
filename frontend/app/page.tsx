import { cookies } from 'next/headers';
import { createClient } from '../lib/supabase/server';
import { isUserAllowedPath } from '../lib/userAllowedPaths';
import HomeTiles from './components/home/HomeTiles';
import { HOME_TILE_ORDER } from './components/home/homeTileKeys';

/**
 * The home page.
 *
 * ⚠⚠ SERVER COMPONENT FOR THE ROLE, CLIENT COMPONENT FOR THE WORDS. The session and the `view_as`
 * cookie are server facts and decide WHICH tiles exist; the language is a client fact
 * (`localStorage`, via `useLang`) and decides what they SAY. Rendering the copy here would leave
 * the page in English whatever the sidebar switch is set to — which is exactly the failure
 * `i18n.ts` warns about, a control that moves nothing reading as broken rather than as unfinished.
 *
 * ⚠ THE TILE LIST AND ITS COPY LIVE IN `homeCopy.ts`, not here. They used to be one array in this
 * file, so adding a page put its English copy in one place and its Dutch copy nowhere; keyed by
 * href in a `Record<HomeTileKey, …>`, a missing translation is now a compile error.
 */
export default async function Home() {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();
  const cookieStore = await cookies();

  // Match the middleware's role + view-as logic so the home tiles always
  // reflect what the user can *actually* navigate to.
  const realRole = (user?.app_metadata as { role?: string } | undefined)?.role === 'admin' ? 'admin' : 'user';
  const viewAs = cookieStore.get('view_as')?.value;
  const effectiveRole: 'admin' | 'user' = realRole === 'admin' && viewAs !== 'user' ? 'admin' : 'user';

  // Derive visibility from the SAME allow-list the route gate uses, so the home
  // page can never advertise a page the user can't open (it used to drift).
  const visible = effectiveRole === 'admin'
    ? HOME_TILE_ORDER
    : HOME_TILE_ORDER.filter((href) => isUserAllowedPath(href));

  return <HomeTiles hrefs={visible} />;
}
