'use client';

import Link, { useLinkStatus } from 'next/link';
import { useEffect, useRef, useState, type ComponentProps } from 'react';
import { trace } from '../../lib/debugTrace';

function PendingIndicator({ href }: { href: ComponentProps<typeof Link>['href'] }) {
  const { pending } = useLinkStatus();
  const started = useRef<number | null>(null);
  useEffect(() => {
    if (pending) started.current = performance.now();
    else if (started.current != null) {
      trace('navigation', 'route committed', { path: typeof href === 'string' ? href.split('?')[0] : href.pathname, ms: Math.round(performance.now() - started.current) });
      started.current = null;
    }
  }, [pending, href]);
  return <span aria-hidden="true" className={`pointer-events-none absolute right-2 top-1/2 -mt-1.5 h-3 w-3 rounded-full border-2 border-current border-t-transparent ${pending ? 'animate-spin opacity-100' : 'opacity-0'}`} />;
}

/** Prefetch cheap loading shells normally; warm the full page only on navigation intent. */
export default function NavigationLink({ children, className, ...props }: ComponentProps<typeof Link>) {
  const [intent, setIntent] = useState(false);
  return <Link {...props} prefetch={intent ? true : null}
    onMouseEnter={() => setIntent(true)} onFocus={() => setIntent(true)} onTouchStart={() => setIntent(true)}
    className={`relative pr-6 ${className ?? ''}`}>
    {children}<PendingIndicator href={props.href} />
  </Link>;
}
