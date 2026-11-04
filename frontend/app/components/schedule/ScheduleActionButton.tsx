import type { ComponentPropsWithoutRef } from 'react';

export default function ScheduleActionButton({ children, ...props }: ComponentPropsWithoutRef<'button'>) {
  return (
    <button type="button" {...props}
      className="inline-flex h-7 w-24 shrink-0 cursor-pointer items-center justify-center whitespace-nowrap rounded-lg border border-neutral-700 bg-page px-1 text-[10px] text-fg-soft transition-colors hover:border-accent-500/50 hover:text-accent-300 disabled:cursor-default disabled:opacity-50">
      <span aria-live="polite">{children}</span>
    </button>
  );
}
