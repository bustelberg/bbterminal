'use client';

/**
 * Tiny inline spinning-circle indicator. Use this for per-button or
 * per-row async work (delete in flight, dropdown row loading, save in
 * flight, etc.). For dead text replacement like "Loading…" prefer
 * `<LoadingDots />` — the pulsing dots match the rest of the UI's
 * loading vocabulary.
 *
 * Single shared definition; previously duplicated in
 * `MomentumBacktester.tsx` + `AirsPortfolioUpload.tsx`.
 */
/**
 * Two ways to size + color the spinner — pick whichever fits the call
 * site:
 *   <Spinner />                              defaults: 12px, indigo-400
 *   <Spinner size={20} />                    explicit pixel size
 *   <Spinner className="h-4 w-4 text-gray-400" />  Tailwind classes
 *
 * The `className` form wins when both are provided.
 */
export default function Spinner({
  size = 12,
  className,
}: {
  size?: number;
  className?: string;
}) {
  const useClassName = !!className;
  return (
    <svg
      className={useClassName ? `animate-spin ${className}` : 'animate-spin text-indigo-400'}
      style={useClassName ? undefined : { width: size, height: size }}
      viewBox="0 0 24 24"
      fill="none"
      aria-label="Loading"
    >
      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
    </svg>
  );
}
