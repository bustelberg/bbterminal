/** The same membership marker in summary lists and expanded holdings. */
export default function BenchmarkMembershipMark({ member, label }: { member?: boolean; label: string }) {
  return member ? <span role="img" aria-label={label} title={label}
    className="inline-block h-2 w-2 shrink-0 rounded-full bg-accent-500 ring-2 ring-accent-500/25" /> : null;
}
