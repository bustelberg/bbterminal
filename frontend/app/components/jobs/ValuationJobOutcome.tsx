export default function ValuationJobOutcome({ message }: { message: string }) {
  const parts = message.match(/^(\d+\/\d+ (?:companies refreshed|bedrijven vernieuwd)); (\d+ (?:failed|mislukt))\.([\s\S]*)$/);
  if (!parts) return <>{message}</>;
  return <>
    <span className={Number.parseInt(parts[1], 10) > 0 ? 'text-pos-400' : 'text-fg-muted'}>{parts[1]}</span>
    {'; '}
    <span className={Number.parseInt(parts[2], 10) > 0 ? 'text-neg-400' : 'text-fg-muted'}>{parts[2]}</span>
    {'.'}{parts[3]}
  </>;
}
