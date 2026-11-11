export function weeklyAxisTicks(dates: string[], start: string, end: string, width: number, nl: boolean) {
  const left = 65, right = 925;
  const scale = 960 / Math.max(160, width);
  const position = (index: number) => left + (right - left) * (index + .5) / Math.max(1, dates.length);
  const boundaries = (size: number) => dates.flatMap((date, index) => index && date.slice(0, size) !== dates[index - 1].slice(0, size) ? [{ date, x: position(index) }] : []);
  const months = boundaries(7);
  const monthly = months.every((item, index) => !index || item.x - months[index - 1].x >= 38 * scale);
  const format = (date: string) => new Date(`${date}T12:00:00Z`).toLocaleDateString(nl ? 'nl-NL' : 'en-GB', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' });
  const first = monthly ? format(start) : start.slice(0, 4);
  const last = monthly ? format(end) : end.slice(0, 4);
  const ticks: { x: number; label: string; anchor: 'start' | 'middle' | 'end' }[] = [{ x: left, label: first, anchor: 'start' }];
  const textWidth = (label: string) => label.length * 6 * scale;
  let occupied = left + textWidth(first);
  for (const item of monthly ? months : boundaries(4)) {
    const label = monthly ? new Date(`${item.date}T12:00:00Z`).toLocaleDateString(nl ? 'nl-NL' : 'en-GB', { month: 'short', timeZone: 'UTC' }) : item.date.slice(0, 4);
    const half = textWidth(label) / 2;
    if (item.x - half < occupied + 12 * scale || item.x + half > right - textWidth(last) - 12 * scale) continue;
    ticks.push({ x: item.x, label, anchor: 'middle' });
    occupied = item.x + half;
  }
  if ((monthly || !ticks.some(tick => tick.label === last)) && right - textWidth(last) > occupied + 12 * scale) ticks.push({ x: right, label: last, anchor: 'end' });
  return ticks;
}
