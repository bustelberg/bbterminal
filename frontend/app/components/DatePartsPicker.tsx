'use client';

const MONTH_NAMES = [
  'January', 'February', 'March', 'April', 'May', 'June',
  'July', 'August', 'September', 'October', 'November', 'December',
];

const daysInMonth = (year: number, month: number) =>
  new Date(year, month, 0).getDate();

export default function DatePartsPicker({
  value,
  onChange,
  minYear,
  maxYear,
  allowEmpty = false,
}: {
  value: string;
  onChange: (iso: string) => void;
  minYear: number;
  maxYear: number;
  allowEmpty?: boolean;
}) {
  const parts = value.split('-');
  const hasValue = value.length > 0 && parts.length === 3;
  const year = hasValue ? parseInt(parts[0], 10) || maxYear : maxYear;
  const month = hasValue ? parseInt(parts[1], 10) || 1 : 1;
  const day = hasValue ? parseInt(parts[2], 10) || 1 : 1;

  const commit = (y: number, m: number, d: number) => {
    const clampedDay = Math.min(d, daysInMonth(y, m));
    const iso = `${y}-${String(m).padStart(2, '0')}-${String(clampedDay).padStart(2, '0')}`;
    onChange(iso);
  };

  const years: number[] = [];
  for (let y = maxYear; y >= minYear; y--) years.push(y);

  const selectCls =
    'bg-page border border-neutral-700 rounded-lg px-2 py-1.5 text-sm text-fg focus:border-accent-500 focus:ring-1 focus:ring-accent-500/30 outline-none';

  return (
    <div className="flex items-center gap-1.5">
      <select
        value={hasValue ? day : ''}
        onChange={e => {
          if (e.target.value === '') onChange('');
          else commit(year, month, parseInt(e.target.value, 10));
        }}
        className={selectCls}
      >
        {allowEmpty && <option value="">—</option>}
        {Array.from({ length: daysInMonth(year, month) }, (_, i) => i + 1).map(d => (
          <option key={d} value={d}>{d}</option>
        ))}
      </select>
      <select
        value={hasValue ? month : ''}
        onChange={e => {
          if (e.target.value === '') onChange('');
          else commit(year, parseInt(e.target.value, 10), day);
        }}
        className={selectCls}
      >
        {allowEmpty && <option value="">—</option>}
        {MONTH_NAMES.map((name, i) => (
          <option key={name} value={i + 1}>{name}</option>
        ))}
      </select>
      <select
        value={hasValue ? year : ''}
        onChange={e => {
          if (e.target.value === '') onChange('');
          else commit(parseInt(e.target.value, 10), month, day);
        }}
        className={selectCls}
      >
        {allowEmpty && <option value="">—</option>}
        {years.map(y => (
          <option key={y} value={y}>{y}</option>
        ))}
      </select>
    </div>
  );
}
