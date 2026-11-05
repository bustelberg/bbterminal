'use client';

import { useEffect, useId, useRef, useState } from 'react';

type Option = { value: string; label: string; description?: string | null };
const searchable = (value: string) => value.normalize('NFD').replace(/[\u0300-\u036f]/g, '').toLocaleLowerCase();

export default function SearchablePortfolioPicker({ label, placeholder, options, value, onSelect, disabled, nl }: {
  label: string; placeholder: string; options: Option[]; value: string;
  onSelect: (value: string) => void; disabled: boolean; nl: boolean;
}) {
  const id = useId();
  const input = useRef<HTMLInputElement>(null);
  const list = useRef<HTMLDivElement>(null);
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState('');
  const [active, setActive] = useState(0);
  const selected = options.find((option) => option.value === value);
  const filtered = options.filter((option) => searchable(`${option.label} ${option.description ?? ''}`).includes(searchable(query.trim())));
  useEffect(() => {
    const menu = list.current;
    const option = menu?.querySelector<HTMLElement>(`[data-index="${active}"]`);
    if (!open || !menu || !option) return;
    // Scroll the options only; scrollIntoView also moves the surrounding page.
    if (option.offsetTop < menu.scrollTop) menu.scrollTop = option.offsetTop;
    else if (option.offsetTop + option.offsetHeight > menu.scrollTop + menu.clientHeight)
      menu.scrollTop = option.offsetTop + option.offsetHeight - menu.clientHeight;
  }, [active, open]);
  function show() { if (!open) { setQuery(''); setActive(0); setOpen(true); } }
  function choose(option: Option) {
    onSelect(option.value); setQuery(''); setOpen(false); input.current?.focus({ preventScroll: true });
  }
  return <div className="relative" onBlur={(event) => {
    if (!event.currentTarget.contains(event.relatedTarget)) { setOpen(false); setQuery(''); }
  }}>
    <label htmlFor={id} className="mb-2 block text-sm font-medium">{label}</label>
    <div className="relative">
      <input ref={input} id={id} role="combobox" disabled={disabled} autoComplete="off"
        aria-autocomplete="list" aria-expanded={open && !disabled} aria-controls={`${id}-options`}
        aria-activedescendant={open && !disabled && filtered[active] ? `${id}-option-${active}` : undefined}
        value={open ? query : selected?.label ?? ''} placeholder={placeholder}
        title={selected ? `${selected.label}${selected.description ? ` · ${selected.description}` : ''}` : undefined}
        onFocus={show} onClick={show}
        onChange={(event) => { setQuery(event.target.value); setActive(0); setOpen(true); if (value) onSelect(''); }}
        onKeyDown={(event) => {
          if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
            event.preventDefault();
            if (!open) show();
            else setActive((index) => Math.max(0, Math.min(filtered.length - 1, index + (event.key === 'ArrowDown' ? 1 : -1))));
          } else if (event.key === 'Enter' && open) {
            event.preventDefault(); if (filtered[active]) choose(filtered[active]);
          } else if (event.key === 'Escape') {
            event.preventDefault(); setOpen(false); setQuery('');
          }
        }} className="block w-full min-w-0 truncate rounded-lg border border-neutral-800/40 bg-elevated py-2 pl-3 pr-9 text-sm outline-none focus:border-accent-500/60 disabled:opacity-50" />
      <svg aria-hidden="true" viewBox="0 0 16 16" className="pointer-events-none absolute right-3 top-3 h-4 w-4 text-fg-muted" fill="none" stroke="currentColor" strokeWidth="1.5"><path d="m4 6 4 4 4-4" /></svg>
    </div>
    {open && !disabled && <div ref={list} id={`${id}-options`} role="listbox" aria-label={label}
      className="absolute left-0 top-full z-30 mt-1 max-h-64 w-full overflow-y-auto overscroll-contain rounded-lg border border-neutral-800/40 bg-elevated p-1 shadow-xl">
      {filtered.map((option, index) => <button type="button" role="option" id={`${id}-option-${index}`} key={option.value}
        aria-selected={option.value === value} data-index={index} tabIndex={-1}
        onMouseDown={(event) => event.preventDefault()} onClick={() => choose(option)}
        className={`block w-full rounded-md px-3 py-2 text-left text-sm hover:bg-accent-500/10 ${active === index ? 'bg-accent-500/10' : ''}`}>
        <span className="block">{option.label}</span>
        {option.description && <span className="mt-0.5 block text-xs text-fg-muted">{option.description}</span>}
      </button>)}
      {!filtered.length && <p className="px-3 py-3 text-sm text-fg-muted">{nl ? 'Geen portefeuilles gevonden.' : 'No portfolios found.'}</p>}
    </div>}
  </div>;
}
