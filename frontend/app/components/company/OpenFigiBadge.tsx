'use client';

/** OpenFIGI verification badge for a stored ISIN. Colour-codes the persisted
 * `openfigi_status`; the mismatch/verified tooltips surface the name OpenFIGI
 * returned so "Hindustan Aeronautics → HAL TRUST" is legible. Shared by the
 * /companies table (CompanyRow) and the frozen-universe inspection panel. */
export default function OpenFigiBadge({
  status,
  name,
  checkedAt,
}: {
  status?: string | null;
  name?: string | null;
  checkedAt?: string | null;
}) {
  const checked = checkedAt ? ` Checked ${new Date(checkedAt).toLocaleString()}.` : '';
  if (!status) return <span className="text-fg-faint text-xs" title="Not yet verified against OpenFIGI.">—</span>;
  if (status === 'no_isin') return <span className="text-fg-faint text-xs" title="No ISIN stored to verify.">no ISIN</span>;
  if (status === 'verified')
    return (
      <span className="px-1.5 py-0.5 text-[10px] font-medium bg-pos-500/15 text-pos-400 border border-pos-500/25 rounded cursor-help"
        title={`OpenFIGI confirms this ISIN${name ? ` → "${name}"` : ''}.${checked}`}>
        ✓ FIGI
      </span>
    );
  if (status === 'mismatch')
    return (
      <span className="px-1.5 py-0.5 text-[10px] font-medium bg-neg-500/15 text-neg-300 border border-neg-500/25 rounded cursor-help"
        title={`OpenFIGI resolves this ISIN to a DIFFERENT company: "${name ?? '(unknown)'}". The stored ISIN is likely wrong.${checked}`}>
        MISMATCH
      </span>
    );
  if (status === 'not_found')
    return (
      <span className="px-1.5 py-0.5 text-[10px] font-medium bg-warn-500/15 text-warn-300 border border-warn-500/30 rounded cursor-help"
        title={`OpenFIGI has no security for this ISIN — it may be invalid or unrecognized.${checked}`}>
        NOT FOUND
      </span>
    );
  return <span className="text-fg-faint text-xs" title="Verification call failed; re-run.">error</span>;
}
