import InfoTip from '../InfoTip';
import { REG, REGIME_METHOD, type RegKey, regKey } from './regimeBands';

/** The 4-regime colour legend + an optional "Now" badge for the current state.
 * Hover a chip for that regime's rule; the "i" explains the full methodology. */
export default function RegimeLegend({ current }: { current?: { bull: boolean; turb: boolean; date: string } }) {
  const cur = current ? regKey(current.bull, current.turb) : null;
  return (
    <div className="flex items-center justify-between gap-3 flex-wrap">
      <div className="flex items-center gap-3 flex-wrap text-[11px]">
        {(Object.keys(REG) as RegKey[]).map((k) => (
          <span key={k} className="flex items-center gap-1.5 text-fg-muted cursor-help" title={`${REG[k].label} — ${REG[k].desc}`}>
            <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ background: REG[k].dot }} />{REG[k].label}
          </span>
        ))}
        <InfoTip text={REGIME_METHOD} />
      </div>
      {cur && (
        <span className="text-[11px] px-2 py-0.5 rounded-full border font-medium"
          style={{ borderColor: REG[cur].dot, color: REG[cur].dot }}
          title={`${REG[cur].label} — ${REG[cur].desc}`}>
          Now: {REG[cur].label}
        </span>
      )}
    </div>
  );
}
