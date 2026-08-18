import io

P = 'app/components/portfolios/DeepValuationTab.tsx'
s = io.open(P, encoding='utf-8').read()

start = s.index("            {r.bridge ? (")
END = """            ) : (
              <p className="text-[12px] text-warn-300">
                No forward P/E ingested — there is no multiple to rerate from, so the expected
                return cannot be computed.
              </p>
            )}
"""
end = s.index(END) + len(END)

NEW = '''            {/**
              * ⚠⚠ THE TABLE IS ALWAYS THE SAME FOUR ROWS. It used to swap for a paragraph whenever
              * `calculateEGM` refused — and refusing is something the READER can cause: type `0`
              * into Exit P/E and the model has no multiple to rerate to, so the entire output box
              * changed shape mid-keystroke. Worse, the paragraph blamed a missing forward P/E,
              * which is a DATA fault, for what was a four-character edit.
              *
              * So the rows are constant and only the VALUES go `n/a`. The reason is carried by the
              * total row's ⓘ, which is present either way — the shape of the panel stops being a
              * signal, which is what lets the numbers be one.
              */}
            <table className="w-full table-fixed text-[12px]">
              <colgroup>
                <col />
                <col className="w-[4.75rem]" />
                <col className="w-[3.75rem]" />
              </colgroup>
              <tbody>
                {(r.bridge?.legs ?? [{ key: 'growth' }, { key: 'yield' }, { key: 'multiple' }] as const)
                  .map((leg) => {
                    const rate = 'rate' in leg ? leg.rate : null;
                    const factor = 'factor' in leg ? leg.factor : null;
                    return (
                      <tr key={leg.key}>
                        <td className="truncate py-0.5 text-fg-muted">
                          {leg.key === 'growth' ? 'Earnings growth'
                            : leg.key === 'yield' ? 'Dividend yield'
                              // ⚠ THE ENDPOINTS COME OFF THE LEG WHERE THERE IS ONE — they travel
                              // with the arithmetic (see `EgmLeg`), so a label can never name a
                              // different pair than the figure beside it was computed from. With no
                              // bridge nothing was computed, so the fields themselves are the only
                              // honest source.
                              : <>Multiple <span className="font-mono text-fg-soft">
                                {mult('from' in leg ? leg.from ?? null : src.forwardPE)} → {mult('to' in leg ? leg.to ?? null : assumptions.exitPE)}</span></>}
                        </td>
                        {/* ⚠ NO `/yr` ON THE LEGS. Every row here is annualised, so repeating the
                            unit four times states one fact four times; it is said ONCE, on the
                            answer, where a reader taking only that number away still gets it. */}
                        <td className={`py-0.5 pl-2 text-right font-mono tabular-nums ${
                          (rate ?? 0) >= 0 ? 'text-fg-soft' : 'text-neg-400'}`}>
                          {rate == null ? 'n/a' : pct1(rate)}
                        </td>
                        <td className="py-0.5 pl-2 text-right font-mono tabular-nums text-fg-faint">
                          {factor == null ? '—' : `×${factor.toFixed(3)}`}
                        </td>
                      </tr>
                    );
                  })}
                <tr className="border-t border-neutral-700/60">
                  <td className="pt-1 font-medium text-fg-strong">
                    Expected return
                    {/* ⚠⚠ THE COMPOUNDING NOTE IS IN THE HOVER, IT DID NOT GO AWAY. It was two
                        lines of prose under the rule. True and load-bearing — but on a panel whose
                        job is "as little as possible", a permanent paragraph about an arithmetic
                        subtlety is the first thing a reader skips, and the `×` column beside it
                        already SHOWS the correct arithmetic to anyone checking. The visible design
                        carries the proof; the hover carries the warning — and, when the model
                        refuses, the reason. */}
                    <InfoTip content={<AspectCard
                      what="The three drivers, compounded — not added."
                      where={r.bridge
                        ? `${pct1(r.bridge.sumOfRates)} is what the rate column sums to; `
                          + `${pct1(r.bridge.rate)} is what the × column multiplies to, and that `
                          + 'is the answer.'
                        : 'Nothing to show: see below.'}
                      when={`Annualised, over ${assumptions.years} years.`}
                      how={r.bridge
                        ? 'Returns compound, so the rates cannot be added — the × column is the arithmetic the model actually performs, and it ties exactly.'
                        : src.forwardPE == null || !(src.forwardPE > 0)
                          ? '⚠ No usable forward P/E for this company — a loss-maker has no multiple to rerate from, and none was ingested. Nothing here depends on your assumptions.'
                          : '⚠ These assumptions produce no valuation — an exit P/E of zero or less, or a growth or hurdle rate at or below −100%, has no compounding path. Adjust the inputs on the left.'} />} />
                  </td>
                  <td className={`pt-1 pl-2 text-right font-mono tabular-nums font-semibold ${
                    (r.bridge?.rate ?? 0) >= 0 ? 'text-pos-500' : 'text-neg-500'}`}>
                    {r.bridge == null ? 'n/a' : <>{pct1(r.bridge.rate)}<span className="font-normal text-fg-faint">/yr</span></>}
                  </td>
                  <td className="pt-1 pl-2 text-right font-mono tabular-nums text-fg-faint">
                    {r.bridge == null ? '—' : `×${r.bridge.factor.toFixed(3)}`}
                  </td>
                </tr>
              </tbody>
            </table>
'''
s = s[:start] + NEW + s[end:]

# the conclusion table renders unconditionally too — same reason.
OLD_GATE = """            {(r.impliedPrice != null || src.price != null) && (
              <table"""
NEW_GATE = """            {/* ⚠ UNCONDITIONAL, like the table above — `n/a` is a value, an absent table is a
                different panel. */}
            {(
              <table"""
assert s.count(OLD_GATE) == 1
s = s.replace(OLD_GATE, NEW_GATE)

io.open(P, 'w', encoding='utf-8', newline='').write(s)
print('ok')
