import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('allocation and selected-sleeve layout', () => {
  it('gives the size-contained scorecard slot the full width of its grid track in both selection states', () => {
    expect(source).toContain('@container/overview grid w-full min-w-0');
    expect(source).not.toContain("!selected ? 'min-w-0 basis-[36rem] flex-1'");
    expect(source).not.toContain('invisible h-0 overflow-hidden pointer-events-none');
  });

  it('keeps the selected tile in a full-width, wrapping flow rather than behind allocation', () => {
    expect(source).toContain('grid-cols-[minmax(0,1fr)]');
    expect(source).toContain('@[79rem]/analyse:grid-cols-[41rem_minmax(0,1fr)]');
    expect(source).toContain('col-start-1 row-start-1 w-full min-w-0');
    expect(source).toContain('flex w-full min-w-0 flex-wrap items-stretch justify-center gap-3 self-center');
  });
});
