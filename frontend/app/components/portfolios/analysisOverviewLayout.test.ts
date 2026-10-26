import { readFileSync } from 'node:fs';
import { describe, expect, it } from 'vitest';

const source = readFileSync(new URL('./PortfolioAnalysisModal.tsx', import.meta.url), 'utf8');

describe('allocation and selected-sleeve layout', () => {
  it('gives the size-contained scorecard slot a real flex width in both selection states', () => {
    expect(source).toContain('@container/overview grid w-full min-w-0 max-w-full basis-[36rem] flex-1');
    expect(source).not.toContain("!selected ? 'min-w-0 basis-[36rem] flex-1'");
    expect(source).not.toContain('invisible h-0 overflow-hidden pointer-events-none');
  });

  it('keeps the selected tile in a full-width, wrapping flow rather than behind allocation', () => {
    expect(source).toContain('flex items-start justify-center gap-8 flex-wrap mb-4');
    expect(source).toContain('col-start-1 row-start-1 w-full min-w-0');
    expect(source).toContain('flex w-full min-w-0 flex-wrap items-stretch justify-center gap-3 self-center');
  });
});
