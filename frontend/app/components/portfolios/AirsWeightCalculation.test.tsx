import React from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import AirsWeightCalculation from './AirsWeightCalculation';

const shown = (node: React.ReactNode) => renderToStaticMarkup(node)
  .replace(/<[^>]*>/g, ' ')
  .replace(/&nbsp;|\u00a0|\u202f/g, ' ')
  .replace(/\s+/g, ' ');

describe('AirsWeightCalculation', () => {
  it('shows every raw AIRS value, their sum, and the actual holding division', () => {
    const text = shown(<AirsWeightCalculation
      components={[
        { name: 'ASML Holding', value_eur: 57_126.80 },
        { name: 'Another holding', value_eur: 1_105_159.48 },
      ]}
      numerator={57_126.80}
      denominator={1_162_286.28}
      result="4.92%"
      holdingName="ASML Holding"
    />);

    for (const value of ['ASML Holding', 'Another holding', '57.126,80', '1.105.159,48',
      '1.162.286,28', '4.92%']) expect(text).toContain(value);
    expect(text).toContain('Som van 2 Beginwaarde-regels');
    expect(text).not.toContain('V_book');
  });
});
