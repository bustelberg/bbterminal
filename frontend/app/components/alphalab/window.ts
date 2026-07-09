// Train/test evaluation windows shared by AlphaLab + Signal Lab. Develop on the
// training set, validate out-of-sample on the test set.

export type RangeId = 'train' | 'test' | 'full';

export const RANGES: Record<RangeId, { label: string; span: string; start?: string; end?: string }> = {
  train: { label: 'Training', span: '2004–2018', start: '2004-01-01', end: '2017-12-31' },
  test: { label: 'Test', span: '2018–now', start: '2018-01-01' },
  full: { label: 'Full', span: '2004–now', start: '2004-01-01' },
};
