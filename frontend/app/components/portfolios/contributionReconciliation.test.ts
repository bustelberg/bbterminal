import { expect, it } from 'vitest';
import { contributionReconciliation as reconcile } from './contributionReconciliation';

it('shows the actual unexplained remainder, not just the certificate adjustment', () => {
  expect(reconcile([4, 1, -0.5], 5.8, true)).toEqual({ calculated: 4.5, reported: 5.8, missing: 1.3 });
  expect(reconcile([6, 1], 5.8, true)?.missing).toBe(-1.2);
});

it('makes the displayed rounded contributions sum exactly to the reported total', () => {
  expect(reconcile([1.234, 2.345], 4.126, true)).toEqual({ calculated: 3.58, reported: 4.13, missing: 0.55 });
  expect(reconcile([0.1, 0.2], 0.3, true)?.missing).toBe(0);
  expect(reconcile([-0.005], 0, true)).toEqual({ calculated: -0.01, reported: 0, missing: 0.01 });
});

it('does not manufacture a reconciliation from missing returns or incomparable dates', () => {
  expect(reconcile([1], 2, false)).toBeNull();
  expect(reconcile([null, undefined], 2, true)).toBeNull();
  expect(reconcile([1], null, true)).toBeNull();
  expect(reconcile([1], NaN, true)).toBeNull();
  expect(reconcile([0, null], 0, true)?.missing).toBe(0);
});
