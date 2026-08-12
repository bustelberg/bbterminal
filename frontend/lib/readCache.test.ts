import { beforeEach, describe, expect, it } from 'vitest';

import {
  dropRead, getRead, invalidateReadCache, isCacheableRead, isMutation, pathOf, putRead,
  READ_MAX_BYTES, readCacheStats, readGeneration, readKey, type CachedRead,
} from './readCache';

/**
 * The rules that decide what is cached, what invalidates it, and what two requests count as the
 * same request. Every one of these is a rule whose failure is SILENT: a wrongly cached read shows a
 * plausible old number, and a missed invalidation shows a chart that ignores the ingest you just
 * ran. None of them raises anything.
 */

const ok = (body: string, status = 200): CachedRead =>
  ({ status, statusText: 'OK', contentType: 'application/json', body });

const settle = () => new Promise((r) => { setTimeout(r, 0); });

beforeEach(() => { invalidateReadCache('test'); });

describe('what may be cached', () => {
  it('caches the fundamental reads the modal lives on', () => {
    for (const [method, url] of [
      ['GET', 'http://x/api/earnings/by-isin/US0378331005/metrics?cadence=annual'],
      ['GET', 'http://x/api/earnings/by-isin/US0378331005/growth-estimates'],
      ['POST', 'http://x/api/earnings/margin-inputs'],
      ['POST', 'http://x/api/earnings/fcf-sbc-yield-inputs'],
      ['POST', 'http://x/api/earnings/fundamental-blend-metrics'],
      ['POST', 'http://x/api/earnings/fundamental-coverage'],
      ['GET', 'http://x/api/asset-pipeline/latest-close/isin/US0378331005?currency=USD'],
    ] as const) {
      expect(isCacheableRead(method, url, '{}'), url).toBe(true);
    }
  });

  it('⚠ NEVER caches a live dashboard read — the whole reason this is an allowlist', () => {
    for (const url of [
      'http://x/api/usage',
      'http://x/api/schedule/stream',
      'http://x/api/ingest/runs/12',
      'http://x/api/data/price-coverage',
      'http://x/api/airs/model-portfolios',
    ]) {
      expect(isCacheableRead('GET', url), url).toBe(false);
    }
  });

  it('⚠ tells the coverage READ from the coverage INGEST — one path is a prefix of the other', () => {
    expect(isCacheableRead('POST', 'http://x/api/earnings/fundamental-coverage', '{}')).toBe(true);
    expect(isCacheableRead('POST', 'http://x/api/earnings/fundamental-coverage/ingest', '{}')).toBe(false);
    expect(isMutation('POST', 'http://x/api/earnings/fundamental-coverage/ingest')).toBe(true);
  });

  it('refuses a body it cannot key on, rather than colliding two uploads', () => {
    const url = 'http://x/api/earnings/margin-inputs';
    expect(isCacheableRead('POST', url, '{"a":1}')).toBe(true);
    expect(isCacheableRead('POST', url, new FormData())).toBe(false);
    expect(isCacheableRead('PUT', url, '{}')).toBe(false);
  });
});

describe('what invalidates', () => {
  it('any write does', () => {
    expect(isMutation('POST', 'http://x/api/airs/basket/fundamentals/ingest/job')).toBe(true);
    expect(isMutation('DELETE', 'http://x/api/universe/labels/foo')).toBe(true);
    expect(isMutation('PATCH', 'http://x/api/scheduled-strategies/1/cash')).toBe(true);
  });

  it('a plain read does not', () => {
    expect(isMutation('GET', 'http://x/api/earnings/by-isin/X/metrics')).toBe(false);
    expect(isMutation('POST', 'http://x/api/earnings/margin-inputs')).toBe(false);
  });

  it('⚠ nor does the blend STREAM — it is a POST, and it is the most expensive read on the page', () => {
    expect(isMutation('POST', 'http://x/api/earnings/fundamental-blend-metrics/stream')).toBe(false);
    // …but it is not cacheable either: an SSE body is consumed frame by frame and cannot be replayed.
    expect(isCacheableRead('POST', 'http://x/api/earnings/fundamental-blend-metrics/stream', '{}')).toBe(false);
  });
});

describe('request identity', () => {
  const url = 'http://x/api/earnings/margin-inputs';

  it('⚠ the BODY separates two cards posting to the same URL', () => {
    expect(readKey('POST', url, '{"universe":"SP500"}', false))
      .not.toBe(readKey('POST', url, '{"portfolio_id":3}', false));
  });

  it('⚠ so does the view-as-user preview', () => {
    expect(readKey('GET', url, undefined, true)).not.toBe(readKey('GET', url, undefined, false));
  });

  it('the same request twice is the same key', () => {
    expect(readKey('POST', url, '{"a":1}', false)).toBe(readKey('POST', url, '{"a":1}', false));
  });

  it('pathOf drops origin and query, which is what the allowlists match on', () => {
    expect(pathOf('http://x/api/earnings/by-isin/US1/metrics?cadence=quarterly'))
      .toBe('/api/earnings/by-isin/US1/metrics');
  });
});

describe('the store', () => {
  it('hands concurrent callers the SAME in-flight read — twelve cards, one fetch', () => {
    const read = Promise.resolve(ok('{"metrics":[]}'));
    putRead('k', read);
    expect(getRead('k')?.read).toBe(read);
    expect(getRead('k')?.read).toBe(read);
  });

  it('keeps a 200 and a 404 — a 404 is the modal\'s empty state, not a failure', async () => {
    putRead('a', Promise.resolve(ok('{}')));
    putRead('b', Promise.resolve(ok('{"detail":"no company"}', 404)));
    await settle();
    expect(getRead('a')).not.toBeNull();
    expect(getRead('b')).not.toBeNull();
  });

  it('⚠ never keeps a 500 or a 401 — those are about the last minute, not about the data', async () => {
    putRead('a', Promise.resolve(ok('boom', 500)));
    putRead('b', Promise.resolve(ok('nope', 401)));
    await settle();
    expect(getRead('a')).toBeNull();
    expect(getRead('b')).toBeNull();
  });

  it('drops a read that threw, so a network blip is retried', async () => {
    const read = Promise.reject(new Error('offline'));
    read.catch(() => {});
    putRead('k', read);
    await settle();
    expect(getRead('k')).toBeNull();
  });

  it('⚠ an invalidation mid-flight WINS — the pre-ingest answer must not install itself after', async () => {
    let resolve: (v: CachedRead) => void = () => {};
    putRead('k', new Promise<CachedRead>((r) => { resolve = r; }));
    invalidateReadCache('ingest finished while that was in flight');
    resolve(ok('{"stale":true}'));
    await settle();
    expect(getRead('k')).toBeNull();
    expect(readCacheStats().entries).toBe(0);
  });

  it('invalidation bumps the generation the SSE blend memo hangs off', () => {
    const before = readGeneration();
    putRead('k', Promise.resolve(ok('{}')));
    invalidateReadCache('a write');
    expect(readGeneration()).toBe(before + 1);
    expect(getRead('k')).toBeNull();
  });

  it('dropRead forgets one entry and leaves the rest', async () => {
    putRead('a', Promise.resolve(ok('{}')));
    putRead('b', Promise.resolve(ok('{}')));
    await settle();
    dropRead('a');
    expect(getRead('a')).toBeNull();
    expect(getRead('b')).not.toBeNull();
  });

  it('evicts oldest-first once the bodies pass the ceiling', async () => {
    // Sized off the ceiling itself so this cannot rot: two fit, the third pushes the first out.
    // (A JS string is UTF-16, so a char costs 2 bytes — the same arithmetic `putRead` does.)
    const big = 'x'.repeat(Math.ceil(READ_MAX_BYTES / 2 / 2.5));
    putRead('one', Promise.resolve(ok(big)));
    await settle();
    putRead('two', Promise.resolve(ok(big)));
    await settle();
    expect(getRead('one')).not.toBeNull();
    putRead('three', Promise.resolve(ok(big)));
    await settle();
    expect(getRead('one')).toBeNull();
    // ⚠ The entry that triggered the eviction is never the one evicted.
    expect(getRead('three')).not.toBeNull();
    expect(readCacheStats().bytes).toBeLessThanOrEqual(READ_MAX_BYTES);
  });
});
