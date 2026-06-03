import { describe, it, expect } from 'vitest';
import { resolveApiUrl } from './useApiData';
import { API_URL } from '../apiUrl';

describe('resolveApiUrl', () => {
  it('prefixes a relative backend path with API_URL', () => {
    expect(resolveApiUrl('/api/companies')).toBe(`${API_URL}/api/companies`);
    expect(resolveApiUrl('/api/momentum/current-picks/42')).toBe(
      `${API_URL}/api/momentum/current-picks/42`,
    );
  });

  it('passes absolute http(s) URLs through unchanged', () => {
    expect(resolveApiUrl('http://example.test/api/x')).toBe('http://example.test/api/x');
    expect(resolveApiUrl('https://example.test/api/x')).toBe('https://example.test/api/x');
  });

  it('keeps query strings intact on relative paths', () => {
    expect(resolveApiUrl('/api/ingest/runs?limit=10')).toBe(`${API_URL}/api/ingest/runs?limit=10`);
  });
});
