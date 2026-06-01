import { defineConfig } from 'vitest/config';

// Minimal vitest config — pure-function tests for the math + matcher
// helpers under `frontend/app/components/earnings/utils.ts` and
// `frontend/app/components/momentum/equityCurve/seriesMath.ts`.
// happy-dom is lighter than jsdom (~10x faster startup), and these
// tests only need basic DOM globals (Date, Math, no actual rendering).
export default defineConfig({
  test: {
    environment: 'happy-dom',
    globals: false,
    // Only pick up *.test.ts(x) — keeps `tsc --noEmit` from compiling
    // test files into a Next.js build, and keeps eslint scope tight.
    include: ['app/**/*.test.{ts,tsx}', 'lib/**/*.test.{ts,tsx}'],
    coverage: {
      provider: 'v8',
      reporter: ['text', 'html'],
      include: [
        'app/components/earnings/utils.ts',
        'app/components/momentum/equityCurve/seriesMath.ts',
      ],
    },
  },
});
