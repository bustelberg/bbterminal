import { defineConfig } from 'vitest/config';

// Minimal vitest config — pure-function tests for the math + matcher
// helpers under `frontend/app/components/earnings/utils.ts` and
// `frontend/app/components/momentum/equityCurve/seriesMath.ts`.
//
// ⚠ `node`, NOT `happy-dom` — AND THE DOM WAS NEVER USED (measured 2026-08-03).
// Booting a DOM per test file cost 50.5s of cumulative worker time across the 40 files,
// against 1.0s of actual assertion execution: 91% of the wall clock was harness. Every
// one of the 535 tests passes under `node`, because nothing here touches a DOM. The two
// files that look like they might are both false positives — `earnings/utils.test.ts`
// has a local variable named `window`, and `provenance.test.tsx` renders through
// `renderToStaticMarkup` from `react-dom/server`, which is pure Node by design.
//
// ⚠ IF A TEST EVER GENUINELY NEEDS A DOM, give that file `// @vitest-environment happy-dom`
// rather than switching this back — one file's need should not re-tax the other 39.
// (happy-dom is still installed for exactly that case.)
//
// `pool: 'threads'` over the v3 default `forks`: same isolation guarantees for pure
// functions, ~1s cheaper to spin up. Measured full-suite wall: happy-dom+forks 11.0s ·
// node+forks 6.6s · node+threads 5.7s.

// THE THREE TIERS, as vitest projects (`npx vitest run --project fast`).
//
// A file's tier is its FILENAME: `foo.integration.test.ts` / `foo.slow.test.ts`, everything else
// `fast`. That is deliberate — a marker inside the file would mean vitest has to load and
// transform the file to discover it should not have run it, which costs most of what the tier
// was meant to save. The name is visible to the glob, to `ls`, and to a reviewer.
//
// ⚠ BOTH NON-FAST PROJECTS ARE EMPTY, AND `integration` MUST STAY THAT WAY. The box at the top
// of CLAUDE.md bans browser/e2e tests and anything hitting a network or a live Supabase; this
// declares the vocabulary so the ban has something to point at, not a slot to fill.
//
// ⚠ NO `passWithNoTests`, SO `--project integration` EXITS 1 WITH "No test files found" — that
// is deliberate, and it is not a bug to be fixed. The alternative sets the flag at the ROOT (it
// is not a valid per-project option, so there is no way to scope it to the empty tiers), which
// would mean the DAY THE FAST GLOB BREAKS, `npm test` collects nothing and reports green: a
// vacuous pass over an empty set, the exact failure the `min-w-[13rem]` regex and the
// `FILES.length > 50` guard in lib/infoIcon.test.ts exist to prevent. A run of the whole suite
// still exits 0 — vitest only objects when a project is selected BY NAME and has no files.
const TIER = {
  fast: ['app/**/*.test.{ts,tsx}', 'lib/**/*.test.{ts,tsx}'],
  integration: ['app/**/*.integration.test.{ts,tsx}', 'lib/**/*.integration.test.{ts,tsx}'],
  slow: ['app/**/*.slow.test.{ts,tsx}', 'lib/**/*.slow.test.{ts,tsx}'],
};

export default defineConfig({
  test: {
    environment: 'node',
    pool: 'threads',
    globals: false,
    // ⚠ NO ROOT-LEVEL `include` — EACH PROJECT OWNS ITS OWN, AND A ROOT ONE IS NOT OVERRIDDEN.
    // With `extends: true` a root `include` wins over the project's, so declaring the fast glob
    // here made all three projects collect all 40 files: 120 files / 1,605 tests, the same 535
    // run three times, every tier reporting green over the same work. Tiers that all select
    // everything are not tiers — and the failure is silent, because the run passes.
    projects: [
      {
        extends: true,
        // `fast` is every test file MINUS the two named tiers — so a new test file is in the
        // default loop by default. Opting out has to be a deliberate rename; forgetting to opt
        // in is not a way to write a test that never runs.
        test: { name: 'fast', include: [...TIER.fast], exclude: [...TIER.integration, ...TIER.slow] },
      },
      { extends: true, test: { name: 'integration', include: [...TIER.integration] } },
      { extends: true, test: { name: 'slow', include: [...TIER.slow] } },
    ],
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
