/**
 * lint-staged config for the BBTerminal monorepo.
 *
 * Each glob's command receives only the staged files that match —
 * commits touching just the backend skip the frontend toolchain
 * entirely (and vice versa). Auto-fixed files are re-staged by
 * lint-staged before the commit lands.
 *
 * `scripts/lint-staged-run.js` is a tiny wrapper that spawns the
 * linter from inside the sub-package's directory (so eslint / ruff
 * find their package-local configs and plugins). The `--` separator
 * splits the linter invocation from the file list lint-staged
 * appends.
 *
 * Typecheck and openapi.json drift are NOT here — they're in
 * `.husky/pre-commit` because they need to see the whole project,
 * not a slice of staged files.
 */
module.exports = {
  'frontend/**/*.{ts,tsx,js,jsx,mjs,cjs}':
    'node scripts/lint-staged-run.js frontend npx eslint --fix --no-warn-ignored --',
  'backend/**/*.py':
    'node scripts/lint-staged-run.js backend uvx ruff check --fix --',
};
