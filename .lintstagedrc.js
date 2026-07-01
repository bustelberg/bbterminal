/**
 * lint-staged config for the BBTerminal monorepo.
 *
 * Only ruff runs here — it's near-instant (~0.4s). eslint was
 * deliberately dropped from the git hooks: its Next flat config loads
 * typescript-eslint on every invocation, so even with `--cache` it
 * costs ~4-6s of pure startup and made every commit that touched a
 * frontend file slow. Hooks stay lean; eslint runs in CI instead.
 *
 * `scripts/lint-staged-run.js` is a tiny wrapper that spawns ruff from
 * inside `backend/` (so it finds `backend/pyproject.toml`). The `--`
 * separator splits the linter invocation from the file list lint-staged
 * appends. Auto-fixed files are re-staged before the commit lands.
 *
 * Typecheck and openapi.json drift are NOT here — they're in
 * `.husky/pre-commit` / `.husky/pre-push` because they need to see the
 * whole project, not a slice of staged files.
 */
module.exports = {
  'backend/**/*.py':
    'node scripts/lint-staged-run.js backend uvx ruff check --fix --',
};
