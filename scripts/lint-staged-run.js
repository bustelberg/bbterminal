#!/usr/bin/env node
/**
 * Tiny helper invoked from `.lintstagedrc.js`. Spawns a linter inside
 * a sub-package's directory so the linter picks up that package's
 * config (eslint reads `frontend/eslint.config.mjs`, ruff reads
 * `backend/pyproject.toml`). The function form of lint-staged emits a
 * command string that runs at the repo root — directly invoking
 * `cd frontend && npx eslint ...` from there fails on Windows because
 * the spawned shell isn't a bash that understands `&&` + relative
 * paths the same way. Using Node's `spawnSync` with an explicit `cwd`
 * sidesteps the platform difference.
 *
 *   node scripts/lint-staged-run.js <subdir> <cmd> [cmdArgs...] -- <file...>
 *
 * The `--` separator splits the linter invocation from the file list
 * that lint-staged appended. Files are passed to the linter as
 * paths relative to <subdir>.
 */
'use strict';

const path = require('path');
const { spawnSync } = require('child_process');

const args = process.argv.slice(2);
const sep = args.indexOf('--');
if (sep < 0) {
  console.error('lint-staged-run: missing "--" separator');
  process.exit(2);
}

const [subdir, cmd, ...cmdArgs] = args.slice(0, sep);
const files = args.slice(sep + 1);
const absSubdir = path.resolve(subdir);
const relFiles = files.map((f) =>
  path.relative(absSubdir, path.resolve(f)).replace(/\\/g, '/'),
);

const result = spawnSync(cmd, [...cmdArgs, ...relFiles], {
  cwd: absSubdir,
  stdio: 'inherit',
  shell: true,
});

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}
process.exit(result.status ?? 0);
