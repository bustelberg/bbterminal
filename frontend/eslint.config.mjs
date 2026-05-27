import { defineConfig, globalIgnores } from "eslint/config";
import nextVitals from "eslint-config-next/core-web-vitals";
import nextTs from "eslint-config-next/typescript";

const eslintConfig = defineConfig([
  ...nextVitals,
  ...nextTs,
  // Override default ignores of eslint-config-next.
  globalIgnores([
    // Default ignores of eslint-config-next:
    ".next/**",
    "out/**",
    "build/**",
    "next-env.d.ts",
  ]),
  {
    // Surface these as warnings (still visible in lint output, doesn't gate
    // CI). Most of these flag patterns that are usually fine in practice but
    // pedantically wrong by the strictest reading of React 19 + TS rules.
    // Drop a rule from this list when you're ready to clean up that class
    // of issues across the codebase.
    rules: {
      "react-hooks/set-state-in-effect": "warn",
      "react-hooks/purity": "warn",
      "@typescript-eslint/no-explicit-any": "warn",
      // Honor the underscore-prefix convention for intentionally-unused
      // bindings — `_idx`, `_unused`, etc. stop tripping the rule. The
      // rest of the rule (genuine accidental-unused) still reports.
      "@typescript-eslint/no-unused-vars": [
        "warn",
        {
          argsIgnorePattern: "^_",
          varsIgnorePattern: "^_",
          caughtErrorsIgnorePattern: "^_",
        },
      ],
    },
  },
]);

export default eslintConfig;
