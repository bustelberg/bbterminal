import path from "node:path";
import type { NextConfig } from "next";

/**
 * ⚠ A PRODUCTION BUILD WITHOUT `NEXT_PUBLIC_API_URL` MUST NOT SHIP. The value is INLINED AT BUILD
 * TIME, so an unset variable cannot be corrected afterwards by editing the Vercel env — it needs a
 * REBUILD, and nothing about the resulting site says so. Before `lib/apiUrl.ts` was hardened, that
 * build baked in `http://localhost:8000` and every visitor's browser was asked to reach their OWN
 * machine; Chrome surfaced it as "Access other apps and services on this device", which reads as
 * the site being invasive rather than misconfigured (measured in production 2026-07-30).
 *
 * The build is the only place this can be caught at zero cost to anyone. `next dev` is exempt —
 * localhost:8000 is the right answer there and always has been.
 */
if (process.env.NODE_ENV === "production" && !process.env.NEXT_PUBLIC_API_URL) {
  throw new Error(
    "NEXT_PUBLIC_API_URL is not set for this production build.\n"
    + "It is inlined at build time, so the deployed site would call the VISITOR'S localhost.\n"
    + "Set it in the Vercel project environment (Production + Preview) and redeploy.",
  );
}

const nextConfig: NextConfig = {
  /* config options here */
  // Temporarily disabled: the React Compiler mis-optimizes our custom store
  // hook (`createStore().use()` wrapping useSyncExternalStore), producing
  // "change in the order of Hooks" errors in unrelated components. Re-enable
  // once https://github.com/facebook/react/issues for this pattern is fixed,
  // or add 'use no memo' directives to the affected files.
  reactCompiler: false,
  // Off so dev mounts effects once (matching prod). With Strict Mode on,
  // every fetch in a mount-effect fires twice in dev — wasted GuruFocus
  // quota and noisy in-flight-request panel.
  reactStrictMode: false,
  devIndicators: false,
  allowedDevOrigins: ['127.0.0.1'],
  turbopack: {
    root: path.resolve(__dirname),
  },
};

export default nextConfig;
