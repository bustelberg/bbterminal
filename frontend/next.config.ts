import path from "node:path";
import type { NextConfig } from "next";

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
