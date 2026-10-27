import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Sidebar from "./components/Sidebar";
import DialogHost from "./components/DialogHost";
import LoadingTracker from "./components/LoadingTracker";
import JobToaster from "./components/jobs/JobToaster";
import { headers } from "next/headers";
import { readVerifiedUser, VERIFIED_USER_HEADER } from "../lib/verifiedUser";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "BBTerminal",
  description: "Financial data terminal",
};

// Explicit viewport so the app scales correctly on phones/tablets in a
// browser. `width=device-width` + `initial-scale=1` is the mobile baseline;
// we deliberately DON'T cap `maximum-scale` so users can still pinch-zoom
// dense tables (accessibility).
export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
};

// proxy.ts validates auth and forwards display identity on the same request. Reusing that result
// keeps first-paint identity without repeating a remote Supabase getUser() round trip.
export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const initialUser = readVerifiedUser((await headers()).get(VERIFIED_USER_HEADER));

  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased dark`}
    >
      {/* Column on mobile (mobile top bar stacks above content; the nav rail
          becomes an off-canvas drawer), row on lg+ (static rail beside content).
          `min-w-0` on the content lets wide tables scroll INSIDE their own
          overflow containers instead of stretching the whole layout past the
          viewport — the main cause of horizontal page scroll. */}
      <body className="h-full flex flex-col lg:flex-row bg-page text-fg">
        <Sidebar initialUser={initialUser} />
        <div className="flex-1 min-w-0 min-h-0 overflow-auto">{children}</div>
        <DialogHost />
        <LoadingTracker />
        {/* Background-job progress. Here rather than in a page because a job outlives the panel
            that started it — mounted in a page it would unmount on the first route change and take
            the progress with it while the server kept working. Renders nothing when idle. */}
        <JobToaster />
      </body>
    </html>
  );
}
