import type { Metadata, Viewport } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Sidebar from "./components/Sidebar";
import DialogHost from "./components/DialogHost";
import LoadingTracker from "./components/LoadingTracker";
import JobToaster from "./components/jobs/JobToaster";
import { createClient } from "../lib/supabase/server";

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

// Pre-resolve the user on the server so the Sidebar can render correctly
// on first paint, even when the client-side `getUser()` would otherwise
// race with cross-tab token refreshes (the "duplicate-tab → sidebar
// disappears" bug). proxy.ts has already validated the cookie session
// for any non-public route by the time we get here, so this call is a
// cheap re-check of the same cookies.
export default async function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  const supabase = await createClient();
  const { data: { user } } = await supabase.auth.getUser();
  const role = ((user?.app_metadata as { role?: string } | undefined)?.role === "admin"
    ? "admin"
    : "user") as "admin" | "user";
  const initialUser = user?.email ? { email: user.email, role } : null;

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
