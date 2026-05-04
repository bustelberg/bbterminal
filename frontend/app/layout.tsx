import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import "./globals.css";
import Sidebar from "./components/Sidebar";
import DialogHost from "./components/DialogHost";
import LoadingTracker from "./components/LoadingTracker";
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
      <body className="h-full flex bg-[#0f1117] text-gray-200">
        <Sidebar initialUser={initialUser} />
        <div className="flex-1 overflow-auto">{children}</div>
        <DialogHost />
        <LoadingTracker />
      </body>
    </html>
  );
}
