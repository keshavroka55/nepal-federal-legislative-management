import type { Metadata } from "next";
import { Inter, Mukta } from "next/font/google";
import { QueryProvider } from "@/components/providers/QueryProvider";
import { ThemeProvider } from "@/components/providers/ThemeProvider";
import { ErrorBoundary } from "@/components/ErrorBoundary";
import "./globals.css";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
});

const mukta = Mukta({
  weight: ["400", "500", "600", "700"],
  variable: "--font-mukta",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Vidhan Tracker - Nepal Federal Legislative Management System",
  description:
    "Track bills through Nepal's parliament in real time. A comprehensive platform for legislative transparency.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body className={`${inter.variable} ${mukta.variable} antialiased`}>
        <ThemeProvider>
          <QueryProvider>
            <ErrorBoundary>{children}</ErrorBoundary>
          </QueryProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
