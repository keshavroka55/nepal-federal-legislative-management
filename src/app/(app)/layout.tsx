import type React from "react";
import { AppSidebar } from "@/components/dashboard/AppSidebar";

const AppLayout = ({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) => {
  return (
    <div
      className="flex h-screen overflow-hidden bg-background text-foreground"
      suppressHydrationWarning
    >
      {/* Sidebar / mobile nav */}
      <AppSidebar />

      {/* Page content — only this area scrolls */}
      <main className="flex-1 min-w-0 overflow-y-auto pt-14 lg:pt-0">
        <div className="mx-auto w-full max-w-8xl p-6">{children}</div>
      </main>
    </div>
  );
};

export default AppLayout;
