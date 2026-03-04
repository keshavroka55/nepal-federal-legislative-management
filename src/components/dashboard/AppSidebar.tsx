"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState, useSyncExternalStore } from "react";
import {
  BarChart3,
  BellRing,
  Bot,
  Building2,
  ChevronRight,
  FileText,
  Gavel,
  LayoutDashboard,
  LogOut,
  Menu,
  Moon,
  Settings,
  Sun,
  Timer,
  X,
} from "lucide-react";
import { useTheme } from "@/components/providers/ThemeProvider";

function useIsMounted() {
  return useSyncExternalStore(
    () => () => {},
    () => true,
    () => false,
  );
}

type NavItem = {
  label: string;
  href: string;
  icon: React.ReactNode;
  badge?: string;
};

type NavSection = {
  title: string;
  items: NavItem[];
};

const navSections: NavSection[] = [
  {
    title: "Main",
    items: [
      {
        label: "Dashboard",
        href: "/dashboard",
        icon: <LayoutDashboard className="h-4 w-4" />,
      },
      {
        label: "Bills",
        href: "/bills",
        icon: <FileText className="h-4 w-4" />,
      },
    ],
  },
  {
    title: "Legislative",
    items: [
      {
        label: "Committees",
        href: "/committees",
        icon: <Building2 className="h-4 w-4" />,
      },
      {
        label: "Voting",
        href: "/voting",
        icon: <Gavel className="h-4 w-4" />,
      },
      {
        label: "Deadlines",
        href: "/deadlines",
        icon: <Timer className="h-4 w-4" />,
      },
    ],
  },
  {
    title: "Intelligence",
    items: [
      {
        label: "AI Analysis",
        href: "/ai",
        icon: <Bot className="h-4 w-4" />,
      },
      {
        label: "Analytics",
        href: "/analytics",
        icon: <BarChart3 className="h-4 w-4" />,
      },
      {
        label: "Alerts",
        href: "/alerts",
        icon: <BellRing className="h-4 w-4" />,
      },
    ],
  },
];

function NavLink({ item, isActive }: { item: NavItem; isActive: boolean }) {
  return (
    <Link
      href={item.href}
      className={[
        "group flex items-center gap-3 w-full rounded-xl px-3 py-2.5 text-sm font-medium transition-all duration-150",
        isActive
          ? "bg-primary/15 text-sidebar-foreground shadow-sm ring-1 ring-primary/30"
          : "text-sidebar-foreground/60 hover:text-sidebar-foreground hover:bg-sidebar-foreground/8",
      ].join(" ")}
    >
      <span
        className={[
          "flex h-8 w-8 shrink-0 items-center justify-center rounded-lg transition-colors duration-150",
          isActive
            ? "bg-primary/25 text-primary"
            : "bg-sidebar-foreground/5 text-sidebar-foreground/50 group-hover:bg-sidebar-foreground/10 group-hover:text-sidebar-foreground/80",
        ].join(" ")}
      >
        {item.icon}
      </span>
      <span className="flex-1 truncate">{item.label}</span>
      {item.badge ? (
        <span
          className={[
            "rounded-full px-2 py-0.5 text-[10px] font-bold leading-none tabular-nums",
            isActive
              ? "bg-primary/30 text-primary"
              : "bg-sidebar-foreground/8 text-sidebar-foreground/50 group-hover:text-sidebar-foreground/70",
          ].join(" ")}
        >
          {item.badge}
        </span>
      ) : null}
      {isActive && (
        <ChevronRight className="h-3.5 w-3.5 shrink-0 text-primary/60" />
      )}
    </Link>
  );
}

function SidebarContent({ pathname }: { pathname: string }) {
  const mounted = useIsMounted();
  const { resolvedTheme, setTheme } = useTheme();
  const isDark = (resolvedTheme ?? "dark") === "dark";
  const toggleTheme = () =>
    setTheme(isDark ? "light" : "dark");

  return (
    <div className="relative z-10 flex h-full flex-col">
      {/* Logo */}
      <div className="flex items-center gap-3 px-5 py-5 border-b border-sidebar-border">
        <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-gradient-to-br from-primary via-violet-500 to-indigo-500 text-xs font-bold text-white shadow-lg shadow-primary/30">
          VF
        </div>
        <div className="min-w-0 leading-tight">
          <p className="text-sm font-semibold text-sidebar-foreground truncate">
            Vidhan
          </p>
          <p className="text-[11px] text-sidebar-foreground/45 truncate">
            Federal Parliament
          </p>
        </div>
      </div>

      {/* Navigation */}
      <nav className="flex-1 overflow-y-auto px-3 py-4 space-y-5 scrollbar-none">
        {navSections.map((section) => (
          <div key={section.title}>
            <p className="mb-1.5 px-3 text-[10px] font-semibold uppercase tracking-widest text-sidebar-foreground/35 select-none">
              {section.title}
            </p>
            <div className="space-y-0.5">
              {section.items.map((item) => (
                <NavLink
                  key={item.href}
                  item={item}
                  isActive={
                    pathname === item.href ||
                    pathname.startsWith(item.href + "/")
                  }
                />
              ))}
            </div>
          </div>
        ))}
      </nav>

      {/* Settings + Theme toggle */}
      <div className="px-3 pb-2 flex items-center gap-1">
        <Link
          href="/settings"
          className="group flex flex-1 items-center gap-3 rounded-xl px-3 py-2.5 text-sm font-medium text-sidebar-foreground/60 hover:text-sidebar-foreground hover:bg-sidebar-foreground/8 transition-all duration-150"
        >
          <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-sidebar-foreground/5 text-sidebar-foreground/50 group-hover:bg-sidebar-foreground/10 group-hover:text-sidebar-foreground/80 transition-colors">
            <Settings className="h-4 w-4" />
          </span>
          <span>Settings</span>
        </Link>
        <button
          type="button"
          onClick={toggleTheme}
          className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg text-sidebar-foreground/60 hover:text-sidebar-foreground hover:bg-sidebar-foreground/10 transition-colors"
          aria-label="Toggle theme"
        >
          {mounted && isDark ? (
            <Sun className="h-4 w-4" />
          ) : (
            <Moon className="h-4 w-4" />
          )}
        </button>
      </div>

      {/* User profile */}
      <div className="border-t border-sidebar-border p-3">
        <div className="flex items-center gap-3 rounded-xl border border-sidebar-foreground/10 bg-sidebar-foreground/5 px-3 py-2.5 hover:border-sidebar-foreground/20 hover:bg-sidebar-foreground/8 transition-all duration-150 group cursor-pointer">
          <div className="relative shrink-0">
            <div className="flex h-9 w-9 items-center justify-center rounded-full bg-gradient-to-br from-slate-400 via-slate-500 to-slate-600 text-xs font-bold text-white">
              ?
            </div>
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-sm font-semibold text-sidebar-foreground truncate leading-tight">
              Guest User
            </p>
            <p className="text-[11px] text-sidebar-foreground/45 truncate">
              Not signed in
            </p>
          </div>
          <LogOut className="h-4 w-4 shrink-0 text-sidebar-foreground/30 group-hover:text-sidebar-foreground/60 transition-colors" />
        </div>
      </div>
    </div>
  );
}

export function AppSidebar() {
  const pathname = usePathname();
  const [mobileOpen, setMobileOpen] = useState(false);
  const mounted = useIsMounted();
  const { resolvedTheme, setTheme } = useTheme();
  const isDark = (resolvedTheme ?? "dark") === "dark";
  const toggleTheme = () =>
    setTheme(isDark ? "light" : "dark");

  return (
    <>
      {/* Desktop sidebar */}
      <aside className="relative hidden lg:flex h-screen w-64 shrink-0 flex-col overflow-hidden border-r border-sidebar-border bg-sidebar">
        <div className="absolute inset-0 bg-gradient-to-b from-primary/5 via-transparent to-transparent pointer-events-none" />
        <SidebarContent pathname={pathname} />
      </aside>

      {/* Mobile topbar */}
      <header className="lg:hidden fixed top-0 inset-x-0 z-40 flex items-center justify-between px-4 h-14 bg-sidebar/95 backdrop-blur border-b border-sidebar-border">
        <div className="flex items-center gap-2.5">
          <div className="flex h-8 w-8 items-center justify-center rounded-xl bg-gradient-to-br from-primary via-violet-500 to-indigo-500 text-[11px] font-bold text-white">
            VF
          </div>
          <span className="text-sm font-semibold text-sidebar-foreground">
            Vidhan
          </span>
        </div>
        <div className="flex items-center gap-1">
          <button
            type="button"
            onClick={toggleTheme}
            className="flex h-9 w-9 items-center justify-center rounded-lg text-sidebar-foreground/60 hover:bg-sidebar-foreground/10 hover:text-sidebar-foreground transition-colors"
            aria-label="Toggle theme"
          >
            {mounted && isDark ? (
              <Sun className="h-4 w-4" />
            ) : (
              <Moon className="h-4 w-4" />
            )}
          </button>
          <button
            type="button"
            onClick={() => setMobileOpen(true)}
            className="flex h-9 w-9 items-center justify-center rounded-lg text-sidebar-foreground/60 hover:bg-sidebar-foreground/10 hover:text-sidebar-foreground transition-colors"
            aria-label="Open navigation"
          >
            <Menu className="h-5 w-5" />
          </button>
        </div>
      </header>

      {/* Mobile drawer backdrop */}
      {mobileOpen && (
        <div
          className="lg:hidden fixed inset-0 z-50 flex"
          role="dialog"
          aria-modal="true"
        >
          <div
            className="absolute inset-0 bg-black/60 backdrop-blur-sm"
            onClick={() => setMobileOpen(false)}
          />
          <div className="relative flex w-72 flex-col bg-sidebar shadow-2xl">
            <div className="absolute inset-0 bg-gradient-to-b from-primary/5 via-transparent to-transparent pointer-events-none" />
            <button
              type="button"
              onClick={() => setMobileOpen(false)}
              className="absolute right-3 top-3 z-10 flex h-8 w-8 items-center justify-center rounded-lg text-sidebar-foreground/60 hover:bg-sidebar-foreground/10 hover:text-sidebar-foreground transition-colors"
              aria-label="Close navigation"
            >
              <X className="h-4 w-4" />
            </button>
            <SidebarContent pathname={pathname} />
          </div>
        </div>
      )}
    </>
  );
}
