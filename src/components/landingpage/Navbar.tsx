"use client";
import Link from "next/link";
import { cn } from "@/lib/utils";
import { motion, AnimatePresence } from "framer-motion";
import { Building2, Sun, Moon, ArrowRight, X, Menu } from "lucide-react";
import { useState, useEffect, useSyncExternalStore } from "react";
import { useTheme } from "@/components/providers/ThemeProvider";

function useIsMounted() {
  return useSyncExternalStore(
    () => () => {},
    () => true,
    () => false,
  );
}

const Navbar = () => {
  const [isScrolled, setIsScrolled] = useState(false);
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const mounted = useIsMounted();
  const { resolvedTheme, setTheme } = useTheme();
  const isDark = (resolvedTheme ?? "dark") === "dark";
  const toggleTheme = () => setTheme(isDark ? "light" : "dark");

  useEffect(() => {
    const handleScroll = () => setIsScrolled(window.scrollY > 10);
    window.addEventListener("scroll", handleScroll);
    return () => window.removeEventListener("scroll", handleScroll);
  }, []);

  const navLinks = ["Features", "How It Works", "Status Tracker", "About"];

  return (
    <nav
      className={cn(
        "fixed top-0 left-0 right-0 z-50 transition-all duration-300 border-b",
        isScrolled
          ? "bg-background/80 backdrop-blur-xl border-border/50 py-3 shadow-sm"
          : "bg-transparent border-transparent py-5",
      )}
    >
      <div className="container mx-auto px-4 md:px-6 flex items-center justify-between">
        <Link href="/" className="flex items-center gap-2.5 group">
          <div className="bg-primary p-1.5 rounded-lg group-hover:scale-105 transition-transform">
            <Building2 className="w-5 h-5 text-primary-foreground" />
          </div>
          <div className="flex flex-col leading-none">
            <span className="text-xl font-bold text-foreground tracking-tight">
              Vidhan Tracker<span className="text-primary">.</span>
            </span>
            <span className="text-[10px] text-muted-foreground tracking-widest uppercase hidden sm:block">
              Legislative Tracker System
            </span>
          </div>
        </Link>

        <div className="hidden md:flex items-center gap-8">
          {navLinks.map((item) => (
            <Link
              key={item}
              href={`#${item.toLowerCase().replace(/\s+/g, "-")}`}
              className="text-sm font-medium text-muted-foreground hover:text-foreground transition-colors relative group"
            >
              {item}
              <span className="absolute -bottom-1 left-0 w-0 h-0.5 bg-primary transition-all group-hover:w-full" />
            </Link>
          ))}
        </div>

        <div className="hidden md:flex items-center gap-4">
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={toggleTheme}
            className="p-2.5 rounded-full bg-muted/50 hover:bg-muted text-foreground transition-colors cursor-pointer"
            aria-label="Toggle theme"
          >
            <AnimatePresence mode="wait" initial={false}>
              {mounted && isDark ? (
                <motion.div
                  key="sun"
                  initial={{ rotate: -90, opacity: 0 }}
                  animate={{ rotate: 0, opacity: 1 }}
                  exit={{ rotate: 90, opacity: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <Sun className="w-4 h-4" />
                </motion.div>
              ) : (
                <motion.div
                  key="moon"
                  initial={{ rotate: 90, opacity: 0 }}
                  animate={{ rotate: 0, opacity: 1 }}
                  exit={{ rotate: -90, opacity: 0 }}
                  transition={{ duration: 0.2 }}
                >
                  <Moon className="w-4 h-4" />
                </motion.div>
              )}
            </AnimatePresence>
          </motion.button>
          <motion.div whileHover={{ scale: 1.02 }} whileTap={{ scale: 0.98 }}>
            <Link
              href="/dashboard"
              className="bg-primary hover:bg-primary/90 text-primary-foreground px-5 py-2.5 rounded-full font-semibold text-sm transition-all hover:shadow-lg hover:shadow-primary/20 flex items-center gap-2"
            >
              View Live Bills
              <ArrowRight className="w-4 h-4" />
            </Link>
          </motion.div>
        </div>

        <div className="flex items-center gap-2 md:hidden">
          <motion.button
            whileHover={{ scale: 1.05 }}
            whileTap={{ scale: 0.95 }}
            onClick={toggleTheme}
            className="p-2 rounded-full bg-muted/50 hover:bg-muted text-foreground transition-colors cursor-pointer"
            aria-label="Toggle theme"
          >
            {mounted && isDark ? <Sun size={18} /> : <Moon size={18} />}
          </motion.button>
          <button
            className="text-foreground p-2"
            onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
          >
            {mobileMenuOpen ? <X size={22} /> : <Menu size={22} />}
          </button>
        </div>
      </div>

      <AnimatePresence>
        {mobileMenuOpen && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: "auto" }}
            exit={{ opacity: 0, height: 0 }}
            className="overflow-hidden bg-background/95 backdrop-blur-xl border-b border-border md:hidden"
          >
            <div className="p-4 flex flex-col gap-1">
              {navLinks.map((item) => (
                <Link
                  key={item}
                  href={`#${item.toLowerCase().replace(/\s+/g, "-")}`}
                  className="text-foreground hover:bg-muted font-medium py-3 px-4 rounded-lg transition-colors"
                  onClick={() => setMobileMenuOpen(false)}
                >
                  {item}
                </Link>
              ))}
              <Link
                href="/dashboard"
                onClick={() => setMobileMenuOpen(false)}
                className="mt-2"
              >
                <span className="block w-full bg-primary text-primary-foreground py-3 rounded-lg font-bold text-center">
                  View Live Bills
                </span>
              </Link>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </nav>
  );
};

export default Navbar;
