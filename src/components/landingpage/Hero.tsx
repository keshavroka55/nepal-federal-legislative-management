"use client";

import React from "react";
import Link from "next/link";
import { motion } from "framer-motion";
import { ArrowRight, ArrowDown, ChevronDown } from "lucide-react";
import { AnimatedNumber, type StatsData } from "./shared";

const Hero = ({ stats }: { stats: StatsData | null }) => {
  const heroStats = [
    { label: "Total Bills Tracked", value: stats?.total ?? 0, suffix: "+" },
    { label: "In Committee", value: stats?.inCommittee ?? 0, suffix: "" },
    { label: "Awaiting Authentication", value: stats?.awaitingAuth ?? 0, suffix: "" },
    { label: "Gazette Published", value: stats?.gazettePublished ?? 0, suffix: "" },
  ];

  return (
    <section className="relative pt-32 pb-20 md:pt-44 md:pb-32 overflow-hidden bg-background">
      {/* Decorative background */}
      <div className="absolute inset-0 -z-10 pointer-events-none overflow-hidden">
        <div className="absolute top-[-20%] right-[-10%] w-[600px] h-[600px] bg-primary/[0.04] rounded-full blur-[120px]" />
        <div className="absolute bottom-[-20%] left-[-10%] w-[600px] h-[600px] bg-secondary/[0.06] rounded-full blur-[120px]" />
        <div
          className="absolute inset-0 opacity-[0.25]"
          style={{
            backgroundImage: "radial-gradient(circle, var(--muted-foreground) 0.8px, transparent 0.8px)",
            backgroundSize: "32px 32px",
          }}
        />
        <svg className="absolute top-20 left-10 w-40 h-40 text-primary/[0.06]" viewBox="0 0 100 100">
          <polygon points="50,5 95,97 5,97" fill="currentColor" />
        </svg>
        <svg className="absolute bottom-20 right-10 w-32 h-32 text-secondary/[0.08]" viewBox="0 0 100 100">
          <polygon points="50,5 95,97 5,97" fill="currentColor" />
        </svg>
      </div>

      <div className="container mx-auto px-4 md:px-6 relative z-10 text-center">
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.7, ease: "easeOut" }}
        >
          <div className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-primary/10 border border-primary/20 text-primary text-xs font-semibold uppercase tracking-wider mb-8">
            <span className="w-2 h-2 rounded-full bg-primary animate-pulse" />
            Live Parliament Tracking
          </div>

          <h1 className="text-4xl md:text-6xl lg:text-7xl font-bold text-foreground tracking-tight mb-4 leading-[1.1]">
            Nepal&apos;s Parliament,
            <br />
            <span className="text-primary">Fully Transparent</span>
          </h1>

          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.3, duration: 0.6 }}
            className="text-xl md:text-2xl text-muted-foreground font-medium mb-6 font-mukta"
          >
            नेपालको संसद, पूर्ण पारदर्शी
          </motion.p>

          <motion.p
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            transition={{ delay: 0.4, duration: 0.6 }}
            className="max-w-2xl mx-auto text-muted-foreground text-base md:text-lg mb-10 leading-relaxed"
          >
            Track every bill from registration to gazette publication. Real-time
            data directly from Pratinidhi Sabha and Rastriya Sabha.
          </motion.p>

          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ delay: 0.5, duration: 0.5 }}
            className="flex flex-col sm:flex-row items-center justify-center gap-4 mb-16"
          >
            <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
              <Link
                href="/dashboard"
                className="w-full sm:w-auto px-8 py-4 bg-primary text-primary-foreground rounded-xl font-bold hover:bg-primary/90 transition-colors flex items-center justify-center gap-2 shadow-lg shadow-primary/20"
              >
                Explore Bills <ArrowRight className="w-4 h-4" />
              </Link>
            </motion.div>
            <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
              <Link
                href="#how-it-works"
                className="w-full sm:w-auto px-8 py-4 bg-background text-foreground border border-border rounded-xl font-semibold hover:bg-muted transition-colors flex items-center justify-center gap-2"
              >
                How It Works <ChevronDown className="w-4 h-4" />
              </Link>
            </motion.div>
          </motion.div>
        </motion.div>

        {/* Stat Bar */}
        <motion.div
          initial={{ opacity: 0, y: 30 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.7, delay: 0.6 }}
          className="grid grid-cols-2 md:grid-cols-4 gap-4 md:gap-6 max-w-4xl mx-auto"
        >
          {heroStats.map((stat, i) => (
            <div
              key={i}
              className="bg-card/60 backdrop-blur-sm border border-border/50 rounded-xl p-5 text-center hover:border-primary/30 transition-colors"
            >
              <div className="text-2xl md:text-3xl font-bold text-foreground mb-1">
                <AnimatedNumber value={stat.value} suffix={stat.suffix} duration={2 + i * 0.3} />
              </div>
              <div className="text-xs text-muted-foreground uppercase tracking-wider">
                {stat.label}
              </div>
            </div>
          ))}
        </motion.div>

        {/* Scroll indicator */}
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 1.5 }}
          className="mt-16 flex justify-center"
        >
          <motion.div
            animate={{ y: [0, 8, 0] }}
            transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
          >
            <ArrowDown className="w-5 h-5 text-muted-foreground" />
          </motion.div>
        </motion.div>
      </div>
    </section>
  );
};

export default Hero;
