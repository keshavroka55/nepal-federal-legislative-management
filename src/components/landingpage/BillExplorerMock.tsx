"use client";

import React from "react";
import Link from "next/link";
import { motion } from "framer-motion";
import { Users, History, CheckCircle2, Clock } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  formatBillStatus,
  formatHouse,
  formatBillType,
  formatCategory,
} from "@/lib/types";
import {
  ALL_STATUSES,
  statusToStepIndex,
  sourceDisplayName,
  FadeIn,
  type BillWithHistory,
} from "./shared";

const BillExplorerMock = ({
  featuredBill,
}: {
  featuredBill: BillWithHistory | null;
}) => {
  const bill = featuredBill;

  if (!bill) {
    return (
      <section id="status-tracker" className="py-24 md:py-32 bg-background">
        <div className="container mx-auto px-4 md:px-6">
          <FadeIn>
            <div className="text-center">
              <span className="text-primary font-bold tracking-wider uppercase text-sm">
                Preview
              </span>
              <h2 className="text-3xl md:text-4xl lg:text-5xl font-bold text-foreground mt-2 mb-3">
                Bill Status Explorer
              </h2>
              <p className="text-muted-foreground max-w-lg mx-auto">
                Loading bill data from the database…
              </p>
              <div className="mt-8 flex justify-center">
                <div className="w-8 h-8 border-4 border-primary/30 border-t-primary rounded-full animate-spin" />
              </div>
            </div>
          </FadeIn>
        </div>
      </section>
    );
  }

  const currentStepIdx = statusToStepIndex(bill.currentStatus);

  const timeline = (bill.statusHistory ?? []).slice(0, 4).map((h) => ({
    date: h.statusDateBs || h.statusDateAd?.toString().slice(0, 10) || "—",
    status: h.rawStatus || formatBillStatus(h.status),
    source: sourceDisplayName(h.source),
  }));

  const displayTitle = bill.titleEn || bill.titleNp || "—";
  const displayTitleNp = bill.titleNp || "";
  const displayCategory = formatCategory(bill.category)?.toUpperCase() || "—";
  const displayType = formatBillType(bill.billType)?.toUpperCase() || "—";
  const displayRegNo = `${bill.year}-${bill.registrationNo}`;
  const displayHouse =
    bill.house === "rastriya_sabha" ? "राष्ट्रिय सभा" : "प्रतिनिधि सभा";
  const displayHouseEn = formatHouse(bill.house) || "—";
  const displaySession = bill.session ? `Session ${bill.session}` : "—";
  const displayYear = bill.year ? `${bill.year} BS` : "—";
  const displayPresenter = bill.presenter || "—";
  const displayMinistry = bill.ministry || "—";
  const presenterInitials =
    displayPresenter !== "—"
      ? displayPresenter
          .split(" ")
          .map((w) => w[0])
          .join("")
          .slice(0, 2)
          .toUpperCase()
      : "—";

  return (
    <section id="status-tracker" className="py-24 md:py-32 bg-background">
      <div className="container mx-auto px-4 md:px-6">
        <FadeIn>
          <div className="flex flex-col md:flex-row justify-between items-start md:items-end mb-12 gap-6">
            <div>
              <span className="text-primary font-bold tracking-wider uppercase text-sm">
                Preview
              </span>
              <h2 className="text-3xl md:text-4xl lg:text-5xl font-bold text-foreground mt-2 mb-3">
                Bill Status Explorer
              </h2>
              <p className="text-muted-foreground max-w-lg">
                A glimpse of how bills look in the Vidhan tracker — rich detail,
                clear progress, complete history.
              </p>
            </div>
            <motion.div whileHover={{ scale: 1.03 }} whileTap={{ scale: 0.97 }}>
              <Link
                href="/dashboard"
                className="px-6 py-2.5 rounded-xl border-2 border-primary text-primary hover:bg-primary hover:text-primary-foreground transition-all font-semibold text-sm block"
              >
                Open Full App →
              </Link>
            </motion.div>
          </div>
        </FadeIn>

        <FadeIn delay={0.15}>
          <div className="rounded-2xl border border-border bg-card shadow-2xl relative overflow-hidden">
            <div className="h-1 w-full bg-gradient-to-r from-primary via-secondary to-primary" />
            <div className="p-6 md:p-10">
              {/* Header */}
              <div className="flex flex-col lg:flex-row justify-between gap-6 pb-8 border-b border-border/50">
                <div>
                  <div className="flex flex-wrap items-center gap-2 mb-3">
                    <span className="bg-primary/10 text-primary px-3 py-1 rounded-full text-xs font-bold border border-primary/20">
                      {displayCategory}
                    </span>
                    <span className="bg-secondary/10 text-secondary px-3 py-1 rounded-full text-xs font-bold border border-secondary/20">
                      {displayType}
                    </span>
                    <span className="text-muted-foreground text-xs font-mono bg-muted px-2 py-1 rounded">
                      REG: {displayRegNo}
                    </span>
                  </div>
                  <h3 className="text-2xl md:text-3xl font-bold text-foreground mb-1">
                    {displayTitle}
                  </h3>
                  <p className="text-lg text-muted-foreground font-mukta">
                    {displayTitleNp}
                  </p>
                </div>
                <div className="flex flex-wrap gap-3 items-start">
                  {[
                    {
                      label: "Origin",
                      main: displayHouse,
                      sub: displayHouseEn,
                    },
                    { label: "Session", main: displaySession, sub: "अधिवेशन" },
                    { label: "Year", main: displayYear, sub: bill.year || "—" },
                  ].map((card, ci) => (
                    <div
                      key={ci}
                      className="bg-muted/50 p-4 rounded-xl text-center min-w-[110px] border border-border/30"
                    >
                      <div className="text-[10px] text-muted-foreground uppercase tracking-wider mb-1.5 font-bold">
                        {card.label}
                      </div>
                      <div className="text-foreground font-semibold text-sm">
                        {card.main}
                      </div>
                      <div className="text-muted-foreground text-xs">
                        {card.sub}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

              {/* Status Progress */}
              <div className="py-8">
                <div className="hidden md:flex items-center mb-4 gap-0">
                  {[
                    { name: "Introduction", span: 4, color: "text-blue-500" },
                    { name: "Deep Scrutiny", span: 3, color: "text-amber-500" },
                    { name: "Second House", span: 2, color: "text-purple-500" },
                    {
                      name: "Authentication",
                      span: 3,
                      color: "text-emerald-500",
                    },
                    { name: "Post", span: 1, color: "text-rose-500" },
                  ].map((g, gi) => (
                    <div
                      key={gi}
                      className="text-center"
                      style={{ flex: g.span }}
                    >
                      <span
                        className={cn(
                          "text-[9px] font-bold uppercase tracking-widest",
                          g.color,
                        )}
                      >
                        {g.name}
                      </span>
                    </div>
                  ))}
                </div>

                <div className="flex items-center gap-0">
                  {ALL_STATUSES.map((s, si) => {
                    const done = si < currentStepIdx;
                    const current = si === currentStepIdx;
                    return (
                      <React.Fragment key={s.key}>
                        <div className="flex flex-col items-center gap-1.5 min-w-0 flex-1 group relative">
                          <div
                            className={cn(
                              "w-6 h-6 md:w-8 md:h-8 rounded-full flex items-center justify-center text-[9px] md:text-[10px] font-bold shrink-0 transition-all",
                              done
                                ? "bg-primary text-primary-foreground"
                                : current
                                  ? "bg-primary/20 text-primary ring-2 ring-primary ring-offset-1 ring-offset-card"
                                  : "bg-muted text-muted-foreground",
                            )}
                          >
                            {done ? (
                              <CheckCircle2 className="w-3 h-3 md:w-4 md:h-4" />
                            ) : current ? (
                              <Clock className="w-3 h-3 md:w-4 md:h-4" />
                            ) : (
                              si + 1
                            )}
                          </div>
                          <span
                            className={cn(
                              "text-[7px] md:text-[9px] text-center font-medium leading-tight hidden md:block",
                              done
                                ? "text-foreground"
                                : current
                                  ? "text-primary font-bold"
                                  : "text-muted-foreground",
                            )}
                          >
                            {s.label}
                          </span>
                          <div className="md:hidden absolute -top-8 left-1/2 -translate-x-1/2 bg-card border border-border px-2 py-0.5 rounded text-[9px] text-foreground whitespace-nowrap opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none shadow-lg z-10">
                            {s.label}
                          </div>
                        </div>
                        {si < ALL_STATUSES.length - 1 && (
                          <div
                            className={cn(
                              "h-[2px] md:h-[3px] rounded-full w-1 md:w-auto md:flex-1 shrink-0 -mt-3 md:-mt-4",
                              done ? "bg-primary" : "bg-muted",
                            )}
                          />
                        )}
                      </React.Fragment>
                    );
                  })}
                </div>

                <div className="mt-4 text-center">
                  <span className="text-xs font-medium text-muted-foreground">
                    Current:{" "}
                  </span>
                  <span className="text-xs font-bold text-primary">
                    {bill.currentStatus
                      ? formatBillStatus(bill.currentStatus)
                      : "Not set"}
                  </span>
                </div>
              </div>

              {/* Details Grid */}
              <div className="grid lg:grid-cols-2 gap-8 pt-4">
                <div>
                  <h4 className="text-foreground font-semibold mb-4 flex items-center gap-2 text-sm uppercase tracking-wider">
                    <Users className="w-4 h-4 text-primary" /> Presenter &
                    Ministry
                  </h4>
                  <div className="flex items-center gap-4 bg-muted/30 p-4 rounded-xl border border-border/30 font-mukta">
                    <div className="w-12 h-12 rounded-full bg-secondary text-secondary-foreground flex items-center justify-center font-bold text-xl">
                      {presenterInitials}
                    </div>
                    <div>
                      <div className="text-foreground font-semibold text-xl">
                        {displayPresenter}
                      </div>
                      <div className="text-muted-foreground text-md">
                        {displayMinistry}
                      </div>
                      <div className="text-muted-foreground text-md">
                        {bill.ministry || "—"}
                      </div>
                    </div>
                  </div>
                </div>

                <div>
                  <h4 className="text-foreground font-semibold mb-4 flex items-center gap-2 text-sm uppercase tracking-wider">
                    <History className="w-4 h-4 text-primary" /> Recent History
                  </h4>
                  <div className="space-y-3">
                    {timeline.map((entry, ei) => (
                      <div key={ei} className="flex items-start gap-3 relative">
                        <div className="flex flex-col items-center">
                          <div
                            className={cn(
                              "w-2.5 h-2.5 rounded-full shrink-0 mt-1.5",
                              ei === 0 ? "bg-primary" : "bg-border",
                            )}
                          />
                          {ei < timeline.length - 1 && (
                            <div className="w-px h-full bg-border/50 min-h-[20px]" />
                          )}
                        </div>
                        <div className="flex-1 pb-2">
                          <div className="text-foreground text-sm font-medium">
                            {entry.status}
                          </div>
                          <div className="flex items-center gap-2 mt-1">
                            <span className="text-muted-foreground text-xs">
                              {entry.date}
                            </span>
                            <span
                              className={cn(
                                "text-[10px] px-2 py-0.5 rounded-full font-medium",
                                entry.source === "Parliament Scrape"
                                  ? "bg-blue-500/10 text-blue-600"
                                  : entry.source === "Gazette Scrape"
                                    ? "bg-emerald-500/10 text-emerald-600"
                                    : "bg-amber-500/10 text-amber-600",
                              )}
                            >
                              {entry.source}
                            </span>
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>

              {/* Source Badges */}
              <div className="pt-8 mt-8 border-t border-border/50 flex flex-wrap items-center gap-3">
                <span className="text-xs text-muted-foreground font-medium uppercase tracking-wider">
                  Data Sources:
                </span>
                {["Parliament Scrape", "Gazette Scrape", "Manual Entry"].map(
                  (src, si) => (
                    <span
                      key={si}
                      className="text-xs px-3 py-1.5 rounded-full bg-muted/50 text-muted-foreground border border-border/50 font-medium"
                    >
                      {src}
                    </span>
                  ),
                )}
              </div>
            </div>
          </div>
        </FadeIn>
      </div>
    </section>
  );
};

export default BillExplorerMock;
