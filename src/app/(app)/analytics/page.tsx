import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Analytics — Vidhan",
  description: "Legislative analytics and trend reporting.",
};

export default function AnalyticsPage() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-bold tracking-tight text-foreground">
        Analytics
      </h1>
      <p className="text-sm text-muted-foreground">
        Analytics route is active and ready for dashboard components.
      </p>
    </div>
  );
}
