import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Alerts — Vidhan",
  description: "Manage bill status alerts and notifications.",
};

export default function AlertsPage() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-bold tracking-tight text-foreground">
        Alerts
      </h1>
      <p className="text-sm text-muted-foreground">
        Alert management route is available.
      </p>
    </div>
  );
}
