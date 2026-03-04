import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Deadlines — Vidhan",
  description: "View upcoming legislative and procedural deadlines.",
};

export default function DeadlinesPage() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-bold tracking-tight text-foreground">
        Deadlines
      </h1>
      <p className="text-sm text-muted-foreground">
        Deadline tracking route is now active.
      </p>
    </div>
  );
}
