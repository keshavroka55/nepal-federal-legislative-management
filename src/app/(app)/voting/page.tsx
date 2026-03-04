import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Voting — Vidhan",
  description: "Review clause voting and house-level voting progress.",
};

export default function VotingPage() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-bold tracking-tight text-foreground">
        Voting
      </h1>
      <p className="text-sm text-muted-foreground">
        Voting dashboards are routed and ready for implementation.
      </p>
    </div>
  );
}
