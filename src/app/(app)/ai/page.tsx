import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "AI Analysis — Vidhan",
  description: "AI-assisted summaries and legislative analysis tools.",
};

export default function AiPage() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-bold tracking-tight text-foreground">
        AI Analysis
      </h1>
      <p className="text-sm text-muted-foreground">
        AI analysis route is available for feature integration.
      </p>
    </div>
  );
}
