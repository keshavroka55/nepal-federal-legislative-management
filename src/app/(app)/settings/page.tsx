import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Settings — Vidhan",
  description: "Configure preferences for the Vidhan tracker.",
};

export default function SettingsPage() {
  return (
    <div className="space-y-3">
      <h1 className="text-2xl font-bold tracking-tight text-foreground">
        Settings
      </h1>
      <p className="text-sm text-muted-foreground">
        Settings route is active.
      </p>
    </div>
  );
}
