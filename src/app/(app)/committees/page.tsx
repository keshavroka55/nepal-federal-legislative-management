import type { Metadata } from "next";
import Link from "next/link";
import { asc } from "drizzle-orm";
import { db } from "@/db/drizzle";
import { committees } from "@/db/schema";

export const metadata: Metadata = {
  title: "Committees — Vidhan",
  description: "Track parliamentary committee activity and bill assignments.",
};

function formatHouse(house: "pratinidhi_sabha" | "rastriya_sabha" | null) {
  if (house === "pratinidhi_sabha") return "House of Representatives";
  if (house === "rastriya_sabha") return "National Assembly";
  return "—";
}

export default async function CommitteesPage() {
  const data = await db
    .select({
      id: committees.id,
      nameNp: committees.nameNp,
      nameEn: committees.nameEn,
      house: committees.house,
    })
    .from(committees)
    .orderBy(
      asc(committees.house),
      asc(committees.nameEn),
      asc(committees.nameNp),
    );

  return (
    <div className="space-y-6">
      <div className="flex flex-col gap-1">
        <h1 className="text-2xl font-bold tracking-tight text-foreground">
          Committees
        </h1>
        <p className="text-sm text-muted-foreground">
          Showing {data.length} committee{data.length === 1 ? "" : "s"} from
          the database.
        </p>
      </div>

      {data.length === 0 ? (
        <div className="rounded-xl border border-border/60 bg-card p-6 text-sm text-muted-foreground">
          No committee records found.
        </div>
      ) : (
        <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-3">
          {data.map((committee) => {
            const title =
              committee.nameNp || committee.nameEn || "Unnamed committee";
            const subtitle =
              committee.nameNp && committee.nameEn ? committee.nameEn : null;

            return (
              <Link
                key={committee.id}
                href={`/committees/${committee.id}`}
                className="rounded-xl border border-border/60 bg-card p-4 shadow-sm transition-colors hover:border-primary/40"
              >
                <article className="space-y-3">
                  <span className="inline-flex rounded-full border border-border bg-secondary px-2 py-0.5 text-[10px] font-semibold text-secondary-foreground">
                    {formatHouse(committee.house)}
                  </span>
                  <h2 className="text-base font-semibold leading-snug text-foreground">
                    {title}
                  </h2>
                  {subtitle ? (
                    <p className="text-xs text-muted-foreground">{subtitle}</p>
                  ) : null}
                </article>
              </Link>
            );
          })}
        </div>
      )}
    </div>
  );
}
