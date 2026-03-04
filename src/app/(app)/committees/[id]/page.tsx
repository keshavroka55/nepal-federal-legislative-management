import type { Metadata } from "next";
import Link from "next/link";
import { notFound } from "next/navigation";
import { ArrowLeft, ExternalLink } from "lucide-react";
import { eq } from "drizzle-orm";
import { db } from "@/db/drizzle";
import { committees } from "@/db/schema";

type Props = { params: Promise<{ id: string }> };

function formatHouse(house: "pratinidhi_sabha" | "rastriya_sabha" | null) {
  if (house === "pratinidhi_sabha") return "House of Representatives";
  if (house === "rastriya_sabha") return "National Assembly";
  return "—";
}

function isValidId(id: string) {
  const parsed = Number(id);
  return Number.isInteger(parsed) && parsed > 0;
}

function escapeRegex(value: string) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

const NEPALI_SECTION_HEADINGS = [
  "समिति विघटन भएको सम्बन्धी जानकारी",
  "परिचय",
  "समितिको कार्यक्षेत्र",
  "समिति गठन",
  "पदेन सदस्य",
  "समिति सदस्यहरु",
  "समिति सचिवालय",
];

function sectionRegex() {
  return new RegExp(
    `(${NEPALI_SECTION_HEADINGS.map(escapeRegex).join("|")})\\s*[:：\\-]*`,
    "g",
  );
}

function splitNepaliLines(text: string) {
  return text
    .replace(/^\s*\*{1,8}\s*(?=\S)/gm, "")
    .replace(/\s+\*{2,}\s+(?=\S)/g, " ")
    .replace(/\s+([०-९]+[.)])/g, "\n$1")
    .replace(/\s+([0-9]+[.)])/g, "\n$1")
    .replace(/\s+(क्र\.\s*सं\.)/g, "\n$1")
    .replace(/\s+(\*{1,8})/g, "\n$1")
    .split(/\n+/)
    .map((line) => line.replace(/^\*+\s*/, "").trim())
    .filter((line) => Boolean(line) && !/^\*+$/.test(line));
}

type NepaliSection = {
  heading: string;
  lines: string[];
};

function formatNepaliIntroduction(
  raw: string | null,
  nameNp: string | null,
  nameEn: string | null,
): NepaliSection[] {
  if (!raw) return [];

  let text = raw.replace(/\s+/g, " ").trim();
  text = text.replace(/^अभिलेख\s*→\s*/, "");

  const removableTitles = [nameNp, nameEn].filter(Boolean) as string[];
  for (const title of removableTitles) {
    const pattern = new RegExp(`^${escapeRegex(title)}\\s*[,।-]*\\s*`);
    text = text.replace(pattern, "");
  }

  if (!text) return [];

  const regex = sectionRegex();
  const matches = Array.from(text.matchAll(regex));

  if (matches.length === 0) {
    return [{ heading: "विवरण", lines: splitNepaliLines(text) }];
  }

  const sections: NepaliSection[] = [];
  const prefix = text.slice(0, matches[0].index ?? 0).trim();
  if (prefix) {
    sections.push({ heading: "सारांश", lines: splitNepaliLines(prefix) });
  }

  for (let i = 0; i < matches.length; i += 1) {
    const current = matches[i];
    const next = matches[i + 1];
    const heading = current[1].trim();
    const bodyStart = (current.index ?? 0) + current[0].length;
    const bodyEnd = next ? (next.index ?? text.length) : text.length;
    const body = text.slice(bodyStart, bodyEnd).trim();
    const lines = splitNepaliLines(body);

    if (lines.length > 0) {
      sections.push({ heading, lines });
    }
  }

  return sections;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { id } = await params;
  if (!isValidId(id)) return { title: "Committee not found — Vidhan" };

  const committee = await db.query.committees.findFirst({
    where: eq(committees.id, Number(id)),
  });

  if (!committee) return { title: "Committee not found — Vidhan" };

  return {
    title: `${committee.nameNp || committee.nameEn || "Committee"} — Vidhan`,
    description:
      committee.introductionEn || committee.introductionNp || undefined,
  };
}

export default async function CommitteeDetailPage({ params }: Props) {
  const { id } = await params;
  if (!isValidId(id)) notFound();

  const committeeId = Number(id);
  const committee = await db.query.committees.findFirst({
    where: eq(committees.id, committeeId),
  });

  if (!committee) notFound();

  const title = committee.nameNp || committee.nameEn || "Unnamed committee";
  const subtitle =
    committee.nameEn && committee.nameNp ? committee.nameEn : null;
  const chair =
    committee.chairpersonEn ||
    committee.chairpersonNp ||
    committee.chairperson ||
    "—";
  const secretary = committee.secretaryEn || committee.secretaryNp || "—";
  const nepaliSections = formatNepaliIntroduction(
    committee.introductionNp,
    committee.nameNp,
    committee.nameEn,
  );

  const sourceLinks = [
    {
      label: "Members Page",
      url: committee.membersPageUrlEn || committee.membersPageUrlNp,
    },
    {
      label: "Parliament Page",
      url: committee.parliamentUrlEn || committee.parliamentUrlNp,
    },
  ].filter((item) => Boolean(item.url)) as { label: string; url: string }[];

  return (
    <div className="space-y-6">
      <Link
        href="/committees"
        className="inline-flex items-center gap-1.5 text-sm text-muted-foreground hover:text-foreground transition-colors"
      >
        <ArrowLeft className="h-4 w-4" />
        Back to Committees
      </Link>

      <div className="rounded-xl border border-border/60 bg-card p-6 space-y-6">
        <header className="space-y-2">
          <span className="inline-flex rounded-full border border-border bg-secondary px-2 py-0.5 text-[10px] font-semibold text-secondary-foreground">
            {formatHouse(committee.house)}
          </span>
          <h1 className="text-2xl font-bold tracking-tight text-foreground">
            {title}
          </h1>
          {subtitle ? (
            <p className="text-sm text-muted-foreground font-mukta">{subtitle}</p>
          ) : null}
        </header>

        <section className="grid gap-3 sm:grid-cols-2">
          <div className="rounded-lg border border-border/50 bg-background p-3">
            <p className="text-xs text-muted-foreground">Chairperson</p>
            <p className="text-sm font-medium text-foreground">{chair}</p>
          </div>
          <div className="rounded-lg border border-border/50 bg-background p-3">
            <p className="text-xs text-muted-foreground">Secretary</p>
            <p className="text-sm font-medium text-foreground">{secretary}</p>
          </div>
          <div className="rounded-lg border border-border/50 bg-background p-3">
            <p className="text-xs text-muted-foreground">Start Date</p>
            <p className="text-sm font-medium text-foreground">
              {committee.startDate || "—"}
            </p>
          </div>
          <div className="rounded-lg border border-border/50 bg-background p-3">
            <p className="text-xs text-muted-foreground">End Date</p>
            <p className="text-sm font-medium text-foreground">
              {committee.endDate || "—"}
            </p>
          </div>
        </section>

        {nepaliSections.length > 0 ? (
          <section className="space-y-4">
            <h2 className="text-sm font-semibold text-foreground">
              परिचय तथा विवरण
            </h2>
            {nepaliSections.map((section, sectionIndex) => (
              <article
                key={`${section.heading}-${sectionIndex}`}
                className="space-y-2 rounded-lg border border-border/50 bg-background p-3"
              >
                <h3 className="text-sm font-semibold text-foreground">
                  {section.heading}
                </h3>
                <div className="space-y-1.5">
                  {section.lines.map((line, index) => (
                    <p
                      key={`${section.heading}-${index}`}
                      className="text-sm leading-6 text-muted-foreground font-mukta"
                    >
                      {line}
                    </p>
                  ))}
                </div>
              </article>
            ))}
          </section>
        ) : committee.introductionEn ? (
          <section className="space-y-2">
            <h2 className="text-sm font-semibold text-foreground">Introduction</h2>
            <p className="text-sm leading-6 text-muted-foreground">
              {committee.introductionEn}
            </p>
          </section>
        ) : null}

        {sourceLinks.length > 0 ? (
          <section className="space-y-2">
            <h2 className="text-sm font-semibold text-foreground">Links</h2>
            <div className="flex flex-wrap gap-2">
              {sourceLinks.map((item) => (
                <a
                  key={item.url}
                  href={item.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex items-center gap-1 rounded-md border border-border/60 px-2.5 py-1.5 text-xs font-medium text-primary hover:bg-primary/5"
                >
                  {item.label}
                  <ExternalLink className="h-3 w-3" />
                </a>
              ))}
            </div>
          </section>
        ) : null}
      </div>
    </div>
  );
}
