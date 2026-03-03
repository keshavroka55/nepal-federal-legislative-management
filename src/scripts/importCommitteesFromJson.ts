import fs from "fs/promises";
import path from "path";
import process from "process";

import { db } from "@/db/drizzle";
import {
  committees,
} from "@/db/schema";
import { sql } from "drizzle-orm";

/* ─── Types ─── */

type CleanedCommittee = {
  house: "HoR" | "NA";
  houseEnum: "pratinidhi_sabha" | "rastriya_sabha";
  slug: string;
  nameNp?: string;
  nameEn?: string;
  introductionNp?: string;
  introductionEn?: string;
  chairperson?: string;
  chairpersonNp?: string;
  chairpersonEn?: string;
  secretaryNp?: string;
  secretaryEn?: string;
  menuLinksNp?: Record<string, string>;
  menuLinksEn?: Record<string, string>;
  membersPageUrlNp?: string;
  membersPageUrlEn?: string;
  parliamentUrlNp?: string;
  parliamentUrlEn?: string;
  startDate?: string | null;
  endDate?: string | null;
};

/* ─── Main ─── */

async function main() {
  try {
    const root = process.cwd();
    const jsonPath = path.join(
      root,
      "services",
      "python",
      "data",
      "output",
      "committees_cleaned.json",
    );

    const raw = await fs.readFile(jsonPath, "utf-8");
    const data = JSON.parse(raw) as CleanedCommittee[];

    if (!Array.isArray(data) || data.length === 0) {
      console.log("No committees found in cleaned JSON; nothing to import.");
      process.exit(0);
    }

    let upserted = 0;

    for (const c of data) {
      const row = {
        house: c.houseEnum,
        slug: c.slug ?? null,
        nameNp: c.nameNp ?? "",
        nameEn: c.nameEn ?? "",
        introductionNp: c.introductionNp ?? "",
        introductionEn: c.introductionEn ?? "",
        chairperson: c.chairperson ?? null,
        chairpersonNp: c.chairpersonNp ?? null,
        chairpersonEn: c.chairpersonEn ?? null,
        secretaryNp: c.secretaryNp ?? null,
        secretaryEn: c.secretaryEn ?? null,
        menuLinksNp: c.menuLinksNp ?? {},
        menuLinksEn: c.menuLinksEn ?? {},
        membersPageUrlNp: c.membersPageUrlNp ?? null,
        membersPageUrlEn: c.membersPageUrlEn ?? null,
        parliamentUrlNp: c.parliamentUrlNp ?? null,
        parliamentUrlEn: c.parliamentUrlEn ?? null,
        startDate: c.startDate ?? null,
        endDate: c.endDate ?? null,
        createdAt: new Date(),
        updatedAt: new Date(),
      };

      // Upsert by stable key: house + slug
      const result = await db
        .insert(committees)
        .values(row)
        .onConflictDoUpdate({
          target: [committees.house, committees.slug],
          set: {
            nameNp: sql`EXCLUDED.name_np`,
            nameEn: sql`EXCLUDED.name_en`,
            introductionNp: sql`EXCLUDED.introduction_np`,
            introductionEn: sql`EXCLUDED.introduction_en`,
            chairperson: sql`EXCLUDED.chairperson`,
            chairpersonNp: sql`EXCLUDED.chairperson_np`,
            chairpersonEn: sql`EXCLUDED.chairperson_en`,
            secretaryNp: sql`EXCLUDED.secretary_np`,
            secretaryEn: sql`EXCLUDED.secretary_en`,
            menuLinksNp: sql`EXCLUDED.menu_links_np`,
            menuLinksEn: sql`EXCLUDED.menu_links_en`,
            membersPageUrlNp: sql`EXCLUDED.members_page_url_np`,
            membersPageUrlEn: sql`EXCLUDED.members_page_url_en`,
            parliamentUrlNp: sql`EXCLUDED.parliament_url_np`,
            parliamentUrlEn: sql`EXCLUDED.parliament_url_en`,
            startDate: sql`EXCLUDED.start_date`,
            endDate: sql`EXCLUDED.end_date`,
            updatedAt: sql`EXCLUDED.updated_at`,
          },
        })
        .returning({ id: committees.id });

      upserted++;

      const committeeId = result[0]?.id;
      if (!committeeId) continue;

      console.log(`✓ Upserted committee: ${c.nameEn || c.nameNp}`);
    }

    console.log(`✓ Upserted ${upserted} committees.`);
    process.exit(0);
  } catch (err) {
    console.error("Failed to import committees from JSON:", err);
    process.exit(1);
  }
}

void main();
