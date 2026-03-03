import { not, relations, sql } from "drizzle-orm";
import {
  boolean,
  date,
  foreignKey,
  index,
  integer,
  jsonb,
  pgEnum,
  pgTable,
  serial,
  text,
  timestamp,
  uniqueIndex,
  uuid,
  varchar,
} from "drizzle-orm/pg-core";

/*==================
Enums 
===================*/
export const committeeType = pgEnum("committee_type", ["HoR", "NA"]);

export const billStatusEnum = pgEnum("bill_status", [
  // Phase 2 — Introduction (scraped from parliament site)
  "registered",
  "first_reading",
  "general_discussion",
  "amendment_window",

  // Phase 3 — Deep Scrutiny (scraped from parliament site)
  "committee_review",
  "clause_voting",
  "first_house_passed",

  // Phase 4 — Second House (scraped from parliament site)
  "second_house",
  "joint_sitting",

  // Phase 5 — Authentication (parliament site + gazette)
  "speaker_certification",
  "assented",
  "gazette_published",

  // Phase 6 — Post Implementation
  "amendment_or_repeal",
]);

export const billHouseEnum = pgEnum("bill_house", [
  "pratinidhi_sabha", // House of Representatives
  "rastriya_sabha", // National Assembly
]);

export const billTypeEnum = pgEnum("bill_type", ["original", "amendment"]);

export const billCategoryEnum = pgEnum("bill_category", [
  "governmental",
  "non_governmental",
]);

export const statusSourceEnum = pgEnum("status_source", [
  "parliament_scrape", // auto scraped from parliamentofnepal.gov.np
  "gazette_scrape", // auto scraped from rajpatra.dop.gov.np
  "manual_entry", // manually entered by admin
]);

/*==================
Commitee schema 
===================*/

export const committee = pgTable("committee", {
  id: uuid("id")
    .default(sql`pg_catalog.gen_random_uuid()`)
    .primaryKey(),
  type: committeeType("type").notNull(),
  name: text("name").notNull().unique(),
  startDate: date("start_date"),
  endDate: date("end_date"),
  introduction: text("introduction").notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
});

// ============================================================
// BILLS TABLE — one row per bill
// ============================================================

export const bills = pgTable(
  "bills",
  {
    id: uuid("id")
      .default(sql`pg_catalog.gen_random_uuid()`)
      .primaryKey(),

    // Identifiers
    parliamentId: text("parliament_id").unique(),
    registrationNo: text("registration_no").notNull(),
    year: text("year").notNull(), // BS year e.g. "2082"
    session: text("session"), // Parliament session number e.g. "6"

    // Bill details
    titleNp: text("title_np"), // Nepali title
    titleEn: text("title_en"), // English title (if available)
    presenter: text("presenter"), // "Hon. Ramnath Adhikari"
    ministry: text("ministry"), // "Ministry of Agriculture..."
    house: billHouseEnum("house"),
    billType: billTypeEnum("bill_type"), // original | amendment
    category: billCategoryEnum("category"), // governmental | non_governmental

    // Current status (updated on every scrape)
    currentStatus: billStatusEnum("current_status"),
    currentPhase: integer("current_phase"), // 2-6 based on status

    // Dates in Bikram Sambat (stored as-is from parliament site)
    registeredDateBs: text("registered_date_bs"), // "2082-03-29"
    authenticatedDateBs: text("authenticated_date_bs"), // "2082-05-19"

    // Dates converted to AD (for querying/sorting)
    registeredDateAd: date("registered_date_ad"),
    authenticatedDateAd: date("authenticated_date_ad"),

    // Document links
    registeredBillUrl: text("registered_bill_url"), // PDF of original bill
    authenticatedBillUrl: text("authenticated_bill_url"), // PDF of authenticated bill

    // Source tracking
    parliamentUrl: text("parliament_url"), // source page URL
    lastScrapedAt: timestamp("last_scraped_at"),

    createdAt: timestamp("created_at").defaultNow(),
    updatedAt: timestamp("updated_at").defaultNow(),
  },
  (table) => ({
    // Unique per registration number + year (a bill can't have same reg no in same year)
    regNoYearIdx: uniqueIndex("bills_reg_no_year_idx").on(
      table.registrationNo,
      table.year,
    ),
    // Fast filter by status
    statusIdx: index("bills_status_idx").on(table.currentStatus),
    // Fast filter by house
    houseIdx: index("bills_house_idx").on(table.house),
    // Fast filter by ministry
    ministryIdx: index("bills_ministry_idx").on(table.ministry),
  }),
);

// ============================================================
// BILL STATUS HISTORY — one row per status change
// This is the core tracking table
// ============================================================

export const billStatusHistory = pgTable(
  "bill_status_history",
  {
    id: uuid("id")
      .default(sql`pg_catalog.gen_random_uuid()`)
      .primaryKey(),
    billId: uuid("bill_id")
      .notNull()
      .references(() => bills.id, { onDelete: "cascade" }),

    // The status this bill moved TO
    status: billStatusEnum("status").notNull(),

    // Raw status string from parliament site (e.g. "Distribution to member")
    rawStatus: text("raw_status"),

    // Where this status change was detected
    source: statusSourceEnum("source").default("parliament_scrape"),

    // Date of status change in BS and AD
    statusDateBs: text("status_date_bs"), // "2082-04-20"
    statusDateAd: date("status_date_ad"),

    notes: text("notes"), // any extra context
    sourceUrl: text("source_url"), // URL where change was found

    recordedAt: timestamp("recorded_at").defaultNow(),
  },
  (table) => ({
    billIdIdx: index("bill_status_history_bill_id_idx").on(table.billId),
  }),
);

// ============================================================
// COMMITTEES — committees that review bills
// ============================================================

export const committees = pgTable("committees", {
  id: serial("id").primaryKey(),
  slug: text("slug"),
  nameNp: text("name_np"),
  nameEn: text("name_en"),
  house: billHouseEnum("house"),
  introductionNp: text("introduction_np"),
  introductionEn: text("introduction_en"),
  chairperson: text("chairperson"),
  chairpersonNp: text("chairperson_np"),
  chairpersonEn: text("chairperson_en"),
  secretaryNp: text("secretary_np"),
  secretaryEn: text("secretary_en"),
  menuLinksNp: jsonb("menu_links_np").$type<Record<string, string>>(),
  menuLinksEn: jsonb("menu_links_en").$type<Record<string, string>>(),
  membersPageUrlNp: text("members_page_url_np"),
  membersPageUrlEn: text("members_page_url_en"),
  parliamentUrlNp: text("parliament_url_np"),
  parliamentUrlEn: text("parliament_url_en"),
  startDate: date("start_date"),
  endDate: date("end_date"),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
}, (table) => ({
  houseSlugUniqueIdx: uniqueIndex("committees_house_slug_uq").on(
    table.house,
    table.slug,
  ),
}));

// ============================================================
// BILL COMMITTEE ASSIGNMENTS — which committee reviewed which bill
// ============================================================

export const billCommitteeAssignments = pgTable("bill_committee_assignments", {
  id: serial("id").primaryKey(),
  billId: uuid("bill_id")
    .notNull()
    .references(() => bills.id, { onDelete: "cascade" }),
  committeeId: integer("committee_id")
    .notNull()
    .references(() => committees.id),
  assignedDateBs: text("assigned_date_bs"),
  assignedDateAd: date("assigned_date_ad"),
  reportSubmittedDateBs: text("report_submitted_date_bs"),
  reportSubmittedDateAd: date("report_submitted_date_ad"),
  createdAt: timestamp("created_at").defaultNow(),
});

// ============================================================
// SCRAPE LOGS — track every scrape run for debugging
// ============================================================

export const scrapeLogs = pgTable("scrape_logs", {
  id: uuid("id").primaryKey(),
  startedAt: timestamp("started_at").defaultNow(),
  finishedAt: timestamp("finished_at"),
  billsFound: integer("bills_found").default(0),
  billsUpdated: integer("bills_updated").default(0),
  billsNew: integer("bills_new").default(0),
  errors: text("errors"), // JSON array of error messages
  status: text("status"), // 'success' | 'partial' | 'failed'
});

// ============================================================
// RELATIONS
// ============================================================

export const billsRelations = relations(bills, ({ many }) => ({
  statusHistory: many(billStatusHistory),
  committeeAssignments: many(billCommitteeAssignments),
}));

export const billStatusHistoryRelations = relations(
  billStatusHistory,
  ({ one }) => ({
    bill: one(bills, {
      fields: [billStatusHistory.billId],
      references: [bills.id],
    }),
  }),
);

export const committeesRelations = relations(committees, ({ many }) => ({
  billAssignments: many(billCommitteeAssignments),
}));

export const billCommitteeAssignmentsRelations = relations(
  billCommitteeAssignments,
  ({ one }) => ({
    bill: one(bills, {
      fields: [billCommitteeAssignments.billId],
      references: [bills.id],
    }),
    committee: one(committees, {
      fields: [billCommitteeAssignments.committeeId],
      references: [committees.id],
    }),
  }),
);
