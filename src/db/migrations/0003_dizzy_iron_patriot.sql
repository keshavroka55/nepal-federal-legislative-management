ALTER TABLE "committees" ADD COLUMN "slug" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "chairperson_np" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "chairperson_en" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "secretary_np" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "secretary_en" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "menu_links_np" jsonb;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "menu_links_en" jsonb;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "members_page_url_np" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "members_page_url_en" text;--> statement-breakpoint
CREATE UNIQUE INDEX "committees_house_slug_uq" ON "committees" USING btree ("house","slug");