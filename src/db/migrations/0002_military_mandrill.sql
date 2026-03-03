ALTER TABLE "committees" ADD COLUMN "chairperson" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "parliament_url_np" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "parliament_url_en" text;--> statement-breakpoint
ALTER TABLE "committees" ADD COLUMN "updated_at" timestamp DEFAULT now();