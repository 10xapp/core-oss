-- Phase 1 grouped notification primitives

ALTER TABLE IF EXISTS "public"."notifications"
    ADD COLUMN IF NOT EXISTS "group_key" "text";

CREATE UNIQUE INDEX IF NOT EXISTS "uq_notifications_grouped_active"
    ON "public"."notifications" USING "btree" ("user_id", "type", "group_key")
    WHERE (("group_key" IS NOT NULL) AND ("archived" = false));
