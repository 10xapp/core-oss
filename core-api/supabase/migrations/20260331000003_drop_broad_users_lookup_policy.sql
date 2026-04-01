-- Remove the broad authenticated lookup on public.users now that caller-facing
-- profile enrichment happens server-side.

DROP POLICY IF EXISTS "Authenticated users can lookup other users" ON "public"."users";
