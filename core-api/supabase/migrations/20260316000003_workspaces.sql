-- Migration: Workspaces
-- Creates workspace infrastructure: workspaces, workspace_members, workspace_apps,
-- workspace_app_members tables plus all related functions, indexes, RLS policies,
-- triggers, and grants.

-- =============================================================================
-- Functions
-- =============================================================================

CREATE OR REPLACE FUNCTION "public"."is_workspace_member"("p_workspace_id" "uuid", "p_user_id" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM workspace_members
        WHERE workspace_id = p_workspace_id
        AND user_id = p_user_id
    );
END;
$$;


ALTER FUNCTION "public"."is_workspace_member"("p_workspace_id" "uuid", "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_workspace_admin"("p_workspace_id" "uuid", "p_user_id" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM workspace_members
        WHERE workspace_id = p_workspace_id
        AND user_id = p_user_id
        AND role IN ('owner', 'admin')
    );
END;
$$;


ALTER FUNCTION "public"."is_workspace_admin"("p_workspace_id" "uuid", "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."is_workspace_owner"("p_workspace_id" "uuid", "p_user_id" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN EXISTS (
        SELECT 1 FROM workspace_members
        WHERE workspace_id = p_workspace_id
        AND user_id = p_user_id
        AND role = 'owner'
    );
END;
$$;


ALTER FUNCTION "public"."is_workspace_owner"("p_workspace_id" "uuid", "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."can_access_workspace_app"("p_workspace_app_id" "uuid", "p_user_id" "uuid" DEFAULT "auth"."uid"()) RETURNS boolean
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_workspace_id UUID;
    v_is_public BOOLEAN;
BEGIN
    -- Get app info
    SELECT workspace_id, is_public INTO v_workspace_id, v_is_public
    FROM workspace_apps
    WHERE id = p_workspace_app_id;

    -- If app not found, deny access
    IF v_workspace_id IS NULL THEN
        RETURN FALSE;
    END IF;

    -- Must be a workspace member first
    IF NOT is_workspace_member(v_workspace_id, p_user_id) THEN
        RETURN FALSE;
    END IF;

    -- If app is public, all workspace members can access
    IF v_is_public THEN
        RETURN TRUE;
    END IF;

    -- For private apps, check explicit membership
    RETURN EXISTS (
        SELECT 1 FROM workspace_app_members
        WHERE workspace_app_id = p_workspace_app_id
        AND user_id = p_user_id
    );
END;
$$;


ALTER FUNCTION "public"."can_access_workspace_app"("p_workspace_app_id" "uuid", "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_workspace_apps"("p_workspace_id" "uuid") RETURNS TABLE("id" "uuid", "app_type" "public"."mini_app_type", "is_public" boolean, "position" integer, "config" "jsonb", "has_access" boolean)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user is workspace member
    IF NOT is_workspace_member(p_workspace_id) THEN
        RAISE EXCEPTION 'Not a member of this workspace';
    END IF;

    RETURN QUERY
    SELECT
        wa.id,
        wa.app_type,
        wa.is_public,
        wa."position",
        wa.config,
        can_access_workspace_app(wa.id) AS has_access
    FROM workspace_apps wa
    WHERE wa.workspace_id = p_workspace_id
    ORDER BY wa."position";
END;
$$;


ALTER FUNCTION "public"."get_workspace_apps"("p_workspace_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_workspace_role"("p_workspace_id" "uuid", "p_user_id" "uuid" DEFAULT "auth"."uid"()) RETURNS "public"."workspace_role"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_role workspace_role;
BEGIN
    SELECT role INTO v_role
    FROM workspace_members
    WHERE workspace_id = p_workspace_id
    AND user_id = p_user_id;

    RETURN v_role;
END;
$$;


ALTER FUNCTION "public"."get_workspace_role"("p_workspace_id" "uuid", "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_default_workspace"() RETURNS TABLE("id" "uuid", "name" "text", "is_default" boolean, "created_at" timestamp with time zone, "role" "public"."workspace_role")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        w.id,
        w.name,
        w.is_default,
        w.created_at,
        wm.role
    FROM workspaces w
    JOIN workspace_members wm ON wm.workspace_id = w.id
    WHERE wm.user_id = auth.uid()
    AND w.is_default = TRUE
    LIMIT 1;
END;
$$;


ALTER FUNCTION "public"."get_default_workspace"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean DEFAULT false, "p_create_default_apps" boolean DEFAULT true) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_workspace_id UUID;
BEGIN
    INSERT INTO workspaces (name, owner_id, is_default)
    VALUES (p_name, p_user_id, p_is_default)
    RETURNING id INTO v_workspace_id;

    INSERT INTO workspace_members (workspace_id, user_id, role)
    VALUES (v_workspace_id, p_user_id, 'owner');

    IF p_create_default_apps THEN
        INSERT INTO workspace_apps (workspace_id, app_type, is_public, position)
        VALUES
            (v_workspace_id, 'chat', TRUE, 0),
            (v_workspace_id, 'messages', TRUE, 1),
            (v_workspace_id, 'projects', TRUE, 2),
            (v_workspace_id, 'files', TRUE, 3),
            (v_workspace_id, 'email', TRUE, 4),
            (v_workspace_id, 'calendar', TRUE, 5);
    END IF;

    RETURN v_workspace_id;
END;
$$;


ALTER FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."reorder_workspace_apps"("p_workspace_id" "uuid", "p_app_positions" "jsonb") RETURNS integer
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public', 'pg_temp'
    AS $$
DECLARE
    v_item JSONB;
    v_updated_count INT := 0;
    v_user_id UUID := auth.uid();
    v_expected_count INT := jsonb_array_length(COALESCE(p_app_positions, '[]'::jsonb));
BEGIN
    -- Validate user is authenticated
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'Not authenticated';
    END IF;

    -- Validate user is admin/owner of the workspace
    IF NOT is_workspace_admin(p_workspace_id, v_user_id) THEN
        RAISE EXCEPTION 'Not authorized to reorder apps in this workspace';
    END IF;

    -- Process each position update
    FOR v_item IN SELECT * FROM jsonb_array_elements(p_app_positions)
    LOOP
        UPDATE workspace_apps
        SET position = (v_item->>'position')::INT
        WHERE id = (v_item->>'id')::UUID
          AND workspace_id = p_workspace_id;

        IF FOUND THEN
            v_updated_count := v_updated_count + 1;
        END IF;
    END LOOP;

    -- Ensure all apps were updated (atomicity check)
    IF v_updated_count <> v_expected_count THEN
        RAISE EXCEPTION 'One or more apps not found in workspace';
    END IF;

    RETURN v_updated_count;
END;
$$;


ALTER FUNCTION "public"."reorder_workspace_apps"("p_workspace_id" "uuid", "p_app_positions" "jsonb") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_workspace_files_app"("p_workspace_id" "uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_app_id UUID;
BEGIN
    SELECT id INTO v_app_id
    FROM workspace_apps
    WHERE workspace_id = p_workspace_id
    AND app_type = 'files';

    RETURN v_app_id;
END;
$$;


ALTER FUNCTION "public"."get_workspace_files_app"("p_workspace_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_workspace_projects_app"("p_workspace_id" "uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_app_id UUID;
BEGIN
    SELECT id INTO v_app_id
    FROM workspace_apps
    WHERE workspace_id = p_workspace_id
    AND app_type = 'projects';

    RETURN v_app_id;
END;
$$;


ALTER FUNCTION "public"."get_workspace_projects_app"("p_workspace_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_workspace_tasks_app"("p_workspace_id" "uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_app_id UUID;
BEGIN
    SELECT id INTO v_app_id
    FROM workspace_apps
    WHERE workspace_id = p_workspace_id
    AND app_type = 'tasks';

    RETURN v_app_id;
END;
$$;


ALTER FUNCTION "public"."get_workspace_tasks_app"("p_workspace_id" "uuid") OWNER TO "postgres";

-- =============================================================================
-- Tables
-- =============================================================================

-- workspaces
CREATE TABLE IF NOT EXISTS "public"."workspaces" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "name" "text" NOT NULL,
    "owner_id" "uuid" NOT NULL,
    "is_default" boolean DEFAULT false,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "emoji" "text",
    "icon_r2_key" "text"
);


ALTER TABLE "public"."workspaces" OWNER TO "postgres";


COMMENT ON COLUMN "public"."workspaces"."icon_r2_key" IS 'R2 object key for workspace icon (proxy URL generated on fetch)';


-- workspace_members
CREATE TABLE IF NOT EXISTS "public"."workspace_members" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "workspace_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "role" "public"."workspace_role" DEFAULT 'member'::"public"."workspace_role" NOT NULL,
    "joined_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."workspace_members" OWNER TO "postgres";


-- workspace_apps
CREATE TABLE IF NOT EXISTS "public"."workspace_apps" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "workspace_id" "uuid" NOT NULL,
    "app_type" "public"."mini_app_type" NOT NULL,
    "is_public" boolean DEFAULT true,
    "position" integer DEFAULT 0,
    "config" "jsonb" DEFAULT '{}'::"jsonb",
    "created_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."workspace_apps" OWNER TO "postgres";


-- workspace_app_members
CREATE TABLE IF NOT EXISTS "public"."workspace_app_members" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "workspace_app_id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "added_by" "uuid",
    "added_at" timestamp with time zone DEFAULT "now"()
);


ALTER TABLE "public"."workspace_app_members" OWNER TO "postgres";

-- =============================================================================
-- Primary Keys and Unique Constraints
-- =============================================================================

ALTER TABLE ONLY "public"."workspaces"
    ADD CONSTRAINT "workspaces_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."workspace_members"
    ADD CONSTRAINT "workspace_members_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."workspace_members"
    ADD CONSTRAINT "workspace_members_workspace_id_user_id_key" UNIQUE ("workspace_id", "user_id");



ALTER TABLE ONLY "public"."workspace_apps"
    ADD CONSTRAINT "workspace_apps_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."workspace_apps"
    ADD CONSTRAINT "workspace_apps_workspace_id_app_type_key" UNIQUE ("workspace_id", "app_type");



ALTER TABLE ONLY "public"."workspace_app_members"
    ADD CONSTRAINT "workspace_app_members_pkey" PRIMARY KEY ("id");



ALTER TABLE ONLY "public"."workspace_app_members"
    ADD CONSTRAINT "workspace_app_members_workspace_app_id_user_id_key" UNIQUE ("workspace_app_id", "user_id");

-- =============================================================================
-- Foreign Keys
-- =============================================================================

-- workspaces.owner_id -> auth.users
ALTER TABLE ONLY "public"."workspaces"
    ADD CONSTRAINT "workspaces_owner_id_fkey" FOREIGN KEY ("owner_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

-- workspace_members.workspace_id -> workspaces
ALTER TABLE ONLY "public"."workspace_members"
    ADD CONSTRAINT "workspace_members_workspace_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE CASCADE;

-- workspace_members.user_id -> auth.users
ALTER TABLE ONLY "public"."workspace_members"
    ADD CONSTRAINT "workspace_members_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

-- workspace_apps.workspace_id -> workspaces
ALTER TABLE ONLY "public"."workspace_apps"
    ADD CONSTRAINT "workspace_apps_workspace_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE CASCADE;

-- workspace_app_members.workspace_app_id -> workspace_apps
ALTER TABLE ONLY "public"."workspace_app_members"
    ADD CONSTRAINT "workspace_app_members_workspace_app_id_fkey" FOREIGN KEY ("workspace_app_id") REFERENCES "public"."workspace_apps"("id") ON DELETE CASCADE;

-- workspace_app_members.user_id -> auth.users
ALTER TABLE ONLY "public"."workspace_app_members"
    ADD CONSTRAINT "workspace_app_members_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

-- workspace_app_members.added_by -> auth.users
ALTER TABLE ONLY "public"."workspace_app_members"
    ADD CONSTRAINT "workspace_app_members_added_by_fkey" FOREIGN KEY ("added_by") REFERENCES "auth"."users"("id") ON DELETE SET NULL;

-- =============================================================================
-- Indexes
-- =============================================================================

-- workspaces indexes
CREATE INDEX "idx_workspaces_owner_id" ON "public"."workspaces" USING "btree" ("owner_id");

CREATE UNIQUE INDEX "one_default_ws_per_owner" ON "public"."workspaces" USING "btree" ("owner_id") WHERE ("is_default" = true);

-- workspace_members indexes
CREATE INDEX "idx_workspace_members_user_id" ON "public"."workspace_members" USING "btree" ("user_id");

CREATE INDEX "idx_workspace_members_workspace_id" ON "public"."workspace_members" USING "btree" ("workspace_id");

-- workspace_apps indexes
CREATE INDEX "idx_workspace_apps_workspace_id" ON "public"."workspace_apps" USING "btree" ("workspace_id");

-- workspace_app_members indexes
CREATE INDEX "idx_workspace_app_members_app_id" ON "public"."workspace_app_members" USING "btree" ("workspace_app_id");

CREATE INDEX "idx_workspace_app_members_user_id" ON "public"."workspace_app_members" USING "btree" ("user_id");

-- =============================================================================
-- Enable Row Level Security
-- =============================================================================

ALTER TABLE "public"."workspaces" ENABLE ROW LEVEL SECURITY;

ALTER TABLE "public"."workspace_members" ENABLE ROW LEVEL SECURITY;

ALTER TABLE "public"."workspace_apps" ENABLE ROW LEVEL SECURITY;

ALTER TABLE "public"."workspace_app_members" ENABLE ROW LEVEL SECURITY;

-- =============================================================================
-- RLS Policies
-- =============================================================================

-- workspaces policies
-- NOTE: "Members and shared users can view workspace" policy moved to
-- 00014_sharing_permissions.sql because it references public.permissions
CREATE POLICY "Members can view workspace" ON "public"."workspaces" FOR SELECT USING ("public"."is_workspace_member"("id"));

CREATE POLICY "Users can create workspaces" ON "public"."workspaces" FOR INSERT WITH CHECK (("auth"."uid"() = "owner_id"));

CREATE POLICY "Admins can update workspace" ON "public"."workspaces" FOR UPDATE USING ("public"."is_workspace_admin"("id"));

CREATE POLICY "Owners can delete non-default workspace" ON "public"."workspaces" FOR DELETE USING (("public"."is_workspace_owner"("id") AND ("is_default" = false)));

-- workspace_members policies
CREATE POLICY "Members can view workspace members" ON "public"."workspace_members" FOR SELECT USING ("public"."is_workspace_member"("workspace_id"));

CREATE POLICY "Admins can add members" ON "public"."workspace_members" FOR INSERT WITH CHECK ("public"."is_workspace_admin"("workspace_id"));

CREATE POLICY "Admins can update member roles" ON "public"."workspace_members" FOR UPDATE USING (("public"."is_workspace_admin"("workspace_id") AND (("role" <> 'owner'::"public"."workspace_role") OR "public"."is_workspace_owner"("workspace_id"))));

CREATE POLICY "Admins can remove non-owner members" ON "public"."workspace_members" FOR DELETE USING (("public"."is_workspace_admin"("workspace_id") AND ("role" <> 'owner'::"public"."workspace_role")));

-- workspace_apps policies
CREATE POLICY "Members can view workspace apps" ON "public"."workspace_apps" FOR SELECT USING (("public"."is_workspace_member"("workspace_id") AND (("is_public" = true) OR "public"."can_access_workspace_app"("id"))));

CREATE POLICY "Admins can create apps" ON "public"."workspace_apps" FOR INSERT WITH CHECK ("public"."is_workspace_admin"("workspace_id"));

CREATE POLICY "Admins can update apps" ON "public"."workspace_apps" FOR UPDATE USING ("public"."is_workspace_admin"("workspace_id"));

CREATE POLICY "Admins can delete apps" ON "public"."workspace_apps" FOR DELETE USING ("public"."is_workspace_admin"("workspace_id"));

-- workspace_app_members policies
CREATE POLICY "Users can view app members" ON "public"."workspace_app_members" FOR SELECT USING ("public"."can_access_workspace_app"("workspace_app_id"));

CREATE POLICY "Admins can add app members" ON "public"."workspace_app_members" FOR INSERT WITH CHECK ((EXISTS ( SELECT 1
   FROM "public"."workspace_apps" "wa"
  WHERE (("wa"."id" = "workspace_app_members"."workspace_app_id") AND "public"."is_workspace_admin"("wa"."workspace_id")))));

CREATE POLICY "Admins can remove app members" ON "public"."workspace_app_members" FOR DELETE USING ((EXISTS ( SELECT 1
   FROM "public"."workspace_apps" "wa"
  WHERE (("wa"."id" = "workspace_app_members"."workspace_app_id") AND "public"."is_workspace_admin"("wa"."workspace_id")))));

-- =============================================================================
-- Triggers
-- =============================================================================

CREATE OR REPLACE TRIGGER "set_workspaces_updated_at" BEFORE UPDATE ON "public"."workspaces" FOR EACH ROW EXECUTE FUNCTION "public"."update_updated_at_column"();

-- =============================================================================
-- GRANTs
-- =============================================================================

-- Table grants
GRANT ALL ON TABLE "public"."workspaces" TO "anon";
GRANT ALL ON TABLE "public"."workspaces" TO "authenticated";
GRANT ALL ON TABLE "public"."workspaces" TO "service_role";

GRANT ALL ON TABLE "public"."workspace_members" TO "anon";
GRANT ALL ON TABLE "public"."workspace_members" TO "authenticated";
GRANT ALL ON TABLE "public"."workspace_members" TO "service_role";

GRANT ALL ON TABLE "public"."workspace_apps" TO "anon";
GRANT ALL ON TABLE "public"."workspace_apps" TO "authenticated";
GRANT ALL ON TABLE "public"."workspace_apps" TO "service_role";

GRANT ALL ON TABLE "public"."workspace_app_members" TO "anon";
GRANT ALL ON TABLE "public"."workspace_app_members" TO "authenticated";
GRANT ALL ON TABLE "public"."workspace_app_members" TO "service_role";

-- Function grants
GRANT ALL ON FUNCTION "public"."is_workspace_member"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."is_workspace_member"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_workspace_member"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."is_workspace_admin"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."is_workspace_admin"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_workspace_admin"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."is_workspace_owner"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."is_workspace_owner"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."is_workspace_owner"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."can_access_workspace_app"("p_workspace_app_id" "uuid", "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."can_access_workspace_app"("p_workspace_app_id" "uuid", "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."can_access_workspace_app"("p_workspace_app_id" "uuid", "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_workspace_apps"("p_workspace_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_workspace_apps"("p_workspace_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_workspace_apps"("p_workspace_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_workspace_role"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_workspace_role"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_workspace_role"("p_workspace_id" "uuid", "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_default_workspace"() TO "anon";
GRANT ALL ON FUNCTION "public"."get_default_workspace"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_default_workspace"() TO "service_role";

GRANT ALL ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."reorder_workspace_apps"("p_workspace_id" "uuid", "p_app_positions" "jsonb") TO "anon";
GRANT ALL ON FUNCTION "public"."reorder_workspace_apps"("p_workspace_id" "uuid", "p_app_positions" "jsonb") TO "authenticated";
GRANT ALL ON FUNCTION "public"."reorder_workspace_apps"("p_workspace_id" "uuid", "p_app_positions" "jsonb") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_workspace_files_app"("p_workspace_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_workspace_files_app"("p_workspace_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_workspace_files_app"("p_workspace_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_workspace_projects_app"("p_workspace_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_workspace_projects_app"("p_workspace_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_workspace_projects_app"("p_workspace_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."get_workspace_tasks_app"("p_workspace_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_workspace_tasks_app"("p_workspace_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_workspace_tasks_app"("p_workspace_id" "uuid") TO "service_role";


-- =============================================================================
-- Function: create_default_workspace_for_user (used by signup trigger in 00018)
-- =============================================================================

CREATE OR REPLACE FUNCTION "public"."create_default_workspace_for_user"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_workspace_id UUID;
BEGIN
    -- Create the public.users row from auth metadata
    INSERT INTO public.users (id, email, name, avatar_url)
    VALUES (
        NEW.id,
        NEW.email,
        COALESCE(NEW.raw_user_meta_data->>'full_name', NEW.raw_user_meta_data->>'name'),
        NEW.raw_user_meta_data->>'avatar_url'
    )
    ON CONFLICT (id) DO NOTHING;

    -- Create the default workspace
    INSERT INTO public.workspaces (name, owner_id, is_default)
    VALUES ('Personal', NEW.id, TRUE)
    RETURNING id INTO v_workspace_id;

    -- Add user as owner
    INSERT INTO public.workspace_members (workspace_id, user_id, role)
    VALUES (v_workspace_id, NEW.id, 'owner');

    -- Create default apps
    INSERT INTO public.workspace_apps (workspace_id, app_type, is_public, position)
    VALUES
        (v_workspace_id, 'chat', TRUE, 0),
        (v_workspace_id, 'messages', TRUE, 1),
        (v_workspace_id, 'projects', TRUE, 2),
        (v_workspace_id, 'files', TRUE, 3),
        (v_workspace_id, 'email', TRUE, 4),
        (v_workspace_id, 'calendar', TRUE, 5);

    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."create_default_workspace_for_user"() OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."create_default_workspace_for_user"() TO "anon";
GRANT ALL ON FUNCTION "public"."create_default_workspace_for_user"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."create_default_workspace_for_user"() TO "service_role";
