-- Migration: Extensions and Types
-- Enables required extensions, creates custom enum types, and defines
-- the generic update_updated_at_column() trigger function used by many tables.

-- =============================================================================
-- Extensions
-- =============================================================================

-- pgvector: prod has this in the public schema.
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;

-- =============================================================================
-- Custom Enum Types
-- =============================================================================

CREATE TYPE "public"."mini_app_type" AS ENUM (
    'files',
    'messages',
    'dashboard',
    'projects',
    'chat',
    'email',
    'calendar',
    'agents'
);


ALTER TYPE "public"."mini_app_type" OWNER TO "postgres";


CREATE TYPE "public"."workspace_invitation_status" AS ENUM (
    'pending',
    'accepted',
    'declined',
    'revoked',
    'expired'
);


ALTER TYPE "public"."workspace_invitation_status" OWNER TO "postgres";


CREATE TYPE "public"."workspace_role" AS ENUM (
    'owner',
    'admin',
    'member'
);


ALTER TYPE "public"."workspace_role" OWNER TO "postgres";

-- =============================================================================
-- Generic Trigger Function
-- =============================================================================

CREATE OR REPLACE FUNCTION "public"."update_updated_at_column"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;


ALTER FUNCTION "public"."update_updated_at_column"() OWNER TO "postgres";

-- =============================================================================
-- GRANTs
-- =============================================================================

GRANT ALL ON FUNCTION "public"."update_updated_at_column"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_updated_at_column"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_updated_at_column"() TO "service_role";
