-- Sync control-plane table for lease-based, single-flight provider syncs.
-- One row per (connection_id, sync_kind).

-- =============================================================================
-- Helper functions
-- =============================================================================

CREATE OR REPLACE FUNCTION "public"."merge_connection_sync_cursor"(
    "p_existing" "text",
    "p_incoming" "text"
) RETURNS "text"
    LANGUAGE "sql"
    IMMUTABLE
    AS $$
    SELECT CASE
        WHEN p_incoming IS NULL OR btrim(p_incoming) = '' THEN p_existing
        WHEN p_existing IS NULL OR btrim(p_existing) = '' THEN p_incoming
        WHEN p_existing ~ '^[0-9]+$' AND p_incoming ~ '^[0-9]+$' THEN
            CASE
                WHEN p_existing::numeric <= p_incoming::numeric THEN p_incoming
                ELSE p_existing
            END
        ELSE p_incoming
    END
    $$;


ALTER FUNCTION "public"."merge_connection_sync_cursor"("p_existing" "text", "p_incoming" "text") OWNER TO "postgres";


-- =============================================================================
-- Tables
-- =============================================================================

CREATE TABLE IF NOT EXISTS "public"."connection_sync_state" (
    "connection_id" "uuid" NOT NULL,
    "provider" "text" NOT NULL,
    "sync_kind" "text" NOT NULL,
    "dirty" boolean DEFAULT false NOT NULL,
    "dirty_since" timestamp with time zone,
    "dirty_generation" bigint DEFAULT 0 NOT NULL,
    "lease_generation" bigint,
    "latest_seen_cursor" "text",
    "last_synced_cursor" "text",
    "lease_owner" "text",
    "lease_expires_at" timestamp with time zone,
    "last_provider_event_at" timestamp with time zone,
    "last_sync_started_at" timestamp with time zone,
    "last_sync_finished_at" timestamp with time zone,
    "last_heartbeat_at" timestamp with time zone,
    "last_error_at" timestamp with time zone,
    "last_error" "text",
    "retry_count" integer DEFAULT 0 NOT NULL,
    "next_retry_at" timestamp with time zone,
    "priority" integer DEFAULT 0 NOT NULL,
    "metadata" "jsonb" DEFAULT '{}'::"jsonb" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"() NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"() NOT NULL
);


ALTER TABLE "public"."connection_sync_state" OWNER TO "postgres";


COMMENT ON TABLE "public"."connection_sync_state" IS 'Control-plane state for background sync execution. One row per ext_connection and sync stream (email/calendar).';
COMMENT ON COLUMN "public"."connection_sync_state"."sync_kind" IS 'Logical sync stream for the connection: email or calendar.';
COMMENT ON COLUMN "public"."connection_sync_state"."dirty_generation" IS 'Monotonic invalidation counter. If it increases during a lease, the worker must keep the row dirty on completion.';
COMMENT ON COLUMN "public"."connection_sync_state"."lease_generation" IS 'dirty_generation observed when the current lease was claimed.';
COMMENT ON COLUMN "public"."connection_sync_state"."metadata" IS 'Pending trigger context for the next sync attempt. Cleared once the stream is clean.';


-- =============================================================================
-- Constraints
-- =============================================================================

ALTER TABLE ONLY "public"."connection_sync_state"
    ADD CONSTRAINT "connection_sync_state_pkey" PRIMARY KEY ("connection_id", "sync_kind");

ALTER TABLE ONLY "public"."connection_sync_state"
    ADD CONSTRAINT "connection_sync_state_connection_id_fkey" FOREIGN KEY ("connection_id") REFERENCES "public"."ext_connections"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."connection_sync_state"
    ADD CONSTRAINT "connection_sync_state_provider_check" CHECK (provider = ANY (ARRAY['google'::text, 'microsoft'::text]));

ALTER TABLE ONLY "public"."connection_sync_state"
    ADD CONSTRAINT "connection_sync_state_sync_kind_check" CHECK (sync_kind = ANY (ARRAY['email'::text, 'calendar'::text]));

ALTER TABLE ONLY "public"."connection_sync_state"
    ADD CONSTRAINT "connection_sync_state_priority_check" CHECK (priority >= 0);


-- =============================================================================
-- Indexes
-- =============================================================================

CREATE INDEX "idx_connection_sync_state_claimable"
ON "public"."connection_sync_state" USING "btree" ("provider", "sync_kind", "priority" DESC, "dirty_since", "next_retry_at", "lease_expires_at")
WHERE "dirty" = true;

CREATE INDEX "idx_connection_sync_state_lease_expires_at"
ON "public"."connection_sync_state" USING "btree" ("lease_expires_at");

CREATE INDEX "idx_connection_sync_state_next_retry_at"
ON "public"."connection_sync_state" USING "btree" ("next_retry_at");


-- =============================================================================
-- Security / triggers
-- =============================================================================

ALTER TABLE "public"."connection_sync_state" ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE TRIGGER "update_connection_sync_state_updated_at"
BEFORE UPDATE ON "public"."connection_sync_state"
FOR EACH ROW EXECUTE FUNCTION "public"."update_updated_at_column"();

GRANT ALL ON TABLE "public"."connection_sync_state" TO "service_role";
GRANT ALL ON FUNCTION "public"."merge_connection_sync_cursor"("p_existing" "text", "p_incoming" "text") TO "service_role";


-- =============================================================================
-- RPCs
-- =============================================================================

CREATE OR REPLACE FUNCTION "public"."mark_connection_sync_dirty"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_latest_seen_cursor" "text" DEFAULT NULL,
    "p_priority" integer DEFAULT 0,
    "p_provider_event_at" timestamp with time zone DEFAULT "now"(),
    "p_metadata" "jsonb" DEFAULT '{}'::"jsonb"
) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_provider text;
    v_now timestamp with time zone := now();
BEGIN
    IF p_sync_kind NOT IN ('email', 'calendar') THEN
        RAISE EXCEPTION 'invalid sync_kind: %', p_sync_kind;
    END IF;

    SELECT provider
    INTO v_provider
    FROM public.ext_connections
    WHERE id = p_connection_id
      AND is_active = true
      AND provider IN ('google', 'microsoft')
    LIMIT 1;

    IF v_provider IS NULL THEN
        RETURN false;
    END IF;

    INSERT INTO public.connection_sync_state (
        connection_id,
        provider,
        sync_kind,
        dirty,
        dirty_since,
        dirty_generation,
        latest_seen_cursor,
        last_provider_event_at,
        priority,
        metadata,
        created_at,
        updated_at
    )
    VALUES (
        p_connection_id,
        v_provider,
        p_sync_kind,
        true,
        v_now,
        1,
        public.merge_connection_sync_cursor(NULL, p_latest_seen_cursor),
        COALESCE(p_provider_event_at, v_now),
        GREATEST(p_priority, 0),
        COALESCE(p_metadata, '{}'::jsonb),
        v_now,
        v_now
    )
    ON CONFLICT (connection_id, sync_kind) DO UPDATE
    SET
        provider = EXCLUDED.provider,
        dirty = true,
        dirty_since = COALESCE(public.connection_sync_state.dirty_since, v_now),
        dirty_generation = public.connection_sync_state.dirty_generation + 1,
        latest_seen_cursor = public.merge_connection_sync_cursor(
            public.connection_sync_state.latest_seen_cursor,
            p_latest_seen_cursor
        ),
        last_provider_event_at = COALESCE(p_provider_event_at, v_now),
        priority = GREATEST(public.connection_sync_state.priority, GREATEST(p_priority, 0)),
        next_retry_at = NULL,
        metadata = CASE
            WHEN p_metadata IS NULL OR p_metadata = '{}'::jsonb THEN public.connection_sync_state.metadata
            ELSE p_metadata
        END,
        updated_at = v_now;

    RETURN true;
END;
$$;


ALTER FUNCTION "public"."mark_connection_sync_dirty"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_latest_seen_cursor" "text",
    "p_priority" integer,
    "p_provider_event_at" timestamp with time zone,
    "p_metadata" "jsonb"
) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."claim_connection_sync_lease"(
    "p_worker_id" "text",
    "p_lease_seconds" integer DEFAULT 120,
    "p_provider" "text" DEFAULT NULL,
    "p_sync_kind" "text" DEFAULT NULL,
    "p_connection_id" "uuid" DEFAULT NULL
) RETURNS TABLE(
    "connection_id" "uuid",
    "provider" "text",
    "sync_kind" "text",
    "dirty_generation" bigint,
    "lease_generation" bigint,
    "latest_seen_cursor" "text",
    "last_synced_cursor" "text",
    "priority" integer,
    "lease_expires_at" timestamp with time zone
)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF p_worker_id IS NULL OR btrim(p_worker_id) = '' THEN
        RAISE EXCEPTION 'worker_id is required';
    END IF;

    RETURN QUERY
    WITH candidate AS (
        SELECT css.connection_id, css.sync_kind
        FROM public.connection_sync_state css
        JOIN public.ext_connections ec
          ON ec.id = css.connection_id
        WHERE css.dirty = true
          AND ec.is_active = true
          AND (css.next_retry_at IS NULL OR css.next_retry_at <= now())
          AND (css.lease_expires_at IS NULL OR css.lease_expires_at <= now())
          AND (p_provider IS NULL OR css.provider = p_provider)
          AND (p_sync_kind IS NULL OR css.sync_kind = p_sync_kind)
          AND (p_connection_id IS NULL OR css.connection_id = p_connection_id)
        ORDER BY css.priority DESC, COALESCE(css.dirty_since, css.updated_at) ASC, css.updated_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    ),
    claimed AS (
        UPDATE public.connection_sync_state css
        SET
            lease_owner = p_worker_id,
            lease_expires_at = now() + make_interval(secs => GREATEST(p_lease_seconds, 15)),
            last_sync_started_at = now(),
            last_heartbeat_at = now(),
            lease_generation = css.dirty_generation,
            updated_at = now()
        FROM candidate
        WHERE css.connection_id = candidate.connection_id
          AND css.sync_kind = candidate.sync_kind
        RETURNING
            css.connection_id,
            css.provider,
            css.sync_kind,
            css.dirty_generation,
            css.lease_generation,
            css.latest_seen_cursor,
            css.last_synced_cursor,
            css.priority,
            css.lease_expires_at
    )
    SELECT
        claimed.connection_id,
        claimed.provider,
        claimed.sync_kind,
        claimed.dirty_generation,
        claimed.lease_generation,
        claimed.latest_seen_cursor,
        claimed.last_synced_cursor,
        claimed.priority,
        claimed.lease_expires_at
    FROM claimed;
END;
$$;


ALTER FUNCTION "public"."claim_connection_sync_lease"(
    "p_worker_id" "text",
    "p_lease_seconds" integer,
    "p_provider" "text",
    "p_sync_kind" "text",
    "p_connection_id" "uuid"
) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."heartbeat_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_lease_seconds" integer DEFAULT 120
) RETURNS boolean
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_updated integer;
BEGIN
    UPDATE public.connection_sync_state
    SET
        lease_expires_at = now() + make_interval(secs => GREATEST(p_lease_seconds, 15)),
        last_heartbeat_at = now(),
        updated_at = now()
    WHERE connection_id = p_connection_id
      AND sync_kind = p_sync_kind
      AND lease_owner = p_worker_id;

    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated > 0;
END;
$$;


ALTER FUNCTION "public"."heartbeat_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_lease_seconds" integer
) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."complete_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_last_synced_cursor" "text" DEFAULT NULL,
    "p_latest_seen_cursor" "text" DEFAULT NULL,
    "p_keep_dirty" boolean DEFAULT false
) RETURNS TABLE(
    "dirty" boolean,
    "dirty_generation" bigint,
    "latest_seen_cursor" "text",
    "last_synced_cursor" "text"
)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN QUERY
    UPDATE public.connection_sync_state css
    SET
        latest_seen_cursor = public.merge_connection_sync_cursor(
            css.latest_seen_cursor,
            COALESCE(p_latest_seen_cursor, p_last_synced_cursor)
        ),
        last_synced_cursor = public.merge_connection_sync_cursor(
            css.last_synced_cursor,
            p_last_synced_cursor
        ),
        dirty = CASE
            WHEN p_keep_dirty THEN true
            WHEN css.dirty_generation > css.lease_generation THEN true
            ELSE false
        END,
        dirty_since = CASE
            WHEN p_keep_dirty THEN css.dirty_since
            WHEN css.dirty_generation > css.lease_generation THEN css.dirty_since
            ELSE NULL
        END,
        lease_owner = NULL,
        lease_expires_at = NULL,
        lease_generation = NULL,
        last_sync_finished_at = now(),
        last_heartbeat_at = now(),
        retry_count = CASE
            WHEN p_keep_dirty THEN css.retry_count
            WHEN css.dirty_generation > css.lease_generation THEN css.retry_count
            ELSE 0
        END,
        priority = CASE
            WHEN p_keep_dirty THEN css.priority
            WHEN css.dirty_generation > css.lease_generation THEN css.priority
            ELSE 0
        END,
        next_retry_at = NULL,
        last_error = NULL,
        last_error_at = NULL,
        metadata = CASE
            WHEN p_keep_dirty THEN css.metadata
            WHEN css.dirty_generation > css.lease_generation THEN css.metadata
            ELSE '{}'::jsonb
        END,
        updated_at = now()
    WHERE css.connection_id = p_connection_id
      AND css.sync_kind = p_sync_kind
      AND css.lease_owner = p_worker_id
    RETURNING
        css.dirty,
        css.dirty_generation,
        css.latest_seen_cursor,
        css.last_synced_cursor;
END;
$$;


ALTER FUNCTION "public"."complete_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_last_synced_cursor" "text",
    "p_latest_seen_cursor" "text",
    "p_keep_dirty" boolean
) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."fail_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_error" "text",
    "p_retry_seconds" integer DEFAULT 60
) RETURNS TABLE(
    "retry_count" integer,
    "next_retry_at" timestamp with time zone
)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN QUERY
    UPDATE public.connection_sync_state css
    SET
        dirty = true,
        lease_owner = NULL,
        lease_expires_at = NULL,
        lease_generation = NULL,
        last_sync_finished_at = now(),
        last_error = LEFT(COALESCE(p_error, 'sync failed'), 4000),
        last_error_at = now(),
        retry_count = css.retry_count + 1,
        next_retry_at = now() + make_interval(secs => GREATEST(p_retry_seconds, 0)),
        updated_at = now()
    WHERE css.connection_id = p_connection_id
      AND css.sync_kind = p_sync_kind
      AND css.lease_owner = p_worker_id
    RETURNING
        css.retry_count,
        css.next_retry_at;
END;
$$;


ALTER FUNCTION "public"."fail_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_error" "text",
    "p_retry_seconds" integer
) OWNER TO "postgres";

GRANT ALL ON FUNCTION "public"."mark_connection_sync_dirty"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_latest_seen_cursor" "text",
    "p_priority" integer,
    "p_provider_event_at" timestamp with time zone,
    "p_metadata" "jsonb"
) TO "service_role";

GRANT ALL ON FUNCTION "public"."claim_connection_sync_lease"(
    "p_worker_id" "text",
    "p_lease_seconds" integer,
    "p_provider" "text",
    "p_sync_kind" "text",
    "p_connection_id" "uuid"
) TO "service_role";

GRANT ALL ON FUNCTION "public"."heartbeat_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_lease_seconds" integer
) TO "service_role";

GRANT ALL ON FUNCTION "public"."complete_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_last_synced_cursor" "text",
    "p_latest_seen_cursor" "text",
    "p_keep_dirty" boolean
) TO "service_role";

GRANT ALL ON FUNCTION "public"."fail_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_error" "text",
    "p_retry_seconds" integer
) TO "service_role";


-- =============================================================================
-- Backfill rows for existing active connections
-- =============================================================================

INSERT INTO public.connection_sync_state (
    connection_id,
    provider,
    sync_kind,
    dirty,
    created_at,
    updated_at
)
SELECT
    ec.id,
    ec.provider,
    sync_kinds.sync_kind,
    false,
    now(),
    now()
FROM public.ext_connections ec
CROSS JOIN (
    VALUES ('email'::text), ('calendar'::text)
) AS sync_kinds(sync_kind)
WHERE ec.is_active = true
  AND ec.provider IN ('google', 'microsoft')
ON CONFLICT (connection_id, sync_kind) DO NOTHING;
