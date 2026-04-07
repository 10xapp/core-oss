-- Replace broad sweep reconciliation with per-stream scheduling.
-- Clean streams get a next_reconcile_at deadline, and workers can claim
-- them directly once they age past that deadline.

ALTER TABLE "public"."connection_sync_state"
ADD COLUMN IF NOT EXISTS "next_reconcile_at" timestamp with time zone;


COMMENT ON COLUMN "public"."connection_sync_state"."next_reconcile_at"
IS 'Next scheduled safety-net reconcile for a clean stream. NULL while dirty or actively retrying.';


CREATE INDEX IF NOT EXISTS "idx_connection_sync_state_next_reconcile_at"
ON "public"."connection_sync_state" USING "btree" ("provider", "sync_kind", "next_reconcile_at")
WHERE "dirty" = false AND "next_reconcile_at" IS NOT NULL;


-- Spread existing clean rows across the default reconcile window so the new
-- claim path can pick them up without a thundering herd when the sweep cron is
-- disabled. 21600 seconds = 6 hours.
UPDATE "public"."connection_sync_state"
SET "next_reconcile_at" = now() + make_interval(secs => floor(random() * 21600)::integer)
WHERE "dirty" = false
  AND "next_reconcile_at" IS NULL;


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
    v_existing record;
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

    -- Short-circuit duplicate cursor-based invalidations for already-dirty
    -- streams. This keeps Gmail notification floods from rewriting the same
    -- row over and over while preserving the "accepted" success contract.
    IF p_latest_seen_cursor IS NOT NULL THEN
        SELECT dirty, latest_seen_cursor
        INTO v_existing
        FROM public.connection_sync_state
        WHERE connection_id = p_connection_id
          AND sync_kind = p_sync_kind;

        IF v_existing IS NOT NULL
           AND v_existing.dirty = true
           AND v_existing.latest_seen_cursor IS NOT NULL
           AND public.merge_connection_sync_cursor(
                v_existing.latest_seen_cursor,
                p_latest_seen_cursor
           ) = v_existing.latest_seen_cursor
        THEN
            RETURN true;
        END IF;
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
        next_reconcile_at,
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
        NULL,
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
        next_reconcile_at = NULL,
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
        WHERE (
                css.dirty = true
                OR (
                    css.dirty = false
                    AND css.next_reconcile_at IS NOT NULL
                    AND css.next_reconcile_at <= now()
                )
              )
          AND ec.is_active = true
          AND (css.next_retry_at IS NULL OR css.next_retry_at <= now())
          AND (css.lease_expires_at IS NULL OR css.lease_expires_at <= now())
          AND (p_provider IS NULL OR css.provider = p_provider)
          AND (p_sync_kind IS NULL OR css.sync_kind = p_sync_kind)
          AND (p_connection_id IS NULL OR css.connection_id = p_connection_id)
        ORDER BY
            CASE WHEN css.dirty THEN 0 ELSE 1 END,
            css.priority DESC,
            COALESCE(css.dirty_since, css.next_reconcile_at, css.updated_at) ASC,
            css.updated_at ASC
        LIMIT 1
        FOR UPDATE SKIP LOCKED
    ),
    claimed AS (
        UPDATE public.connection_sync_state css
        SET
            dirty = true,
            dirty_since = CASE
                WHEN css.dirty THEN css.dirty_since
                ELSE now()
            END,
            dirty_generation = CASE
                WHEN css.dirty THEN css.dirty_generation
                ELSE css.dirty_generation + 1
            END,
            lease_owner = p_worker_id,
            lease_expires_at = now() + make_interval(secs => GREATEST(p_lease_seconds, 15)),
            last_sync_started_at = now(),
            last_heartbeat_at = now(),
            lease_generation = CASE
                WHEN css.dirty THEN css.dirty_generation
                ELSE css.dirty_generation + 1
            END,
            priority = CASE
                WHEN css.dirty THEN css.priority
                ELSE 0
            END,
            metadata = CASE
                WHEN css.dirty THEN css.metadata
                ELSE '{"source": "reconciliation"}'::jsonb
            END,
            next_reconcile_at = NULL,
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


DROP FUNCTION IF EXISTS "public"."complete_connection_sync_lease"(
    uuid,
    text,
    text,
    text,
    text,
    boolean
);


CREATE OR REPLACE FUNCTION "public"."complete_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_last_synced_cursor" "text" DEFAULT NULL,
    "p_latest_seen_cursor" "text" DEFAULT NULL,
    "p_keep_dirty" boolean DEFAULT false,
    "p_reconcile_interval_seconds" integer DEFAULT 21600
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
        next_reconcile_at = CASE
            WHEN p_keep_dirty THEN NULL
            WHEN css.dirty_generation > css.lease_generation THEN NULL
            ELSE now() + make_interval(
                secs => GREATEST(COALESCE(p_reconcile_interval_seconds, 21600), 300)
            )
        END,
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
    "p_keep_dirty" boolean,
    "p_reconcile_interval_seconds" integer
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


GRANT ALL ON FUNCTION "public"."complete_connection_sync_lease"(
    "p_connection_id" "uuid",
    "p_sync_kind" "text",
    "p_worker_id" "text",
    "p_last_synced_cursor" "text",
    "p_latest_seen_cursor" "text",
    "p_keep_dirty" boolean,
    "p_reconcile_interval_seconds" integer
) TO "service_role";
