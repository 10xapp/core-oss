-- Split worker claim modes and add jitter to reconcile rescheduling.

DROP FUNCTION IF EXISTS "public"."claim_connection_sync_lease"(
    text,
    integer,
    text,
    text,
    uuid
);


CREATE OR REPLACE FUNCTION "public"."claim_connection_sync_lease"(
    "p_worker_id" "text",
    "p_lease_seconds" integer DEFAULT 120,
    "p_provider" "text" DEFAULT NULL,
    "p_sync_kind" "text" DEFAULT NULL,
    "p_connection_id" "uuid" DEFAULT NULL,
    "p_claim_mode" "text" DEFAULT 'any'
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

    IF p_claim_mode NOT IN ('any', 'dirty_only', 'reconcile_only') THEN
        RAISE EXCEPTION 'invalid claim_mode: %', p_claim_mode;
    END IF;

    RETURN QUERY
    WITH candidate AS (
        SELECT css.connection_id, css.sync_kind
        FROM public.connection_sync_state css
        JOIN public.ext_connections ec
          ON ec.id = css.connection_id
        WHERE (
                (
                    p_claim_mode IN ('any', 'dirty_only')
                    AND css.dirty = true
                )
                OR (
                    p_claim_mode IN ('any', 'reconcile_only')
                    AND css.dirty = false
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
    "p_connection_id" "uuid",
    "p_claim_mode" "text"
) OWNER TO "postgres";


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
                    + floor(
                        random() * GREATEST(COALESCE(p_reconcile_interval_seconds, 21600), 300) * 0.2
                    )::integer
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


GRANT ALL ON FUNCTION "public"."claim_connection_sync_lease"(
    "p_worker_id" "text",
    "p_lease_seconds" integer,
    "p_provider" "text",
    "p_sync_kind" "text",
    "p_connection_id" "uuid",
    "p_claim_mode" "text"
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
