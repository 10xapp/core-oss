-- Skip redundant control-plane writes when a stream is already dirty and
-- the incoming cursor does not advance the newest seen checkpoint.
--
-- This primarily reduces Gmail webhook churn, where Pub/Sub can deliver
-- multiple notifications for the same mailbox carrying the same or an older
-- historyId while the stream is already dirty and waiting to be processed.

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
