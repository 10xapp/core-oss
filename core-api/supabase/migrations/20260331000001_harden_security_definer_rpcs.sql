-- Harden exposed SECURITY DEFINER RPCs without breaking existing authenticated
-- API callers. User-scoped RPCs now fail closed unless the caller's JWT user
-- matches p_user_id or the request is made with the service_role JWT.

CREATE OR REPLACE FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean DEFAULT false, "p_create_default_apps" boolean DEFAULT true) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_workspace_id UUID;
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

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


CREATE OR REPLACE FUNCTION "public"."get_email_counts_by_account"("p_user_id" "uuid", "p_account_ids" "uuid"[] DEFAULT NULL::"uuid"[]) RETURNS TABLE("account_id" "uuid", "provider_email" "text", "provider" "text", "inbox_unread_count" bigint, "drafts_count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    WITH email_counts AS (
        SELECT
            e.ext_connection_id AS account_id,
            COUNT(DISTINCT CASE
                WHEN 'inbox' = ANY(e.normalized_labels)
                 AND e.is_read = false
                THEN e.composite_thread_id
            END)::bigint AS inbox_unread_count,
            COUNT(*) FILTER (
                WHERE 'draft' = ANY(e.normalized_labels)
            )::bigint AS drafts_count
        FROM emails e
        WHERE e.user_id = p_user_id
          AND e.is_trashed = false
          AND (p_account_ids IS NULL OR e.ext_connection_id = ANY(p_account_ids))
        GROUP BY e.ext_connection_id
    )
    SELECT
        ec.id AS account_id,
        ec.provider_email,
        ec.provider,
        COALESCE(c.inbox_unread_count, 0)::bigint AS inbox_unread_count,
        COALESCE(c.drafts_count, 0)::bigint AS drafts_count
    FROM ext_connections ec
    LEFT JOIN email_counts c ON c.account_id = ec.id
    WHERE ec.user_id = p_user_id
      AND ec.is_active = true
      AND (p_account_ids IS NULL OR ec.id = ANY(p_account_ids))
    ORDER BY ec.account_order;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."get_email_threads"("p_user_id" "uuid", "p_max_results" integer DEFAULT 50, "p_label_filter" "text" DEFAULT NULL::"text", "p_offset" integer DEFAULT 0, "p_ext_connection_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("thread_id" "text", "latest_external_id" "text", "subject" "text", "sender" "text", "snippet" "text", "labels" "text"[], "is_unread" boolean, "is_starred" boolean, "received_at" timestamp with time zone, "has_attachments" boolean, "message_count" bigint, "participant_count" bigint, "ai_summary" "text", "ai_important" boolean, "ai_analyzed" boolean, "ext_connection_id" "uuid")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    WITH normalized_emails AS (
        SELECT
            e.*,
            normalize_subject(COALESCE(e.subject, '')) as norm_subject,
            e.thread_id || '|||' || normalize_subject(COALESCE(e.subject, '(No Subject)')) as composite_thread_id
        FROM emails e
        WHERE e.user_id = p_user_id
          AND e.is_trashed = false
          AND (p_label_filter IS NULL OR p_label_filter = ANY(e.labels))
          AND (p_ext_connection_id IS NULL OR e.ext_connection_id = p_ext_connection_id)
    ),
    all_thread_emails AS (
        SELECT DISTINCT
            ne.composite_thread_id,
            'SENT' = ANY(ae.labels) as is_sent_email
        FROM normalized_emails ne
        JOIN emails ae ON ae.thread_id = ne.thread_id
                       AND ae.user_id = p_user_id
                       AND ae.is_trashed = false
    ),
    full_thread_counts AS (
        SELECT
            ne.composite_thread_id,
            COUNT(DISTINCT ae.id) as total_msg_count
        FROM (SELECT DISTINCT n.composite_thread_id, n.thread_id FROM normalized_emails n) ne
        JOIN emails ae ON ae.thread_id = ne.thread_id
                       AND ae.user_id = p_user_id
                       AND ae.is_trashed = false
        GROUP BY ne.composite_thread_id
    ),
    thread_engagement AS (
        SELECT
            composite_thread_id,
            BOOL_OR(is_sent_email) as user_has_engaged
        FROM all_thread_emails
        GROUP BY composite_thread_id
    ),
    thread_aggregates AS (
        SELECT
            e.composite_thread_id,
            e.thread_id as original_thread_id,
            COUNT(DISTINCT e.id) as msg_count,
            MAX(e.received_at) as latest_date,
            BOOL_OR(NOT e.is_read) as has_unread,
            BOOL_OR(e.is_starred) as has_starred,
            BOOL_OR(e.has_attachments) as has_attach,
            COUNT(DISTINCT e.from) as unique_senders,
            ARRAY_AGG(DISTINCT label ORDER BY label) FILTER (WHERE label IS NOT NULL) as all_labels,
            (array_agg(e.ext_connection_id))[1] as thread_ext_connection_id
        FROM normalized_emails e
        LEFT JOIN LATERAL unnest(e.labels) as label ON true
        GROUP BY e.composite_thread_id, e.thread_id
    ),
    latest_in_thread AS (
        SELECT DISTINCT ON (e.composite_thread_id)
            e.composite_thread_id,
            e.thread_id,
            e.external_id,
            e.subject,
            e.from as sender,
            e.snippet,
            e.received_at,
            e.ai_summary,
            e.ai_important,
            e.ai_analyzed
        FROM normalized_emails e
        ORDER BY e.composite_thread_id, e.received_at DESC
    )
    SELECT
        t.original_thread_id as thread_id,
        l.external_id as latest_external_id,
        l.subject,
        l.sender,
        l.snippet,
        t.all_labels as labels,
        t.has_unread as is_unread,
        t.has_starred as is_starred,
        l.received_at,
        t.has_attach as has_attachments,
        COALESCE(ftc.total_msg_count, t.msg_count) as message_count,
        t.unique_senders as participant_count,
        l.ai_summary,
        CASE
            WHEN COALESCE(te.user_has_engaged, false) THEN true
            ELSE COALESCE(l.ai_important, false)
        END as ai_important,
        l.ai_analyzed,
        t.thread_ext_connection_id as ext_connection_id
    FROM thread_aggregates t
    JOIN latest_in_thread l ON l.composite_thread_id = t.composite_thread_id
    LEFT JOIN thread_engagement te ON te.composite_thread_id = t.composite_thread_id
    LEFT JOIN full_thread_counts ftc ON ftc.composite_thread_id = t.composite_thread_id
    ORDER BY l.received_at DESC
    LIMIT p_max_results
    OFFSET p_offset;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."get_email_threads_unified"("p_user_id" "uuid", "p_max_results" integer DEFAULT 50, "p_label_filter" "text" DEFAULT NULL::"text", "p_offset" integer DEFAULT 0, "p_ext_connection_ids" "uuid"[] DEFAULT NULL::"uuid"[]) RETURNS TABLE("thread_id" "text", "latest_external_id" "text", "subject" "text", "sender" "text", "snippet" "text", "labels" "text"[], "normalized_labels" "text"[], "is_unread" boolean, "is_starred" boolean, "received_at" timestamp with time zone, "has_attachments" boolean, "message_count" bigint, "participant_count" bigint, "ai_summary" "text", "ai_important" boolean, "ai_analyzed" boolean, "ext_connection_id" "uuid", "account_email" "text", "account_provider" "text", "account_avatar" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    WITH filtered_emails AS (
        SELECT
            e.id, e.composite_thread_id, e.thread_id, e.external_id,
            e.subject, e.from, e.snippet, e.received_at,
            e.is_read, e.is_starred, e.has_attachments,
            e.labels, e.normalized_labels, e.ext_connection_id,
            e.ai_summary, e.ai_important, e.ai_analyzed
        FROM emails e
        WHERE e.user_id = p_user_id
          AND (
              (p_label_filter = 'trash' AND e.is_trashed = true)
              OR (p_label_filter IS DISTINCT FROM 'trash' AND e.is_trashed = false)
          )
          AND (p_label_filter IS NULL OR p_label_filter = ANY(e.normalized_labels))
          AND (p_ext_connection_ids IS NULL OR e.ext_connection_id = ANY(p_ext_connection_ids))
    ),
    thread_engagement AS (
        SELECT
            fe.composite_thread_id,
            BOOL_OR('sent' = ANY(COALESCE(ae.normalized_labels, '{}'))) AS user_has_engaged
        FROM (SELECT DISTINCT f.composite_thread_id, f.thread_id FROM filtered_emails f) fe
        JOIN emails ae ON ae.thread_id = fe.thread_id
                       AND ae.user_id = p_user_id
                       AND (
                           (p_label_filter = 'trash' AND ae.is_trashed = true)
                           OR (p_label_filter IS DISTINCT FROM 'trash' AND ae.is_trashed = false)
                       )
        GROUP BY fe.composite_thread_id
    ),
    full_thread_counts AS (
        SELECT
            fe.composite_thread_id,
            COUNT(DISTINCT ae.id)::bigint AS total_msg_count
        FROM (SELECT DISTINCT f.composite_thread_id, f.thread_id FROM filtered_emails f) fe
        JOIN emails ae ON ae.thread_id = fe.thread_id
                       AND ae.user_id = p_user_id
                       AND ae.is_trashed = false
        GROUP BY fe.composite_thread_id
    ),
    thread_aggregates AS (
        SELECT
            e.composite_thread_id,
            e.thread_id AS original_thread_id,
            COUNT(*)::bigint AS msg_count,
            MAX(e.received_at) AS latest_date,
            BOOL_OR(NOT e.is_read) AS has_unread,
            BOOL_OR(e.is_starred) AS has_starred,
            BOOL_OR(e.has_attachments) AS has_attach,
            COUNT(DISTINCT e.from)::bigint AS unique_senders,
            (array_agg(e.ext_connection_id ORDER BY e.received_at DESC NULLS LAST))[1] AS thread_ext_connection_id
        FROM filtered_emails e
        GROUP BY e.composite_thread_id, e.thread_id
    ),
    thread_labels AS (
        SELECT
            e.composite_thread_id,
            e.thread_id AS original_thread_id,
            ARRAY_AGG(DISTINCT label ORDER BY label) FILTER (WHERE label IS NOT NULL) AS all_labels
        FROM filtered_emails e
        LEFT JOIN LATERAL unnest(e.labels) AS label ON true
        GROUP BY e.composite_thread_id, e.thread_id
    ),
    thread_normalized_labels AS (
        SELECT
            e.composite_thread_id,
            e.thread_id AS original_thread_id,
            ARRAY_AGG(DISTINCT nlabel ORDER BY nlabel) FILTER (WHERE nlabel IS NOT NULL) AS all_normalized_labels
        FROM filtered_emails e
        LEFT JOIN LATERAL unnest(COALESCE(e.normalized_labels, '{}')) AS nlabel ON true
        GROUP BY e.composite_thread_id, e.thread_id
    ),
    latest_in_thread AS (
        SELECT DISTINCT ON (e.composite_thread_id)
            e.composite_thread_id,
            e.thread_id,
            e.external_id,
            e.subject,
            e.from AS sender,
            e.snippet,
            e.received_at,
            e.ai_summary,
            e.ai_important,
            e.ai_analyzed
        FROM filtered_emails e
        ORDER BY e.composite_thread_id, e.received_at DESC
    )
    SELECT
        t.original_thread_id AS thread_id,
        l.external_id AS latest_external_id,
        l.subject,
        l.sender,
        l.snippet,
        tl.all_labels AS labels,
        tnl.all_normalized_labels AS normalized_labels,
        t.has_unread AS is_unread,
        t.has_starred AS is_starred,
        l.received_at,
        t.has_attach AS has_attachments,
        COALESCE(ftc.total_msg_count, t.msg_count) AS message_count,
        t.unique_senders AS participant_count,
        l.ai_summary,
        CASE
            WHEN COALESCE(te.user_has_engaged, false) THEN true
            ELSE COALESCE(l.ai_important, false)
        END AS ai_important,
        l.ai_analyzed,
        t.thread_ext_connection_id AS ext_connection_id,
        ec.provider_email AS account_email,
        ec.provider AS account_provider,
        ec.metadata->>'picture' AS account_avatar
    FROM thread_aggregates t
    JOIN latest_in_thread l ON l.composite_thread_id = t.composite_thread_id
    LEFT JOIN thread_engagement te ON te.composite_thread_id = t.composite_thread_id
    LEFT JOIN full_thread_counts ftc ON ftc.composite_thread_id = t.composite_thread_id
    LEFT JOIN thread_labels tl
        ON tl.composite_thread_id = t.composite_thread_id
       AND tl.original_thread_id = t.original_thread_id
    LEFT JOIN thread_normalized_labels tnl
        ON tnl.composite_thread_id = t.composite_thread_id
       AND tnl.original_thread_id = t.original_thread_id
    LEFT JOIN ext_connections ec ON ec.id = t.thread_ext_connection_id
    ORDER BY l.received_at DESC
    LIMIT p_max_results
    OFFSET p_offset;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."backfill_entities"() RETURNS TABLE("entity_type" "text", "count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role' THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'email', created_at FROM emails
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'todo', created_at FROM todos
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'document', created_at FROM documents
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'calendar_event', created_at FROM calendar_events
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'conversation', created_at FROM conversations
    ON CONFLICT (id) DO NOTHING;

    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT m.id, c.user_id, 'message', m.created_at
    FROM messages m
    JOIN conversations c ON m.conversation_id = c.id
    ON CONFLICT (id) DO NOTHING;

    RETURN QUERY
    SELECT e.entity_type, COUNT(*)::BIGINT
    FROM entities e
    GROUP BY e.entity_type
    ORDER BY e.entity_type;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_user_id UUID;
BEGIN
    SELECT user_id INTO v_user_id FROM entities WHERE id = p_entity_id;

    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM v_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    UPDATE entities
    SET embedding = p_embedding
    WHERE id = p_entity_id;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 5, "similarity_threshold" double precision DEFAULT 0.7) RETURNS TABLE("id" "uuid", "entity_type" "text", "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        e.id,
        e.entity_type,
        e.created_at,
        (1 - (e.embedding <=> query_embedding))::FLOAT as similarity
    FROM entities e
    WHERE e.user_id = p_user_id
      AND e.id != exclude_entity_id
      AND e.embedding IS NOT NULL
      AND (exclude_type IS NULL OR e.entity_type != exclude_type)
      AND (1 - (e.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[] DEFAULT NULL::"text"[], "p_limit" integer DEFAULT 10) RETURNS TABLE("related_entity_id" "uuid", "entity_type" "text", "relationship" "text", "confidence" double precision, "direction" "text", "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        r.target_entity_id as related_entity_id,
        e.entity_type,
        r.relationship,
        r.confidence,
        'outgoing'::TEXT as direction,
        r.created_at
    FROM memory_relationships r
    JOIN entities e ON e.id = r.target_entity_id
    WHERE r.user_id = p_user_id
      AND r.source_entity_id = p_entity_id
      AND r.is_active = true
      AND (p_relationship_types IS NULL OR r.relationship = ANY(p_relationship_types))

    UNION ALL

    SELECT
        r.source_entity_id as related_entity_id,
        e.entity_type,
        r.relationship,
        r.confidence,
        'incoming'::TEXT as direction,
        r.created_at
    FROM memory_relationships r
    JOIN entities e ON e.id = r.source_entity_id
    WHERE r.user_id = p_user_id
      AND r.target_entity_id = p_entity_id
      AND r.is_active = true
      AND (p_relationship_types IS NULL OR r.relationship = ANY(p_relationship_types))

    ORDER BY confidence DESC
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[] DEFAULT NULL::"text"[], "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3, "include_related" boolean DEFAULT true) RETURNS TABLE("entity_id" "uuid", "entity_type" "text", "similarity" double precision, "related_entities" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    WITH matched_entities AS (
        SELECT
            e.id,
            e.entity_type,
            (1 - (e.embedding <=> query_embedding))::FLOAT as similarity
        FROM entities e
        WHERE e.user_id = p_user_id
          AND e.embedding IS NOT NULL
          AND (p_entity_types IS NULL OR e.entity_type = ANY(p_entity_types))
          AND (1 - (e.embedding <=> query_embedding)) >= similarity_threshold
        ORDER BY e.embedding <=> query_embedding
        LIMIT p_limit
    ),
    entity_relationships AS (
        SELECT
            me.id as entity_id,
            CASE WHEN include_related THEN (
                SELECT jsonb_agg(jsonb_build_object(
                    'id', rel.related_entity_id,
                    'type', rel.entity_type,
                    'relationship', rel.relationship,
                    'direction', rel.direction
                ))
                FROM (
                    SELECT
                        r.target_entity_id as related_entity_id,
                        e2.entity_type,
                        r.relationship,
                        'outgoing' as direction
                    FROM memory_relationships r
                    JOIN entities e2 ON e2.id = r.target_entity_id
                    WHERE r.source_entity_id = me.id AND r.is_active = true

                    UNION ALL

                    SELECT
                        r.source_entity_id as related_entity_id,
                        e2.entity_type,
                        r.relationship,
                        'incoming' as direction
                    FROM memory_relationships r
                    JOIN entities e2 ON e2.id = r.source_entity_id
                    WHERE r.target_entity_id = me.id AND r.is_active = true

                    LIMIT 5
                ) rel
            ) ELSE NULL END as related
        FROM matched_entities me
    )
    SELECT
        me.id as entity_id,
        me.entity_type,
        me.similarity,
        COALESCE(er.related, '[]'::jsonb) as related_entities
    FROM matched_entities me
    LEFT JOIN entity_relationships er ON er.entity_id = me.id
    ORDER BY me.similarity DESC;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "title" "text", "last_message" "text", "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        c.id,
        c.title,
        (
            SELECT m.content
            FROM messages m
            WHERE m.conversation_id = c.id
            ORDER BY m.created_at DESC
            LIMIT 1
        ) as last_message,
        c.created_at,
        (1 - (e.embedding <=> query_embedding))::FLOAT as similarity
    FROM entities e
    JOIN conversations c ON e.id = c.id
    WHERE e.user_id = p_user_id
      AND e.entity_type = 'conversation'
      AND e.embedding IS NOT NULL
      AND (1 - (e.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "title" "text", "content" "text", "updated_at" timestamp with time zone, "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        d.id,
        d.title,
        d.content,
        d.updated_at,
        d.created_at,
        (1 - (e.embedding <=> query_embedding))::FLOAT as similarity
    FROM entities e
    JOIN documents d ON e.id = d.id
    WHERE e.user_id = p_user_id
      AND e.entity_type = 'document'
      AND e.embedding IS NOT NULL
      AND d.is_folder = false
      AND (1 - (e.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "conversation_entity_id" "uuid", "summary" "text", "key_topics" "text"[], "decisions" "text"[], "action_items" "text"[], "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        me.id,
        me.conversation_entity_id,
        me.summary,
        me.key_topics,
        me.decisions,
        me.action_items,
        me.created_at,
        (1 - (me.embedding <=> query_embedding))::FLOAT as similarity
    FROM memory_episodes me
    WHERE me.user_id = p_user_id
      AND me.is_active = true
      AND me.embedding IS NOT NULL
      AND (1 - (me.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY me.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "category" "text", "key" "text", "value" "text", "confidence" double precision, "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        mf.id,
        mf.category,
        mf.key,
        mf.value,
        mf.confidence,
        mf.created_at,
        (1 - (mf.embedding <=> query_embedding))::FLOAT as similarity
    FROM memory_facts mf
    WHERE mf.user_id = p_user_id
      AND mf.is_active = true
      AND mf.embedding IS NOT NULL
      AND (1 - (mf.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY mf.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."semantic_search_todos"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "title" "text", "notes" "text", "due_at" timestamp with time zone, "is_completed" boolean, "priority" integer, "tags" "text"[], "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        t.id,
        t.title,
        t.notes,
        t.due_at,
        t.is_completed,
        t.priority,
        t.tags,
        t.created_at,
        (1 - (e.embedding <=> query_embedding))::FLOAT as similarity
    FROM entities e
    JOIN todos t ON e.id = t.id
    WHERE e.user_id = p_user_id
      AND e.entity_type = 'todo'
      AND e.embedding IS NOT NULL
      AND (1 - (e.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."semantic_search_workouts"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "title" "text", "notes" "text", "workout_type" "text", "duration_minutes" integer, "completed_at" timestamp with time zone, "scheduled_at" timestamp with time zone, "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT
        w.id,
        w.title,
        w.notes,
        w.workout_type,
        w.duration_minutes,
        w.completed_at,
        w.scheduled_at,
        w.created_at,
        (1 - (e.embedding <=> query_embedding))::FLOAT as similarity
    FROM entities e
    JOIN workouts w ON e.id = w.id
    WHERE e.user_id = p_user_id
      AND e.entity_type = 'workout'
      AND e.embedding IS NOT NULL
      AND (1 - (e.embedding <=> query_embedding)) >= similarity_threshold
    ORDER BY e.embedding <=> query_embedding
    LIMIT p_limit;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") RETURNS TABLE("id" "uuid", "category" "text", "key" "text", "value" "text", "confidence" double precision, "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    SELECT DISTINCT ON (mf.category, mf.key)
        mf.id,
        mf.category,
        mf.key,
        mf.value,
        mf.confidence,
        mf.created_at
    FROM memory_facts mf
    WHERE mf.user_id = p_user_id
      AND mf.is_active = true
    ORDER BY mf.category, mf.key, mf.created_at DESC;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision DEFAULT 1.0, "p_source_type" "text" DEFAULT NULL::"text", "p_source_entity_id" "uuid" DEFAULT NULL::"uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_current_id UUID;
    v_new_id UUID;
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    SELECT id INTO v_current_id
    FROM memory_facts
    WHERE user_id = p_user_id
      AND category = p_category
      AND key = p_key
      AND is_active = true
    ORDER BY created_at DESC
    LIMIT 1;

    INSERT INTO memory_facts (
        user_id, category, key, value, confidence,
        source_type, source_entity_id, supersedes_id
    )
    VALUES (
        p_user_id, p_category, p_key, p_value, p_confidence,
        p_source_type, p_source_entity_id, v_current_id
    )
    RETURNING id INTO v_new_id;

    IF v_current_id IS NOT NULL THEN
        UPDATE memory_facts
        SET is_active = false
        WHERE id = v_current_id;
    END IF;

    RETURN v_new_id;
END;
$$;


CREATE OR REPLACE FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision DEFAULT 1.0) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_existing_id UUID;
    v_new_id UUID;
BEGIN
    IF COALESCE(current_setting('role', true), '') <> 'service_role'
       AND (auth.uid() IS NULL OR auth.uid() IS DISTINCT FROM p_user_id) THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    SELECT id INTO v_existing_id
    FROM memory_relationships
    WHERE user_id = p_user_id
      AND source_entity_id = p_source_entity_id
      AND target_entity_id = p_target_entity_id
      AND relationship = p_relationship
      AND is_active = true
    LIMIT 1;

    IF v_existing_id IS NOT NULL THEN
        RETURN v_existing_id;
    END IF;

    INSERT INTO memory_relationships (
        user_id, source_entity_id, target_entity_id,
        relationship, confidence
    )
    VALUES (
        p_user_id, p_source_entity_id, p_target_entity_id,
        p_relationship, p_confidence
    )
    RETURNING id INTO v_new_id;

    RETURN v_new_id;
END;
$$;


REVOKE ALL ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."create_workspace_with_defaults"("p_name" "text", "p_user_id" "uuid", "p_is_default" boolean, "p_create_default_apps" boolean) TO "service_role";

REVOKE ALL ON FUNCTION "public"."get_email_counts_by_account"("p_user_id" "uuid", "p_account_ids" "uuid"[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."get_email_counts_by_account"("p_user_id" "uuid", "p_account_ids" "uuid"[]) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."get_email_counts_by_account"("p_user_id" "uuid", "p_account_ids" "uuid"[]) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."get_email_counts_by_account"("p_user_id" "uuid", "p_account_ids" "uuid"[]) TO "service_role";

REVOKE ALL ON FUNCTION "public"."get_email_threads"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_id" "uuid") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."get_email_threads"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_id" "uuid") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."get_email_threads"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_id" "uuid") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."get_email_threads"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_id" "uuid") TO "service_role";

REVOKE ALL ON FUNCTION "public"."get_email_threads_unified"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_ids" "uuid"[]) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."get_email_threads_unified"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_ids" "uuid"[]) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."get_email_threads_unified"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_ids" "uuid"[]) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."get_email_threads_unified"("p_user_id" "uuid", "p_max_results" integer, "p_label_filter" "text", "p_offset" integer, "p_ext_connection_ids" "uuid"[]) TO "service_role";

REVOKE ALL ON FUNCTION "public"."backfill_entities"() FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."backfill_entities"() FROM "anon";
REVOKE ALL ON FUNCTION "public"."backfill_entities"() FROM "authenticated";
GRANT EXECUTE ON FUNCTION "public"."backfill_entities"() TO "service_role";

REVOKE ALL ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") TO "service_role";

REVOKE ALL ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) TO "service_role";

REVOKE ALL ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) TO "service_role";

REVOKE ALL ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search_todos"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search_todos"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_todos"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_todos"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."semantic_search_workouts"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."semantic_search_workouts"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_workouts"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."semantic_search_workouts"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

REVOKE ALL ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") TO "service_role";

REVOKE ALL ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") TO "service_role";

REVOKE ALL ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) FROM PUBLIC;
REVOKE ALL ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) FROM "anon";
GRANT EXECUTE ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) TO "authenticated";
GRANT EXECUTE ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) TO "service_role";
