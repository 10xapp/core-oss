-- Fix thread message_count to count ALL emails in a thread, not just label-filtered ones.
-- Previously, viewing inbox would only count inbox-labeled emails, missing sent replies.

-- Fix get_email_threads (legacy single-account function)
CREATE OR REPLACE FUNCTION "public"."get_email_threads"("p_user_id" "uuid", "p_max_results" integer DEFAULT 50, "p_label_filter" "text" DEFAULT NULL::"text", "p_offset" integer DEFAULT 0, "p_ext_connection_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("thread_id" "text", "latest_external_id" "text", "subject" "text", "sender" "text", "snippet" "text", "labels" "text"[], "is_unread" boolean, "is_starred" boolean, "received_at" timestamp with time zone, "has_attachments" boolean, "message_count" bigint, "participant_count" bigint, "ai_summary" "text", "ai_important" boolean, "ai_analyzed" boolean, "ext_connection_id" "uuid")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
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
    -- Get ALL emails for these threads (regardless of label filter) to check user engagement
    all_thread_emails AS (
        SELECT DISTINCT
            ne.composite_thread_id,
            'SENT' = ANY(ae.labels) as is_sent_email
        FROM normalized_emails ne
        JOIN emails ae ON ae.thread_id = ne.thread_id
                       AND ae.user_id = p_user_id
                       AND ae.is_trashed = false
    ),
    -- Count ALL emails per thread (regardless of label filter)
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
    -- Check if user has engaged in each thread
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
            -- Get the ext_connection_id (all emails in a thread should have the same one)
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
        -- HARD RULE: If user has engaged (sent/replied) in this thread, always mark as important
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

-- Fix get_email_threads_unified (multi-account function, actively used)
CREATE OR REPLACE FUNCTION "public"."get_email_threads_unified"("p_user_id" "uuid", "p_max_results" integer DEFAULT 50, "p_label_filter" "text" DEFAULT NULL::"text", "p_offset" integer DEFAULT 0, "p_ext_connection_ids" "uuid"[] DEFAULT NULL::"uuid"[]) RETURNS TABLE("thread_id" "text", "latest_external_id" "text", "subject" "text", "sender" "text", "snippet" "text", "labels" "text"[], "normalized_labels" "text"[], "is_unread" boolean, "is_starred" boolean, "received_at" timestamp with time zone, "has_attachments" boolean, "message_count" bigint, "participant_count" bigint, "ai_summary" "text", "ai_important" boolean, "ai_analyzed" boolean, "ext_connection_id" "uuid", "account_email" "text", "account_provider" "text", "account_avatar" "text")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
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
    -- Count ALL emails per thread (regardless of label filter) for accurate message_count
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
