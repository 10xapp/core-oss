-- Migration: Search & Embeddings (entities, memory_facts, memory_episodes, memory_relationships, user_memory)
-- Functions: insert_entity, insert_message_entity, delete_entity, backfill_entities,
--            update_entity_embedding, find_similar_entities, get_related_entities,
--            search_with_relationships, full_text_search, semantic_search,
--            semantic_search_conversations, semantic_search_documents, semantic_search_episodes,
--            semantic_search_memory,
--            get_active_memory_facts, write_memory_fact, write_relationship

----------------------------------------------------------------------------
-- Functions
----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION "public"."insert_entity"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_entity_type TEXT;
BEGIN
    -- Determine entity type from table name
    v_entity_type := TG_ARGV[0];

    -- Insert into entities (ignore conflicts for idempotency)
    INSERT INTO entities (id, user_id, entity_type, created_at)
    VALUES (NEW.id, NEW.user_id, v_entity_type, COALESCE(NEW.created_at, now()))
    ON CONFLICT (id) DO NOTHING;

    RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."insert_entity"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."insert_message_entity"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_user_id UUID;
BEGIN
    -- Get user_id from parent conversation
    SELECT user_id INTO v_user_id
    FROM conversations
    WHERE id = NEW.conversation_id;

    IF v_user_id IS NOT NULL THEN
        INSERT INTO entities (id, user_id, entity_type, created_at)
        VALUES (NEW.id, v_user_id, 'message', COALESCE(NEW.created_at, now()))
        ON CONFLICT (id) DO NOTHING;
    END IF;

    RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."insert_message_entity"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."delete_entity"() RETURNS "trigger"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    DELETE FROM entities WHERE id = OLD.id;
    RETURN OLD;
END;
$$;

ALTER FUNCTION "public"."delete_entity"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."backfill_entities"() RETURNS TABLE("entity_type" "text", "count" bigint)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Backfill emails
    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'email', created_at FROM emails
    ON CONFLICT (id) DO NOTHING;

    -- Backfill documents
    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'document', created_at FROM documents
    ON CONFLICT (id) DO NOTHING;

    -- Backfill calendar_events
    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'calendar_event', created_at FROM calendar_events
    ON CONFLICT (id) DO NOTHING;

    -- Backfill conversations
    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT id, user_id, 'conversation', created_at FROM conversations
    ON CONFLICT (id) DO NOTHING;

    -- Backfill messages (join with conversations to get user_id)
    INSERT INTO entities (id, user_id, entity_type, created_at)
    SELECT m.id, c.user_id, 'message', m.created_at
    FROM messages m
    JOIN conversations c ON m.conversation_id = c.id
    ON CONFLICT (id) DO NOTHING;

    -- Return counts
    RETURN QUERY
    SELECT e.entity_type, COUNT(*)::BIGINT
    FROM entities e
    GROUP BY e.entity_type
    ORDER BY e.entity_type;
END;
$$;

ALTER FUNCTION "public"."backfill_entities"() OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") RETURNS "void"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_user_id UUID;
BEGIN
    -- Get user_id from entity
    SELECT user_id INTO v_user_id FROM entities WHERE id = p_entity_id;

    -- Verify user authorization
    IF auth.uid() != v_user_id AND auth.uid() IS NOT NULL THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    -- Update embedding
    UPDATE entities
    SET embedding = p_embedding
    WHERE id = p_entity_id;
END;
$$;

ALTER FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text" DEFAULT NULL::"text", "p_limit" integer DEFAULT 5, "similarity_threshold" double precision DEFAULT 0.7) RETURNS TABLE("id" "uuid", "entity_type" "text", "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[] DEFAULT NULL::"text"[], "p_limit" integer DEFAULT 10) RETURNS TABLE("related_entity_id" "uuid", "entity_type" "text", "relationship" "text", "confidence" double precision, "direction" "text", "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    RETURN QUERY
    -- Outgoing relationships (source -> target)
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

    -- Incoming relationships (target <- source)
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

ALTER FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[] DEFAULT NULL::"text"[], "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3, "include_related" boolean DEFAULT true) RETURNS TABLE("entity_id" "uuid", "entity_type" "text", "similarity" double precision, "related_entities" "jsonb")
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[] DEFAULT ARRAY['emails'::"text", 'calendar'::"text", 'documents'::"text"], "result_limit" integer DEFAULT 50, "p_user_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("id" "uuid", "type" "text", "title" "text", "content" "text", "rank" double precision, "metadata" "jsonb")
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
  ts_query tsquery;
  actual_user_id uuid;
BEGIN
  -- SECURITY: Always use authenticated user's ID, ignore parameter to prevent RLS bypass
  actual_user_id := auth.uid();
  IF actual_user_id IS NULL THEN
    RAISE EXCEPTION 'Not authenticated';
  END IF;

  ts_query := websearch_to_tsquery('english', search_query);

  RETURN QUERY
  SELECT * FROM (
    SELECT
      e.id,
      'email'::text as type,
      e.subject as title,
      e.snippet as content,
      ts_rank(e.search_vector, ts_query)::float as rank,
      jsonb_build_object(
        'from', e."from",
        'received_at', e.received_at,
        'external_id', e.external_id,
        'is_read', e.is_read,
        'labels', e.labels
      ) as metadata
    FROM emails e
    WHERE 'emails' = ANY(search_types)
      AND e.user_id = actual_user_id
      AND e.search_vector @@ ts_query

    UNION ALL

    SELECT
      c.id,
      'calendar'::text as type,
      c.title,
      c.description as content,
      ts_rank(c.search_vector, ts_query)::float as rank,
      jsonb_build_object(
        'start_time', c.start_time,
        'end_time', c.end_time,
        'location', c.location,
        'external_id', c.external_id,
        'is_all_day', c.is_all_day
      ) as metadata
    FROM calendar_events c
    WHERE 'calendar' = ANY(search_types)
      AND c.user_id = actual_user_id
      AND c.search_vector @@ ts_query

    UNION ALL

    SELECT
      d.id,
      'document'::text as type,
      d.title,
      LEFT(d.content, 500) as content,
      ts_rank(d.search_vector, ts_query)::float as rank,
      jsonb_build_object(
        'is_folder', d.is_folder,
        'updated_at', d.updated_at,
        'parent_id', d.parent_id
      ) as metadata
    FROM documents d
    WHERE 'documents' = ANY(search_types)
      AND d.user_id = actual_user_id
      AND d.search_vector @@ ts_query
      AND d.is_folder = false
  ) results
  ORDER BY rank DESC
  LIMIT result_limit;
END;
$$;

ALTER FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[] DEFAULT ARRAY['emails'::"text", 'messages'::"text", 'documents'::"text", 'calendar'::"text"], "match_threshold" double precision DEFAULT 0.3, "result_limit" integer DEFAULT 10, "p_user_id" "uuid" DEFAULT NULL::"uuid") RETURNS TABLE("id" "uuid", "type" "text", "title" "text", "content" "text", "similarity" double precision, "metadata" "jsonb", "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" SECURITY DEFINER
    AS $$
DECLARE
  actual_user_id uuid;
BEGIN
  -- SECURITY: Always use authenticated user's ID
  actual_user_id := auth.uid();
  IF actual_user_id IS NULL THEN
    RAISE EXCEPTION 'Not authenticated';
  END IF;

  RETURN QUERY
  SELECT * FROM (
    -- Emails
    SELECT
      e.id,
      'email'::text as type,
      e.subject as title,
      e.snippet as content,
      (1 - (e.embedding <=> query_embedding))::float as similarity,
      jsonb_build_object(
        'from', e."from",
        'to', e."to",
        'received_at', e.received_at,
        'thread_id', e.thread_id,
        'is_read', e.is_read,
        'labels', e.labels
      ) as metadata,
      COALESCE(e.received_at, e.created_at) as created_at
    FROM emails e
    WHERE 'emails' = ANY(search_types)
      AND e.user_id = actual_user_id
      AND e.embedding IS NOT NULL
      AND (1 - (e.embedding <=> query_embedding)) > match_threshold

    UNION ALL

    -- Channel messages
    SELECT
      m.id,
      'message'::text as type,
      NULL::text as title,
      LEFT(m.content, 500) as content,
      (1 - (m.embedding <=> query_embedding))::float as similarity,
      jsonb_build_object(
        'channel_id', m.channel_id,
        'user_id', m.user_id,
        'agent_id', m.agent_id,
        'reply_count', m.reply_count,
        'thread_parent_id', m.thread_parent_id
      ) as metadata,
      m.created_at
    FROM channel_messages m
    WHERE 'messages' = ANY(search_types)
      AND m.embedding IS NOT NULL
      AND (1 - (m.embedding <=> query_embedding)) > match_threshold

    UNION ALL

    -- Documents
    SELECT
      d.id,
      'document'::text as type,
      d.title,
      LEFT(d.content, 500) as content,
      (1 - (d.embedding <=> query_embedding))::float as similarity,
      jsonb_build_object(
        'is_folder', d.is_folder,
        'updated_at', d.updated_at,
        'parent_id', d.parent_id
      ) as metadata,
      d.created_at
    FROM documents d
    WHERE 'documents' = ANY(search_types)
      AND d.user_id = actual_user_id
      AND d.embedding IS NOT NULL
      AND d.is_folder = false
      AND (1 - (d.embedding <=> query_embedding)) > match_threshold

    UNION ALL

    -- Calendar events
    SELECT
      c.id,
      'calendar'::text as type,
      c.title,
      c.description as content,
      (1 - (c.embedding <=> query_embedding))::float as similarity,
      jsonb_build_object(
        'start_time', c.start_time,
        'end_time', c.end_time,
        'location', c.location,
        'is_all_day', c.is_all_day,
        'external_id', c.external_id
      ) as metadata,
      c.created_at
    FROM calendar_events c
    WHERE 'calendar' = ANY(search_types)
      AND c.user_id = actual_user_id
      AND c.embedding IS NOT NULL
      AND (1 - (c.embedding <=> query_embedding)) > match_threshold
  ) results
  ORDER BY similarity DESC
  LIMIT result_limit;
END;
$$;

ALTER FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "title" "text", "last_message" "text", "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "title" "text", "content" "text", "updated_at" timestamp with time zone, "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "conversation_entity_id" "uuid", "summary" "text", "key_topics" "text"[], "decisions" "text"[], "action_items" "text"[], "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer DEFAULT 10, "similarity_threshold" double precision DEFAULT 0.3) RETURNS TABLE("id" "uuid", "category" "text", "key" "text", "value" "text", "confidence" double precision, "created_at" timestamp with time zone, "similarity" double precision)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user authorization
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") RETURNS TABLE("id" "uuid", "category" "text", "key" "text", "value" "text", "confidence" double precision, "created_at" timestamp with time zone)
    LANGUAGE "plpgsql" STABLE SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    -- Verify user owns these facts
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
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

ALTER FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision DEFAULT 1.0, "p_source_type" "text" DEFAULT NULL::"text", "p_source_entity_id" "uuid" DEFAULT NULL::"uuid") RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_current_id UUID;
    v_new_id UUID;
BEGIN
    -- Verify user owns this fact (security check)
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    -- Find current active fact for this key
    SELECT id INTO v_current_id
    FROM memory_facts
    WHERE user_id = p_user_id
      AND category = p_category
      AND key = p_key
      AND is_active = true
    ORDER BY created_at DESC
    LIMIT 1;

    -- Insert new fact
    INSERT INTO memory_facts (
        user_id, category, key, value, confidence,
        source_type, source_entity_id, supersedes_id
    )
    VALUES (
        p_user_id, p_category, p_key, p_value, p_confidence,
        p_source_type, p_source_entity_id, v_current_id
    )
    RETURNING id INTO v_new_id;

    -- Invalidate old fact
    IF v_current_id IS NOT NULL THEN
        UPDATE memory_facts
        SET is_active = false
        WHERE id = v_current_id;
    END IF;

    RETURN v_new_id;
END;
$$;

ALTER FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") OWNER TO "postgres";


CREATE OR REPLACE FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision DEFAULT 1.0) RETURNS "uuid"
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
DECLARE
    v_existing_id UUID;
    v_new_id UUID;
BEGIN
    -- Verify user owns the entities
    IF auth.uid() != p_user_id AND auth.uid() IS NOT NULL THEN
        RAISE EXCEPTION 'Unauthorized';
    END IF;

    -- Check if relationship already exists (same source, target, type)
    SELECT id INTO v_existing_id
    FROM memory_relationships
    WHERE user_id = p_user_id
      AND source_entity_id = p_source_entity_id
      AND target_entity_id = p_target_entity_id
      AND relationship = p_relationship
      AND is_active = true
    LIMIT 1;

    -- If exists, return existing id (idempotent)
    IF v_existing_id IS NOT NULL THEN
        RETURN v_existing_id;
    END IF;

    -- Insert new relationship
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

ALTER FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) OWNER TO "postgres";


----------------------------------------------------------------------------
-- Tables
----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS "public"."entities" (
    "id" "uuid" NOT NULL,
    "user_id" "uuid" NOT NULL,
    "entity_type" "text" NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "embedding" "public"."vector"(1536)
);

ALTER TABLE "public"."entities" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."memory_facts" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "entity_id" "uuid",
    "category" "text" NOT NULL,
    "key" "text" NOT NULL,
    "value" "text" NOT NULL,
    "confidence" double precision DEFAULT 1.0,
    "source_type" "text",
    "source_entity_id" "uuid",
    "supersedes_id" "uuid",
    "is_active" boolean DEFAULT true,
    "embedding" "public"."vector"(1536),
    "created_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."memory_facts" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."memory_episodes" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "conversation_entity_id" "uuid",
    "summary" "text" NOT NULL,
    "key_topics" "text"[],
    "decisions" "text"[],
    "action_items" "text"[],
    "supersedes_id" "uuid",
    "is_active" boolean DEFAULT true,
    "embedding" "public"."vector"(1536),
    "created_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."memory_episodes" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."memory_relationships" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "source_entity_id" "uuid" NOT NULL,
    "target_entity_id" "uuid" NOT NULL,
    "relationship" "text" NOT NULL,
    "confidence" double precision DEFAULT 1.0,
    "is_active" boolean DEFAULT true,
    "created_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."memory_relationships" OWNER TO "postgres";


CREATE TABLE IF NOT EXISTS "public"."user_memory" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "user_id" "uuid" NOT NULL,
    "memory_type" "text" NOT NULL,
    "content" "text" DEFAULT ''::"text" NOT NULL,
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "created_at" timestamp with time zone DEFAULT "now"()
);

ALTER TABLE "public"."user_memory" OWNER TO "postgres";


----------------------------------------------------------------------------
-- Primary Keys & Unique Constraints
----------------------------------------------------------------------------

ALTER TABLE ONLY "public"."entities"
    ADD CONSTRAINT "entities_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."memory_facts"
    ADD CONSTRAINT "memory_facts_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."memory_episodes"
    ADD CONSTRAINT "memory_episodes_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."memory_relationships"
    ADD CONSTRAINT "memory_relationships_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."user_memory"
    ADD CONSTRAINT "user_memory_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."user_memory"
    ADD CONSTRAINT "user_memory_user_id_memory_type_key" UNIQUE ("user_id", "memory_type");


----------------------------------------------------------------------------
-- Foreign Keys
----------------------------------------------------------------------------

ALTER TABLE ONLY "public"."entities"
    ADD CONSTRAINT "entities_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."memory_facts"
    ADD CONSTRAINT "memory_facts_entity_id_fkey" FOREIGN KEY ("entity_id") REFERENCES "public"."entities"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."memory_facts"
    ADD CONSTRAINT "memory_facts_supersedes_id_fkey" FOREIGN KEY ("supersedes_id") REFERENCES "public"."memory_facts"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."memory_facts"
    ADD CONSTRAINT "memory_facts_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."memory_episodes"
    ADD CONSTRAINT "memory_episodes_conversation_entity_id_fkey" FOREIGN KEY ("conversation_entity_id") REFERENCES "public"."entities"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."memory_episodes"
    ADD CONSTRAINT "memory_episodes_supersedes_id_fkey" FOREIGN KEY ("supersedes_id") REFERENCES "public"."memory_episodes"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."memory_episodes"
    ADD CONSTRAINT "memory_episodes_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."memory_relationships"
    ADD CONSTRAINT "memory_relationships_source_entity_id_fkey" FOREIGN KEY ("source_entity_id") REFERENCES "public"."entities"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."memory_relationships"
    ADD CONSTRAINT "memory_relationships_target_entity_id_fkey" FOREIGN KEY ("target_entity_id") REFERENCES "public"."entities"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."memory_relationships"
    ADD CONSTRAINT "memory_relationships_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."user_memory"
    ADD CONSTRAINT "user_memory_user_id_fkey" FOREIGN KEY ("user_id") REFERENCES "auth"."users"("id") ON DELETE CASCADE;


----------------------------------------------------------------------------
-- Indexes
----------------------------------------------------------------------------

-- entities indexes
CREATE INDEX "idx_entities_embedding" ON "public"."entities" USING "hnsw" ("embedding" "public"."vector_cosine_ops");
CREATE INDEX "idx_entities_user_created" ON "public"."entities" USING "btree" ("user_id", "created_at" DESC);
CREATE INDEX "idx_entities_user_type" ON "public"."entities" USING "btree" ("user_id", "entity_type");

-- memory_facts indexes
CREATE INDEX "idx_memory_facts_active" ON "public"."memory_facts" USING "btree" ("user_id", "is_active") WHERE ("is_active" = true);
CREATE INDEX "idx_memory_facts_embedding" ON "public"."memory_facts" USING "hnsw" ("embedding" "public"."vector_cosine_ops");
CREATE INDEX "idx_memory_facts_user_key" ON "public"."memory_facts" USING "btree" ("user_id", "category", "key");

-- memory_episodes indexes
CREATE INDEX "idx_memory_episodes_conversation" ON "public"."memory_episodes" USING "btree" ("conversation_entity_id");
CREATE INDEX "idx_memory_episodes_embedding" ON "public"."memory_episodes" USING "hnsw" ("embedding" "public"."vector_cosine_ops");
CREATE INDEX "idx_memory_episodes_user" ON "public"."memory_episodes" USING "btree" ("user_id", "is_active") WHERE ("is_active" = true);

-- memory_relationships indexes
CREATE INDEX "idx_memory_rel_source" ON "public"."memory_relationships" USING "btree" ("source_entity_id") WHERE ("is_active" = true);
CREATE INDEX "idx_memory_rel_target" ON "public"."memory_relationships" USING "btree" ("target_entity_id") WHERE ("is_active" = true);
CREATE INDEX "idx_memory_rel_user" ON "public"."memory_relationships" USING "btree" ("user_id", "is_active") WHERE ("is_active" = true);

-- user_memory indexes
CREATE INDEX "idx_user_memory_user" ON "public"."user_memory" USING "btree" ("user_id");


----------------------------------------------------------------------------
-- Row Level Security
----------------------------------------------------------------------------

ALTER TABLE "public"."entities" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."memory_facts" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."memory_episodes" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."memory_relationships" ENABLE ROW LEVEL SECURITY;
ALTER TABLE "public"."user_memory" ENABLE ROW LEVEL SECURITY;


----------------------------------------------------------------------------
-- Policies: entities
----------------------------------------------------------------------------

CREATE POLICY "Users can view own entities" ON "public"."entities" FOR SELECT USING (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can insert own entities" ON "public"."entities" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can update own entities" ON "public"."entities" FOR UPDATE USING (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can delete own entities" ON "public"."entities" FOR DELETE USING (("auth"."uid"() = "user_id"));


----------------------------------------------------------------------------
-- Policies: memory_facts
----------------------------------------------------------------------------

CREATE POLICY "Users can view own facts" ON "public"."memory_facts" FOR SELECT USING (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can insert own facts" ON "public"."memory_facts" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can update own facts" ON "public"."memory_facts" FOR UPDATE USING (("auth"."uid"() = "user_id"));


----------------------------------------------------------------------------
-- Policies: memory_episodes
----------------------------------------------------------------------------

CREATE POLICY "Users can view own episodes" ON "public"."memory_episodes" FOR SELECT USING (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can insert own episodes" ON "public"."memory_episodes" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can update own episodes" ON "public"."memory_episodes" FOR UPDATE USING (("auth"."uid"() = "user_id"));


----------------------------------------------------------------------------
-- Policies: memory_relationships
----------------------------------------------------------------------------

CREATE POLICY "Users can view own relationships" ON "public"."memory_relationships" FOR SELECT USING (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can insert own relationships" ON "public"."memory_relationships" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can update own relationships" ON "public"."memory_relationships" FOR UPDATE USING (("auth"."uid"() = "user_id"));


----------------------------------------------------------------------------
-- Policies: user_memory
----------------------------------------------------------------------------

CREATE POLICY "Users can view own memory" ON "public"."user_memory" FOR SELECT USING (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can insert own memory" ON "public"."user_memory" FOR INSERT WITH CHECK (("auth"."uid"() = "user_id"));
CREATE POLICY "Users can update own memory" ON "public"."user_memory" FOR UPDATE USING (("auth"."uid"() = "user_id"));


----------------------------------------------------------------------------
-- Triggers
----------------------------------------------------------------------------

CREATE OR REPLACE TRIGGER "update_user_memory_updated_at" BEFORE UPDATE ON "public"."user_memory" FOR EACH ROW EXECUTE FUNCTION "public"."update_updated_at_column"();


----------------------------------------------------------------------------
-- Grants: Functions
----------------------------------------------------------------------------

GRANT ALL ON FUNCTION "public"."insert_entity"() TO "anon";
GRANT ALL ON FUNCTION "public"."insert_entity"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."insert_entity"() TO "service_role";

GRANT ALL ON FUNCTION "public"."insert_message_entity"() TO "anon";
GRANT ALL ON FUNCTION "public"."insert_message_entity"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."insert_message_entity"() TO "service_role";

GRANT ALL ON FUNCTION "public"."delete_entity"() TO "anon";
GRANT ALL ON FUNCTION "public"."delete_entity"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."delete_entity"() TO "service_role";

GRANT ALL ON FUNCTION "public"."backfill_entities"() TO "anon";
GRANT ALL ON FUNCTION "public"."backfill_entities"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."backfill_entities"() TO "service_role";

GRANT ALL ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") TO "anon";
GRANT ALL ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_entity_embedding"("p_entity_id" "uuid", "p_embedding" "public"."vector") TO "service_role";

GRANT ALL ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."find_similar_entities"("query_embedding" "public"."vector", "p_user_id" "uuid", "exclude_entity_id" "uuid", "exclude_type" "text", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) TO "anon";
GRANT ALL ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_related_entities"("p_user_id" "uuid", "p_entity_id" "uuid", "p_relationship_types" "text"[], "p_limit" integer) TO "service_role";

GRANT ALL ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) TO "anon";
GRANT ALL ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_with_relationships"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_entity_types" "text"[], "p_limit" integer, "similarity_threshold" double precision, "include_related" boolean) TO "service_role";

GRANT ALL ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."full_text_search"("search_query" "text", "search_types" "text"[], "result_limit" integer, "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search"("query_embedding" "public"."vector", "search_types" "text"[], "match_threshold" double precision, "result_limit" integer, "p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_conversations"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_documents"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_episodes"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."semantic_search_memory"("query_embedding" "public"."vector", "p_user_id" "uuid", "p_limit" integer, "similarity_threshold" double precision) TO "service_role";

GRANT ALL ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."get_active_memory_facts"("p_user_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") TO "anon";
GRANT ALL ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") TO "authenticated";
GRANT ALL ON FUNCTION "public"."write_memory_fact"("p_user_id" "uuid", "p_category" "text", "p_key" "text", "p_value" "text", "p_confidence" double precision, "p_source_type" "text", "p_source_entity_id" "uuid") TO "service_role";

GRANT ALL ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) TO "anon";
GRANT ALL ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) TO "authenticated";
GRANT ALL ON FUNCTION "public"."write_relationship"("p_user_id" "uuid", "p_source_entity_id" "uuid", "p_target_entity_id" "uuid", "p_relationship" "text", "p_confidence" double precision) TO "service_role";


----------------------------------------------------------------------------
-- Grants: Tables
----------------------------------------------------------------------------

GRANT ALL ON TABLE "public"."entities" TO "anon";
GRANT ALL ON TABLE "public"."entities" TO "authenticated";
GRANT ALL ON TABLE "public"."entities" TO "service_role";

GRANT ALL ON TABLE "public"."memory_facts" TO "anon";
GRANT ALL ON TABLE "public"."memory_facts" TO "authenticated";
GRANT ALL ON TABLE "public"."memory_facts" TO "service_role";

GRANT ALL ON TABLE "public"."memory_episodes" TO "anon";
GRANT ALL ON TABLE "public"."memory_episodes" TO "authenticated";
GRANT ALL ON TABLE "public"."memory_episodes" TO "service_role";

GRANT ALL ON TABLE "public"."memory_relationships" TO "anon";
GRANT ALL ON TABLE "public"."memory_relationships" TO "authenticated";
GRANT ALL ON TABLE "public"."memory_relationships" TO "service_role";

GRANT ALL ON TABLE "public"."user_memory" TO "anon";
GRANT ALL ON TABLE "public"."user_memory" TO "authenticated";
GRANT ALL ON TABLE "public"."user_memory" TO "service_role";
