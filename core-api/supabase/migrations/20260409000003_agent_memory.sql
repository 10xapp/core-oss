-- ============================================================================
-- Migration: Agent Memory & Institutional Knowledge
-- Description: agent_memories, agent_knowledge_base tables with vector search
--              functions, indexes, RLS policies, triggers, and grants
-- ============================================================================

SET check_function_bodies = false;

-- ==========================================================
-- FUNCTIONS
-- ==========================================================

-- update_agent_memories_updated_at
CREATE OR REPLACE FUNCTION "public"."update_agent_memories_updated_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."update_agent_memories_updated_at"() OWNER TO "postgres";

-- update_agent_knowledge_base_updated_at
CREATE OR REPLACE FUNCTION "public"."update_agent_knowledge_base_updated_at"() RETURNS "trigger"
    LANGUAGE "plpgsql"
    AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

ALTER FUNCTION "public"."update_agent_knowledge_base_updated_at"() OWNER TO "postgres";

-- search_agent_memories: vector similarity search over an agent's memories
CREATE OR REPLACE FUNCTION "public"."search_agent_memories"(
    "p_agent_id" "uuid",
    "p_workspace_id" "uuid",
    "p_embedding" vector(1536),
    "p_match_count" integer DEFAULT 10,
    "p_similarity_threshold" float DEFAULT 0.5
) RETURNS TABLE (
    "id" "uuid",
    "memory_type" "text",
    "content" "text",
    "summary" "text",
    "tags" "text"[],
    "relevance_score" float,
    "similarity" float,
    "created_at" timestamp with time zone
)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        am.id,
        am.memory_type,
        am.content,
        am.summary,
        am.tags,
        am.relevance_score,
        1 - (am.embedding <=> p_embedding) AS similarity,
        am.created_at
    FROM agent_memories am
    WHERE am.agent_id = p_agent_id
      AND am.workspace_id = p_workspace_id
      AND am.expired_at IS NULL
      AND am.embedding IS NOT NULL
      AND 1 - (am.embedding <=> p_embedding) > p_similarity_threshold
    ORDER BY am.embedding <=> p_embedding
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."search_agent_memories"("p_agent_id" "uuid", "p_workspace_id" "uuid", "p_embedding" vector(1536), "p_match_count" integer, "p_similarity_threshold" float) OWNER TO "postgres";

-- search_knowledge_base: vector similarity search over workspace knowledge
CREATE OR REPLACE FUNCTION "public"."search_knowledge_base"(
    "p_workspace_id" "uuid",
    "p_embedding" vector(1536),
    "p_match_count" integer DEFAULT 10,
    "p_similarity_threshold" float DEFAULT 0.5
) RETURNS TABLE (
    "id" "uuid",
    "category" "text",
    "key" "text",
    "value" "text",
    "confidence" float,
    "similarity" float,
    "created_at" timestamp with time zone
)
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    RETURN QUERY
    SELECT
        kb.id,
        kb.category,
        kb.key,
        kb.value,
        kb.confidence,
        1 - (kb.embedding <=> p_embedding) AS similarity,
        kb.created_at
    FROM agent_knowledge_base kb
    WHERE kb.workspace_id = p_workspace_id
      AND kb.embedding IS NOT NULL
      AND 1 - (kb.embedding <=> p_embedding) > p_similarity_threshold
    ORDER BY kb.embedding <=> p_embedding
    LIMIT p_match_count;
END;
$$;

ALTER FUNCTION "public"."search_knowledge_base"("p_workspace_id" "uuid", "p_embedding" vector(1536), "p_match_count" integer, "p_similarity_threshold" float) OWNER TO "postgres";

-- decay_agent_memories: reduce relevance_score of old, unaccessed memories
CREATE OR REPLACE FUNCTION "public"."decay_agent_memories"() RETURNS void
    LANGUAGE "plpgsql" SECURITY DEFINER
    SET "search_path" TO 'public'
    AS $$
BEGIN
    UPDATE agent_memories
    SET relevance_score = relevance_score * decay_factor
    WHERE expired_at IS NULL
      AND relevance_score > 0.01
      AND last_accessed_at < NOW() - INTERVAL '7 days';

    -- Expire memories that have decayed below threshold
    UPDATE agent_memories
    SET expired_at = NOW()
    WHERE expired_at IS NULL
      AND relevance_score <= 0.01;
END;
$$;

ALTER FUNCTION "public"."decay_agent_memories"() OWNER TO "postgres";

-- ==========================================================
-- TABLES
-- ==========================================================

-- agent_memories
CREATE TABLE IF NOT EXISTS "public"."agent_memories" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "agent_id" "uuid" NOT NULL,
    "workspace_id" "uuid" NOT NULL,
    "memory_type" "text" NOT NULL,
    "content" "text" NOT NULL,
    "summary" "text",
    "embedding" vector(1536),
    "relevance_score" float DEFAULT 1.0 NOT NULL,
    "access_count" integer DEFAULT 0 NOT NULL,
    "last_accessed_at" timestamp with time zone,
    "source_type" "text",
    "source_id" "text",
    "tags" "text"[] DEFAULT '{}'::"text"[] NOT NULL,
    "is_shared" boolean DEFAULT false NOT NULL,
    "decay_factor" float DEFAULT 0.95 NOT NULL,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    "expired_at" timestamp with time zone,
    CONSTRAINT "agent_memories_memory_type_check" CHECK (("memory_type" = ANY (ARRAY['episodic'::"text", 'semantic'::"text", 'procedural'::"text", 'preference'::"text"]))),
    CONSTRAINT "agent_memories_decay_factor_check" CHECK (("decay_factor" > 0 AND "decay_factor" <= 1))
);

ALTER TABLE "public"."agent_memories" OWNER TO "postgres";

COMMENT ON TABLE "public"."agent_memories" IS 'Long-term memory storage for AI agents with vector embeddings and decay';

COMMENT ON COLUMN "public"."agent_memories"."memory_type" IS 'episodic=events, semantic=facts, procedural=how-to, preference=user prefs';

COMMENT ON COLUMN "public"."agent_memories"."decay_factor" IS 'Multiplier applied to relevance_score on each decay cycle (0-1)';

-- agent_knowledge_base
CREATE TABLE IF NOT EXISTS "public"."agent_knowledge_base" (
    "id" "uuid" DEFAULT "gen_random_uuid"() NOT NULL,
    "workspace_id" "uuid" NOT NULL,
    "category" "text" NOT NULL,
    "key" "text" NOT NULL,
    "value" "text" NOT NULL,
    "embedding" vector(1536),
    "confidence" float DEFAULT 1.0 NOT NULL,
    "source_agent_id" "uuid",
    "verified_by" "uuid",
    "verified_at" timestamp with time zone,
    "created_at" timestamp with time zone DEFAULT "now"(),
    "updated_at" timestamp with time zone DEFAULT "now"(),
    CONSTRAINT "agent_knowledge_base_confidence_check" CHECK (("confidence" >= 0 AND "confidence" <= 1))
);

ALTER TABLE "public"."agent_knowledge_base" OWNER TO "postgres";

COMMENT ON TABLE "public"."agent_knowledge_base" IS 'Shared institutional knowledge base for workspace agents';

-- ==========================================================
-- PRIMARY KEYS & UNIQUE CONSTRAINTS
-- ==========================================================

ALTER TABLE ONLY "public"."agent_memories"
    ADD CONSTRAINT "agent_memories_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."agent_knowledge_base"
    ADD CONSTRAINT "agent_knowledge_base_pkey" PRIMARY KEY ("id");

ALTER TABLE ONLY "public"."agent_knowledge_base"
    ADD CONSTRAINT "agent_knowledge_base_workspace_category_key_key" UNIQUE ("workspace_id", "category", "key");

-- ==========================================================
-- FOREIGN KEYS
-- ==========================================================

-- agent_memories FKs
ALTER TABLE ONLY "public"."agent_memories"
    ADD CONSTRAINT "agent_memories_agent_id_fkey" FOREIGN KEY ("agent_id") REFERENCES "public"."agent_instances"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."agent_memories"
    ADD CONSTRAINT "agent_memories_workspace_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE CASCADE;

-- agent_knowledge_base FKs
ALTER TABLE ONLY "public"."agent_knowledge_base"
    ADD CONSTRAINT "agent_knowledge_base_workspace_id_fkey" FOREIGN KEY ("workspace_id") REFERENCES "public"."workspaces"("id") ON DELETE CASCADE;

ALTER TABLE ONLY "public"."agent_knowledge_base"
    ADD CONSTRAINT "agent_knowledge_base_source_agent_id_fkey" FOREIGN KEY ("source_agent_id") REFERENCES "public"."agent_instances"("id") ON DELETE SET NULL;

ALTER TABLE ONLY "public"."agent_knowledge_base"
    ADD CONSTRAINT "agent_knowledge_base_verified_by_fkey" FOREIGN KEY ("verified_by") REFERENCES "auth"."users"("id") ON DELETE SET NULL;

-- ==========================================================
-- INDEXES
-- ==========================================================

-- agent_memories indexes
CREATE INDEX "idx_agent_memories_agent_id" ON "public"."agent_memories" USING "btree" ("agent_id");

CREATE INDEX "idx_agent_memories_workspace_id" ON "public"."agent_memories" USING "btree" ("workspace_id");

CREATE INDEX "idx_agent_memories_memory_type" ON "public"."agent_memories" USING "btree" ("memory_type");

CREATE INDEX "idx_agent_memories_tags" ON "public"."agent_memories" USING "gin" ("tags");

CREATE INDEX "idx_agent_memories_embedding" ON "public"."agent_memories" USING "hnsw" ("embedding" vector_cosine_ops);

CREATE INDEX "idx_agent_memories_created_at" ON "public"."agent_memories" USING "btree" ("created_at" DESC);

CREATE INDEX "idx_agent_memories_relevance_score" ON "public"."agent_memories" USING "btree" ("relevance_score" DESC);

CREATE INDEX "idx_agent_memories_is_shared" ON "public"."agent_memories" USING "btree" ("is_shared") WHERE ("is_shared" = true);

CREATE INDEX "idx_agent_memories_decay_candidates" ON "public"."agent_memories" USING "btree" ("last_accessed_at") WHERE ("expired_at" IS NULL AND "relevance_score" > 0.01);

-- agent_knowledge_base indexes
CREATE INDEX "idx_agent_knowledge_base_workspace_id" ON "public"."agent_knowledge_base" USING "btree" ("workspace_id");

CREATE INDEX "idx_agent_knowledge_base_category" ON "public"."agent_knowledge_base" USING "btree" ("category");

CREATE INDEX "idx_agent_knowledge_base_embedding" ON "public"."agent_knowledge_base" USING "hnsw" ("embedding" vector_cosine_ops);

-- ==========================================================
-- ROW LEVEL SECURITY
-- ==========================================================

ALTER TABLE "public"."agent_memories" ENABLE ROW LEVEL SECURITY;

ALTER TABLE "public"."agent_knowledge_base" ENABLE ROW LEVEL SECURITY;

-- ==========================================================
-- POLICIES: agent_memories
-- ==========================================================

CREATE POLICY "Users can view memories in their workspaces" ON "public"."agent_memories" FOR SELECT USING (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE ("wm"."user_id" = "auth"."uid"()))));

CREATE POLICY "Workspace admins can insert memories" ON "public"."agent_memories" FOR INSERT WITH CHECK (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE (("wm"."user_id" = "auth"."uid"()) AND ("wm"."role" = ANY (ARRAY['owner'::"public"."workspace_role", 'admin'::"public"."workspace_role"]))))));

CREATE POLICY "Workspace admins can update memories" ON "public"."agent_memories" FOR UPDATE USING (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE (("wm"."user_id" = "auth"."uid"()) AND ("wm"."role" = ANY (ARRAY['owner'::"public"."workspace_role", 'admin'::"public"."workspace_role"]))))));

CREATE POLICY "Workspace admins can delete memories" ON "public"."agent_memories" FOR DELETE USING (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE (("wm"."user_id" = "auth"."uid"()) AND ("wm"."role" = ANY (ARRAY['owner'::"public"."workspace_role", 'admin'::"public"."workspace_role"]))))));

-- ==========================================================
-- POLICIES: agent_knowledge_base
-- ==========================================================

CREATE POLICY "Users can view knowledge in their workspaces" ON "public"."agent_knowledge_base" FOR SELECT USING (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE ("wm"."user_id" = "auth"."uid"()))));

CREATE POLICY "Workspace admins can insert knowledge" ON "public"."agent_knowledge_base" FOR INSERT WITH CHECK (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE (("wm"."user_id" = "auth"."uid"()) AND ("wm"."role" = ANY (ARRAY['owner'::"public"."workspace_role", 'admin'::"public"."workspace_role"]))))));

CREATE POLICY "Workspace admins can update knowledge" ON "public"."agent_knowledge_base" FOR UPDATE USING (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE (("wm"."user_id" = "auth"."uid"()) AND ("wm"."role" = ANY (ARRAY['owner'::"public"."workspace_role", 'admin'::"public"."workspace_role"]))))));

CREATE POLICY "Workspace admins can delete knowledge" ON "public"."agent_knowledge_base" FOR DELETE USING (("workspace_id" IN ( SELECT "wm"."workspace_id"
   FROM "public"."workspace_members" "wm"
  WHERE (("wm"."user_id" = "auth"."uid"()) AND ("wm"."role" = ANY (ARRAY['owner'::"public"."workspace_role", 'admin'::"public"."workspace_role"]))))));

-- ==========================================================
-- TRIGGERS
-- ==========================================================

CREATE OR REPLACE TRIGGER "trigger_agent_memories_updated_at" BEFORE UPDATE ON "public"."agent_memories" FOR EACH ROW EXECUTE FUNCTION "public"."update_agent_memories_updated_at"();

CREATE OR REPLACE TRIGGER "trigger_agent_knowledge_base_updated_at" BEFORE UPDATE ON "public"."agent_knowledge_base" FOR EACH ROW EXECUTE FUNCTION "public"."update_agent_knowledge_base_updated_at"();

-- ==========================================================
-- GRANTS: functions
-- ==========================================================

GRANT ALL ON FUNCTION "public"."update_agent_memories_updated_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_agent_memories_updated_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_agent_memories_updated_at"() TO "service_role";

GRANT ALL ON FUNCTION "public"."update_agent_knowledge_base_updated_at"() TO "anon";
GRANT ALL ON FUNCTION "public"."update_agent_knowledge_base_updated_at"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."update_agent_knowledge_base_updated_at"() TO "service_role";

GRANT ALL ON FUNCTION "public"."search_agent_memories"("p_agent_id" "uuid", "p_workspace_id" "uuid", "p_embedding" vector(1536), "p_match_count" integer, "p_similarity_threshold" float) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_agent_memories"("p_agent_id" "uuid", "p_workspace_id" "uuid", "p_embedding" vector(1536), "p_match_count" integer, "p_similarity_threshold" float) TO "service_role";

GRANT ALL ON FUNCTION "public"."search_knowledge_base"("p_workspace_id" "uuid", "p_embedding" vector(1536), "p_match_count" integer, "p_similarity_threshold" float) TO "authenticated";
GRANT ALL ON FUNCTION "public"."search_knowledge_base"("p_workspace_id" "uuid", "p_embedding" vector(1536), "p_match_count" integer, "p_similarity_threshold" float) TO "service_role";

GRANT ALL ON FUNCTION "public"."decay_agent_memories"() TO "anon";
GRANT ALL ON FUNCTION "public"."decay_agent_memories"() TO "authenticated";
GRANT ALL ON FUNCTION "public"."decay_agent_memories"() TO "service_role";

-- ==========================================================
-- GRANTS: tables
-- ==========================================================

GRANT ALL ON TABLE "public"."agent_memories" TO "anon";
GRANT ALL ON TABLE "public"."agent_memories" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_memories" TO "service_role";

GRANT ALL ON TABLE "public"."agent_knowledge_base" TO "anon";
GRANT ALL ON TABLE "public"."agent_knowledge_base" TO "authenticated";
GRANT ALL ON TABLE "public"."agent_knowledge_base" TO "service_role";
