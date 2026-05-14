-- Migration: tune autovacuum for the hot connection_sync_state queue table.
--
-- This table is updated on every dirty mark, lease heartbeat, completion, and
-- retry transition. Default autovacuum thresholds are too lax for a hot queue
-- table and can allow avoidable bloat on small-tier Postgres.

ALTER TABLE "public"."connection_sync_state"
SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_vacuum_threshold = 50,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_analyze_threshold = 50
);
