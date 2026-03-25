# Supabase Schema

## Structure

```
supabase/
├── config.toml                  # Supabase project config
├── migrations/                  # 21 domain-organized migration files
│   ├── 00001_extensions_and_types.sql    # pgvector, enums, update_updated_at_column()
│   ├── 00002_core_tables.sql             # users, ext_connections, push_subscriptions, user_preferences
│   ├── 00003_workspaces.sql              # workspaces, members, apps, app_members + RLS helpers
│   ├── 00004_email_system.sql            # emails + thread RPCs + FTS
│   ├── 00005_calendar_system.sql         # calendar_events
│   ├── 00006_todos_system.sql            # todos, habit_completions + streak RPCs
│   ├── 00007_files_and_documents.sql     # files, documents, note_attachments, document_versions
│   ├── 00008_chat_system.sql             # conversations, messages, chat_attachments
│   ├── 00009_sounds_system.sql           # sounds, playlists, user_listens
│   ├── 00010_agents.sql                  # agent_templates/instances/tasks/steps/conversations
│   ├── 00011_channels_and_messaging.sql  # channels, members, messages, reactions, read_status
│   ├── 00012_projects.sql                # boards, states, issues, labels, assignees, comments
│   ├── 00013_notifications_and_invitations.sql
│   ├── 00014_sharing_permissions.sql     # permissions, access_requests + RLS updates
│   ├── 00015_search_and_embeddings.sql   # entities, memory_*, semantic_search RPCs
│   ├── 00016_sites_and_builder.sql       # builder_projects/conversations/versions/messages
│   ├── 00018_signup_trigger.sql          # auth.users trigger -> create_default_workspace
│   ├── 00019_realtime_config.sql         # ALTER PUBLICATION for realtime-enabled tables
│   ├── 00020_seed_data.sql               # Brand influencer agent template
│   └── 00021_storage_config.sql          # agent-data bucket + storage RLS
├── baseline/                    # Canonical schema dumps from prod
│   ├── canonical_baseline.sql   # pg_dump of public schema
│   └── canonical_storage.sql    # pg_dump of storage schema
└── README.md                    # This file
```

## Migration Consolidation (2026-03-16)

The original 133 incremental migration files were consolidated into 21 domain-organized files. The canonical schema was pulled from prod using `supabase db dump`, then mechanically split into logical domains respecting FK dependency order.

**Rollback**: Original 133 files are in git history on `main` (one commit before this merge).

## Cutover: Applying to Existing Environments

After the consolidation, existing environments (prod/staging) have the old 133 migrations in their `supabase_migrations` history table. You **cannot** just `db push` — that would try to replay the consolidated files against an already-populated schema.

**Per-environment cutover sequence:**

```bash
# 1. Revoke all old migration versions from the history table
#    (repeat for every old timestamp — the full list is in migrations_archive/)
supabase migration repair --status reverted 20251121000000
supabase migration repair --status reverted 20251121000001
# ... all 133 old timestamps ...

# 2. Mark the new consolidated migrations as already applied
supabase migration repair --status applied 20260316000001
supabase migration repair --status applied 20260316000002
# ... all 21 new timestamps ...

# 3. Verify — should show all 21 as Applied, zero pending
supabase migration list

# 4. Confirm no schema drift
supabase db diff --linked
# Must produce zero output
```

**For fresh installs** (new dev environments, CI):
```bash
supabase db reset   # replays all 21 migrations from scratch
```

## Quick Reference

```bash
# See what would be applied (dry run)
supabase db push --dry-run

# Push new migrations to remote database
supabase db push

# Check migration status
supabase migration list

# Diff local schema vs prod
supabase db diff --linked
```

## Creating a New Migration

```bash
# Naming convention: YYYYMMDDHHMMSS_description.sql
supabase migration new my_migration_name
```

Write idempotent SQL:

```sql
-- Tables: use IF NOT EXISTS
CREATE TABLE IF NOT EXISTS my_table (...);

-- Indexes: use IF NOT EXISTS
CREATE INDEX IF NOT EXISTS idx_my_table_user_id ON my_table(user_id);

-- Policies: drop first, then create
DROP POLICY IF EXISTS "Users can view own data" ON my_table;
CREATE POLICY "Users can view own data" ON my_table FOR SELECT USING (auth.uid() = user_id);

-- Triggers: drop first, then create
DROP TRIGGER IF EXISTS update_my_table_updated_at ON my_table;
CREATE TRIGGER update_my_table_updated_at BEFORE UPDATE ON my_table
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
```

## Common Patterns

- **UUID PKs**: `id UUID PRIMARY KEY DEFAULT gen_random_uuid()` (NOT uuid_generate_v4)
- **User FKs**: `user_id UUID NOT NULL REFERENCES auth.users(id) ON DELETE CASCADE`
- **RLS**: Always enable + create SELECT/INSERT/UPDATE/DELETE policies
- **Updated at**: Use existing `update_updated_at_column()` trigger function
- **SECURITY DEFINER functions**: Always add `SET search_path = public`

## Troubleshooting

```bash
# Mark migration as already applied
supabase migration repair --status applied 20260316000001

# Mark migration as reverted
supabase migration repair --status reverted 20260316000001

# View migration history
supabase migration list
```
