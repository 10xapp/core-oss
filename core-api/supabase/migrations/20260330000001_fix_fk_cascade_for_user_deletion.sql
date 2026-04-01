-- Fix foreign keys missing ON DELETE CASCADE/SET NULL that block user deletion
-- Issue: https://github.com/10xapp/core-api/issues/277

-- channels.created_by → CASCADE
ALTER TABLE channels DROP CONSTRAINT channels_created_by_fkey;
ALTER TABLE channels ADD CONSTRAINT channels_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES auth.users(id) ON DELETE CASCADE;

-- channel_messages.user_id → CASCADE
ALTER TABLE channel_messages DROP CONSTRAINT channel_messages_user_id_fkey;
ALTER TABLE channel_messages ADD CONSTRAINT channel_messages_user_id_fkey
  FOREIGN KEY (user_id) REFERENCES auth.users(id) ON DELETE CASCADE;

-- agent_conversations.created_by → CASCADE
ALTER TABLE agent_conversations DROP CONSTRAINT agent_conversations_created_by_fkey;
ALTER TABLE agent_conversations ADD CONSTRAINT agent_conversations_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES auth.users(id) ON DELETE CASCADE;

-- agent_instances.created_by → CASCADE
ALTER TABLE agent_instances DROP CONSTRAINT agent_instances_created_by_fkey;
ALTER TABLE agent_instances ADD CONSTRAINT agent_instances_created_by_fkey
  FOREIGN KEY (created_by) REFERENCES auth.users(id) ON DELETE CASCADE;

-- permissions.granted_by → SET NULL (preserve grants, just clear who granted them)
ALTER TABLE permissions DROP CONSTRAINT permissions_granted_by_fkey;
ALTER TABLE permissions ADD CONSTRAINT permissions_granted_by_fkey
  FOREIGN KEY (granted_by) REFERENCES auth.users(id) ON DELETE SET NULL;

-- access_requests.reviewed_by → SET NULL (preserve audit trail, just clear reviewer)
ALTER TABLE access_requests DROP CONSTRAINT access_requests_reviewed_by_fkey;
ALTER TABLE access_requests ADD CONSTRAINT access_requests_reviewed_by_fkey
  FOREIGN KEY (reviewed_by) REFERENCES auth.users(id) ON DELETE SET NULL;
