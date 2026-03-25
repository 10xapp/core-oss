import { useMessagesStore } from '../stores/messagesStore';

export interface CachedChannelMetadata {
  channelName?: string;
  workspaceAppId?: string;
}

export function getCachedChannelMetadata(channelId: string): CachedChannelMetadata | null {
  const state = useMessagesStore.getState();

  const activeChannel = state.channels.find((channel) => channel.id === channelId);
  if (activeChannel) {
    return {
      channelName: activeChannel.name,
      workspaceAppId: activeChannel.workspace_app_id,
    };
  }

  const activeDm = state.dms.find((dm) => dm.id === channelId);
  if (activeDm) {
    return { workspaceAppId: activeDm.workspace_app_id };
  }

  for (const cache of Object.values(state.workspaceCache)) {
    const cachedChannel = (cache.channels || []).find((channel) => channel.id === channelId);
    if (cachedChannel) {
      return {
        channelName: cachedChannel.name,
        workspaceAppId: cachedChannel.workspace_app_id,
      };
    }

    const cachedDm = (cache.dms || []).find((dm) => dm.id === channelId);
    if (cachedDm) {
      return { workspaceAppId: cachedDm.workspace_app_id };
    }
  }

  return null;
}
