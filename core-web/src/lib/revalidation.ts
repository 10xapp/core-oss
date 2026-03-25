import { useMessagesStore } from '../stores/messagesStore';
import { useNotificationStore } from '../stores/notificationStore';
import { useFilesStore } from '../stores/filesStore';
import { useCalendarStore } from '../stores/calendarStore';
import { useWorkspaceStore } from '../stores/workspaceStore';
import { getChannelMessages } from '../api/client';
import { playMessageNotification } from './notificationSound';
import { showMessageNotification } from './messageNotification';
import { getCachedChannelMetadata } from './messageChannelMetadata';
import { toast } from 'sonner';

const RESUME_REVALIDATE_THRESHOLD_MS = 30_000;
const RESUME_RESUBSCRIBE_THRESHOLD_MS = 60_000;
const REALTIME_STALE_THRESHOLD_MS = 60_000;
const REVALIDATE_THROTTLE_MS = 10_000;
const RESUBSCRIBE_THROTTLE_MS = 10_000;
const NOTIFICATION_STALE_MS = 5 * 60 * 1000;
const MAX_RECOVERY_MESSAGE_TOASTS = 3;

let lastHiddenAt: number | null = null;
let lastVisibleAt: number | null = null;
let lastRevalidateAt: number | null = null;
let lastResubscribeAt: number | null = null;
let lastRealtimeEventAt: number | null = null;

function getAwayDuration(): number | null {
  if (lastHiddenAt === null || lastVisibleAt === null) return null;
  if (lastVisibleAt < lastHiddenAt) return null;
  return lastVisibleAt - lastHiddenAt;
}

export function noteHidden() {
  lastHiddenAt = Date.now();
}

export function noteVisible() {
  lastVisibleAt = Date.now();
}

export function markRealtimeEvent() {
  lastRealtimeEventAt = Date.now();
}

export function shouldRevalidateOnResume(): boolean {
  const now = Date.now();
  const awayDuration = getAwayDuration();
  if (awayDuration !== null) {
    return awayDuration >= RESUME_REVALIDATE_THRESHOLD_MS;
  }

  if (lastRevalidateAt === null) return false;
  return now - lastRevalidateAt >= RESUME_REVALIDATE_THRESHOLD_MS;
}

export function shouldResubscribeOnResume(): boolean {
  const now = Date.now();
  if (lastResubscribeAt && now - lastResubscribeAt < RESUBSCRIBE_THROTTLE_MS) {
    return false;
  }

  const awayDuration = getAwayDuration();
  const realtimeStale = !lastRealtimeEventAt || now - lastRealtimeEventAt >= REALTIME_STALE_THRESHOLD_MS;

  if (awayDuration !== null) {
    if (awayDuration >= RESUME_RESUBSCRIBE_THRESHOLD_MS) return true;
    if (awayDuration >= RESUME_REVALIDATE_THRESHOLD_MS && realtimeStale) return true;
    return false;
  }

  return realtimeStale;
}

export function markResubscribeAttempt() {
  lastResubscribeAt = Date.now();
}

async function showRecoveredMessageToasts(
  changedChannels: Array<{ channelId: string; diff: number }>,
): Promise<{ shownChannels: number; remainingChannels: number; remainingMessages: number }> {
  const isViewingMessages = window.location.pathname.includes('/messages');
  const activeChannelId = useMessagesStore.getState().activeChannelId;
  const toastCandidates = changedChannels.filter(
    ({ channelId }) => !(isViewingMessages && channelId === activeChannelId),
  );

  if (toastCandidates.length === 0) {
    return {
      shownChannels: 0,
      remainingChannels: 0,
      remainingMessages: 0,
    };
  }

  let shownChannels = 0;

  await Promise.all(
    toastCandidates
      .slice(0, MAX_RECOVERY_MESSAGE_TOASTS)
      .map(async ({ channelId, diff }) => {
        try {
          const result = await getChannelMessages(channelId, { limit: 1 });
          const latestMessage = result.messages[result.messages.length - 1];
          const cachedChannel = getCachedChannelMetadata(channelId);

          if (latestMessage) {
            showMessageNotification({
              senderName: latestMessage.user?.name || latestMessage.user?.email || 'Someone',
              senderAvatar: latestMessage.user?.avatar_url,
              content: latestMessage.content || '',
              channelName: cachedChannel?.channelName,
            });
          } else {
            showMessageNotification({
              senderName: 'Someone',
              content: diff === 1 ? 'New message' : `${diff} new messages`,
              channelName: cachedChannel?.channelName,
            });
          }

          shownChannels += 1;
        } catch (error) {
          console.warn('[Revalidation] Failed to fetch latest message for toast:', error);

          const cachedChannel = getCachedChannelMetadata(channelId);
          showMessageNotification({
            senderName: 'Someone',
            content: diff === 1 ? 'New message' : `${diff} new messages`,
            channelName: cachedChannel?.channelName,
          });
          shownChannels += 1;
        }
      }),
  );

  const remaining = toastCandidates.slice(MAX_RECOVERY_MESSAGE_TOASTS);
  return {
    shownChannels,
    remainingChannels: remaining.length,
    remainingMessages: remaining.reduce((sum, item) => sum + item.diff, 0),
  };
}

export function revalidateActiveData(reason: string) {
  const now = Date.now();
  console.log('[Revalidation]', reason);
  if (lastRevalidateAt && now - lastRevalidateAt < REVALIDATE_THROTTLE_MS) {
    return;
  }
  lastRevalidateAt = now;

  const messagesStore = useMessagesStore.getState();
  if (messagesStore.activeChannelId) {
    void messagesStore.fetchMessages(messagesStore.activeChannelId, true);
  }

  // Snapshot unread counts before fetch, then compare after to detect missed messages
  const oldUnreads = { ...messagesStore.unreadCounts };
  const messageAppIds = useWorkspaceStore.getState().workspaces.flatMap((workspace) =>
    workspace.apps
      .filter((app) => app.type === 'messages')
      .map((app) => app.id),
  );
  const unreadRefresh = messageAppIds.length > 0
    ? messagesStore.fetchAllUnreadCounts(messageAppIds)
    : messagesStore.fetchUnreadCounts();

  void unreadRefresh.then(async () => {
    const newUnreads = useMessagesStore.getState().unreadCounts;
    const changedChannels: Array<{ channelId: string; diff: number }> = [];

    for (const [channelId, count] of Object.entries(newUnreads)) {
      const diff = count - (oldUnreads[channelId] || 0);
      if (diff > 0) {
        changedChannels.push({ channelId, diff });
      }
    }

    if (changedChannels.length === 0) return;

    const isViewingMessages = window.location.pathname.includes('/messages');
    const activeChannelId = useMessagesStore.getState().activeChannelId;
    const toastCandidateCount = changedChannels.filter(
      ({ channelId }) => !(isViewingMessages && channelId === activeChannelId),
    ).length;

    if (toastCandidateCount === 0) return;

    playMessageNotification();

    const { shownChannels, remainingChannels, remainingMessages } =
      await showRecoveredMessageToasts(changedChannels);

    if (shownChannels === 0 || remainingChannels > 0) {
      const totalMessages = shownChannels === 0
        ? changedChannels.reduce((sum, item) => sum + item.diff, 0)
        : remainingMessages;
      const totalChannels = shownChannels === 0
        ? changedChannels.length
        : remainingChannels;
      const channelLabel = totalChannels === 1 ? 'channel' : 'channels';
      const messageLabel = totalMessages === 1 ? 'message' : 'messages';
      const prefix = shownChannels === 0 ? '' : 'More: ';

      toast(`${prefix}${totalMessages} new ${messageLabel} in ${totalChannels} ${channelLabel}`, {
        duration: 4000,
        position: 'top-right',
      });
    }
  });

  void messagesStore.fetchChannels(true);
  if (messagesStore.dms.length > 0) {
    void messagesStore.fetchDMs();
  }

  const filesStore = useFilesStore.getState();
  if (filesStore.workspaceAppId) {
    void filesStore.fetchDocuments(filesStore.currentFolderId, { background: true });
  }

  const notificationStore = useNotificationStore.getState();
  void notificationStore.fetchUnreadCount();
  const notificationStale =
    !notificationStore.lastFetched || now - notificationStore.lastFetched > NOTIFICATION_STALE_MS;
  if (notificationStore.isOpen || notificationStale) {
    void notificationStore.fetchNotifications();
  }

  const calendarStore = useCalendarStore.getState();
  if (calendarStore.events.length > 0 || calendarStore.lastFetched) {
    void calendarStore.refreshEvents();
  }
}
