import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { api } from '../api/client';

// ============================================================================
// Types
// ============================================================================

export interface Notification {
  id: string;
  user_id: string;
  workspace_id: string | null;
  type: string;
  title: string;
  body: string | null;
  resource_type: string | null;
  resource_id: string | null;
  group_key?: string | null;
  actor_id: string | null;
  read: boolean;
  seen: boolean;
  archived: boolean;
  data: Record<string, any>;
  created_at: string;
}

type NotificationSnapshot = Pick<Notification, 'id' | 'read' | 'archived'> & Partial<Notification>;

interface NotificationState {
  // Data
  notifications: Notification[];
  unreadCount: number;

  // UI state
  isLoading: boolean;
  hasMore: boolean;
  isOpen: boolean;

  // Cache
  lastFetched: number | null;

  // Actions
  fetchNotifications: (workspaceId?: string) => Promise<void>;
  fetchMore: (workspaceId?: string) => Promise<void>;
  fetchUnreadCount: (workspaceId?: string) => Promise<void>;
  markAsRead: (notificationId: string) => Promise<void>;
  markAllAsRead: (workspaceId?: string) => Promise<void>;
  archiveNotification: (notificationId: string) => Promise<void>;
  handleRealtimeInsert: (notification: Notification) => void;
  handleRealtimeUpdate: (
    notification: Notification,
    previousNotification?: NotificationSnapshot | null,
  ) => void;
  setOpen: (open: boolean) => void;
  toggleOpen: () => void;
  preload: () => Promise<void>;
  reset: () => void;
}

const STALE_TIME = 5 * 60 * 1000; // 5 minutes
const PAGE_SIZE = 30;

function sortNotifications(notifications: Notification[]): Notification[] {
  return [...notifications].sort(
    (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
  );
}

function isNotificationUnread(notification: Pick<Notification, 'read' | 'archived'>): boolean {
  return !notification.read && !notification.archived;
}

function upsertNotification(
  notifications: Notification[],
  notification: Notification,
): Notification[] {
  const existingIndex = notifications.findIndex((item) => item.id === notification.id);
  if (existingIndex === -1) {
    return sortNotifications([notification, ...notifications]);
  }

  const next = notifications.slice();
  next[existingIndex] = { ...next[existingIndex], ...notification };
  return sortNotifications(next);
}

export const useNotificationStore = create<NotificationState>()(
  persist(
    (set, get) => ({
      // Initial state
      notifications: [],
      unreadCount: 0,
      isLoading: false,
      hasMore: true,
      isOpen: false,
      lastFetched: null,

      fetchNotifications: async (workspaceId?: string) => {
        set({ isLoading: true });
        try {
          const params = new URLSearchParams();
          params.append('limit', String(PAGE_SIZE));
          params.append('offset', '0');
          if (workspaceId) params.append('workspace_id', workspaceId);

          const result = await api<{ notifications: Notification[]; count: number }>(
            `/notifications?${params.toString()}`
          );
          set({
            notifications: result.notifications,
            hasMore: result.count >= PAGE_SIZE,
            lastFetched: Date.now(),
            isLoading: false,
          });
        } catch (e) {
          console.error('Failed to fetch notifications:', e);
          set({ isLoading: false });
        }
      },

      fetchMore: async (workspaceId?: string) => {
        const { notifications, hasMore, isLoading } = get();
        if (!hasMore || isLoading) return;

        set({ isLoading: true });
        try {
          const params = new URLSearchParams();
          params.append('limit', String(PAGE_SIZE));
          params.append('offset', String(notifications.length));
          if (workspaceId) params.append('workspace_id', workspaceId);

          const result = await api<{ notifications: Notification[]; count: number }>(
            `/notifications?${params.toString()}`
          );
          set({
            notifications: [...notifications, ...result.notifications],
            hasMore: result.count >= PAGE_SIZE,
            isLoading: false,
          });
        } catch (e) {
          console.error('Failed to fetch more notifications:', e);
          set({ isLoading: false });
        }
      },

      fetchUnreadCount: async (workspaceId?: string) => {
        try {
          const params = workspaceId ? `?workspace_id=${workspaceId}` : '';
          const result = await api<{ count: number }>(`/notifications/unread-count${params}`);
          set({ unreadCount: result.count });
        } catch (e) {
          console.error('Failed to fetch unread count:', e);
        }
      },

      markAsRead: async (notificationId: string) => {
        const notif = get().notifications.find(n => n.id === notificationId);
        if (!notif || notif.read) return;

        // Optimistic update
        set((s) => ({
          notifications: s.notifications.map(n =>
            n.id === notificationId ? { ...n, read: true, seen: true } : n
          ),
          unreadCount: Math.max(0, s.unreadCount - 1),
        }));

        try {
          await api(`/notifications/${notificationId}/read`, { method: 'PATCH' });
        } catch (e) {
          // Revert on failure
          set((s) => ({
            notifications: s.notifications.map(n =>
              n.id === notificationId ? { ...n, read: false } : n
            ),
            unreadCount: s.unreadCount + 1,
          }));
          console.error('Failed to mark notification as read:', e);
        }
      },

      markAllAsRead: async (workspaceId?: string) => {
        const prevNotifications = get().notifications;
        const prevCount = get().unreadCount;

        // Optimistic update
        set((s) => ({
          notifications: s.notifications.map(n => ({ ...n, read: true, seen: true })),
          unreadCount: 0,
        }));

        try {
          await api('/notifications/mark-all-read', {
            method: 'POST',
            body: JSON.stringify({ workspace_id: workspaceId || null }),
          });
        } catch (e) {
          // Revert on failure
          set({ notifications: prevNotifications, unreadCount: prevCount });
          console.error('Failed to mark all as read:', e);
        }
      },

      archiveNotification: async (notificationId: string) => {
        const notif = get().notifications.find(n => n.id === notificationId);

        // Optimistic remove from list
        set((s) => ({
          notifications: s.notifications.filter(n => n.id !== notificationId),
          unreadCount: notif && !notif.read ? Math.max(0, s.unreadCount - 1) : s.unreadCount,
        }));

        try {
          await api(`/notifications/${notificationId}`, { method: 'DELETE' });
        } catch (e) {
          // Revert: re-insert
          if (notif) {
            set((s) => ({
              notifications: [notif, ...s.notifications].sort(
                (a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime()
              ),
              unreadCount: !notif.read ? s.unreadCount + 1 : s.unreadCount,
            }));
          }
          console.error('Failed to archive notification:', e);
        }
      },

      handleRealtimeInsert: (notification: Notification) => {
        set((s) => {
          const existing = s.notifications.find((item) => item.id === notification.id);
          const wasUnread = existing ? isNotificationUnread(existing) : false;
          const isUnread = isNotificationUnread(notification);

          return {
            notifications: upsertNotification(s.notifications, notification),
            unreadCount: Math.max(
              0,
              s.unreadCount + (existing ? Number(isUnread) - Number(wasUnread) : isUnread ? 1 : 0)
            ),
          };
        });
      },

      handleRealtimeUpdate: (
        notification: Notification,
        previousNotification?: NotificationSnapshot | null,
      ) => {
        set((s) => {
          const existing = s.notifications.find((item) => item.id === notification.id);
          const previousUnread = existing
            ? isNotificationUnread(existing)
            : previousNotification
              && typeof previousNotification.read === 'boolean'
              && typeof previousNotification.archived === 'boolean'
                ? isNotificationUnread(previousNotification)
                : null;
          const nextUnread = isNotificationUnread(notification);

          let unreadCount = s.unreadCount;
          if (previousUnread !== null && previousUnread !== nextUnread) {
            unreadCount += nextUnread ? 1 : -1;
          }

          return {
            notifications: notification.archived
              ? s.notifications.filter((item) => item.id !== notification.id)
              : upsertNotification(s.notifications, notification),
            unreadCount: Math.max(0, unreadCount),
          };
        });
      },

      setOpen: (open: boolean) => set({ isOpen: open }),
      toggleOpen: () => set((s) => ({ isOpen: !s.isOpen })),

      preload: async () => {
        const { lastFetched } = get();

        if (lastFetched && Date.now() - lastFetched < STALE_TIME) {
          return; // Fresh enough
        }

        // Fetch unread count (lightweight, for badge)
        await get().fetchUnreadCount();
        // Fetch recent notifications (for feed)
        await get().fetchNotifications();
      },

      reset: () => set({
        notifications: [],
        unreadCount: 0,
        isLoading: false,
        hasMore: true,
        isOpen: false,
        lastFetched: null,
      }),
    }),
    {
      name: 'notification-store',
      version: 1,
      partialize: (state) => ({
        unreadCount: state.unreadCount,
        lastFetched: state.lastFetched,
      }),
    }
  )
);
