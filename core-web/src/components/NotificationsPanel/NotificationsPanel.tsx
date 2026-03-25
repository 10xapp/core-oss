import { useRef, useCallback, useEffect } from 'react';
import { useNavigate } from 'react-router-dom';
import { AnimatePresence, motion } from 'motion/react';
import { useUIStore } from '../../stores/uiStore';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { useWorkspaceStore } from '../../stores/workspaceStore';
import { navigateFromNotification } from '../../lib/notificationNavigation';
import NotificationItem from '../Notifications/NotificationItem';

export default function NotificationsPanel() {
  const isNotificationsPanelOpen = useUIStore((s) => s.isNotificationsPanelOpen);
  const setNotificationsPanelOpen = useUIStore((s) => s.setNotificationsPanelOpen);
  const navigate = useNavigate();

  const {
    notifications,
    isLoading,
    hasMore,
    fetchNotifications,
    fetchMore,
    markAsRead,
    markAllAsRead,
    archiveNotification,
  } = useNotificationStore();

  const panelRef = useRef<HTMLDivElement>(null);
  const hasFetchedRef = useRef(false);

  // Fetch on first open
  useEffect(() => {
    if (isNotificationsPanelOpen && !hasFetchedRef.current) {
      hasFetchedRef.current = true;
      fetchNotifications();
    }
  }, [isNotificationsPanelOpen, fetchNotifications]);

  // Close on click outside
  useEffect(() => {
    if (!isNotificationsPanelOpen) return;
    function handleClickOutside(e: MouseEvent) {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        const target = e.target as HTMLElement;
        if (target.closest('[data-notification-bell]')) return;
        setNotificationsPanelOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isNotificationsPanelOpen, setNotificationsPanelOpen]);

  const unreadCount = notifications.filter(n => !n.read).length;

  const handleNavigate = useCallback((notification: Notification) => {
    setNotificationsPanelOpen(false);
    navigateFromNotification(notification, navigate);
  }, [navigate, setNotificationsPanelOpen]);

  const handleInviteResolved = useCallback(async (
    action: 'accept' | 'decline',
    _notification: Notification,
  ) => {
    if (action === 'accept') {
      await useWorkspaceStore.getState().fetchInitData();
    }
    await fetchNotifications();
  }, [fetchNotifications]);

  return (
    <AnimatePresence>
      {isNotificationsPanelOpen && (
        <motion.div
          ref={panelRef}
          initial={{ opacity: 0, y: -8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -8 }}
          transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
          className="absolute top-full right-0 mt-2 w-[380px] max-h-[70vh] bg-white rounded-xl shadow-xl border border-border-gray flex flex-col z-50 overflow-hidden"
        >
          {/* Header */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-border-gray">
            <h3 className="text-sm font-semibold text-text-body">Notifications</h3>
            {unreadCount > 0 && (
              <button
                onClick={() => markAllAsRead()}
                className="text-xs text-brand-primary hover:text-brand-primary/80 font-medium"
              >
                Mark all read
              </button>
            )}
          </div>

          {/* Notification list */}
          <div className="flex-1 overflow-y-auto">
            {isLoading && notifications.length === 0 ? (
              <div className="flex items-center justify-center py-8 text-text-tertiary text-sm">
                Loading...
              </div>
            ) : notifications.length === 0 ? (
              <div className="flex flex-col items-center justify-center py-12 px-4 text-center">
                <div className="w-12 h-12 rounded-full bg-bg-gray flex items-center justify-center mb-3">
                  <svg
                    className="w-5 h-5 text-text-tertiary"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                    strokeWidth={1.5}
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      d="M14.857 17.082a23.848 23.848 0 005.454-1.31A8.967 8.967 0 0118 9.75v-.7V9A6 6 0 006 9v.75a8.967 8.967 0 01-2.312 6.022c1.733.64 3.56 1.085 5.455 1.31m5.714 0a24.255 24.255 0 01-5.714 0m5.714 0a3 3 0 11-5.714 0"
                    />
                  </svg>
                </div>
                <p className="text-sm text-text-secondary font-medium">You're all caught up</p>
                <p className="text-xs text-text-tertiary mt-1">No new notifications</p>
              </div>
            ) : (
              <>
                {notifications.map((notification) => (
                  <div key={notification.id} className="group border-b border-border-gray/50 last:border-0">
                    <NotificationItem
                      notification={notification}
                      onRead={markAsRead}
                      onArchive={archiveNotification}
                      onClick={handleNavigate}
                      onInviteResolved={handleInviteResolved}
                    />
                  </div>
                ))}
                {hasMore && (
                  <button
                    onClick={() => fetchMore()}
                    disabled={isLoading}
                    className="w-full py-2 text-xs text-text-tertiary hover:text-text-secondary hover:bg-bg-gray transition-colors"
                  >
                    {isLoading ? 'Loading...' : 'Load more'}
                  </button>
                )}
              </>
            )}
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
