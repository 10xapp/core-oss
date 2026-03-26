import { useEffect, useRef } from 'react';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { useWorkspaceStore } from '../../stores/workspaceStore';
import NotificationItem from './NotificationItem';

interface NotificationPanelProps {
  onNavigate: (notification: Notification) => void;
}

export default function NotificationPanel({ onNavigate }: NotificationPanelProps) {
  const {
    notifications,
    isLoading,
    hasMore,
    isOpen,
    setOpen,
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
    if (isOpen && !hasFetchedRef.current) {
      hasFetchedRef.current = true;
      fetchNotifications();
    }
  }, [isOpen, fetchNotifications]);

  // Close on click outside
  useEffect(() => {
    if (!isOpen) return;
    function handleClickOutside(e: MouseEvent) {
      if (panelRef.current && !panelRef.current.contains(e.target as Node)) {
        // Don't close if clicking the bell button itself
        const target = e.target as HTMLElement;
        if (target.closest('[data-notification-bell]')) return;
        setOpen(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [isOpen, setOpen]);

  if (!isOpen) return null;

  const unreadCount = notifications.filter(n => !n.read).length;

  const handleInviteResolved = async (
    action: 'accept' | 'decline',
  ) => {
    if (action === 'accept') {
      await useWorkspaceStore.getState().fetchInitData();
    }
    await fetchNotifications();
  };

  return (
    <div
      ref={panelRef}
      className="fixed left-[72px] bottom-16 w-[380px] max-h-[70vh] bg-bg-main rounded-xl shadow-xl border border-border-gray flex flex-col z-50 overflow-hidden"
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
          <div className="flex items-center justify-center py-12 text-text-tertiary text-sm">
            Loading...
          </div>
        ) : notifications.length === 0 ? (
          <div className="flex flex-col items-center justify-center py-12 px-4 text-center">
            <div className="w-12 h-12 rounded-full bg-bg-gray flex items-center justify-center mb-3">
              <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
                <path d="M12 22c1.1 0 2-.9 2-2h-4c0 1.1.9 2 2 2zm6-6v-5c0-3.07-1.63-5.64-4.5-6.32V4c0-.83-.67-1.5-1.5-1.5s-1.5.67-1.5 1.5v.68C7.64 5.36 6 7.92 6 11v5l-2 2v1h16v-1l-2-2z" fill="#9ca3af"/>
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
                  onClick={onNavigate}
                  onInviteResolved={handleInviteResolved}
                />
              </div>
            ))}
            {hasMore && (
              <button
                onClick={() => fetchMore()}
                disabled={isLoading}
                className="w-full py-3 text-xs text-text-tertiary hover:text-text-secondary hover:bg-bg-gray transition-colors"
              >
                {isLoading ? 'Loading...' : 'Load more'}
              </button>
            )}
          </>
        )}
      </div>
    </div>
  );
}
