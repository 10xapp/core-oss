import { useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { useNotificationStore, type Notification } from '../../stores/notificationStore';
import { navigateFromNotification } from '../../lib/notificationNavigation';
import NotificationPanel from './NotificationPanel';

export default function NotificationBell() {
  const { unreadCount, toggleOpen, isOpen, setOpen } = useNotificationStore();
  const navigate = useNavigate();

  const handleNavigate = useCallback((notification: Notification) => {
    setOpen(false);
    navigateFromNotification(notification, navigate);
  }, [navigate, setOpen]);

  return (
    <>
      <button
        data-notification-bell
        onClick={toggleOpen}
        title="Notifications"
        className={`relative w-10 h-10 flex items-center justify-center rounded-xl transition-all ${
          isOpen
            ? 'bg-bg-gray text-text-body'
            : 'text-text-tertiary hover:text-text-secondary hover:bg-bg-gray/50'
        }`}
      >
        {/* Bell icon */}
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
          <path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9" />
          <path d="M13.73 21a2 2 0 0 1-3.46 0" />
        </svg>

        {/* Unread badge */}
        {unreadCount > 0 && (
          <span className="absolute -top-0.5 -right-0.5 min-w-[18px] h-[18px] bg-red-500 rounded-full border-2 border-bg-main-sidebar flex items-center justify-center">
            <span className="text-[10px] font-semibold text-white leading-none px-0.5">
              {unreadCount > 99 ? '99+' : unreadCount}
            </span>
          </span>
        )}
      </button>

      <NotificationPanel onNavigate={handleNavigate} />
    </>
  );
}
