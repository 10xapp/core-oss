import { useState } from 'react';
import MiniAppSettingsModal from './MiniAppSettingsModal';
import { Bell, Settings, MessageCircle } from 'lucide-react';
import { Icon } from '../ui/Icon';
import { useUIStore } from '../../stores/uiStore';
import { useNotificationStore } from '../../stores/notificationStore';
import NotificationsPanel from '../NotificationsPanel/NotificationsPanel';

// Icon button styles
const iconBtn =
  'w-7 h-7 flex items-center justify-center rounded-md transition-all outline-none focus:outline-none';
const iconBtnActive = 'bg-gray-100 text-black';
const iconBtnInactive = 'text-text-icon hover:bg-gray-50';

/**
 * HeaderButtons - Inline buttons for AI chat, notifications, and settings
 * Use this component inside your view's header/filter bar row
 */
export function HeaderButtons({ onSettingsClick, settingsButtonRef }: { onSettingsClick?: () => void; settingsButtonRef?: React.RefObject<HTMLButtonElement | null> } = {}) {
  const [showSettingsModal, setShowSettingsModal] = useState(false);

  // UI toggles
  const isSidebarChatOpen = useUIStore((s) => s.isSidebarChatOpen);
  const toggleSidebarChat = useUIStore((s) => s.toggleSidebarChat);
  const isNotificationsPanelOpen = useUIStore((s) => s.isNotificationsPanelOpen);
  const toggleNotificationsPanel = useUIStore((s) => s.toggleNotificationsPanel);

  // Notification count
  const unreadCount = useNotificationStore((s) => s.unreadCount);

  return (
    <>
      <div className="flex items-center gap-2">
        {/* Notifications toggle + dropdown */}
        <div className="relative" data-notification-bell>
          <button
            onClick={toggleNotificationsPanel}
            title="Notifications"
            className={`${iconBtn} ${isNotificationsPanelOpen ? iconBtnActive : iconBtnInactive} relative`}
          >
            <Icon icon={Bell} size={20} active={isNotificationsPanelOpen} />
            {unreadCount > 0 && (
              <span className="absolute -top-0.5 -right-0.5 min-w-[14px] h-3.5 px-0.5 bg-red-500 text-white text-[9px] font-semibold rounded-full flex items-center justify-center">
                {unreadCount > 99 ? '99+' : unreadCount}
              </span>
            )}
          </button>
          <NotificationsPanel />
        </div>

        {/* Settings */}
        <button
          ref={settingsButtonRef}
          onClick={() => onSettingsClick ? onSettingsClick() : setShowSettingsModal(true)}
          title="App Settings"
          className={`${iconBtn} ${showSettingsModal || (onSettingsClick && false) ? iconBtnActive : iconBtnInactive}`}
        >
          <Icon icon={Settings} size={20} active={showSettingsModal || (onSettingsClick && false)} />
        </button>

        {/* AI Chat toggle */}
        <button
          onClick={toggleSidebarChat}
          title="AI Chat"
          className={`${iconBtn} ${isSidebarChatOpen ? iconBtnActive : iconBtnInactive}`}
        >
          <Icon icon={MessageCircle} size={20} active={isSidebarChatOpen} className="-scale-x-100" />
        </button>
      </div>

      {/* App Settings Modal */}
      <MiniAppSettingsModal
        isOpen={showSettingsModal}
        onClose={() => setShowSettingsModal(false)}
        appName="App"
      />
    </>
  );
}

// Default export for backwards compatibility - now just renders nothing
// Views should import and use HeaderButtons directly in their headers
export default function MiniAppHeader() {
  return null;
}
