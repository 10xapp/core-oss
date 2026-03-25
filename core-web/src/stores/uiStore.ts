import { create } from 'zustand';

interface UIState {
  isSidebarChatOpen: boolean;
  toggleSidebarChat: () => void;
  setSidebarChatOpen: (open: boolean) => void;
  isNotificationsPanelOpen: boolean;
  toggleNotificationsPanel: () => void;
  setNotificationsPanelOpen: (open: boolean) => void;
  isVersionHistoryOpen: boolean;
  toggleVersionHistory: () => void;
  setVersionHistoryOpen: (open: boolean) => void;
  isChatHistoryOpen: boolean;
  setChatHistoryOpen: (open: boolean) => void;
  toggleChatHistory: () => void;
}

export const useUIStore = create<UIState>((set) => ({
  isSidebarChatOpen: false,
  toggleSidebarChat: () => set((state) => ({
    isSidebarChatOpen: !state.isSidebarChatOpen,
    // Close notifications panel when opening chat
    isNotificationsPanelOpen: state.isSidebarChatOpen ? state.isNotificationsPanelOpen : false,
  })),
  setSidebarChatOpen: (open: boolean) => set({
    isSidebarChatOpen: open,
    // Close notifications panel when opening chat
    isNotificationsPanelOpen: open ? false : undefined,
  }),
  isNotificationsPanelOpen: false,
  toggleNotificationsPanel: () => set((state) => ({
    isNotificationsPanelOpen: !state.isNotificationsPanelOpen,
    // Close chat panel when opening notifications
    isSidebarChatOpen: state.isNotificationsPanelOpen ? state.isSidebarChatOpen : false,
  })),
  setNotificationsPanelOpen: (open: boolean) => set({
    isNotificationsPanelOpen: open,
    // Close chat panel when opening notifications
    isSidebarChatOpen: open ? false : undefined,
  }),
  isVersionHistoryOpen: false,
  toggleVersionHistory: () => set((state) => ({ isVersionHistoryOpen: !state.isVersionHistoryOpen })),
  setVersionHistoryOpen: (open: boolean) => set({ isVersionHistoryOpen: open }),
  isChatHistoryOpen: false,
  setChatHistoryOpen: (open: boolean) => set({ isChatHistoryOpen: open }),
  toggleChatHistory: () => set((state) => ({ isChatHistoryOpen: !state.isChatHistoryOpen })),
}));
