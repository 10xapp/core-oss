import { create } from 'zustand';

export interface DisplayMessage {
  id: string;
  role: 'user' | 'assistant';
  content: string;
}

interface SidebarChatState {
  messages: DisplayMessage[];
  activeConversationId: string | null;
  streamingConversationId: string | null;

  // Actions
  addMessage: (message: DisplayMessage) => void;
  setMessages: (messages: DisplayMessage[]) => void;
  setActiveConversationId: (id: string | null) => void;
  setStreamingConversationId: (id: string | null) => void;
  clearChat: () => void;
}

export const useSidebarChatStore = create<SidebarChatState>()(
  (set) => ({
    messages: [],
    activeConversationId: null,
    streamingConversationId: null,

    addMessage: (message) =>
      set((state) => ({ messages: [...state.messages, message] })),

    setMessages: (messages) => set({ messages }),

    setActiveConversationId: (id) => set({ activeConversationId: id }),

    setStreamingConversationId: (id) => set({ streamingConversationId: id }),

    clearChat: () =>
      set({
        messages: [],
        activeConversationId: null,
        streamingConversationId: null,
      }),
  })
);
