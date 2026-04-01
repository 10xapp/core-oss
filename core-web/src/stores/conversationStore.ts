import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { getConversations, deleteConversation, renameConversation, type Conversation } from '../api/client';

interface ConversationState {
  conversations: Conversation[];
  isLoading: boolean;
  isRevalidating: boolean;
  activeConversationId: string | null;
  lastFetched: number | null;
  /** True once the user has started or resumed a chat this browser session (not persisted). */
  hasChattedThisSession: boolean;
  fetchConversations: () => Promise<void>;
  addConversation: (conversation: Conversation) => void;
  removeConversation: (id: string) => Promise<void>;
  updateConversationTitle: (id: string, title: string) => Promise<void>;
  setActiveConversationId: (id: string | null) => void;
}

export const useConversationStore = create<ConversationState>()(
  persist(
    (set, get) => ({
      conversations: [],
      isLoading: false,
      isRevalidating: false,
      activeConversationId: null,
      lastFetched: null,
      hasChattedThisSession: false,

      fetchConversations: async () => {
        const { conversations: cachedConversations, lastFetched } = get();
        const hasCachedData = cachedConversations.length > 0 && lastFetched;

        // If we have cached data, show it immediately and revalidate in background
        if (hasCachedData) {
          set({ isRevalidating: true });
        } else {
          set({ isLoading: true });
        }

        try {
          const data = await getConversations();
          set({
            conversations: Array.isArray(data) ? data : [],
            lastFetched: Date.now(),
          });
        } catch (err) {
          console.log('Failed to fetch conversations:', err);
          // On error, keep cached data if available
        } finally {
          set({ isLoading: false, isRevalidating: false });
        }
      },

      addConversation: (conversation: Conversation) => {
        const { conversations } = get();
        // Add to beginning of list (most recent first)
        set({ conversations: [conversation, ...conversations], hasChattedThisSession: true });
      },

      removeConversation: async (id: string) => {
        try {
          await deleteConversation(id);
          const { conversations } = get();
          set({ conversations: conversations.filter((c) => c.id !== id) });
        } catch (err) {
          console.error('Failed to delete conversation:', err);
          throw err;
        }
      },

      updateConversationTitle: async (id: string, title: string) => {
        const { conversations } = get();
        set({
          conversations: conversations.map((c) =>
            c.id === id ? { ...c, title } : c
          ),
        });
        try {
          await renameConversation(id, title);
        } catch (err) {
          const oldTitle = conversations.find(c => c.id === id)?.title;
          set({
            conversations: get().conversations.map(c =>
              c.id === id ? { ...c, title: oldTitle ?? c.title } : c
            ),
          });
          console.error('Failed to rename conversation:', err);
        }
      },

      setActiveConversationId: (id: string | null) => {
        set({
          activeConversationId: id,
          // Mark session as active once the user opens any conversation
          ...(id ? { hasChattedThisSession: true } : {}),
        });
      },
    }),
    {
      name: 'core-conversation-storage-v1',
      partialize: (state) => ({
        conversations: state.conversations,
        activeConversationId: state.activeConversationId,
        lastFetched: state.lastFetched,
      }),
    }
  )
);
