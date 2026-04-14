import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import {
  getAgentMemories,
  storeAgentMemory,
  searchAgentMemories,
  updateAgentMemory,
  deleteAgentMemory,
  getWorkspaceKnowledge,
  addWorkspaceKnowledge,
  searchWorkspaceKnowledge,
  verifyKnowledgeEntry,
  deleteKnowledgeEntry,
  type AgentMemory,
  type MemorySearchResult,
  type KnowledgeEntry,
  type KnowledgeSearchResult,
} from '../api/client';

interface AgentMemoryState {
  // Memory state
  memories: AgentMemory[];
  memorySearchResults: MemorySearchResult[];
  currentAgentId: string | null;
  isLoadingMemories: boolean;
  isSearchingMemories: boolean;

  // Knowledge state
  knowledgeEntries: KnowledgeEntry[];
  knowledgeSearchResults: KnowledgeSearchResult[];
  currentWorkspaceId: string | null;
  isLoadingKnowledge: boolean;
  isSearchingKnowledge: boolean;

  // Memory actions
  fetchMemories: (agentId: string, memoryType?: string) => Promise<void>;
  addMemory: (
    agentId: string,
    data: {
      workspace_id: string;
      memory_type: string;
      content: string;
      summary?: string;
      tags?: string[];
      is_shared?: boolean;
    },
  ) => Promise<AgentMemory>;
  searchMemories: (
    agentId: string,
    query: string,
    workspaceId: string,
  ) => Promise<void>;
  editMemory: (
    memoryId: string,
    data: { content?: string; summary?: string; tags?: string[] },
  ) => Promise<void>;
  removeMemory: (memoryId: string) => Promise<void>;
  clearMemorySearch: () => void;

  // Knowledge actions
  fetchKnowledge: (workspaceId: string, category?: string) => Promise<void>;
  addKnowledge: (
    workspaceId: string,
    data: { category: string; key: string; value: string; source_agent_id?: string },
  ) => Promise<KnowledgeEntry>;
  searchKnowledge: (workspaceId: string, query: string) => Promise<void>;
  verifyKnowledge: (knowledgeId: string) => Promise<void>;
  removeKnowledge: (knowledgeId: string) => Promise<void>;
  clearKnowledgeSearch: () => void;

  // Realtime handlers
  handleMemoryRealtimeInsert: (memory: AgentMemory) => void;
  handleMemoryRealtimeDelete: (memoryId: string) => void;
  handleKnowledgeRealtimeInsert: (entry: KnowledgeEntry) => void;
  handleKnowledgeRealtimeDelete: (entryId: string) => void;
}

export const useAgentMemoryStore = create<AgentMemoryState>()(
  persist(
    (set, get) => ({
      memories: [],
      memorySearchResults: [],
      currentAgentId: null,
      isLoadingMemories: false,
      isSearchingMemories: false,

      knowledgeEntries: [],
      knowledgeSearchResults: [],
      currentWorkspaceId: null,
      isLoadingKnowledge: false,
      isSearchingKnowledge: false,

      // ===== Memory actions =====

      fetchMemories: async (agentId: string, memoryType?: string) => {
        const { currentAgentId, memories: cached } = get();
        const isSameAgent = currentAgentId === agentId;

        if (!isSameAgent || cached.length === 0) {
          set({ isLoadingMemories: true, memories: [] });
        }
        set({ currentAgentId: agentId });

        try {
          const data = await getAgentMemories(agentId, memoryType);
          if (get().currentAgentId !== agentId) return;
          set({ memories: data.memories || [] });
        } catch (err) {
          console.error('Failed to fetch agent memories:', err);
        } finally {
          if (get().currentAgentId === agentId) {
            set({ isLoadingMemories: false });
          }
        }
      },

      addMemory: async (agentId, data) => {
        const memory = await storeAgentMemory(agentId, data);
        const { memories } = get();
        set({ memories: [memory, ...memories] });
        return memory;
      },

      searchMemories: async (agentId, query, workspaceId) => {
        set({ isSearchingMemories: true });
        try {
          const data = await searchAgentMemories(agentId, { query, workspace_id: workspaceId });
          set({ memorySearchResults: data.results || [] });
        } catch (err) {
          console.error('Failed to search memories:', err);
        } finally {
          set({ isSearchingMemories: false });
        }
      },

      editMemory: async (memoryId, data) => {
        const updated = await updateAgentMemory(memoryId, data);
        const { memories } = get();
        set({
          memories: memories.map((m) => (m.id === memoryId ? { ...m, ...updated } : m)),
        });
      },

      removeMemory: async (memoryId) => {
        await deleteAgentMemory(memoryId);
        const { memories } = get();
        set({ memories: memories.filter((m) => m.id !== memoryId) });
      },

      clearMemorySearch: () => set({ memorySearchResults: [] }),

      // ===== Knowledge actions =====

      fetchKnowledge: async (workspaceId: string, category?: string) => {
        const { currentWorkspaceId, knowledgeEntries: cached } = get();
        const isSameWorkspace = currentWorkspaceId === workspaceId;

        if (!isSameWorkspace || cached.length === 0) {
          set({ isLoadingKnowledge: true, knowledgeEntries: [] });
        }
        set({ currentWorkspaceId: workspaceId });

        try {
          const data = await getWorkspaceKnowledge(workspaceId, category);
          if (get().currentWorkspaceId !== workspaceId) return;
          set({ knowledgeEntries: data.entries || [] });
        } catch (err) {
          console.error('Failed to fetch knowledge:', err);
        } finally {
          if (get().currentWorkspaceId === workspaceId) {
            set({ isLoadingKnowledge: false });
          }
        }
      },

      addKnowledge: async (workspaceId, data) => {
        const entry = await addWorkspaceKnowledge(workspaceId, data);
        const { knowledgeEntries } = get();
        set({ knowledgeEntries: [entry, ...knowledgeEntries] });
        return entry;
      },

      searchKnowledge: async (workspaceId, query) => {
        set({ isSearchingKnowledge: true });
        try {
          const data = await searchWorkspaceKnowledge(workspaceId, { query });
          set({ knowledgeSearchResults: data.results || [] });
        } catch (err) {
          console.error('Failed to search knowledge:', err);
        } finally {
          set({ isSearchingKnowledge: false });
        }
      },

      verifyKnowledge: async (knowledgeId) => {
        const updated = await verifyKnowledgeEntry(knowledgeId);
        const { knowledgeEntries } = get();
        set({
          knowledgeEntries: knowledgeEntries.map((e) =>
            e.id === knowledgeId ? { ...e, ...updated } : e,
          ),
        });
      },

      removeKnowledge: async (knowledgeId) => {
        await deleteKnowledgeEntry(knowledgeId);
        const { knowledgeEntries } = get();
        set({ knowledgeEntries: knowledgeEntries.filter((e) => e.id !== knowledgeId) });
      },

      clearKnowledgeSearch: () => set({ knowledgeSearchResults: [] }),

      // ===== Realtime handlers =====

      handleMemoryRealtimeInsert: (memory: AgentMemory) => {
        const { memories } = get();
        if (memories.some((m) => m.id === memory.id)) return;
        set({ memories: [memory, ...memories] });
      },

      handleMemoryRealtimeDelete: (memoryId: string) => {
        const { memories } = get();
        set({ memories: memories.filter((m) => m.id !== memoryId) });
      },

      handleKnowledgeRealtimeInsert: (entry: KnowledgeEntry) => {
        const { knowledgeEntries } = get();
        if (knowledgeEntries.some((e) => e.id === entry.id)) return;
        set({ knowledgeEntries: [entry, ...knowledgeEntries] });
      },

      handleKnowledgeRealtimeDelete: (entryId: string) => {
        const { knowledgeEntries } = get();
        set({ knowledgeEntries: knowledgeEntries.filter((e) => e.id !== entryId) });
      },
    }),
    {
      name: 'core-agent-memory-storage-v1',
      partialize: (state) => ({
        currentAgentId: state.currentAgentId,
        currentWorkspaceId: state.currentWorkspaceId,
      }),
    },
  ),
);
