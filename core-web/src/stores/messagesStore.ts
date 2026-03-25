import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import {
  getChannels,
  getChannelMessages,
  sendChannelMessage,
  updateChannelMessage,
  deleteChannelMessage,
  createChannel,
  updateChannel,
  deleteChannel,
  addMessageReaction,
  removeMessageReaction,
  getThreadReplies,
  getUserDMs,
  getOrCreateDM,
  getUnreadCounts,
  markChannelRead,
  type Channel,
  type ChannelMessage,
  type ContentBlock,
  type DMChannel,
} from '../api/client';
import { useAuthStore } from './authStore';

// Track pending reaction operations to prevent duplicates
// Exported so realtime handlers can check before adding reactions
export const pendingReactions = new Set<string>();

// Maximum number of channels to keep in cache (to prevent localStorage bloat)
const MAX_CACHED_CHANNELS = 10;

// Flag to disable persistence after quota exceeded (prevents error spam)
let persistenceDisabled = false;

const IMAGE_PRELOAD_TIMEOUT_MS = 5000;

// Helper to strip large URL data from file blocks while preserving dimensions
// This allows cached messages to render with correct layout before images load
function stripFileBlocks(messages: ChannelMessage[]): ChannelMessage[] {
  return messages.map(msg => ({
    ...msg,
    blocks: msg.blocks.map(block => {
      if (block.type === 'file') {
        // Keep file block structure and dimensions, but remove large URL data
        const { url, preview, ...metadata } = block.data;
        return {
          ...block,
          data: {
            ...metadata,
            // Keep a marker that this is a stripped block
            _urlStripped: true,
          }
        };
      }
      return block;
    })
  }));
}

// Helper to limit cache size by keeping only the most recently accessed channels
function limitCacheSize(
  cache: Record<string, ChannelMessage[]>,
  activeChannelId: string | null,
  maxChannels: number = MAX_CACHED_CHANNELS
): Record<string, ChannelMessage[]> {
  const channelIds = Object.keys(cache);

  // If under limit, no need to prune
  if (channelIds.length <= maxChannels) {
    return cache;
  }

  // Always keep active channel
  const protectedChannels = new Set(activeChannelId ? [activeChannelId] : []);

  // Keep the most recent (maxChannels - 1) channels plus active channel
  const sortedChannelIds = channelIds
    .filter(id => !protectedChannels.has(id))
    .slice(-1 * (maxChannels - protectedChannels.size));

  const keptChannelIds = [...protectedChannels, ...sortedChannelIds];

  const prunedCache: Record<string, ChannelMessage[]> = {};
  for (const channelId of keptChannelIds) {
    if (cache[channelId]) {
      prunedCache[channelId] = cache[channelId];
    }
  }

  return prunedCache;
}

function getRenderableImageUrl(block: ContentBlock): string | null {
  if (block.type !== 'file') return null;

  const data = block.data as Record<string, unknown>;
  const mimeType = typeof data.mime_type === 'string' ? data.mime_type : '';
  if (!mimeType.startsWith('image/')) return null;

  const candidate =
    (typeof data.chat_url === 'string' && data.chat_url) ||
    (typeof data.url === 'string' && data.url) ||
    (typeof data.preview_url === 'string' && data.preview_url) ||
    (typeof data.full_url === 'string' && data.full_url) ||
    null;

  return candidate;
}

function preloadImage(url: string, timeoutMs: number): Promise<void> {
  if (typeof Image === 'undefined') return Promise.resolve();

  return new Promise((resolve) => {
    let settled = false;
    const img = new Image();

    const complete = () => {
      if (settled) return;
      settled = true;
      clearTimeout(timeoutId);
      img.onload = null;
      img.onerror = null;
      resolve();
    };

    const timeoutId = setTimeout(complete, timeoutMs);
    img.onload = complete;
    img.onerror = complete;
    img.src = url;

    // If browser already has this image cached, avoid waiting for events.
    if (img.complete) {
      complete();
    }
  });
}

async function preloadMessageImages(blocks: ContentBlock[]): Promise<void> {
  const urls = Array.from(
    new Set(
      blocks
        .map(getRenderableImageUrl)
        .filter((url): url is string => Boolean(url))
    )
  );

  if (urls.length === 0) return;

  await Promise.all(urls.map((url) => preloadImage(url, IMAGE_PRELOAD_TIMEOUT_MS)));
}

// Cache structure for channels and DMs per workspace
interface WorkspaceCache {
  [workspaceAppId: string]: {
    channels: Channel[];
    dms: DMChannel[];
    activeChannelId: string | null;
    lastFetched: number;
  };
}

interface MessagesState {
  // Data
  channels: Channel[];
  dms: DMChannel[];
  messages: ChannelMessage[];
  threadReplies: ChannelMessage[];
  unreadCounts: Record<string, number>;

  // Per-channel message cache for instant switching
  messagesCache: Record<string, ChannelMessage[]>;

  // Pagination state for infinite scroll
  hasMoreMessages: Record<string, boolean>;
  isLoadingOlderMessages: boolean;

  // Recently visited channels (for keeping them mounted)
  visitedChannelIds: string[];

  // Thread participants cache - maps messageId to array of unique users who replied
  threadParticipants: Record<string, Array<{
    id: string;
    avatar_url?: string;
    name?: string;
    email?: string;
  }>>;

  // Cached channels and DMs per workspace (persisted)
  workspaceCache: WorkspaceCache;

  // Selection state
  activeChannelId: string | null;
  activeThreadId: string | null;

  // Loading states
  isLoadingChannels: boolean;
  isLoadingDMs: boolean;
  isLoadingMessages: boolean;
  isLoadingThread: boolean;
  isSending: boolean;
  error: string | null;

  // Current workspace app context
  workspaceAppId: string | null;

  // Actions
  setWorkspaceAppId: (id: string | null, targetChannelId?: string) => void;
  setActiveChannel: (channelId: string | null) => void;
  setActiveThread: (messageId: string | null) => void;

  // Channel operations
  fetchChannels: (background?: boolean) => Promise<void>;
  addChannel: (name: string, description?: string, isPrivate?: boolean) => Promise<Channel>;
  editChannel: (channelId: string, updates: { name?: string; description?: string }) => Promise<void>;
  removeChannel: (channelId: string) => Promise<void>;

  // DM operations
  fetchDMs: () => Promise<void>;
  startDM: (participantIds: string[]) => Promise<DMChannel | null>;

  // Unread operations
  fetchUnreadCounts: () => Promise<void>;
  fetchAllUnreadCounts: (workspaceAppIds: string[]) => Promise<void>;
  preloadAllWorkspaceChannels: (workspaceAppIds: string[]) => Promise<void>;
  markAsRead: (channelId: string) => Promise<void>;

  // Message operations
  fetchMessages: (channelId?: string, background?: boolean) => Promise<void>;
  fetchOlderMessages: (channelId?: string) => Promise<void>;
  sendMessage: (blocks: ContentBlock[], threadParentId?: string) => Promise<ChannelMessage | null>;
  addOptimisticMessage: (blocks: ContentBlock[], threadParentId?: string) => string;
  finalizeOptimisticMessage: (tempId: string, blocks: ContentBlock[], threadParentId?: string) => Promise<ChannelMessage | null>;
  removeOptimisticMessage: (tempId: string, threadParentId?: string) => void;
  editMessage: (messageId: string, blocks: ContentBlock[]) => Promise<void>;
  removeMessage: (messageId: string) => Promise<void>;
  preloadAllChannels: () => Promise<void>;

  // Thread operations
  fetchThread: (messageId: string) => Promise<void>;
  fetchThreadParticipants: (messageId: string) => Promise<void>;
  clearThread: () => void;

  // Reaction operations
  addReaction: (messageId: string, emoji: string) => Promise<void>;
  removeReaction: (messageId: string, emoji: string) => Promise<void>;

  // Share
  shareMessage: (
    targetChannelId: string,
    originalMessage: ChannelMessage,
    originalChannelName: string,
    commentBlocks?: ContentBlock[],
  ) => Promise<ChannelMessage | null>;

  // Utility
  getChannelById: (channelId: string) => Channel | undefined;
  getDMById: (dmId: string) => DMChannel | undefined;
  clearError: () => void;
  preload: (appId: string) => void;
  preloadBackground: (appId: string) => Promise<void>;
}

function resolveValidActiveChannelId(
  preferredId: string | null | undefined,
  channels: Array<{ id: string }> = [],
  dms: Array<{ id: string }> = []
): string | null {
  if (preferredId && [...channels, ...dms].some((item) => item.id === preferredId)) {
    return preferredId;
  }
  return channels[0]?.id || dms[0]?.id || null;
}

export const useMessagesStore = create<MessagesState>()(
  persist(
    (set, get) => ({
      // Initial state
      channels: [],
      dms: [],
      messages: [],
      threadReplies: [],
      unreadCounts: {},
      messagesCache: {},
      hasMoreMessages: {},
      isLoadingOlderMessages: false,
      visitedChannelIds: [],
      threadParticipants: {},
      workspaceCache: {},
      activeChannelId: null,
      activeThreadId: null,
      isLoadingChannels: false,
      isLoadingDMs: false,
      isLoadingMessages: false,
      isLoadingThread: false,
      isSending: false,
      error: null,
      workspaceAppId: null,

      setWorkspaceAppId: (id, targetChannelId) => {
        const currentId = get().workspaceAppId;
        const STALE_TIME = 5 * 60 * 1000; // 5 minutes

        // Skip if same workspace already set (unless we have a specific target channel)
        if (id === currentId && !targetChannelId) return;

        if (!id) {
          set({ workspaceAppId: null, channels: [], dms: [], activeChannelId: null, messages: [] });
          return;
        }

        // Load from cache immediately if available
        const cache = get().workspaceCache[id];
        if (cache) {
          // Use targetChannelId if provided (e.g., from notification click), otherwise use cached
          const resolvedActiveChannelId = targetChannelId || resolveValidActiveChannelId(
            cache.activeChannelId,
            cache.channels || [],
            cache.dms || []
          );
          // Also load messages from messagesCache for instant display
          const cachedMessages = resolvedActiveChannelId ? get().messagesCache[resolvedActiveChannelId] || [] : [];
          const hasMessagesCache = cachedMessages.length > 0;

          // Atomic update: set new workspace data in a single call to prevent stale data flash
          set({
            workspaceAppId: id,
            channels: cache.channels,
            dms: cache.dms || [],
            activeChannelId: resolvedActiveChannelId,
            messages: cachedMessages,
            isLoadingChannels: false,
            isLoadingDMs: false,
            isLoadingMessages: !hasMessagesCache,
          });
          // Fetch messages in background (don't show loading spinner if we have cache)
          if (resolvedActiveChannelId) {
            get().fetchMessages(resolvedActiveChannelId, hasMessagesCache);
            // Don't mark as read here - only mark as read when user explicitly views
            // the channel in MessagesView (handled by setActiveChannel or MessagesView)
          }
          // Only revalidate in background if cache is stale
          const isStale = !cache.lastFetched || Date.now() - cache.lastFetched > STALE_TIME;
          if (isStale) {
            // Run all fetches in parallel for faster revalidation
            setTimeout(() => {
              Promise.all([
                get().fetchChannels(true),
                get().fetchDMs(),
                get().fetchUnreadCounts(),
              ]);
            }, 0);
          } else {
            // Cache is fresh - only fetch unread counts (they change frequently)
            setTimeout(() => {
              get().fetchUnreadCounts();
            }, 0);
          }
        } else {
          set({ workspaceAppId: id, channels: [], dms: [], activeChannelId: targetChannelId || null });
          // Fetch channels, DMs, and unread counts for new workspace - run in parallel
          setTimeout(() => {
            Promise.all([
              get().fetchChannels(),
              get().fetchDMs(),
              get().fetchUnreadCounts(),
            ]);
          }, 0);
        }
      },

      setActiveChannel: (channelId) => {
        const { workspaceAppId, workspaceCache, activeChannelId: currentChannelId, messagesCache, visitedChannelIds } = get();

        // Skip if same channel already selected
        if (channelId === currentChannelId) return;

        // Use cached messages if available for instant display
        const cachedMessages = channelId ? messagesCache[channelId] || [] : [];
        const hasCache = cachedMessages.length > 0;

        // Track visited channels (keep last 5 for mounted state)
        let newVisitedIds = visitedChannelIds;
        if (channelId) {
          // Remove if already in list, then add to front
          newVisitedIds = [channelId, ...visitedChannelIds.filter(id => id !== channelId)].slice(0, 5);
        }

        set({
          activeChannelId: channelId,
          activeThreadId: null,
          messages: cachedMessages,
          threadReplies: [],
          visitedChannelIds: newVisitedIds,
        });

        // Update cache with active channel selection
        if (workspaceAppId && workspaceCache[workspaceAppId]) {
          set({
            workspaceCache: {
              ...workspaceCache,
              [workspaceAppId]: {
                ...workspaceCache[workspaceAppId],
                activeChannelId: channelId,
              },
            },
          });
        }

        if (channelId) {
          // Fetch messages in background (don't show loading if we have cache)
          get().fetchMessages(channelId, hasCache);
        }
      },

  setActiveThread: (messageId) => {
    set({ activeThreadId: messageId, threadReplies: [] });
    if (messageId) {
      get().fetchThread(messageId);
    }
  },

  fetchChannels: async (background = false) => {
    const { workspaceAppId, channels: currentChannels } = get();
    if (!workspaceAppId) return;

    // Only show loading state if not background fetch and no cached data
    if (!background && currentChannels.length === 0) {
      set({ isLoadingChannels: true, error: null });
    }

    try {
      const result = await getChannels(workspaceAppId);

      // After await, user may have switched workspaces — check before updating
      const state = get();
      const stillOnSameWorkspace = state.workspaceAppId === workspaceAppId;
      const existingCache = state.workspaceCache[workspaceAppId] || {};
      const resolvedActiveChannelId = resolveValidActiveChannelId(
        existingCache.activeChannelId || state.activeChannelId,
        result.channels,
        existingCache.dms || state.dms || []
      );
      const newCache = {
        ...state.workspaceCache,
        [workspaceAppId]: {
          ...existingCache,
          channels: result.channels,
          activeChannelId: resolvedActiveChannelId,
          lastFetched: Date.now(),
        },
      };

      if (stillOnSameWorkspace) {
        set({
          channels: result.channels,
          workspaceCache: newCache,
          isLoadingChannels: false,
        });

        // Auto-select a valid channel if selection is missing or stale
        if (resolvedActiveChannelId && state.activeChannelId !== resolvedActiveChannelId) {
          get().setActiveChannel(resolvedActiveChannelId);
        }
      } else {
        // Only update the cache, don't touch global channels/activeChannelId
        set({ workspaceCache: newCache });
      }
    } catch (err) {
      // Only set error if not a background fetch
      if (!background) {
        set({
          error: err instanceof Error ? err.message : 'Failed to fetch channels',
          isLoadingChannels: false,
        });
      } else {
        set({ isLoadingChannels: false });
      }
    }
  },

  addChannel: async (name, description, isPrivate = false) => {
    const { workspaceAppId } = get();
    if (!workspaceAppId) throw new Error('No workspace app selected');

    set({ error: null });
    try {
      const result = await createChannel(workspaceAppId, { name, description, is_private: isPrivate });
      const channel = result.channel;

      // Update inside set() to avoid race with realtime subscription
      set((state) => {
        // Skip if realtime already added this channel
        if (state.channels.some(c => c.id === channel.id)) {
          return state;
        }
        const newChannels = [...state.channels, channel];
        return {
          channels: newChannels,
          workspaceCache: {
            ...state.workspaceCache,
            [workspaceAppId]: {
              ...state.workspaceCache[workspaceAppId],
              channels: newChannels,
              lastFetched: Date.now(),
            },
          },
        };
      });
      return channel;
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to create channel';
      set({ error: message });
      throw err;
    }
  },

  editChannel: async (channelId, updates) => {
    set({ error: null });
    try {
      const result = await updateChannel(channelId, updates);
      set((state) => {
        const updatedWorkspaceCache = Object.fromEntries(
          Object.entries(state.workspaceCache).map(([appId, cache]) => [
            appId,
            {
              ...cache,
              channels: (cache.channels || []).map((c) => (c.id === channelId ? result.channel : c)),
            },
          ])
        ) as WorkspaceCache;

        return {
          channels: state.channels.map((c) => (c.id === channelId ? result.channel : c)),
          workspaceCache: updatedWorkspaceCache,
        };
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to update channel' });
      throw err;
    }
  },

  removeChannel: async (channelId) => {
    set({ error: null });
    try {
      await deleteChannel(channelId);
      const state = get();
      const updatedWorkspaceCache = Object.fromEntries(
        Object.entries(state.workspaceCache).map(([appId, cache]) => {
          const nextChannels = (cache.channels || []).filter((c) => c.id !== channelId);
          return [
            appId,
            {
              ...cache,
              channels: nextChannels,
              activeChannelId: resolveValidActiveChannelId(
                cache.activeChannelId === channelId ? null : cache.activeChannelId,
                nextChannels,
                cache.dms || []
              ),
            },
          ];
        })
      ) as WorkspaceCache;

      set({
        channels: state.channels.filter((c) => c.id !== channelId),
        visitedChannelIds: state.visitedChannelIds.filter((id) => id !== channelId),
        workspaceCache: updatedWorkspaceCache,
      });

      // If we deleted the active channel, switch to another
      if (state.activeChannelId === channelId) {
        const remaining = state.channels.filter((c) => c.id !== channelId);
        state.setActiveChannel(resolveValidActiveChannelId(null, remaining, state.dms));
      }
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to delete channel' });
      throw err;
    }
  },

  // DM operations
  fetchDMs: async () => {
    const { workspaceAppId, dms: currentDMs } = get();
    if (!workspaceAppId) return;

    // Only show loading if no cached DMs
    if (currentDMs.length === 0) {
      set({ isLoadingDMs: true });
    }

    try {
      const result = await getUserDMs(workspaceAppId);

      // After await, check if still on the same workspace
      const state = get();
      const stillOnSameWorkspace = state.workspaceAppId === workspaceAppId;
      const existingCache = state.workspaceCache[workspaceAppId] || {};
      const newCache = {
        ...state.workspaceCache,
        [workspaceAppId]: {
          ...existingCache,
          dms: result.dms,
        },
      };

      if (stillOnSameWorkspace) {
        set({
          dms: result.dms,
          isLoadingDMs: false,
          workspaceCache: newCache,
        });
      } else {
        // Only update the cache, don't touch global DMs state
        set({ workspaceCache: newCache });
      }
    } catch (err) {
      console.error('Failed to fetch DMs:', err);
      set({ isLoadingDMs: false });
    }
  },

  startDM: async (participantIds) => {
    const { workspaceAppId } = get();
    if (!workspaceAppId) return null;

    set({ error: null });
    try {
      const result = await getOrCreateDM(workspaceAppId, participantIds);
      const dm = result.dm;

      // Add to DMs list if not already there
      set((state) => {
        const exists = state.dms.some((d) => d.id === dm.id);
        if (exists) return state;
        return { dms: [dm, ...state.dms] };
      });

      // Select the DM channel
      get().setActiveChannel(dm.id);

      return dm;
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to start DM' });
      return null;
    }
  },

  // Unread operations
  fetchUnreadCounts: async () => {
    const { workspaceAppId } = get();
    if (!workspaceAppId) return;

    try {
      const result = await getUnreadCounts(workspaceAppId);
      // Merge with existing counts (don't replace - preserves other workspaces' counts)
      set((state) => ({
        unreadCounts: { ...state.unreadCounts, ...result.unread_counts },
      }));
    } catch (err) {
      console.error('Failed to fetch unread counts:', err);
    }
  },

  fetchAllUnreadCounts: async (workspaceAppIds) => {
    if (workspaceAppIds.length === 0) return;

    try {
      // Fetch unread counts for all workspace apps in parallel
      const results = await Promise.all(
        workspaceAppIds.map((appId) => getUnreadCounts(appId).catch(() => ({ unread_counts: {} })))
      );

      // Merge all unread counts into a single object
      const allCounts: Record<string, number> = {};
      results.forEach((result) => {
        Object.assign(allCounts, result.unread_counts);
      });

      set((state) => ({
        unreadCounts: { ...state.unreadCounts, ...allCounts },
      }));
    } catch (err) {
      console.error('Failed to fetch all unread counts:', err);
    }
  },

  preloadAllWorkspaceChannels: async (workspaceAppIds) => {
    if (workspaceAppIds.length === 0) return;

    try {
      // Fetch channels and DMs for all workspace apps in parallel
      const results = await Promise.all(
        workspaceAppIds.map(async (appId) => {
          try {
            const [channelsResult, dmsResult] = await Promise.all([
              getChannels(appId),
              getUserDMs(appId),
            ]);
            return { appId, channels: channelsResult.channels, dms: dmsResult.dms };
          } catch {
            return { appId, channels: [], dms: [] };
          }
        })
      );

      // Update workspace cache with fetched channels/DMs (preserve existing cache data)
      set((state) => {
        const newCache = { ...state.workspaceCache };
        results.forEach(({ appId, channels, dms }) => {
          // Only update if we don't have cached data, or if existing cache is stale
          const existing = newCache[appId];
          if (!existing || !existing.channels || existing.channels.length === 0) {
            const resolvedActiveChannelId = resolveValidActiveChannelId(
              existing?.activeChannelId,
              channels,
              dms
            );
            newCache[appId] = {
              ...existing,
              channels,
              dms,
              activeChannelId: resolvedActiveChannelId,
              lastFetched: Date.now(),
            };
          }
        });
        return { workspaceCache: newCache };
      });
    } catch (err) {
      console.error('Failed to preload workspace channels:', err);
    }
  },

  markAsRead: async (channelId) => {
    try {
      await markChannelRead(channelId);
      // Clear the unread count for this channel locally
      set((state) => {
        const newCounts = { ...state.unreadCounts };
        delete newCounts[channelId];
        return { unreadCounts: newCounts };
      });
    } catch (err) {
      console.error('Failed to mark channel as read:', err);
    }
  },

  fetchMessages: async (channelId, background = false) => {
    const targetChannelId = channelId || get().activeChannelId;
    if (!targetChannelId) return;

    // Only show loading state if not a background fetch
    if (!background) {
      set({ isLoadingMessages: true, error: null });
    }

    try {
      const result = await getChannelMessages(targetChannelId, { limit: 50 });

      // Merge fetched messages with any newer messages in cache (from realtime)
      // This prevents losing messages that arrived via realtime but aren't in API response yet
      // IMPORTANT: Read cache inside set() to get latest value and avoid race conditions
      const fetchedIds = new Set(result.messages.map((m) => m.id));
      const newestFetchedTime = result.messages.length > 0
        ? new Date(result.messages[result.messages.length - 1].created_at).getTime()
        : 0;

      // Update cache and messages atomically, reading latest cache state
      set((state) => {
        const existingCache = state.messagesCache[targetChannelId] || [];

        // Keep any cached messages that are newer than the newest fetched message
        // and aren't already in the fetched results (i.e., realtime messages not yet in API)
        const realtimeMessages = existingCache.filter(
          (m) => !fetchedIds.has(m.id) && new Date(m.created_at).getTime() > newestFetchedTime
        );

        const mergedMessages = [...result.messages, ...realtimeMessages];

        // Only update displayed messages if this is still the active channel
        const shouldUpdateMessages = targetChannelId === state.activeChannelId;

        // For inactive channels, strip file blocks immediately to save space
        const messagesToCache = shouldUpdateMessages
          ? mergedMessages
          : stripFileBlocks(mergedMessages);

        return {
          messagesCache: {
            ...state.messagesCache,
            [targetChannelId]: messagesToCache,
          },
          hasMoreMessages: {
            ...state.hasMoreMessages,
            [targetChannelId]: result.has_more ?? result.messages.length === 50,
          },
          ...(shouldUpdateMessages ? { messages: mergedMessages } : {}),
          isLoadingMessages: shouldUpdateMessages || !background ? false : state.isLoadingMessages,
        };
      });
    } catch (err) {
      if (!background) {
        set({
          error: err instanceof Error ? err.message : 'Failed to fetch messages',
          isLoadingMessages: false,
        });
      }
    }
  },

  fetchOlderMessages: async (channelId) => {
    const targetChannelId = channelId || get().activeChannelId;
    if (!targetChannelId) return;

    const { hasMoreMessages, isLoadingOlderMessages } = get();
    if (!hasMoreMessages[targetChannelId] || isLoadingOlderMessages) return;

    set({ isLoadingOlderMessages: true });

    try {
      // Get the oldest message currently loaded
      const currentMessages = get().messagesCache[targetChannelId] || get().messages;
      const oldestMessage = currentMessages[0];
      if (!oldestMessage) {
        set({ isLoadingOlderMessages: false });
        return;
      }

      const result = await getChannelMessages(targetChannelId, {
        limit: 50,
        beforeId: oldestMessage.id,
      });

      set((state) => {
        const existing = targetChannelId === state.activeChannelId
          ? state.messages
          : (state.messagesCache[targetChannelId] || []);

        // Deduplicate
        const existingIds = new Set(existing.map((m) => m.id));
        const newMessages = result.messages.filter((m) => !existingIds.has(m.id));

        const mergedMessages = [...newMessages, ...existing];
        const shouldUpdateMessages = targetChannelId === state.activeChannelId;

        return {
          messagesCache: {
            ...state.messagesCache,
            [targetChannelId]: shouldUpdateMessages ? mergedMessages : stripFileBlocks(mergedMessages),
          },
          hasMoreMessages: {
            ...state.hasMoreMessages,
            [targetChannelId]: result.has_more ?? result.messages.length === 50,
          },
          ...(shouldUpdateMessages ? { messages: mergedMessages } : {}),
          isLoadingOlderMessages: false,
        };
      });
    } catch (err) {
      console.error('Failed to fetch older messages:', err);
      set({ isLoadingOlderMessages: false });
    }
  },

  sendMessage: async (blocks, threadParentId) => {
    const { activeChannelId } = get();
    if (!activeChannelId) return null;

    // Build optimistic message for instant display
    const authState = useAuthStore.getState();
    const currentUser = authState.user;
    const userProfile = authState.userProfile;
    const tempId = `temp-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    const optimisticMessage: ChannelMessage = {
      id: tempId,
      channel_id: activeChannelId,
      user_id: currentUser?.id || '',
      content: '',
      blocks,
      is_edited: false,
      thread_parent_id: threadParentId,
      reply_count: 0,
      created_at: new Date().toISOString(),
      user: currentUser ? {
        id: currentUser.id,
        email: currentUser.email || '',
        name: userProfile?.name || currentUser.user_metadata?.name,
        avatar_url: userProfile?.avatar_url || currentUser.user_metadata?.avatar_url,
      } : undefined,
      reactions: [],
    };

    // Add optimistic message to state immediately
    if (threadParentId) {
      set((state) => ({
        isSending: true,
        error: null,
        threadReplies: [...state.threadReplies, optimisticMessage],
        messages: state.messages.map((m) =>
          m.id === threadParentId ? { ...m, reply_count: m.reply_count + 1 } : m
        ),
        messagesCache: {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).map((m) =>
            m.id === threadParentId ? { ...m, reply_count: m.reply_count + 1 } : m
          ),
        },
      }));
    } else {
      set((state) => ({
        isSending: true,
        error: null,
        messages: [...state.messages, optimisticMessage],
        messagesCache: {
          ...state.messagesCache,
          [activeChannelId]: [...(state.messagesCache[activeChannelId] || []), optimisticMessage],
        },
      }));
    }

    try {
      const result = await sendChannelMessage(activeChannelId, blocks, threadParentId);
      const message = result.message;

      // Replace optimistic message with real server message
      if (threadParentId) {
        set((state) => ({
          isSending: false,
          threadReplies: state.threadReplies.map((m) => m.id === tempId ? message : m),
        }));
      } else {
        set((state) => ({
          isSending: false,
          messages: state.messages.map((m) => m.id === tempId ? message : m),
          messagesCache: {
            ...state.messagesCache,
            [activeChannelId]: (state.messagesCache[activeChannelId] || []).map((m) =>
              m.id === tempId ? message : m
            ),
          },
        }));
      }

      return message;
    } catch (err) {
      // Remove optimistic message on failure
      if (threadParentId) {
        set((state) => ({
          error: err instanceof Error ? err.message : 'Failed to send message',
          isSending: false,
          threadReplies: state.threadReplies.filter((m) => m.id !== tempId),
          messages: state.messages.map((m) =>
            m.id === threadParentId ? { ...m, reply_count: Math.max(0, m.reply_count - 1) } : m
          ),
        }));
      } else {
        set((state) => ({
          error: err instanceof Error ? err.message : 'Failed to send message',
          isSending: false,
          messages: state.messages.filter((m) => m.id !== tempId),
          messagesCache: {
            ...state.messagesCache,
            [activeChannelId]: (state.messagesCache[activeChannelId] || []).filter((m) => m.id !== tempId),
          },
        }));
      }
      return null;
    }
  },

  // Add an optimistic message to state (for file uploads that need background processing)
  addOptimisticMessage: (blocks, threadParentId) => {
    const { activeChannelId } = get();
    if (!activeChannelId) return '';

    const authState = useAuthStore.getState();
    const currentUser = authState.user;
    const userProfile = authState.userProfile;
    const tempId = `temp-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    const optimisticMessage: ChannelMessage = {
      id: tempId,
      channel_id: activeChannelId,
      user_id: currentUser?.id || '',
      content: '',
      blocks,
      is_edited: false,
      thread_parent_id: threadParentId,
      reply_count: 0,
      created_at: new Date().toISOString(),
      user: currentUser ? {
        id: currentUser.id,
        email: currentUser.email || '',
        name: userProfile?.name || currentUser.user_metadata?.name,
        avatar_url: userProfile?.avatar_url || currentUser.user_metadata?.avatar_url,
      } : undefined,
      reactions: [],
    };

    if (threadParentId) {
      set((state) => ({
        threadReplies: [...state.threadReplies, optimisticMessage],
        messages: state.messages.map((m) =>
          m.id === threadParentId ? { ...m, reply_count: m.reply_count + 1 } : m
        ),
        messagesCache: {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).map((m) =>
            m.id === threadParentId ? { ...m, reply_count: m.reply_count + 1 } : m
          ),
        },
      }));
    } else {
      set((state) => ({
        messages: [...state.messages, optimisticMessage],
        messagesCache: {
          ...state.messagesCache,
          [activeChannelId]: [...(state.messagesCache[activeChannelId] || []), optimisticMessage],
        },
      }));
    }

    return tempId;
  },

  // Finalize an optimistic message by sending to API and replacing with real message
  finalizeOptimisticMessage: async (tempId, blocks, threadParentId) => {
    const { activeChannelId } = get();
    if (!activeChannelId) return null;

    try {
      const result = await sendChannelMessage(activeChannelId, blocks, threadParentId);
      const message = result.message;
      await preloadMessageImages(message.blocks);

      // Replace optimistic message with real server message
      if (threadParentId) {
        set((state) => ({
          threadReplies: state.threadReplies.map((m) => m.id === tempId ? message : m),
        }));
      } else {
        set((state) => ({
          messages: state.messages.map((m) => m.id === tempId ? message : m),
          messagesCache: {
            ...state.messagesCache,
            [activeChannelId]: (state.messagesCache[activeChannelId] || []).map((m) =>
              m.id === tempId ? message : m
            ),
          },
        }));
      }

      return message;
    } catch (err) {
      // Remove optimistic message on failure
      get().removeOptimisticMessage(tempId, threadParentId);
      set({
        error: err instanceof Error ? err.message : 'Failed to send message',
      });
      console.error('Failed to send message:', err);
      return null;
    }
  },

  // Remove an optimistic message (on failure)
  removeOptimisticMessage: (tempId, threadParentId) => {
    const { activeChannelId } = get();
    if (!activeChannelId) return;

    if (threadParentId) {
      set((state) => ({
        threadReplies: state.threadReplies.filter((m) => m.id !== tempId),
        messages: state.messages.map((m) =>
          m.id === threadParentId ? { ...m, reply_count: Math.max(0, m.reply_count - 1) } : m
        ),
        messagesCache: {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).map((m) =>
            m.id === threadParentId ? { ...m, reply_count: Math.max(0, m.reply_count - 1) } : m
          ),
        },
      }));
    } else {
      set((state) => ({
        messages: state.messages.filter((m) => m.id !== tempId),
        messagesCache: {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).filter((m) => m.id !== tempId),
        },
      }));
    }
  },

  editMessage: async (messageId, blocks) => {
    const { activeChannelId } = get();
    set({ error: null });
    try {
      const result = await updateChannelMessage(messageId, blocks);
      const updatedMessage = result.message;

      set((state) => {
        // Update messages cache for the active channel
        const updatedCache = activeChannelId ? {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).map((m) =>
            m.id === messageId ? updatedMessage : m
          ),
        } : state.messagesCache;

        return {
          messages: state.messages.map((m) => (m.id === messageId ? updatedMessage : m)),
          threadReplies: state.threadReplies.map((m) => (m.id === messageId ? updatedMessage : m)),
          messagesCache: updatedCache,
        };
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to edit message' });
      throw err;
    }
  },

  removeMessage: async (messageId) => {
    const { activeChannelId } = get();
    set({ error: null });
    try {
      await deleteChannelMessage(messageId);
      set((state) => {
        // Update messages cache for the active channel
        const updatedCache = activeChannelId ? {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).filter((m) => m.id !== messageId),
        } : state.messagesCache;

        return {
          messages: state.messages.filter((m) => m.id !== messageId),
          threadReplies: state.threadReplies.filter((m) => m.id !== messageId),
          messagesCache: updatedCache,
        };
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to delete message' });
      throw err;
    }
  },

  fetchThread: async (messageId) => {
    set({ isLoadingThread: true, error: null });
    try {
      const result = await getThreadReplies(messageId);
      const { activeChannelId } = get();

      // Update thread replies and sync parent message's reply_count
      // This self-heals stale cache data when user opens a thread
      set((state) => {
        const syncReplyCount = (m: ChannelMessage) =>
          m.id === messageId ? { ...m, reply_count: result.count } : m;

        const updatedCache = activeChannelId && state.messagesCache[activeChannelId]
          ? {
              ...state.messagesCache,
              [activeChannelId]: state.messagesCache[activeChannelId].map(syncReplyCount),
            }
          : state.messagesCache;

        return {
          threadReplies: result.replies,
          isLoadingThread: false,
          messages: state.messages.map(syncReplyCount),
          messagesCache: updatedCache,
        };
      });
    } catch (err) {
      set({
        error: err instanceof Error ? err.message : 'Failed to fetch thread',
        isLoadingThread: false,
      });
    }
  },

  fetchThreadParticipants: async (messageId) => {
    // Check if already cached
    const cached = get().threadParticipants[messageId];
    if (cached) return;

    try {
      // Fetch thread replies to extract unique participants
      const result = await getThreadReplies(messageId, { limit: 50 });
      const replies = result.replies;
      const { activeChannelId } = get();

      // Extract unique users (up to 4)
      const seenIds = new Set<string>();
      const participants: Array<{ id: string; avatar_url?: string; name?: string; email?: string }> = [];

      for (const reply of replies) {
        if (reply.user && !seenIds.has(reply.user.id)) {
          seenIds.add(reply.user.id);
          participants.push({
            id: reply.user.id,
            avatar_url: reply.user.avatar_url,
            name: reply.user.name,
            email: reply.user.email,
          });

          // Limit to 4 participants
          if (participants.length >= 4) break;
        }
      }

      // Cache the participants and sync reply_count on parent message
      set((state) => {
        const syncReplyCount = (m: ChannelMessage) =>
          m.id === messageId ? { ...m, reply_count: result.count } : m;

        const updatedCache = activeChannelId && state.messagesCache[activeChannelId]
          ? {
              ...state.messagesCache,
              [activeChannelId]: state.messagesCache[activeChannelId].map(syncReplyCount),
            }
          : state.messagesCache;

        return {
          threadParticipants: {
            ...state.threadParticipants,
            [messageId]: participants,
          },
          messages: state.messages.map(syncReplyCount),
          messagesCache: updatedCache,
        };
      });
    } catch (err) {
      console.error('Failed to fetch thread participants:', err);
    }
  },

  clearThread: () => {
    set({ activeThreadId: null, threadReplies: [] });
  },

  addReaction: async (messageId, emoji) => {
    // Prevent duplicate calls for the same message+emoji
    const reactionKey = `${messageId}:${emoji}`;
    if (pendingReactions.has(reactionKey)) {
      return;
    }
    pendingReactions.add(reactionKey);

    const { activeChannelId } = get();
    set({ error: null });
    try {
      const result = await addMessageReaction(messageId, emoji);
      const reaction = result.reaction;

      // Helper to add reaction if not already present
      const addReactionToMessage = (m: ChannelMessage) => {
        if (m.id !== messageId) return m;
        // Check by ID first, then by emoji+user_id as fallback
        const exists = (m.reactions || []).some(
          (r) => r.id === reaction.id || (r.emoji === reaction.emoji && r.user_id === reaction.user_id)
        );
        if (exists) return m;
        return { ...m, reactions: [...(m.reactions || []), reaction] };
      };

      // Add reaction to the message (only if not already present)
      set((state) => {
        // Update cache if we have an active channel
        const updatedCache = activeChannelId ? {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).map(addReactionToMessage),
        } : state.messagesCache;

        return {
          messages: state.messages.map(addReactionToMessage),
          threadReplies: state.threadReplies.map(addReactionToMessage),
          messagesCache: updatedCache,
        };
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to add reaction' });
    } finally {
      pendingReactions.delete(reactionKey);
    }
  },

  removeReaction: async (messageId, emoji) => {
    const { activeChannelId } = get();
    const currentUserId = useAuthStore.getState().user?.id;
    set({ error: null });
    try {
      await removeMessageReaction(messageId, emoji);

      // Helper to remove only the current user's reaction (not all reactions with this emoji)
      const removeReactionFromMessage = (m: ChannelMessage) =>
        m.id === messageId
          ? { ...m, reactions: (m.reactions || []).filter((r) => !(r.emoji === emoji && r.user_id === currentUserId)) }
          : m;

      // Remove reaction from the message (match by emoji since we don't have reaction id)
      set((state) => {
        // Update cache if we have an active channel
        const updatedCache = activeChannelId ? {
          ...state.messagesCache,
          [activeChannelId]: (state.messagesCache[activeChannelId] || []).map(removeReactionFromMessage),
        } : state.messagesCache;

        return {
          messages: state.messages.map(removeReactionFromMessage),
          threadReplies: state.threadReplies.map(removeReactionFromMessage),
          messagesCache: updatedCache,
        };
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to remove reaction' });
    }
  },

  preloadAllChannels: async () => {
    const { channels, dms, messagesCache } = get();

    // Preload messages for all channels and DMs that aren't already cached
    const allChannelIds = [
      ...channels.map((c) => c.id),
      ...dms.map((d) => d.id),
    ];

    // Filter out already cached channels
    const uncachedIds = allChannelIds.filter((id) => !messagesCache[id]);

    // Preload in batches to avoid overwhelming the server
    const batchSize = 3;
    for (let i = 0; i < uncachedIds.length; i += batchSize) {
      const batch = uncachedIds.slice(i, i + batchSize);
      await Promise.all(
        batch.map((channelId) => get().fetchMessages(channelId, true))
      );
    }
  },

  shareMessage: async (targetChannelId, originalMessage, originalChannelName, commentBlocks) => {
    const blocks: ContentBlock[] = [
      ...(commentBlocks || []),
      {
        type: 'shared_message',
        data: {
          original_message_id: originalMessage.id,
          original_channel_id: originalMessage.channel_id,
          original_channel_name: originalChannelName,
          original_user_name: originalMessage.user?.name || originalMessage.user?.email || 'Unknown',
          original_user_avatar: originalMessage.user?.avatar_url,
          original_content: originalMessage.content,
          original_blocks: originalMessage.blocks,
          original_created_at: originalMessage.created_at,
        },
      },
    ];

    // Build optimistic message for instant display
    const authState = useAuthStore.getState();
    const currentUser = authState.user;
    const userProfile = authState.userProfile;
    const tempId = `temp-${Date.now()}-${Math.random().toString(36).slice(2)}`;

    const optimisticMessage: ChannelMessage = {
      id: tempId,
      channel_id: targetChannelId,
      user_id: currentUser?.id || '',
      content: '',
      blocks,
      is_edited: false,
      reply_count: 0,
      created_at: new Date().toISOString(),
      user: currentUser ? {
        id: currentUser.id,
        email: currentUser.email || '',
        name: userProfile?.name || currentUser.user_metadata?.name,
        avatar_url: userProfile?.avatar_url || currentUser.user_metadata?.avatar_url,
      } : undefined,
      reactions: [],
    };

    // Add optimistic message to target channel's cache (and messages if it's active)
    const { activeChannelId } = get();
    set((state) => ({
      messagesCache: {
        ...state.messagesCache,
        [targetChannelId]: [...(state.messagesCache[targetChannelId] || []), optimisticMessage],
      },
      // Also update messages array if target channel is currently active
      ...(activeChannelId === targetChannelId ? {
        messages: [...state.messages, optimisticMessage],
      } : {}),
    }));

    try {
      const result = await sendChannelMessage(targetChannelId, blocks);
      const message = result.message;

      // Replace optimistic message with real server message
      set((state) => ({
        messagesCache: {
          ...state.messagesCache,
          [targetChannelId]: (state.messagesCache[targetChannelId] || []).map((m) =>
            m.id === tempId ? message : m
          ),
        },
        // Also update messages array if target channel is currently active
        ...(state.activeChannelId === targetChannelId ? {
          messages: state.messages.map((m) => m.id === tempId ? message : m),
        } : {}),
      }));

      return message;
    } catch (err) {
      // Remove optimistic message on failure
      set((state) => ({
        error: err instanceof Error ? err.message : 'Failed to forward message',
        messagesCache: {
          ...state.messagesCache,
          [targetChannelId]: (state.messagesCache[targetChannelId] || []).filter((m) => m.id !== tempId),
        },
        ...(state.activeChannelId === targetChannelId ? {
          messages: state.messages.filter((m) => m.id !== tempId),
        } : {}),
      }));
      return null;
    }
  },

  getChannelById: (channelId) => {
    return get().channels.find((c) => c.id === channelId);
  },

  getDMById: (dmId) => {
    return get().dms.find((d) => d.id === dmId);
  },

  clearError: () => set({ error: null }),

      preload: (appId: string) => {
        const current = get().workspaceAppId;
        if (current === appId) {
          // Already initialized, just ensure all channels are preloaded
          setTimeout(() => get().preloadAllChannels(), 500);
          return;
        }
        // Switch workspace — handles cache restore + bg revalidation
        get().setWorkspaceAppId(appId);
      },

      /**
       * Background preload: fetches channels/DMs into cache WITHOUT switching active workspace.
       * Used for horizontal preloading across workspaces.
       */
      preloadBackground: async (appId: string) => {
        const STALE = 5 * 60 * 1000;
        const cache = get().workspaceCache[appId];

        // Skip if cache is fresh
        if (cache && cache.lastFetched && Date.now() - cache.lastFetched < STALE) {
          return;
        }

        try {
          // Fetch channels and DMs in parallel
          const [channelsResult, dmsResult] = await Promise.all([
            getChannels(appId),
            getUserDMs(appId),
          ]);

          // Determine the active channel for this workspace
          const activeChannelId = resolveValidActiveChannelId(
            cache?.activeChannelId,
            channelsResult.channels,
            dmsResult.dms
          );

          // Update workspace cache
          set((state) => ({
            workspaceCache: {
              ...state.workspaceCache,
              [appId]: {
                channels: channelsResult.channels,
                dms: dmsResult.dms,
                activeChannelId,
                lastFetched: Date.now(),
              },
            },
          }));

          // Also preload messages for the active channel into messagesCache
          if (activeChannelId && !get().messagesCache[activeChannelId]) {
            try {
              const messagesResult = await getChannelMessages(activeChannelId, { limit: 50 });
              set((state) => ({
                messagesCache: {
                  ...state.messagesCache,
                  [activeChannelId]: messagesResult.messages,
                },
              }));
            } catch (msgErr) {
              // Non-critical - messages will be fetched when user switches
              console.log(`[MessagesStore] Background message preload skipped for ${activeChannelId}`);
            }
          }
        } catch (err) {
          console.error(`[MessagesStore] Background preload failed for ${appId}:`, err);
        }
      },
    }),
    {
      name: 'messages-workspace-cache-v5',
      partialize: (state) => {
        // Step 1: Limit cache to most recent channels to prevent unbounded growth
        const limitedCache = limitCacheSize(state.messagesCache, state.activeChannelId);

        // Step 2: Clean up messagesCache - keep file blocks only for active channel
        // Strip file blocks from all other channels to save localStorage space
        const cleanedMessagesCache: Record<string, ChannelMessage[]> = {};

        for (const [channelId, messages] of Object.entries(limitedCache)) {
          if (channelId === state.activeChannelId) {
            // Keep everything for active channel (but limit to most recent 50 messages)
            cleanedMessagesCache[channelId] = messages.slice(-50);
          } else {
            // Strip file blocks from inactive channels (keep text data, limit to 30 messages)
            cleanedMessagesCache[channelId] = stripFileBlocks(messages).slice(-30);
          }
        }

        return {
          workspaceCache: state.workspaceCache,
          messagesCache: cleanedMessagesCache,
          unreadCounts: state.unreadCounts,
        };
      },
      storage: {
        getItem: (name) => {
          const str = localStorage.getItem(name);
          return str ? JSON.parse(str) : null;
        },
        setItem: (name, value) => {
          // If persistence was previously disabled due to quota, silently skip
          if (persistenceDisabled) {
            return;
          }

          try {
            localStorage.setItem(name, JSON.stringify(value));
          } catch (error) {
            if (error instanceof Error && error.name === 'QuotaExceededError') {
              // Strategy 1: Clear this cache and retry
              localStorage.removeItem(name);

              try {
                localStorage.setItem(name, JSON.stringify(value));
                return;
              } catch (retryError) {
                // Strategy 2: Try to save only the workspace cache (drop message cache)
                try {
                  const minimalValue = {
                    state: {
                      ...value.state,
                      messagesCache: {}, // Drop all message cache
                    },
                    version: value.version,
                  };
                  localStorage.setItem(name, JSON.stringify(minimalValue));
                  return;
                } catch (minimalError) {
                  // Strategy 3: Disable persistence and log once
                  if (!persistenceDisabled) {
                    persistenceDisabled = true;
                    console.warn('[MessagesStore] LocalStorage quota exhausted. Persistence disabled - app will continue without caching.');
                  }
                }
              }
            } else {
              console.error('[MessagesStore] Failed to save to localStorage:', error);
            }
          }
        },
        removeItem: (name) => {
          localStorage.removeItem(name);
        },
      },
    }
  )
);
