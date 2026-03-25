import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import {
  getWorkspaces,
  getWorkspaceApps,
  getDefaultWorkspace,
  getInitData,
  getSharedWithMe,
  createWorkspace as apiCreateWorkspace,
  deleteWorkspace as apiDeleteWorkspace,
  updateWorkspace as apiUpdateWorkspace,
  createWorkspaceApp as apiCreateWorkspaceApp,
  deleteWorkspaceApp as apiDeleteWorkspaceApp,
  type Workspace as ApiWorkspace,
  type WorkspaceApp as ApiWorkspaceApp,
  type SharedResource,
} from '../api/client';

// Frontend types that map to API types
export interface MiniApp {
  id: string;
  type: 'chat' | 'team' | 'files' | 'messages' | 'dashboard' | 'projects' | 'email' | 'calendar' | 'agents';
  name: string;
  icon: string;
  workspaceId: string;
  isPublic: boolean;
  position: number;
}

export interface Workspace {
  id: string;
  name: string;
  emoji?: string;
  icon_r2_key?: string; // R2 key stored in DB
  icon_url?: string; // Generated signed proxy URL
  apps: MiniApp[];
  isExpanded: boolean;
  isDefault: boolean;
  role?: string;
  isShared?: boolean;
  created_at?: string;
}

// Map app type to display info
const APP_DISPLAY_INFO: Record<string, { name: string; icon: string; emoji: string }> = {
  chat: { name: 'Chat', icon: 'Comment01Icon', emoji: '💬' },
  team: { name: 'Team', icon: 'ChatDots', emoji: '💬' },
  messages: { name: 'Messages', icon: 'ChatCircle', emoji: '💬' },
  files: { name: 'Files', icon: 'FolderOpen', emoji: '📁' },
  dashboard: { name: 'Personal', icon: 'SquaresFour', emoji: '📊' },
  projects: { name: 'Projects', icon: 'Briefcase', emoji: '📋' },
  email: { name: 'Email', icon: 'Mail', emoji: '📧' },
  calendar: { name: 'Calendar', icon: 'Calendar', emoji: '📅' },
  agents: { name: 'Agents', icon: 'AI', emoji: '🤖' },
};

const APP_ORDER = [
  'chat',
  'team',
  'messages',
  'files',
  'dashboard',
  'projects',
  'email',
  'calendar',
];

const APP_POSITION = APP_ORDER.reduce<Record<string, number>>((acc, appType, index) => {
  acc[appType] = index;
  return acc;
}, {});

// Convert API app to frontend MiniApp
function toMiniApp(apiApp: ApiWorkspaceApp): MiniApp {
  const displayInfo = APP_DISPLAY_INFO[apiApp.app_type] || {
    name: apiApp.app_type,
    icon: 'Circle',
    emoji: '•',
  };

  return {
    id: apiApp.id,
    type: apiApp.app_type,
    name: displayInfo.name,
    icon: displayInfo.icon,
    workspaceId: apiApp.workspace_id,
    isPublic: apiApp.is_public,
    position: apiApp.position,
  };
}

// Convert API workspace to frontend Workspace
function toWorkspace(apiWorkspace: ApiWorkspace, apps: MiniApp[]): Workspace {
  return {
    id: apiWorkspace.id,
    name: apiWorkspace.name,
    emoji: apiWorkspace.emoji,
    icon_r2_key: apiWorkspace.icon_r2_key,
    icon_url: apiWorkspace.icon_url,
    apps: apps.sort((a, b) => a.position - b.position),
    isExpanded: false,
    isDefault: apiWorkspace.is_default,
    role: apiWorkspace.role,
    isShared: !!apiWorkspace.is_shared,
    created_at: apiWorkspace.created_at,
  };
}

function buildSharedAppsByWorkspace(resources: SharedResource[]): Record<string, ApiWorkspaceApp[]> {
  const map: Record<string, ApiWorkspaceApp[]> = {};

  for (const resource of resources) {
    if (!resource.workspace_id || !resource.workspace_app_id || !resource.app_type) continue;

    const apps = map[resource.workspace_id] || [];
    if (apps.some((app) => app.app_type === resource.app_type)) continue;

    apps.push({
      id: resource.workspace_app_id,
      workspace_id: resource.workspace_id,
      app_type: resource.app_type as ApiWorkspaceApp['app_type'],
      is_public: true,
      position: APP_POSITION[resource.app_type] ?? 0,
      config: {},
      created_at: new Date().toISOString(),
    });
    map[resource.workspace_id] = apps;
  }

  return map;
}

function resolveActiveWorkspaceId(
  currentActiveId: string | null,
  workspaces: Workspace[],
  defaultWorkspaceId?: string
): string | null {
  if (currentActiveId && workspaces.some((ws) => ws.id === currentActiveId)) {
    return currentActiveId;
  }
  return defaultWorkspaceId || workspaces[0]?.id || null;
}

function resolveValidCachedChannelId(
  candidateId: string | null | undefined,
  channels: Array<{ id: string }> = [],
  dms: Array<{ id: string }> = []
): string | null {
  if (candidateId && [...channels, ...dms].some((item) => item.id === candidateId)) {
    return candidateId;
  }
  return channels[0]?.id || dms[0]?.id || null;
}

let fetchInitDataPromise: Promise<void> | null = null;

interface WorkspaceState {
  workspaces: Workspace[];
  activeWorkspaceId: string | null;
  activeAppId: string | null;
  isLoading: boolean;
  isRevalidating: boolean;
  error: string | null;
  lastFetched: number | null;
  // Session-only: tracks last app type visited per workspace (not persisted)
  sessionAppByWorkspace: Record<string, string>;

  // Actions
  fetchWorkspaces: () => Promise<void>;
  fetchInitData: () => Promise<void>;
  initializeFromCache: () => void;
  addWorkspace: (name: string, emoji?: string) => Promise<Workspace & { welcomeNoteId?: string }>;
  removeWorkspace: (id: string) => Promise<void>;
  updateWorkspace: (id: string, updates: { name?: string; emoji?: string; icon_r2_key?: string; clear_icon?: boolean }) => Promise<void>;
  renameWorkspace: (id: string, name: string) => Promise<void>; // Deprecated: use updateWorkspace
  toggleWorkspace: (id: string) => void;

  // Mini app actions
  addMiniApp: (workspaceId: string, appType: string) => Promise<void>;
  removeMiniApp: (workspaceId: string, appId: string) => Promise<void>;
  reorderApps: (workspaceId: string, fromIndex: number, toIndex: number) => void;

  setActiveWorkspace: (id: string | null) => void;
  setActiveApp: (workspaceId: string, appId: string) => void;
  // Record the app type for a workspace during the session
  recordSessionApp: (workspaceId: string, appType: string) => void;
  // Get the session app for a workspace, or null if not visited this session
  getSessionApp: (workspaceId: string) => string | null;

  // Get apps for active workspace
  getActiveApps: () => MiniApp[];
}

export const useWorkspaceStore = create<WorkspaceState>()(
  persist(
    (set, get) => ({
      workspaces: [],
      activeWorkspaceId: null,
      activeAppId: null,
      isLoading: false,
      isRevalidating: false,
      error: null,
      lastFetched: null,
      sessionAppByWorkspace: {},

      // Initialize from cache - called on app start to show cached data immediately
      initializeFromCache: () => {
        const { workspaces, lastFetched } = get();
        // If we have cached workspaces, set them as ready (not loading)
        if (workspaces.length > 0 && lastFetched) {
          set({ isLoading: false });
        }
      },

      fetchWorkspaces: async () => {
        const { workspaces: cachedWorkspaces, lastFetched } = get();
        const hasCachedData = cachedWorkspaces.length > 0 && lastFetched;

        // If we have cached data, show it immediately and revalidate in background
        if (hasCachedData) {
          set({ isRevalidating: true, error: null });
        } else {
          set({ isLoading: true, error: null });
        }

        try {
          // Fetch all workspaces
          let apiWorkspaces = await getWorkspaces();

          // If no workspaces returned, try to get/create the default personal workspace
          if (apiWorkspaces.length === 0) {
            console.log('[Workspaces] No workspaces found, fetching default workspace...');
            try {
              const defaultWorkspace = await getDefaultWorkspace();
              apiWorkspaces = [defaultWorkspace];
            } catch (defaultErr) {
              console.error('[Workspaces] Failed to get default workspace:', defaultErr);
            }
          }

          const sharedWorkspaceIds = apiWorkspaces.filter((ws) => ws.is_shared).map((ws) => ws.id);
          let sharedAppsByWorkspace: Record<string, ApiWorkspaceApp[]> = {};

          if (sharedWorkspaceIds.length > 0) {
            try {
              const shared = await getSharedWithMe({ limit: 200 });
              sharedAppsByWorkspace = buildSharedAppsByWorkspace(shared.items);
            } catch (err) {
              console.warn('[Workspaces] Failed to load shared resources:', err);
            }
          }

          // Fetch apps for each workspace in parallel
          const workspacesWithApps = await Promise.all(
            apiWorkspaces.map(async (ws) => {
              try {
                if (ws.is_shared) {
                  const sharedApps = sharedAppsByWorkspace[ws.id] || [];
                  const apps = sharedApps.map(toMiniApp);
                  return toWorkspace(ws, apps);
                }

                const apiApps = await getWorkspaceApps(ws.id);
                const apps = apiApps.map(toMiniApp);
                return toWorkspace(ws, apps);
              } catch (err) {
                console.error(`Failed to fetch apps for workspace ${ws.id}:`, err);
                return toWorkspace(ws, []);
              }
            })
          );

          // Preserve UI state (isExpanded) from cached workspaces
          const mergedWorkspaces = workspacesWithApps.map((ws) => {
            const cached = cachedWorkspaces.find((c) => c.id === ws.id);
            return cached ? { ...ws, isExpanded: cached.isExpanded } : ws;
          });

          // Set default active workspace if none selected
          const defaultWs = mergedWorkspaces.find((ws) => ws.isDefault);
          const currentActive = get().activeWorkspaceId;

          set({
            workspaces: mergedWorkspaces,
            activeWorkspaceId: resolveActiveWorkspaceId(
              currentActive,
              mergedWorkspaces,
              defaultWs?.id
            ),
            isLoading: false,
            isRevalidating: false,
            lastFetched: Date.now(),
          });
        } catch (err) {
          console.error('Failed to fetch workspaces:', err);
          // On error, keep cached data if available
          set({
            error: err instanceof Error ? err.message : 'Failed to fetch workspaces',
            isLoading: false,
            isRevalidating: false,
          });
        }
      },

      fetchInitData: async () => {
        if (fetchInitDataPromise) {
          return fetchInitDataPromise;
        }

        fetchInitDataPromise = (async () => {
          const { workspaces: cachedWorkspaces, lastFetched } = get();
          const hasCachedData = cachedWorkspaces.length > 0 && lastFetched;

          if (hasCachedData) {
            set({ isRevalidating: true, error: null });
          } else {
            set({ isLoading: true, error: null });
          }

          try {
            const initData = await getInitData();

            const sharedWorkspaceIds = initData.workspaces.filter((ws) => ws.is_shared).map((ws) => ws.id);
            let sharedAppsByWorkspace: Record<string, ApiWorkspaceApp[]> = {};
            if (sharedWorkspaceIds.length > 0) {
              try {
                const shared = await getSharedWithMe({ limit: 200 });
                sharedAppsByWorkspace = buildSharedAppsByWorkspace(shared.items);
              } catch (err) {
                console.warn('[Workspaces] Failed to load shared resources:', err);
              }
            }

            // Convert API workspaces to frontend workspaces (apps are already included)
            const workspacesWithApps = initData.workspaces.map((ws) => {
              const apiApps = ws.is_shared
                ? (sharedAppsByWorkspace[ws.id] || ws.apps || [])
                : (ws.apps || []);
              const apps = apiApps.map(toMiniApp);
              return toWorkspace(ws, apps);
            });

            // Preserve UI state from cached workspaces
            const mergedWorkspaces = workspacesWithApps.map((ws) => {
              const cached = cachedWorkspaces.find((c) => c.id === ws.id);
              return cached ? { ...ws, isExpanded: cached.isExpanded } : ws;
            });

            const defaultWs = mergedWorkspaces.find((ws) => ws.isDefault);
            const currentActive = get().activeWorkspaceId;

            set({
              workspaces: mergedWorkspaces,
              activeWorkspaceId: resolveActiveWorkspaceId(
                currentActive,
                mergedWorkspaces,
                defaultWs?.id
              ),
              isLoading: false,
              isRevalidating: false,
              lastFetched: Date.now(),
            });

            // Populate messagesStore caches from the batched response
            const { useMessagesStore } = await import('./messagesStore');
            const messagesState = useMessagesStore.getState();

            // Build workspace cache entries for each message app
            const newWorkspaceCache = { ...messagesState.workspaceCache };
            for (const [appId, channels] of Object.entries(initData.channels_by_app)) {
              const dms = initData.dms_by_app[appId] || [];
              const existing = newWorkspaceCache[appId];
              newWorkspaceCache[appId] = {
                channels,
                dms,
                activeChannelId: resolveValidCachedChannelId(
                  existing?.activeChannelId,
                  channels,
                  dms
                ),
                lastFetched: Date.now(),
              };
            }

            useMessagesStore.setState((state) => ({
              workspaceCache: newWorkspaceCache,
              unreadCounts: { ...state.unreadCounts, ...initData.unread_counts },
            }));

            // Propagate onboarding status to auth store
            if (initData.onboarding_completed_at !== undefined) {
              const { useAuthStore } = await import('./authStore');
              const authState = useAuthStore.getState();
              useAuthStore.setState({
                onboardingCompletedAt: initData.onboarding_completed_at ?? null,
                ...(authState.userProfile && {
                  userProfile: {
                    ...authState.userProfile,
                    onboarding_completed_at: initData.onboarding_completed_at,
                  },
                }),
              });
            }
          } catch (err) {
            console.error('Failed to fetch init data:', err);
            // Fallback to regular fetchWorkspaces + channel/unread bootstrap
            set({ isLoading: false, isRevalidating: false });
            await get().fetchWorkspaces();

            // Re-bootstrap channels and unread counts (replaces the old App.tsx effects)
            const { useMessagesStore } = await import('./messagesStore');
            const fallbackWorkspaces = get().workspaces;
            const messageAppIds = fallbackWorkspaces
              .flatMap((ws) => ws.apps || [])
              .filter((app) => app.type === 'messages')
              .map((app) => app.id);
            if (messageAppIds.length > 0) {
              const messagesStore = useMessagesStore.getState();
              Promise.all([
                messagesStore.preloadAllWorkspaceChannels(messageAppIds),
                messagesStore.fetchAllUnreadCounts(messageAppIds),
              ]);
            }
          }
        })();

        try {
          await fetchInitDataPromise;
        } finally {
          fetchInitDataPromise = null;
        }
      },

      addWorkspace: async (name, emoji) => {
        try {
          const { workspace: newWorkspace, welcome_note_id } = await apiCreateWorkspace(name, true);
          const apps = await getWorkspaceApps(newWorkspace.id);
          const workspace = toWorkspace(newWorkspace, apps.map(toMiniApp));

          // Override emoji if provided
          if (emoji) {
            workspace.emoji = emoji;
          }

          set((state) => ({
            workspaces: [...state.workspaces, workspace],
          }));

          return { ...workspace, welcomeNoteId: welcome_note_id ?? undefined };
        } catch (err) {
          console.error('Failed to create workspace:', err);
          throw err;
        }
      },

      removeWorkspace: async (id) => {
        try {
          await apiDeleteWorkspace(id);
          set((state) => ({
            workspaces: state.workspaces.filter((w) => w.id !== id),
            activeWorkspaceId: state.activeWorkspaceId === id ? null : state.activeWorkspaceId,
          }));
        } catch (err) {
          console.error('Failed to delete workspace:', err);
          throw err;
        }
      },

      updateWorkspace: async (id, updates) => {
        try {
          const updatedWorkspace = await apiUpdateWorkspace(id, updates);
          set((state) => ({
            workspaces: state.workspaces.map((w) =>
              w.id === id
                ? {
                    ...w,
                    name: updatedWorkspace.name,
                    emoji: updatedWorkspace.emoji,
                    icon_r2_key: updatedWorkspace.icon_r2_key,
                    icon_url: updatedWorkspace.icon_url,
                  }
                : w
            ),
          }));
        } catch (err) {
          console.error('Failed to update workspace:', err);
          throw err;
        }
      },

      renameWorkspace: async (id, name) => {
        // Deprecated: use updateWorkspace instead
        try {
          await apiUpdateWorkspace(id, { name });
          set((state) => ({
            workspaces: state.workspaces.map((w) =>
              w.id === id ? { ...w, name } : w
            ),
          }));
        } catch (err) {
          console.error('Failed to rename workspace:', err);
          throw err;
        }
      },

      toggleWorkspace: (id) =>
        set((state) => ({
          workspaces: state.workspaces.map((w) =>
            w.id === id ? { ...w, isExpanded: !w.isExpanded } : w
          ),
        })),

      addMiniApp: async (workspaceId, appType) => {
        try {
          const newApp = await apiCreateWorkspaceApp(workspaceId, appType);
          const miniApp = toMiniApp(newApp);
          set((state) => ({
            workspaces: state.workspaces.map((w) =>
              w.id === workspaceId
                ? { ...w, apps: [...w.apps, miniApp] }
                : w
            ),
          }));
        } catch (err) {
          console.error('Failed to add mini app:', err);
          throw err;
        }
      },

      removeMiniApp: async (workspaceId, appId) => {
        try {
          await apiDeleteWorkspaceApp(workspaceId, appId);
          set((state) => ({
            workspaces: state.workspaces.map((w) =>
              w.id === workspaceId
                ? { ...w, apps: w.apps.filter((a) => a.id !== appId) }
                : w
            ),
          }));
        } catch (err) {
          console.error('Failed to remove mini app:', err);
          throw err;
        }
      },

      reorderApps: (workspaceId, fromIndex, toIndex) => {
        const state = get();
        const workspace = state.workspaces.find(w => w.id === workspaceId);
        if (!workspace) return;

        const newApps = [...workspace.apps];
        const [movedApp] = newApps.splice(fromIndex, 1);
        newApps.splice(toIndex, 0, movedApp);

        // Update positions locally only (persisted via zustand storage)
        const updatedApps = newApps.map((app, index) => ({ ...app, position: index }));

        set((state) => ({
          workspaces: state.workspaces.map((w) => {
            if (w.id !== workspaceId) return w;
            return { ...w, apps: updatedApps };
          }),
        }));
      },

      setActiveWorkspace: (id) => set({ activeWorkspaceId: id }),

      setActiveApp: (workspaceId, appId) =>
        set({ activeWorkspaceId: workspaceId, activeAppId: appId }),

      recordSessionApp: (workspaceId, appType) =>
        set((state) => ({
          sessionAppByWorkspace: {
            ...state.sessionAppByWorkspace,
            [workspaceId]: appType,
          },
        })),

      getSessionApp: (workspaceId) => {
        const state = get();
        return state.sessionAppByWorkspace[workspaceId] || null;
      },

      getActiveApps: () => {
        const state = get();
        if (!state.activeWorkspaceId) {
          // Return all unique apps across workspaces
          const allApps = state.workspaces.flatMap((w) => w.apps);
          const uniqueApps = allApps.reduce((acc, app) => {
            if (!acc.find((a) => a.type === app.type)) {
              acc.push(app);
            }
            return acc;
          }, [] as MiniApp[]);
          return uniqueApps;
        }
        const workspace = state.workspaces.find((w) => w.id === state.activeWorkspaceId);
        return workspace?.apps || [];
      },
    }),
    {
      name: 'core-workspace-storage-v4',
      partialize: (state) => ({
        // Persist full workspace data for instant load
        activeWorkspaceId: state.activeWorkspaceId,
        activeAppId: state.activeAppId,
        workspaces: state.workspaces,
        lastFetched: state.lastFetched,
        sessionAppByWorkspace: state.sessionAppByWorkspace,
      }),
    }
  )
);
