import { create } from 'zustand';
import { persist } from 'zustand/middleware';
import { useAuthStore } from './authStore';
import { useWorkspaceStore } from './workspaceStore';
import {
  getDocuments,
  getDocument,
  createDocument,
  createFolder,
  updateDocument,
  deleteDocument,
  favoriteDocument,
  unfavoriteDocument,
  getPresignedUploadUrl,
  confirmFileUpload,
  reorderDocuments as reorderDocumentsApi,
  type Document,
} from '../api/client';
import { markLocalDocumentReorder } from '../lib/documentRealtimeGuard';
import { resolveUploadMimeType } from '../utils/uploadMime';

interface Breadcrumb {
  id?: string;
  title: string;
}

interface WorkspaceDocCache {
  documentsByFolder: Record<string, Document[]>;
  lastFetched: number;
}

// Pending edit that hasn't been saved to the server yet
interface PendingEdit {
  noteId: string;
  title: string;
  content: string;
  timestamp: number;
}

interface FilesState {
  // Data - keyed by folderId (undefined = root)
  documentsByFolder: Record<string, Document[]>;

  // Workspace-level cache for instant switching
  workspaceDocCache: Record<string, WorkspaceDocCache>;

  // UI State
  workspaceAppId: string | null;
  currentFolderId: string | undefined;
  breadcrumbs: Breadcrumb[];
  selectedNoteId: string | null;
  searchQuery: string;
  sortBy: 'name' | 'date' | 'size' | 'manual';
  sortDirection: 'asc' | 'desc';

  // Loading / Status
  isLoading: boolean;
  isSyncing: boolean;
  error: string | null;
  accessDenied: { resourceType: string; resourceId: string } | null;
  isUploading: boolean;
  uploadProgress: number;
  isSavingNote: boolean;
  isActivelyEditing: boolean;

  // Pending edits (for crash recovery)
  pendingEdits: Record<string, PendingEdit>;

  // Rename state
  renamingId: string | null;

  // Actions
  setWorkspaceAppId: (id: string | null) => void;
  fetchDocuments: (folderId?: string, options?: { background?: boolean }) => Promise<void>;
  navigateToFolder: (folderId: string | undefined, title: string) => void;
  navigateToBreadcrumb: (index: number) => void;
  setSelectedNote: (noteId: string | null) => void;
  setSearchQuery: (query: string) => void;
  setRenamingId: (id: string | null) => void;
  setActivelyEditing: (editing: boolean) => void;
  preload: (appId: string) => void;
  preloadBackground: (appId: string) => Promise<void>;

  // CRUD
  addNote: (parentId?: string) => Promise<Document | null>;
  addFolder: (parentId?: string) => Promise<Document | null>;
  renameDocument: (docId: string, newTitle: string) => Promise<void>;
  saveNote: (noteId: string, title: string, content: string) => Promise<void>;
  updateNoteOptimistic: (noteId: string, title: string, content: string) => void;
  clearPendingEdit: (noteId: string) => void;
  flushPendingEdits: () => Promise<void>;
  removeDocument: (docId: string) => Promise<void>;
  toggleFavorite: (docId: string) => Promise<void>;
  uploadFiles: (files: FileList, parentId?: string) => Promise<void>;
  reorderDocuments: (folderId: string | undefined, docIds: string[]) => void;
  moveDocument: (docId: string, targetFolderId: string | undefined) => Promise<void>;
  duplicateDocument: (docId: string) => Promise<Document | null>;
  setSortBy: (sortBy: 'name' | 'date' | 'size' | 'manual') => void;
  setSortDirection: (dir: 'asc' | 'desc') => void;
  hydrateNoteContent: (noteId: string) => Promise<void>;
  loadSharedDocument: (docId: string) => Promise<Document | null>;
  clearAccessDenied: () => void;

  // Getters
  getCurrentDocuments: () => Document[];
  getSelectedNote: () => Document | null;
}

type FilesPersistedState = Pick<FilesState, 'pendingEdits'>;

const ROOT_KEY = '__root__';

function folderKey(id?: string): string {
  return id || ROOT_KEY;
}

// Track fetch versions per workspace+folder to prevent stale responses from overwriting newer data
// Key format: `${workspaceAppId}:${folderKey}` to prevent cross-workspace collisions
const fetchVersions: Record<string, number> = {};

function fetchVersionKey(workspaceAppId: string, folderId?: string): string {
  return `${workspaceAppId}:${folderKey(folderId)}`;
}

// Track temp ID to real ID mappings for optimistic creates
// This allows renaming to work even if the user types fast and submits before the API responds
const tempIdToRealId: Record<string, string> = {};

// Track pending renames for temp IDs that haven't been resolved yet
// If user renames before API returns, we store it here and apply when real ID arrives
const pendingRenames: Record<string, string> = {};

// Guard to avoid overlapping background flush loops
let isFlushingPendingEdits = false;

// Guard to avoid duplicate in-flight hydration requests for the same note
const hydratingNoteIds = new Set<string>();

// Last confirmed server updated_at per note, used for optimistic locking.
const noteVersionTokens: Record<string, string> = {};

// Serialize note saves per note ID so overlapping autosaves do not race each other.
const inFlightNoteSaves: Record<string, Promise<void>> = {};

function rememberNoteVersionToken(doc: Pick<Document, 'id' | 'updated_at'> | null | undefined) {
  if (doc?.updated_at) {
    noteVersionTokens[doc.id] = doc.updated_at;
  }
}

/** Update or clear the optimistic-lock token for a note.
 *  Called from realtime handlers (to stay current) and keepalive saves (to
 *  invalidate a token that the fire-and-forget save will make stale). */
export function updateNoteVersionToken(docId: string, updatedAt?: string) {
  if (updatedAt) {
    noteVersionTokens[docId] = updatedAt;
  } else {
    delete noteVersionTokens[docId];
  }
}

function rememberNoteVersionTokens(docs: Document[]) {
  for (const doc of docs) {
    rememberNoteVersionToken(doc);
  }
}

async function runWithNoteSaveLock<T>(noteId: string, work: () => Promise<T>): Promise<T> {
  const previous = inFlightNoteSaves[noteId];
  if (previous) {
    try {
      await previous;
    } catch {
      // Let the next save continue even if a previous save failed.
    }
  }

  let release!: () => void;
  const gate = new Promise<void>((resolve) => {
    release = resolve;
  });
  inFlightNoteSaves[noteId] = gate;

  try {
    return await work();
  } finally {
    release();
    if (inFlightNoteSaves[noteId] === gate) {
      delete inFlightNoteSaves[noteId];
    }
  }
}

// Helper to pre-fetch subfolder contents for instant expansion and counts
function prefetchSubfolders(get: () => FilesState) {
  const { documentsByFolder } = get();
  const rootKey = ROOT_KEY;
  const rootDocs = documentsByFolder[rootKey] || [];
  const folders = rootDocs.filter(doc => doc.is_folder || doc.type === 'folder');

  for (const folder of folders) {
    // Only fetch if not already cached
    if (!documentsByFolder[folder.id]) {
      setTimeout(() => {
        void get().fetchDocuments(folder.id, { background: true });
      }, 0);
    }
  }
}

export const useFilesStore = create<FilesState>()(
  persist<FilesState, [], [], FilesPersistedState>(
    (set, get) => ({
  documentsByFolder: {},
  workspaceDocCache: {},
  workspaceAppId: null,
  currentFolderId: undefined,
  breadcrumbs: [{ title: 'All' }],
  selectedNoteId: null,
  searchQuery: '',
  sortBy: 'manual',
  sortDirection: 'asc',
  isLoading: false,
  isSyncing: false,
  error: null,
  accessDenied: null,
  isUploading: false,
  uploadProgress: 0,
  isSavingNote: false,
  isActivelyEditing: false,
  pendingEdits: {},
  renamingId: null,

  setWorkspaceAppId: (id) => {
    const prev = get().workspaceAppId;
    if (id !== prev) {
      // Save current workspace data to cache before switching
      if (prev) {
        const { documentsByFolder } = get();
        if (Object.keys(documentsByFolder).length > 0) {
          set((state) => ({
            workspaceDocCache: {
              ...state.workspaceDocCache,
              [prev]: {
                documentsByFolder,
                lastFetched: Date.now(),
              },
            },
          }));
        }
      }

      // Try to restore from cache for the new workspace
      const cache = id ? get().workspaceDocCache[id] : null;
      if (cache) {
        // Apply pending edits to cached documents
        const { pendingEdits } = get();
        let documentsByFolder = cache.documentsByFolder;
        if (Object.keys(pendingEdits).length > 0) {
          documentsByFolder = { ...documentsByFolder };
          for (const [folderId, docs] of Object.entries(documentsByFolder)) {
            documentsByFolder[folderId] = docs.map(doc => {
              const pendingEdit = pendingEdits[doc.id];
              if (pendingEdit) {
                return { ...doc, title: pendingEdit.title, content: pendingEdit.content };
              }
              return doc;
            });
          }
        }

        set({
          workspaceAppId: id,
          documentsByFolder,
          currentFolderId: undefined,
          breadcrumbs: [{ title: 'All' }],
          selectedNoteId: null,
          searchQuery: '',
          isLoading: false,
          isSyncing: false,
          error: null,
        });
        // Pre-fetch subfolder contents for instant expansion and counts
        setTimeout(() => prefetchSubfolders(get), 0);
        // Revalidate in background only if cache is stale (>5min)
        if (Date.now() - cache.lastFetched > 5 * 60 * 1000) {
          setTimeout(() => get().fetchDocuments(undefined), 0);
        }
      } else {
        set({
          workspaceAppId: id,
          documentsByFolder: {},
          currentFolderId: undefined,
          breadcrumbs: [{ title: 'All' }],
          selectedNoteId: null,
          searchQuery: '',
          isLoading: false,
          isSyncing: false,
          error: null,
        });
        if (id) {
          get().fetchDocuments(undefined);
        }
      }

      if (id) {
        // Try flushing crash-recovered edits when Files app becomes active.
        setTimeout(() => {
          void get().flushPendingEdits();
        }, 0);
      }
    }
  },

  fetchDocuments: async (folderId, options?: { background?: boolean }) => {
    const { workspaceAppId, sortBy, sortDirection, documentsByFolder, isActivelyEditing, currentFolderId } = get();
    if (!workspaceAppId) return;

    const key = folderKey(folderId);
    const versionKey = fetchVersionKey(workspaceAppId, folderId);
    const hasCache = !!documentsByFolder[key];
    const isBackground = options?.background ?? false;

    // Skip background syncs when user is actively editing to prevent content flicker
    // Allow initial loads (no cache) to proceed
    if (hasCache && isActivelyEditing) {
      return;
    }

    // Increment version for this workspace+folder so stale responses are ignored
    // Using workspace-aware key prevents cross-workspace race conditions
    const version = (fetchVersions[versionKey] || 0) + 1;
    fetchVersions[versionKey] = version;

    // Only show loading spinner when fetching the current folder context (not subfolders)
    // This prevents the spinner from showing when expanding folder dropdowns
    const isCurrentFolder = folderId === currentFolderId;
    if (!hasCache && isCurrentFolder && !isBackground) {
      set({ isLoading: true });
    } else if (!isBackground) {
      set({ isSyncing: true });
    }

    try {
      const result = await getDocuments({
        workspaceAppId,
        parentId: folderId,
        sortBy: sortBy === 'manual' ? 'position' : sortBy,
        sortDirection,
      });
      // Only apply if this is still the latest fetch for this workspace+folder
      if (fetchVersions[versionKey] === version) {
        // Apply any pending edits to the fetched documents
        // This ensures unsaved edits aren't lost when data is refreshed from API
        const docsWithPendingEdits = (result.documents || []).map(doc => {
          const pendingEdit = get().pendingEdits[doc.id];
          if (pendingEdit) {
            return { ...doc, title: pendingEdit.title, content: pendingEdit.content };
          }
          return doc;
        });
        rememberNoteVersionTokens(docsWithPendingEdits);

        set((state) => {
          const newDocsByFolder = {
            ...state.documentsByFolder,
            [key]: docsWithPendingEdits,
          };

          // Stale-data guard: if user switched workspace mid-flight,
          // update only the cache for the original workspace, not active state
          if (state.workspaceAppId !== workspaceAppId) {
            const existingCache = state.workspaceDocCache[workspaceAppId];
            return {
              workspaceDocCache: {
                ...state.workspaceDocCache,
                [workspaceAppId]: {
                  documentsByFolder: {
                    ...(existingCache?.documentsByFolder || {}),
                    [key]: docsWithPendingEdits,
                  },
                  lastFetched: Date.now(),
                },
              },
            };
          }

          // Update workspace cache
          const newWorkspaceCache = { ...state.workspaceDocCache };
          newWorkspaceCache[workspaceAppId] = {
            documentsByFolder: newDocsByFolder,
            lastFetched: Date.now(),
          };
          return {
            documentsByFolder: newDocsByFolder,
            workspaceDocCache: newWorkspaceCache,
            error: null,
          };
        });

        // Pre-fetch contents of all folders to enable instant expansion and show counts
        // Only do this for non-background fetches to avoid infinite loops
        if (!isBackground) {
          setTimeout(() => prefetchSubfolders(get), 0);
        }
      }
    } catch (err) {
      if (fetchVersions[versionKey] === version) {
        // Only set error if still on the same workspace
        if (get().workspaceAppId === workspaceAppId) {
          set({ error: err instanceof Error ? err.message : 'Failed to load files' });
        }
      }
    } finally {
      if (fetchVersions[versionKey] === version) {
        // Only update loading state if still on the same workspace
        if (get().workspaceAppId === workspaceAppId) {
          set({ isLoading: false, isSyncing: false });
        }
      }
    }
  },

  navigateToFolder: (folderId, title) => {
    if (folderId === undefined) {
      set({
        currentFolderId: undefined,
        breadcrumbs: [{ title: 'All' }],
        selectedNoteId: null,
      });
    } else {
      set((state) => {
        const existingIndex = state.breadcrumbs.findIndex(b => b.id === folderId);
        const breadcrumbs = existingIndex >= 0
          ? state.breadcrumbs.slice(0, existingIndex + 1)
          : [...state.breadcrumbs, { id: folderId, title }];
        return { currentFolderId: folderId, breadcrumbs, selectedNoteId: null };
      });
    }
    get().fetchDocuments(folderId);
  },

  navigateToBreadcrumb: (index) => {
    const { breadcrumbs } = get();
    const crumb = breadcrumbs[index];
    if (!crumb) return;
    get().navigateToFolder(crumb.id, crumb.title);
  },

  setSelectedNote: (noteId) => {
    set({ selectedNoteId: noteId });
  },

  setSearchQuery: (query) => {
    set({ searchQuery: query });
  },

  setRenamingId: (id) => {
    set({ renamingId: id });
  },

  setActivelyEditing: (editing) => {
    set({ isActivelyEditing: editing });
  },

  preload: (appId: string) => {
    const STALE = 5 * 60 * 1000;
    const cache = get().workspaceDocCache[appId];
    if (cache && Date.now() - cache.lastFetched < STALE) return;

    const current = get().workspaceAppId;
    if (current === appId) {
      // Already active, just revalidate root
      get().fetchDocuments(get().currentFolderId);
    } else {
      // Switch workspace — handles cache restore + bg revalidation
      get().setWorkspaceAppId(appId);
    }
  },

  /**
   * Background preload: fetches root documents into cache WITHOUT switching active workspace.
   * Used for horizontal preloading across workspaces.
   */
  preloadBackground: async (appId: string) => {
    const STALE = 5 * 60 * 1000;
    const cache = get().workspaceDocCache[appId];

    // Skip if cache is fresh
    if (cache && Date.now() - cache.lastFetched < STALE) {
      return;
    }

    try {
      // Fetch root folder documents
      const result = await getDocuments({
        workspaceAppId: appId,
        parentId: undefined,
        sortBy: 'date',
        sortDirection: 'desc',
      });

      // Update ONLY the cache, not the active state
      // Apply pending edits to preserve unsaved changes
      set((state) => {
        const docs = (result.documents || []).map(doc => {
          const pendingEdit = state.pendingEdits[doc.id];
          if (pendingEdit) {
            return { ...doc, title: pendingEdit.title, content: pendingEdit.content };
          }
          return doc;
        });
        rememberNoteVersionTokens(docs);

        // Evict oldest cache entries to bound memory (keep max 3)
        const MAX_CACHED = 3;
        const cache = { ...state.workspaceDocCache };
        const entries = Object.entries(cache);
        if (entries.length >= MAX_CACHED && !cache[appId]) {
          const oldest = entries.sort(([, a], [, b]) => a.lastFetched - b.lastFetched)[0];
          if (oldest) delete cache[oldest[0]];
        }

        return {
          workspaceDocCache: {
            ...cache,
            [appId]: {
              documentsByFolder: { [ROOT_KEY]: docs },
              lastFetched: Date.now(),
            },
          },
        };
      });
    } catch (err) {
      console.error(`[FilesStore] Background preload failed for ${appId}:`, err);
    }
  },

  addNote: async (parentId) => {
    const { workspaceAppId } = get();
    if (!workspaceAppId) return null;

    // Create optimistic note immediately for instant UI feedback
    const tempId = `temp-${Date.now()}`;
    const stableKey = `note-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const now = new Date().toISOString();
    const optimisticNote = {
      id: tempId,
      user_id: '',
      workspace_app_id: workspaceAppId,
      title: 'Untitled',
      content: '',
      type: 'note' as const,
      is_folder: false,
      parent_id: parentId,
      position: 0,
      is_archived: false,
      is_favorite: false,
      is_public: false,
      created_at: now,
      updated_at: now,
      _stableKey: stableKey, // Used for React key to prevent re-animation
    };

    const key = folderKey(parentId);

    // Add optimistic note to store immediately
    // Also add to root cache so it appears in "All" view instantly
    set((state) => {
      const updated = { ...state.documentsByFolder };
      updated[key] = [optimisticNote, ...(updated[key] || [])];
      // If creating in a subfolder, also add to root so "All" view updates immediately
      if (parentId && updated[ROOT_KEY]) {
        updated[ROOT_KEY] = [optimisticNote, ...updated[ROOT_KEY]];
      }
      return {
        documentsByFolder: updated,
        selectedNoteId: tempId,
      };
    });

    try {
      // Create note on server in background
      const newNote = await createDocument({
        workspaceAppId,
        title: 'Untitled',
        content: '',
        parentId,
      });
      rememberNoteVersionToken(newNote);

      // Replace optimistic note with real note, preserving stableKey for animation
      set((state) => {
        const updated = { ...state.documentsByFolder };
        const realNote = { ...newNote, _stableKey: stableKey };
        updated[key] = updated[key].map(d =>
          d.id === tempId ? realNote : d
        );
        // Also update in root cache if it exists there
        if (parentId && updated[ROOT_KEY]) {
          updated[ROOT_KEY] = updated[ROOT_KEY].map(d =>
            d.id === tempId ? realNote : d
          );
        }
        return {
          documentsByFolder: updated,
          // Update selectedNoteId if it was the temp note
          selectedNoteId: state.selectedNoteId === tempId ? newNote.id : state.selectedNoteId,
        };
      });
      return newNote;
    } catch (err) {
      // Remove optimistic note on error
      set((state) => {
        const updated = { ...state.documentsByFolder };
        updated[key] = updated[key].filter(d => d.id !== tempId);
        // Also remove from root cache if it was added there
        if (parentId && updated[ROOT_KEY]) {
          updated[ROOT_KEY] = updated[ROOT_KEY].filter(d => d.id !== tempId);
        }
        return {
          documentsByFolder: updated,
          selectedNoteId: state.selectedNoteId === tempId ? null : state.selectedNoteId,
          error: err instanceof Error ? err.message : 'Failed to create note',
        };
      });
      return null;
    }
  },

  addFolder: async (parentId) => {
    const { workspaceAppId } = get();
    if (!workspaceAppId) return null;

    // Create optimistic folder immediately for instant UI feedback
    const tempId = `temp-folder-${Date.now()}`;
    const stableKey = `folder-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const now = new Date().toISOString();
    const optimisticFolder = {
      id: tempId,
      user_id: '',
      workspace_app_id: workspaceAppId,
      title: '', // Empty title - placeholder will show in edit mode
      content: '',
      type: 'folder' as const,
      is_folder: true,
      parent_id: parentId,
      position: 0,
      is_archived: false,
      is_favorite: false,
      is_public: false,
      created_at: now,
      updated_at: now,
      _stableKey: stableKey, // Used for React key to prevent re-animation
      _isNew: true, // Flag to indicate this is a newly created folder
    };

    const key = folderKey(parentId);

    // Add optimistic folder to store immediately
    // Also add to root cache so it appears in "All" view instantly
    set((state) => {
      const updated = { ...state.documentsByFolder };
      updated[key] = [optimisticFolder, ...(updated[key] || [])];
      // If creating in a subfolder, also add to root so "All" view updates immediately
      if (parentId && updated[ROOT_KEY]) {
        updated[ROOT_KEY] = [optimisticFolder, ...updated[ROOT_KEY]];
      }
      return {
        documentsByFolder: updated,
        renamingId: tempId,
      };
    });

    // Return the optimistic folder immediately so UI can enter edit mode
    // API call happens in background
    createFolder({
      workspaceAppId,
      title: 'New Folder',
      parentId,
    }).then(async (realFolder) => {
      // Store mapping so rename can resolve temp ID to real ID
      tempIdToRealId[tempId] = realFolder.id;

      // Check for pending rename that was submitted before API returned
      const pendingTitle = pendingRenames[tempId];
      if (pendingTitle) {
        delete pendingRenames[tempId];
        // Apply pending rename to the real folder
        try {
          await updateDocument(realFolder.id, { title: pendingTitle });
          realFolder = { ...realFolder, title: pendingTitle };
        } catch {
          // If rename fails, use the server's title
        }
      }

      // Replace optimistic folder with real folder, preserving stableKey
      set((state) => {
        const updated = { ...state.documentsByFolder };
        const realFolderWithKey = { ...realFolder, _stableKey: stableKey };
        updated[key] = updated[key].map(d =>
          d.id === tempId ? realFolderWithKey : d
        );
        // Also update in root cache if it exists there
        if (parentId && updated[ROOT_KEY]) {
          updated[ROOT_KEY] = updated[ROOT_KEY].map(d =>
            d.id === tempId ? realFolderWithKey : d
          );
        }
        return {
          documentsByFolder: updated,
          // Update renamingId if it was the temp folder
          renamingId: state.renamingId === tempId ? realFolder.id : state.renamingId,
        };
      });
    }).catch((err) => {
      // Remove optimistic folder on error
      set((state) => {
        const updated = { ...state.documentsByFolder };
        updated[key] = updated[key].filter(d => d.id !== tempId);
        // Also remove from root cache if it was added there
        if (parentId && updated[ROOT_KEY]) {
          updated[ROOT_KEY] = updated[ROOT_KEY].filter(d => d.id !== tempId);
        }
        return {
          documentsByFolder: updated,
          renamingId: state.renamingId === tempId ? null : state.renamingId,
          error: err instanceof Error ? err.message : 'Failed to create folder',
        };
      });
    });

    return optimisticFolder;
  },

  renameDocument: async (docId, newTitle) => {
    // Check if this is a temp ID that hasn't been resolved yet
    const isTempId = docId.startsWith('temp-folder-');
    const realId = tempIdToRealId[docId];

    // Optimistic update across all folder caches
    // Update both temp ID and real ID in case the document hasn't been replaced yet
    set((state) => {
      const updated = { ...state.documentsByFolder };
      for (const key of Object.keys(updated)) {
        updated[key] = updated[key].map(d =>
          (d.id === docId || (realId && d.id === realId)) ? { ...d, title: newTitle } : d
        );
      }
      return { documentsByFolder: updated, renamingId: null };
    });

    // If this is a temp ID without a real ID yet, store pending rename for later
    if (isTempId && !realId) {
      pendingRenames[docId] = newTitle;
      return;
    }

    const idToUpdate = realId || docId;

    try {
      await updateDocument(idToUpdate, { title: newTitle });
      // Clean up temp ID mapping after successful rename
      if (tempIdToRealId[docId]) {
        delete tempIdToRealId[docId];
      }
    } catch (err) {
      // Refetch on error
      get().fetchDocuments(get().currentFolderId);
      set({ error: err instanceof Error ? err.message : 'Failed to rename' });
    }
  },

  saveNote: async (noteId, title, content) => {
    return runWithNoteSaveLock(noteId, async () => {
      set({ isSavingNote: true });
      try {
        const expectedUpdatedAt = noteVersionTokens[noteId];

        let updatedDoc;
        try {
          updatedDoc = await updateDocument(noteId, {
            title,
            content,
            expectedUpdatedAt,
          });
        } catch (firstErr) {
          const firstMsg = firstErr instanceof Error ? firstErr.message : String(firstErr);
          const isConflict = firstMsg.includes('409') || firstMsg.includes('another session');
          if (!isConflict) throw firstErr;

          // 409 Conflict — stale token (e.g. another tab saved first).
          // Retry once without the optimistic lock so the latest edit wins.
          delete noteVersionTokens[noteId];
          updatedDoc = await updateDocument(noteId, { title, content });
        }

        rememberNoteVersionToken(updatedDoc);

        // Update in all folder caches with the content we saved (not server response)
        // to ensure switching notes preserves edits
        // NOTE: We intentionally preserve the original updated_at to prevent
        // the note from jumping in the sort order during auto-saves
        set((state) => {
          const currentPending = state.pendingEdits[noteId];
          const hasNewerPending =
            !!currentPending &&
            (currentPending.title !== title || currentPending.content !== content);

          // A newer edit exists locally, so keep it and ignore this stale save completion.
          if (hasNewerPending) {
            return {};
          }

          const newCache = { ...state.documentsByFolder };
          for (const key of Object.keys(newCache)) {
            newCache[key] = newCache[key].map(d =>
              d.id === noteId ? { ...d, title, content } : d
            );
          }
          // Clear pending edit after successful save
          const newPendingEdits = { ...state.pendingEdits };
          delete newPendingEdits[noteId];
          // Sync to workspaceDocCache so persisted data is fresh on reload
          const newWorkspaceDocCache = { ...state.workspaceDocCache };
          if (state.workspaceAppId) {
            newWorkspaceDocCache[state.workspaceAppId] = {
              documentsByFolder: newCache,
              lastFetched: Date.now(),
            };
          }
          return { documentsByFolder: newCache, pendingEdits: newPendingEdits, workspaceDocCache: newWorkspaceDocCache };
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);

        const isNotFound = msg.toLowerCase().includes('not found') || msg.includes('404');

        if (isNotFound) {
          // Stale pending edit (e.g., temp/deleted note) should not block future unload checks.
          set((state) => {
            const newPendingEdits = { ...state.pendingEdits };
            delete newPendingEdits[noteId];
            return { pendingEdits: newPendingEdits };
          });
        } else {
          console.error('Failed to save note:', err);
          throw err instanceof Error ? err : new Error(msg);
        }
      } finally {
        set({ isSavingNote: false });
      }
    });
  },

  // Immediately update the store with new content (optimistic update)
  // This ensures edits survive page refresh even if API hasn't been called yet
  updateNoteOptimistic: (noteId, title, content) => {
    set((state) => {
      // Update documentsByFolder immediately
      const newCache = { ...state.documentsByFolder };
      for (const key of Object.keys(newCache)) {
        newCache[key] = newCache[key].map(d =>
          d.id === noteId ? { ...d, title, content } : d
        );
      }
      // Track as pending edit (will be persisted to localStorage)
      const newPendingEdits = {
        ...state.pendingEdits,
        [noteId]: { noteId, title, content, timestamp: Date.now() }
      };
      return { documentsByFolder: newCache, pendingEdits: newPendingEdits };
    });
  },

  // Clear a pending edit (called after successful save)
  clearPendingEdit: (noteId) => {
    set((state) => {
      const newPendingEdits = { ...state.pendingEdits };
      delete newPendingEdits[noteId];
      return { pendingEdits: newPendingEdits };
    });
  },

  flushPendingEdits: async () => {
    if (isFlushingPendingEdits) return;

    // Don't attempt to flush if auth session isn't ready yet
    const token = useAuthStore.getState().getAccessToken();
    if (!token) return;

    const pending = Object.values(get().pendingEdits).sort(
      (a, b) => a.timestamp - b.timestamp
    );
    if (pending.length === 0) return;

    isFlushingPendingEdits = true;
    try {
      for (const edit of pending) {
        const latestEdit = get().pendingEdits[edit.noteId];
        if (!latestEdit) continue;

        // Skip and clear edits with temp IDs — these notes were never created
        // server-side (e.g. page closed before the create API responded)
        if (edit.noteId.startsWith('temp-')) {
          set((state) => {
            const newPendingEdits = { ...state.pendingEdits };
            delete newPendingEdits[edit.noteId];
            return { pendingEdits: newPendingEdits };
          });
          continue;
        }

        try {
          await get().saveNote(edit.noteId, latestEdit.title, latestEdit.content);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          // If auth isn't ready yet, stop and retry next activation/hydration.
          if (msg.includes('401') || msg.includes('403')) {
            break;
          }
        }
      }
    } finally {
      isFlushingPendingEdits = false;
    }
  },

  removeDocument: async (docId) => {
    const { selectedNoteId, currentFolderId, workspaceAppId } = get();

    // Invalidate any in-flight fetches so they don't restore the deleted doc
    if (workspaceAppId) {
      const versionKey = fetchVersionKey(workspaceAppId, currentFolderId);
      fetchVersions[versionKey] = (fetchVersions[versionKey] || 0) + 1;
    }

    // Optimistic removal
    if (selectedNoteId === docId) {
      set({ selectedNoteId: null });
    }
    set((state) => {
      const updated = { ...state.documentsByFolder };
      for (const k of Object.keys(updated)) {
        updated[k] = updated[k].filter(d => d.id !== docId);
      }

      // Also update workspaceDocCache so deletion persists across refresh
      const newWorkspaceDocCache = { ...state.workspaceDocCache };
      if (workspaceAppId && newWorkspaceDocCache[workspaceAppId]) {
        const cachedFolders = { ...newWorkspaceDocCache[workspaceAppId].documentsByFolder };
        for (const k of Object.keys(cachedFolders)) {
          cachedFolders[k] = cachedFolders[k].filter(d => d.id !== docId);
        }
        newWorkspaceDocCache[workspaceAppId] = {
          ...newWorkspaceDocCache[workspaceAppId],
          documentsByFolder: cachedFolders,
        };
      }

      return { documentsByFolder: updated, workspaceDocCache: newWorkspaceDocCache };
    });

    try {
      await deleteDocument(docId);
      // Don't refetch immediately - optimistic removal is sufficient
      // and immediate refetch can restore the doc if backend hasn't fully processed
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes('not found') && !msg.includes('404')) {
        // Refetch on error to restore state
        get().fetchDocuments(get().currentFolderId);
        set({ error: msg });
      }
    }
  },

  toggleFavorite: async (docId) => {
    // Find the doc
    const { documentsByFolder } = get();
    let doc: Document | undefined;
    for (const docs of Object.values(documentsByFolder)) {
      doc = docs.find(d => d.id === docId);
      if (doc) break;
    }
    if (!doc) return;

    try {
      const updated = doc.is_favorite
        ? await unfavoriteDocument(docId)
        : await favoriteDocument(docId);
      set((state) => {
        const newCache = { ...state.documentsByFolder };
        for (const key of Object.keys(newCache)) {
          newCache[key] = newCache[key].map(d =>
            d.id === docId ? updated : d
          );
        }
        return { documentsByFolder: newCache };
      });
    } catch (err) {
      set({ error: err instanceof Error ? err.message : 'Failed to toggle favorite' });
    }
  },

  uploadFiles: async (files, parentId) => {
    const { workspaceAppId } = get();
    if (!workspaceAppId) return;

    // Resolve workspaceId from the current app mapping to avoid mismatches.
    const workspaceState = useWorkspaceStore.getState();
    const workspaceForApp = workspaceState.workspaces.find((ws) =>
      ws.apps.some((app) => app.id === workspaceAppId)
    );
    // Only send workspaceId when we can map it from the current app.
    // Falling back to activeWorkspaceId can produce mismatches.
    const workspaceId = workspaceForApp?.id;

    set({ isUploading: true, uploadProgress: 0, error: null });

    try {
      const key = folderKey(parentId);

      // Separate markdown files (sequential, lightweight) from binary uploads
      const markdownFiles: File[] = [];
      const binaryFiles: File[] = [];
      for (const file of files) {
        const isMarkdown = file.type === 'text/markdown' || file.name.endsWith('.md') || file.name.endsWith('.markdown');
        if (isMarkdown) markdownFiles.push(file);
        else binaryFiles.push(file);
      }

      // Handle markdown imports sequentially (fast, no R2 involved)
      for (const file of markdownFiles) {
        const content = await new Promise<string>((resolve, reject) => {
          const reader = new FileReader();
          reader.onload = () => resolve(reader.result as string);
          reader.onerror = () => reject(reader.error);
          reader.readAsText(file);
        });

        const title = file.name.replace(/\.(md|markdown)$/i, '');
        const newDoc = await createDocument({
          workspaceAppId,
          title,
          content,
          parentId,
        });

        set((state) => ({
          documentsByFolder: {
            ...state.documentsByFolder,
            [key]: [newDoc, ...(state.documentsByFolder[key] || [])],
          },
        }));
      }

      // Upload binary files in parallel batches (max 3 concurrent)
      const BATCH_SIZE = 3;
      let processed = 0;
      let failedCount = 0;
      const failedFiles: string[] = [];

      const uploadOne = async (file: File) => {
        try {
          const t0 = performance.now();
          const uploadInfo = await getPresignedUploadUrl({
            workspaceAppId,
            workspaceId: workspaceId ?? undefined,
            filename: file.name,
            contentType: resolveUploadMimeType(file),
            fileSize: file.size,
            parentId,
            createDocument: true,
          });
          const t1 = performance.now();
          console.log(`[Upload] ${file.name} presigned URL: ${Math.round(t1 - t0)}ms`);

          const uploadResponse = await fetch(uploadInfo.upload_url, {
            method: 'PUT',
            headers: uploadInfo.headers,
            body: file,
          });
          if (!uploadResponse.ok) {
            const errorText = await uploadResponse.text().catch(() => '');
            throw new Error(`R2 upload failed (${uploadResponse.status}): ${errorText.slice(0, 200)}`);
          }

          const t2 = performance.now();
          console.log(`[Upload] ${file.name} R2 upload (${(file.size / 1024).toFixed(1)}KB): ${Math.round(t2 - t1)}ms`);

          const result = await confirmFileUpload(uploadInfo.file_id, {
            workspaceAppId,
            parentId,
            createDocument: true,
          });
          const t3 = performance.now();
          console.log(`[Upload] ${file.name} confirm: ${Math.round(t3 - t2)}ms | total: ${Math.round(t3 - t0)}ms`);

          if (result.document) {
            const doc: Document = { ...result.document, file: { id: result.file.id, filename: result.file.filename, file_type: result.file.content_type, file_size: result.file.file_size, r2_key: result.file.r2_key, status: 'uploaded' } };
            set((state) => {
              const newDocsByFolder = {
                ...state.documentsByFolder,
                [key]: [doc, ...(state.documentsByFolder[key] || [])],
              };
              // Also update workspaceDocCache so the file persists across refresh
              const newWorkspaceDocCache = { ...state.workspaceDocCache };
              if (workspaceAppId && newWorkspaceDocCache[workspaceAppId]) {
                newWorkspaceDocCache[workspaceAppId] = {
                  ...newWorkspaceDocCache[workspaceAppId],
                  documentsByFolder: newDocsByFolder,
                  lastFetched: Date.now(),
                };
              }
              return { documentsByFolder: newDocsByFolder, workspaceDocCache: newWorkspaceDocCache };
            });
          }
        } finally {
          processed++;
          if (binaryFiles.length > 0) {
            set({ uploadProgress: Math.round((processed / binaryFiles.length) * 100) });
          }
        }
      };

      for (let i = 0; i < binaryFiles.length; i += BATCH_SIZE) {
        const batch = binaryFiles.slice(i, i + BATCH_SIZE);
        const results = await Promise.allSettled(batch.map(uploadOne));
        // Log failures but don't abort other uploads
        for (let j = 0; j < results.length; j++) {
          const r = results[j];
          if (r.status === 'rejected') {
            failedCount++;
            const failedFile = batch[j]?.name;
            if (failedFile) failedFiles.push(failedFile);
            console.error('[Upload] File failed:', r.reason);
          }
        }
      }

      set({ uploadProgress: 100 });
      if (failedCount > 0) {
        const names = failedFiles.slice(0, 3).join(', ');
        set({
          error: `Failed to upload ${failedCount} file${failedCount > 1 ? 's' : ''}${names ? ` (${names})` : ''}.`,
        });
      }
    } catch {
      set({ error: 'Upload failed. Please try again.' });
    } finally {
      set({ isUploading: false, uploadProgress: 0 });
    }
  },

  reorderDocuments: (folderId, docIds) => {
    const { workspaceAppId } = get();
    const key = folderKey(folderId);
    const docs = get().documentsByFolder[key] || [];
    const docIdSet = new Set(docIds);

    // Reorder must always operate on a full folder snapshot.
    // Guard against filtered/partial lists (e.g. active search).
    const hasFullSnapshot =
      docIds.length === docs.length &&
      docs.every((doc) => docIdSet.has(doc.id));
    if (!hasFullSnapshot) {
      return;
    }

    markLocalDocumentReorder(docIds, workspaceAppId);

    set((state) => {
      const currentDocs = state.documentsByFolder[key] || [];
      const docMap = new Map(currentDocs.map(d => [d.id, d]));
      const reordered = docIds
        .map(id => docMap.get(id))
        .filter((d): d is Document => d !== undefined);
      const remaining = currentDocs.filter(d => !docIds.includes(d.id));
      return {
        documentsByFolder: {
          ...state.documentsByFolder,
          [key]: [...reordered, ...remaining],
        },
      };
    });

    // Persist positions to backend
    const positions = docIds.map((id, index) => ({ id, position: index }));
    reorderDocumentsApi(positions).catch((err) => {
      console.error('Failed to persist reorder:', err);
      void get().fetchDocuments(folderId);
    });
  },

  setSortBy: (sortBy) => set({ sortBy }),
  setSortDirection: (dir) => set({ sortDirection: dir }),

  hydrateNoteContent: async (noteId) => {
    if (hydratingNoteIds.has(noteId)) return;
    hydratingNoteIds.add(noteId);
    try {
      const doc = await getDocument(noteId);
      rememberNoteVersionToken(doc);
      set((state) => {
        const newDocs = { ...state.documentsByFolder };
        const pendingEdit = state.pendingEdits[noteId];
        const hydratedContent = pendingEdit ? pendingEdit.content : doc.content;
        for (const key of Object.keys(newDocs)) {
          newDocs[key] = newDocs[key].map(d =>
            d.id === noteId ? { ...d, content: hydratedContent } : d
          );
        }
        return { documentsByFolder: newDocs };
      });
    } catch (err) {
      console.error('[FilesStore] Failed to hydrate note content:', err);
    } finally {
      hydratingNoteIds.delete(noteId);
    }
  },

  loadSharedDocument: async (docId) => {
    try {
      const doc = await getDocument(docId);
      if (!doc) return null;
      rememberNoteVersionToken(doc);
      set((state) => {
        const sharedDocs = state.documentsByFolder['__shared__'] || [];
        const exists = sharedDocs.some((d) => d.id === docId);
        return {
          accessDenied: null,
          documentsByFolder: {
            ...state.documentsByFolder,
            __shared__: exists
              ? sharedDocs.map((d) => (d.id === docId ? doc : d))
              : [...sharedDocs, doc],
          },
        };
      });
      return doc;
    } catch (err) {
      const status = (err as Error & { status?: number })?.status;
      if (status === 403) {
        set({ accessDenied: { resourceType: 'document', resourceId: docId } });
        return null;
      }
      console.error('[FilesStore] Failed to load shared document:', err);
      return null;
    }
  },

  clearAccessDenied: () => {
    set({ accessDenied: null });
  },

  moveDocument: async (docId, targetFolderId) => {
    const { currentFolderId, documentsByFolder } = get();
    let sourceKey = folderKey(currentFolderId);
    let sourceDocs = documentsByFolder[sourceKey] || [];
    let doc = sourceDocs.find(d => d.id === docId);

    // If not found in current folder, locate globally (expanded folder rows can drag too)
    if (!doc) {
      for (const [key, docs] of Object.entries(documentsByFolder)) {
        const found = docs.find(d => d.id === docId);
        if (found) {
          sourceKey = key;
          sourceDocs = docs;
          doc = found;
          break;
        }
      }
    }

    if (!doc) return;

    const currentParentId = doc.parent_id ?? undefined;
    if (currentParentId === targetFolderId) return;

    const targetKey = folderKey(targetFolderId);

    // Optimistic update: remove from source, add to target
    set((state) => {
      const updated = { ...state.documentsByFolder };
      // Remove from source
      updated[sourceKey] = (updated[sourceKey] || []).filter(d => d.id !== docId);
      // Add to target (at the beginning)
      const movedDoc = { ...doc, parent_id: targetFolderId };
      updated[targetKey] = [movedDoc, ...(updated[targetKey] || [])];
      return { documentsByFolder: updated };
    });

    try {
      await updateDocument(docId, { parentId: targetFolderId ?? null });
    } catch (err) {
      // Revert on error
      const foldersToRefresh = new Set([folderKey(currentFolderId), sourceKey, targetKey]);
      for (const key of foldersToRefresh) {
        void get().fetchDocuments(key === ROOT_KEY ? undefined : key);
      }
      set({ error: err instanceof Error ? err.message : 'Failed to move document' });
    }
  },

  duplicateDocument: async (docId) => {
    const { workspaceAppId, documentsByFolder } = get();
    if (!workspaceAppId) return null;

    // Find the source document
    let sourceDoc: Document | undefined;
    for (const docs of Object.values(documentsByFolder)) {
      sourceDoc = docs.find(d => d.id === docId);
      if (sourceDoc) break;
    }
    if (!sourceDoc) return null;

    // Fetch full content if not loaded
    let content = sourceDoc.content || '';
    if (!content) {
      try {
        const full = await getDocument(docId);
        content = full.content || '';
      } catch {
        // proceed with empty content
      }
    }

    const title = `${sourceDoc.title || 'Untitled'} (copy)`;
    const parentId = sourceDoc.parent_id;
    const tempId = `temp-${Date.now()}`;
    const stableKey = `dup-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const now = new Date().toISOString();

    const optimisticDoc: Document = {
      id: tempId,
      user_id: '',
      workspace_app_id: workspaceAppId,
      title,
      content,
      icon: sourceDoc.icon,
      type: sourceDoc.type || 'note',
      is_folder: false,
      parent_id: parentId,
      position: 0,
      tags: sourceDoc.tags,
      is_archived: false,
      is_favorite: false,
      is_public: false,
      created_at: now,
      updated_at: now,
      _stableKey: stableKey,
    } as Document;

    const key = folderKey(parentId);

    // Optimistic insert
    set((state) => {
      const updated = { ...state.documentsByFolder };
      const sourceIndex = updated[key]?.findIndex(d => d.id === docId) ?? -1;
      if (sourceIndex >= 0 && updated[key]) {
        // Insert right after the source doc
        updated[key] = [
          ...updated[key].slice(0, sourceIndex + 1),
          optimisticDoc,
          ...updated[key].slice(sourceIndex + 1),
        ];
      } else {
        updated[key] = [optimisticDoc, ...(updated[key] || [])];
      }
      if (parentId && updated[ROOT_KEY]) {
        const rootIndex = updated[ROOT_KEY].findIndex(d => d.id === docId);
        if (rootIndex >= 0) {
          updated[ROOT_KEY] = [
            ...updated[ROOT_KEY].slice(0, rootIndex + 1),
            optimisticDoc,
            ...updated[ROOT_KEY].slice(rootIndex + 1),
          ];
        } else {
          updated[ROOT_KEY] = [optimisticDoc, ...updated[ROOT_KEY]];
        }
      }
      return { documentsByFolder: updated };
    });

    try {
      const newDoc = await createDocument({
        workspaceAppId,
        title,
        content,
        icon: sourceDoc.icon,
        parentId,
        tags: sourceDoc.tags,
      });
      rememberNoteVersionToken(newDoc);

      set((state) => {
        const updated = { ...state.documentsByFolder };
        const realDoc = { ...newDoc, _stableKey: stableKey };
        updated[key] = (updated[key] || []).map(d => d.id === tempId ? realDoc : d);
        if (parentId && updated[ROOT_KEY]) {
          updated[ROOT_KEY] = updated[ROOT_KEY].map(d => d.id === tempId ? realDoc : d);
        }
        return { documentsByFolder: updated };
      });
      return newDoc;
    } catch (err) {
      set((state) => {
        const updated = { ...state.documentsByFolder };
        updated[key] = (updated[key] || []).filter(d => d.id !== tempId);
        if (parentId && updated[ROOT_KEY]) {
          updated[ROOT_KEY] = updated[ROOT_KEY].filter(d => d.id !== tempId);
        }
        return {
          documentsByFolder: updated,
          error: err instanceof Error ? err.message : 'Failed to duplicate document',
        };
      });
      return null;
    }
  },

  getCurrentDocuments: () => {
    const { documentsByFolder, currentFolderId, searchQuery, sortBy, sortDirection } = get();
    let docs = documentsByFolder[folderKey(currentFolderId)] || [];

    // Apply search filter
    if (searchQuery) {
      docs = docs.filter(d =>
        d.title.toLowerCase().includes(searchQuery.toLowerCase())
      );
    }

    // Sort client-side to ensure consistent ordering regardless of background fetches
    // This prevents notes from jumping around when saves update their updated_at
    if (sortBy === 'manual') {
      return docs;
    }

    const sorted = [...docs].sort((a, b) => {
      let comparison = 0;

      switch (sortBy) {
        case 'name':
          comparison = (a.title || '').localeCompare(b.title || '');
          break;
        case 'size':
          const sizeA = a.file?.file_size || a.file_size || 0;
          const sizeB = b.file?.file_size || b.file_size || 0;
          comparison = sizeA - sizeB;
          break;
        case 'date':
        default:
          const dateA = new Date(a.updated_at || a.created_at).getTime();
          const dateB = new Date(b.updated_at || b.created_at).getTime();
          comparison = dateA - dateB;
          break;
      }

      return sortDirection === 'desc' ? -comparison : comparison;
    });

    return sorted;
  },

  getSelectedNote: () => {
    const { selectedNoteId, documentsByFolder } = get();
    if (!selectedNoteId) return null;
    for (const docs of Object.values(documentsByFolder)) {
      const found = docs.find(d => d.id === selectedNoteId);
      if (found) return found;
    }
    return null;
  },
    }),
    {
      name: 'core-files-storage-v2',
      storage: {
        getItem: (name) => {
          const value = localStorage.getItem(name);
          return value ? JSON.parse(value) : null;
        },
        setItem: (name, value) => {
          try {
            localStorage.setItem(name, JSON.stringify(value));
          } catch {
            // Payload is just pendingEdits — if it still fails, clear it
            localStorage.removeItem(name);
          }
        },
        removeItem: (name) => localStorage.removeItem(name),
      },
      partialize: (state): FilesPersistedState => ({
        // Only persist pendingEdits for crash recovery.
        // workspaceDocCache lives in memory only — the preloader re-fetches on load.
        pendingEdits: state.pendingEdits,
      }),
      merge: (persisted, current) => {
        const persistedState = persisted as {
          pendingEdits?: Record<string, PendingEdit>;
        };

        return {
          ...current,
          pendingEdits: persistedState?.pendingEdits || {},
        };
      },
      onRehydrateStorage: () => (state, error) => {
        if (error || !state) return;
        // Replay queue to server after restoring local crash-recovery state.
        setTimeout(() => {
          void state.flushPendingEdits();
        }, 0);
      },
    }
  )
);
