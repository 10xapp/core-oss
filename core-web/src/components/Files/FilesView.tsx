import {
  lazy,
  Suspense,
  useState,
  useEffect,
  useCallback,
  useRef,
  useMemo,
} from "react";
import { useParams, useNavigate, useSearchParams } from "react-router-dom";
import { createPortal } from "react-dom";
import { motion, AnimatePresence } from "motion/react";
import {
  ChevronRightIcon,
  ChevronUpDownIcon,
  EllipsisHorizontalIcon,
  ArrowUturnLeftIcon,
  UsersIcon,
} from "@heroicons/react/24/outline";
import { HugeiconsIcon } from "@hugeicons/react";
import {
  Folder01Icon,
  FolderAddIcon,
  File01Icon,
  Image01Icon,
  Video01Icon,
  File02Icon,
  VolumeHighIcon,
  Upload04Icon,
  Cancel01Icon,
  Download04Icon,
  Add01Icon,
  PencilEdit01Icon,
  Copy01Icon,
  FolderExportIcon,
  Delete02Icon,
} from "@hugeicons-pro/core-stroke-standard";
import {
  DndContext,
  pointerWithin,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useDraggable,
  useSensor,
  useSensors,
  useDroppable,
  DragOverlay,
  type DragEndEvent,
  type DragStartEvent,
  type DragMoveEvent,
  type CollisionDetection,
} from "@dnd-kit/core";
import {
  SortableContext,
  verticalListSortingStrategy,
  useSortable,
} from "@dnd-kit/sortable";
import Dropdown from "../Dropdown/Dropdown";
import ConfirmModal from "../Projects/components/ConfirmModal";
import type { Editor } from "@tiptap/react";
import { getFileDownloadUrl, getPresignedUploadUrl, confirmFileUpload, type Document } from "../../api/client";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import { useFilesStore, updateNoteVersionToken } from "../../stores/filesStore";
import { useAuthStore } from "../../stores/authStore";
import { usePermissionStore } from "../../stores/permissionStore";
import { SIDEBAR } from "../../lib/sidebar";
import { API_BASE } from "../../lib/apiBase";

type SaveStatus = "saved" | "saving" | "unsaved" | "error";

// Fire-and-forget save using keepalive fetch — survives page unload
function sendKeepaliveSave(
  noteId: string,
  title: string,
  content: string,
) {
  // Invalidate the optimistic-lock token because this fire-and-forget save
  // bypasses it.  The backend will bump updated_at on success, so any token
  // we hold is now stale.  Clearing it means the next normal save will omit
  // expected_updated_at and succeed without a 409.
  updateNoteVersionToken(noteId);

  const token = useAuthStore.getState().getAccessToken();
  fetch(`${API_BASE}/documents/${noteId}`, {
    method: "PATCH",
    headers: {
      "Content-Type": "application/json",
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify({
      title,
      content,
    }),
    keepalive: true,
  }).catch(() => {});
}

function flushAllPendingWithKeepalive(): boolean {
  const state = useFilesStore.getState();
  const pending = Object.values(state.pendingEdits);
  if (pending.length === 0) return false;

  for (const edit of pending) {
    sendKeepaliveSave(edit.noteId, edit.title, edit.content);
  }

  return true;
}
import VersionHistoryPanel from "./VersionHistoryPanel";
import { HeaderButtons } from "../MiniAppHeader";
import FilesSettingsModal from "./FilesSettingsModal";
import RequestAccessCard from "../RequestAccessCard";
import { useUIStore } from "../../stores/uiStore";

// Helper to handle chunk load failures (e.g., after deployment with new hashes)
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function lazyWithRetry(importFn: () => Promise<any>) {
  return lazy(() =>
    importFn().catch(() => {
      // Chunk failed to load - reload the page to get fresh assets
      window.location.reload();
      // Return a placeholder while reloading
      return { default: () => null };
    })
  );
}

const PdfViewer = lazyWithRetry(() => import("./PdfViewer"));
const DocxViewer = lazyWithRetry(() => import("./DocxViewer"));
const PptxViewer = lazyWithRetry(() => import("./PptxViewer"));
const VideoViewer = lazyWithRetry(() => import("./VideoViewer"));
const AudioViewer = lazyWithRetry(() => import("./AudioViewer"));
const XlsxViewer = lazyWithRetry(() => import("./XlsxViewer"));
const NoteEditor = lazyWithRetry(() => import("./NoteEditor"));
const NoteToolbar = lazy(async () => {
  try {
    const mod = await import("./NoteEditor");
    return { default: mod.NoteToolbar };
  } catch {
    window.location.reload();
    return { default: () => null };
  }
});

// Sortable wrapper for document items - Notion-style (no shifting, just drag handle)
function SortableDocItem({
  id,
  children,
}: {
  id: string;
  children: (props: { dragAttributes: ReturnType<typeof useSortable>["attributes"]; dragListeners: ReturnType<typeof useSortable>["listeners"] }) => React.ReactNode;
}) {
  const { attributes, listeners, setNodeRef, isDragging } = useSortable({ id });
  // No transform - items stay in place. Just hide the dragged item.
  const style = {
    opacity: isDragging ? 0.4 : 1,
  };
  return (
    <div ref={setNodeRef} style={style}>
      {children({ dragAttributes: attributes, dragListeners: listeners })}
    </div>
  );
}

// Draggable wrapper for nested items (items inside expanded folders)
function DraggableDocItem({
  id,
  children,
}: {
  id: string;
  children: (props: { dragAttributes: ReturnType<typeof useDraggable>["attributes"]; dragListeners: ReturnType<typeof useDraggable>["listeners"] }) => React.ReactNode;
}) {
  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({ id });
  // Don't move or show the original item while dragging - only show the drag overlay
  const style = {
    opacity: isDragging ? 0 : 1,
  };
  return (
    <div ref={setNodeRef} style={style}>
      {children({ dragAttributes: attributes, dragListeners: listeners })}
    </div>
  );
}

// Droppable wrapper for folder drop targets
function DroppableFolder({
  id,
  children,
}: {
  id: string;
  children: (props: { isOver: boolean }) => React.ReactNode;
}) {
  const { setNodeRef, isOver } = useDroppable({ id });
  return <div ref={setNodeRef}>{children({ isOver })}</div>;
}

// Get file mime type from document (handles nested file object)
function getFileMimeType(doc: Document): string {
  return doc.file?.file_type || doc.file_type || "";
}

// Check if document is a note
function isNote(doc: Document): boolean {
  return (
    doc.type === "note" ||
    (!doc.file_id && !doc.file_url && !doc.is_folder && doc.type !== "folder" && doc.type !== "file")
  );
}

// Get icon size from class string
function getIconSize(sizeClass: string): number {
  if (sizeClass.includes("w-3")) return 12;
  if (sizeClass.includes("w-4")) return 16;
  if (sizeClass.includes("w-5")) return 20;
  if (sizeClass.includes("w-6")) return 24;
  if (sizeClass.includes("w-8")) return 32;
  if (sizeClass.includes("w-12")) return 48;
  return 20;
}

// Get icon for file type
function getFileIcon(doc: Document, size: string = "w-5 h-5") {
  const iconSize = getIconSize(size);

  // Folders
  if (doc.is_folder || doc.type === "folder") {
    return <HugeiconsIcon icon={Folder01Icon} size={iconSize} />;
  }

  // Notes (editable text documents)
  if (isNote(doc)) {
    return <HugeiconsIcon icon={File02Icon} size={iconSize} />;
  }

  // Files (uploaded files) - check mime type
  const mimeType = getFileMimeType(doc);
  if (mimeType.startsWith("image/")) {
    return <HugeiconsIcon icon={Image01Icon} size={iconSize} />;
  }
  if (mimeType.startsWith("video/")) {
    return <HugeiconsIcon icon={Video01Icon} size={iconSize} />;
  }
  if (mimeType.startsWith("audio/")) {
    return <HugeiconsIcon icon={VolumeHighIcon} size={iconSize} />;
  }
  if (mimeType === "application/pdf") {
    return <HugeiconsIcon icon={File02Icon} size={iconSize} />;
  }
  if (
    mimeType ===
      "application/vnd.openxmlformats-officedocument.presentationml.presentation" ||
    mimeType === "application/vnd.ms-powerpoint"
  ) {
    return <HugeiconsIcon icon={File02Icon} size={iconSize} />;
  }
  if (
    mimeType.includes("zip") ||
    mimeType.includes("compressed") ||
    mimeType.includes("archive")
  ) {
    return <HugeiconsIcon icon={File01Icon} size={iconSize} />;
  }
  if (
    mimeType.startsWith("text/") ||
    mimeType.includes("document") ||
    mimeType.includes("word")
  ) {
    return <HugeiconsIcon icon={File02Icon} size={iconSize} />;
  }
  return <HugeiconsIcon icon={File01Icon} size={iconSize} />;
}

// Format date like Apple Notes - time for today, "Yesterday", or short date
function formatDateRelative(dateString?: string): string {
  if (!dateString) return "";
  const date = new Date(dateString);
  const now = new Date();

  // Check if same day
  const isToday = date.toDateString() === now.toDateString();
  if (isToday) {
    return date.toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  }

  // Check if yesterday
  const yesterday = new Date(now);
  yesterday.setDate(yesterday.getDate() - 1);
  const isYesterday = date.toDateString() === yesterday.toDateString();
  if (isYesterday) {
    return "Yesterday";
  }

  // Otherwise show short date
  return date.toLocaleDateString([], { month: "short", day: "numeric" });
}

const SAVE_DEBOUNCE_MS = 2000;

// Recursive component to render nested folder contents
function NestedFolderContents({
  items,
  depth,
  expandedFolders,
  documentsByFolder,
  selectedNoteId,
  onFolderToggle,
  onFolderOpen,
  onItemClick,
  onRename,
  onDelete,
  editingItemId,
  editingItemName,
  setEditingItemName,
  onRenameKeyDown,
  onRenameSubmit,
}: {
  items: Document[];
  depth: number;
  expandedFolders: Set<string>;
  documentsByFolder: Record<string, Document[]>;
  selectedNoteId: string | null;
  onFolderToggle: (folder: Document) => void;
  onFolderOpen: (folder: Document) => void;
  onItemClick: (doc: Document) => void;
  onRename: (doc: Document) => void;
  onDelete: (doc: Document, e: React.MouseEvent) => void;
  editingItemId: string | null;
  editingItemName: string;
  setEditingItemName: (name: string) => void;
  onRenameKeyDown: (e: React.KeyboardEvent, docId: string) => void;
  onRenameSubmit: (docId: string) => void;
}) {
  const [openMenuId, setOpenMenuId] = useState<string | null>(null);
  const menuRefs = useRef<Map<string, HTMLButtonElement>>(new Map());

  return (
    <div className="pl-3 space-y-0.5">
      {items.map((item) => {
        const isFolder = item.is_folder || item.type === "folder";
        const isNestedExpanded = expandedFolders.has(item.id);
        const nestedContents = documentsByFolder[item.id] || [];

        if (isFolder) {
          return (
            <DraggableDocItem key={item.id} id={item.id}>
              {({ dragAttributes, dragListeners }) => (
                <div>
                  <div
                    {...dragAttributes}
                    {...dragListeners}
                    onClick={() => onFolderToggle(item)}
                    className={`w-full flex items-center pr-3 h-[32px] rounded-md text-sm transition-colors cursor-pointer hover:bg-black/5 ${SIDEBAR.item} active:cursor-grabbing group`}
                  >
                    <span className="shrink-0 ml-3 mr-2 relative w-4 h-4">
                      <HugeiconsIcon icon={Folder01Icon} size={16} className={`absolute inset-0 transition-opacity ${isNestedExpanded ? "opacity-0" : "group-hover:opacity-0"}`} />
                      <ChevronRightIcon className={`w-4 h-4 absolute inset-0 transition-all duration-150 ${isNestedExpanded ? "opacity-100 rotate-90" : "opacity-0 group-hover:opacity-100"}`} />
                    </span>
                    {editingItemId === item.id ? (
                      <input
                        type="text"
                        value={editingItemName}
                        onChange={(e) => setEditingItemName(e.target.value)}
                        onKeyDown={(e) => onRenameKeyDown(e, item.id)}
                        onBlur={() => onRenameSubmit(item.id)}
                        onClick={(e) => e.stopPropagation()}
                        autoFocus
                        placeholder="New Folder"
                        className="flex-1 text-sm bg-transparent px-1 py-0.5 focus:outline-none placeholder:text-text-tertiary"
                      />
                    ) : (
                      <span className="flex-1 text-left truncate">
                        {item.title || "Untitled"}
                      </span>
                    )}
                    {/* Three-dot menu + count overlay in same position */}
                    {editingItemId !== item.id && (
                    <div className="relative shrink-0">
                      <button
                        ref={(el) => {
                          if (el) menuRefs.current.set(item.id, el);
                        }}
                        onClick={(e) => {
                          e.stopPropagation();
                          setOpenMenuId(openMenuId === item.id ? null : item.id);
                        }}
                        className="p-0.5 rounded text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors opacity-0 group-hover:opacity-100"
                        title="More options"
                      >
                        <EllipsisHorizontalIcon className="w-3 h-3" />
                      </button>
                      {nestedContents.length > 0 && (
                        <span className="absolute inset-0 flex items-center justify-center text-xs text-text-tertiary tabular-nums pointer-events-none group-hover:hidden">
                          {nestedContents.length}
                        </span>
                      )}
                      <Dropdown
                        isOpen={openMenuId === item.id}
                        onClose={() => setOpenMenuId(null)}
                        trigger={{ current: menuRefs.current.get(item.id) || null }}
                      >
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setOpenMenuId(null);
                            onRename(item);
                          }}
                          className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                        >
                          <HugeiconsIcon icon={PencilEdit01Icon} size={14} />
                          Rename
                        </button>
                        <button
                          onClick={(e) => {
                            setOpenMenuId(null);
                            onDelete(item, e);
                          }}
                          className="w-full px-3 py-1.5 text-left text-sm text-red-500 hover:bg-bg-gray flex items-center gap-2"
                        >
                          <HugeiconsIcon icon={Delete02Icon} size={14} />
                          Delete
                        </button>
                      </Dropdown>
                    </div>
                    )}
                  </div>
                  <AnimatePresence>
                    {isNestedExpanded && nestedContents.length > 0 && (
                      <motion.div
                        initial={{ height: 0, opacity: 0 }}
                        animate={{ height: "auto", opacity: 1 }}
                        exit={{ height: 0, opacity: 0 }}
                        transition={{ duration: 0.15 }}
                        className="overflow-hidden"
                      >
                        <NestedFolderContents
                          items={nestedContents}
                          depth={depth + 1}
                          expandedFolders={expandedFolders}
                          documentsByFolder={documentsByFolder}
                          selectedNoteId={selectedNoteId}
                          onFolderToggle={onFolderToggle}
                          onFolderOpen={onFolderOpen}
                          onItemClick={onItemClick}
                          onRename={onRename}
                          onDelete={onDelete}
                          editingItemId={editingItemId}
                          editingItemName={editingItemName}
                          setEditingItemName={setEditingItemName}
                          onRenameKeyDown={onRenameKeyDown}
                          onRenameSubmit={onRenameSubmit}
                        />
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>
              )}
            </DraggableDocItem>
          );
        }

        return (
          <DraggableDocItem key={item.id} id={item.id}>
            {({ dragAttributes, dragListeners }) => (
              <div
                {...dragAttributes}
                {...dragListeners}
                onClick={() => onItemClick(item)}
                className={`w-full flex items-center pr-3 h-[32px] rounded-md text-sm transition-colors cursor-pointer active:cursor-grabbing group ${
                  selectedNoteId === item.id
                    ? SIDEBAR.selected
                    : `${SIDEBAR.item} hover:bg-black/5`
                }`}
              >
                <span className="shrink-0 ml-3 mr-2">
                  {getFileIcon(item, "w-4 h-4")}
                </span>
                {editingItemId === item.id ? (
                  <input
                    type="text"
                    value={editingItemName}
                    onChange={(e) => setEditingItemName(e.target.value)}
                    onKeyDown={(e) => onRenameKeyDown(e, item.id)}
                    onBlur={() => onRenameSubmit(item.id)}
                    onClick={(e) => e.stopPropagation()}
                    autoFocus
                    placeholder="Untitled"
                    className="flex-1 text-sm bg-transparent px-1 py-0.5 focus:outline-none placeholder:text-text-tertiary"
                  />
                ) : (
                  <span className="flex-1 text-left truncate">
                    {item.title || "Untitled"}
                  </span>
                )}
                {/* Three-dot menu */}
                {editingItemId !== item.id && (
                <div className="relative shrink-0">
                  <button
                    ref={(el) => {
                      if (el) menuRefs.current.set(item.id, el);
                    }}
                    onClick={(e) => {
                      e.stopPropagation();
                      setOpenMenuId(openMenuId === item.id ? null : item.id);
                    }}
                    className={`p-0.5 rounded text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors ${
                      selectedNoteId === item.id ? "opacity-100" : "opacity-0 group-hover:opacity-100"
                    }`}
                  >
                    <EllipsisHorizontalIcon className="w-3 h-3" />
                  </button>
                      <Dropdown
                        isOpen={openMenuId === item.id}
                        onClose={() => setOpenMenuId(null)}
                        trigger={{ current: menuRefs.current.get(item.id) || null }}
                      >
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            setOpenMenuId(null);
                            onRename(item);
                      }}
                      className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                    >
                      <HugeiconsIcon icon={PencilEdit01Icon} size={14} />
                      Rename
                    </button>
                    <button
                      onClick={(e) => {
                        setOpenMenuId(null);
                        onDelete(item, e);
                      }}
                      className="w-full px-3 py-1.5 text-left text-sm text-red-500 hover:bg-bg-gray flex items-center gap-2"
                    >
                      <HugeiconsIcon icon={Delete02Icon} size={14} />
                      Delete
                    </button>
                  </Dropdown>
                </div>
                )}
              </div>
            )}
          </DraggableDocItem>
        );
      })}
    </div>
  );
}

export default function FilesView() {
  const { workspaceId, documentId: urlDocumentId } = useParams<{ workspaceId: string; documentId?: string }>();
  const navigate = useNavigate();
  const workspaces = useWorkspaceStore((state) => state.workspaces);
  const currentUser = useAuthStore((state) => state.user);
  const sharedWithMe = usePermissionStore((state) => state.sharedWithMe);
  const toggleVersionHistory = useUIStore((state) => state.toggleVersionHistory);
  const isLoadingSharedWithMe = usePermissionStore((state) => state.isLoadingSharedWithMe);
  const fetchSharedWithMe = usePermissionStore((state) => state.fetchSharedWithMe);

  // Find the files app for this specific workspace
  const workspace = workspaces.find((w) => w.id === workspaceId);
  const filesApp = workspace?.apps.find((app) => app.type === "files");

  // Store state - select raw values, compute derived values with useMemo
  const documentsByFolder = useFilesStore((state) => state.documentsByFolder);
  const sortBy = useFilesStore((state) => state.sortBy);
  const sortDirection = useFilesStore((state) => state.sortDirection);
  const isLoading = useFilesStore((state) => state.isLoading);
  const error = useFilesStore((state) => state.error);
  const accessDenied = useFilesStore((state) => state.accessDenied);
  const clearAccessDenied = useFilesStore((state) => state.clearAccessDenied);
  const searchQuery = useFilesStore((state) => state.searchQuery);
  const isUploading = useFilesStore((state) => state.isUploading);
  const uploadProgress = useFilesStore((state) => state.uploadProgress);
  const pendingEdits = useFilesStore((state) => state.pendingEdits);

  const selectedNoteId = useFilesStore((state) => state.selectedNoteId);
  const currentFolderId = useFilesStore((state) => state.currentFolderId);

  // Compute documents from raw state (avoids infinite loop from calling getter in selector)
  const documents = useMemo(() => {
    const ROOT_KEY = "__root__";
    const folderKey = currentFolderId || ROOT_KEY;
    let docs = documentsByFolder[folderKey] || [];

    // Apply search filter
    if (searchQuery) {
      docs = docs.filter((d) =>
        d.title.toLowerCase().includes(searchQuery.toLowerCase()),
      );
    }

    // Sort all items together (folders, notes, files intermixed)
    if (sortBy === "manual") {
      return docs; // Position order from store
    }

    return [...docs].sort((a, b) => {
      let comparison = 0;
      switch (sortBy) {
        case "name":
          comparison = (a.title || "").localeCompare(b.title || "");
          break;
        case "size": {
          const sizeA = a.file?.file_size || a.file_size || 0;
          const sizeB = b.file?.file_size || b.file_size || 0;
          comparison = sizeA - sizeB;
          break;
        }
        case "date":
        default: {
          const dateA = new Date(a.updated_at || a.created_at).getTime();
          const dateB = new Date(b.updated_at || b.created_at).getTime();
          comparison = dateA - dateB;
          break;
        }
      }
      return sortDirection === "desc" ? -comparison : comparison;
    });
  }, [documentsByFolder, currentFolderId, searchQuery, sortBy, sortDirection]);

  const canReorder = searchQuery.trim().length === 0;

  // Store actions
  const setWorkspaceAppId = useFilesStore((state) => state.setWorkspaceAppId);
  const fetchDocuments = useFilesStore((state) => state.fetchDocuments);
  const navigateToFolder = useFilesStore((state) => state.navigateToFolder);
  const navigateToBreadcrumb = useFilesStore((state) => state.navigateToBreadcrumb);
  const breadcrumbs = useFilesStore((state) => state.breadcrumbs);
  const moveDocument = useFilesStore((state) => state.moveDocument);
  const duplicateDocument = useFilesStore((state) => state.duplicateDocument);

  const setSelectedNote = useFilesStore((state) => state.setSelectedNote);
  const addNote = useFilesStore((state) => state.addNote);
  const addFolder = useFilesStore((state) => state.addFolder);
  const removeDocument = useFilesStore((state) => state.removeDocument);
  const uploadFiles = useFilesStore((state) => state.uploadFiles);
  const storeSaveNote = useFilesStore((state) => state.saveNote);
  const flushAllPendingEdits = useFilesStore((state) => state.flushPendingEdits);
  const updateNoteOptimistic = useFilesStore((state) => state.updateNoteOptimistic);
  const setActivelyEditing = useFilesStore((state) => state.setActivelyEditing);
  const hydrateNoteContent = useFilesStore((state) => state.hydrateNoteContent);
  const setSortBy = useFilesStore((state) => state.setSortBy);
  const setSortDirection = useFilesStore((state) => state.setSortDirection);
  const reorderDocuments = useFilesStore((state) => state.reorderDocuments);
  const renamingId = useFilesStore((state) => state.renamingId);
  const setRenamingId = useFilesStore((state) => state.setRenamingId);
  const loadSharedDocument = useFilesStore((state) => state.loadSharedDocument);

  // DnD sensors with activation constraint to avoid triggering on clicks
  const sensors = useSensors(
    useSensor(PointerSensor, { activationConstraint: { distance: 5 } }),
    useSensor(KeyboardSensor),
  );

  // Track active drag for overlay
  const [activeDragId, setActiveDragId] = useState<string | null>(null);
  const allDocuments = useMemo(
    () => Object.values(documentsByFolder).flat(),
    [documentsByFolder],
  );
  const activeDragItem = useMemo(() => {
    if (!activeDragId) return null;
    return allDocuments.find((d) => d.id === activeDragId) || null;
  }, [activeDragId, allDocuments]);

  // Track drop intent for visual indicators
  const [dropIntent, setDropIntent] = useState<{
    type: 'reorder' | 'moveInto';
    targetId: string;
    insertPosition?: 'before' | 'after'; // For reorder: show line before or after targetId
  } | null>(null);

  // Dwell time for folder nesting
  const dwellTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const dwellTargetRef = useRef<string | null>(null);

  const clearDwellTimer = useCallback(() => {
    if (dwellTimerRef.current) {
      clearTimeout(dwellTimerRef.current);
      dwellTimerRef.current = null;
    }
  }, []);

  const handleDragStart = useCallback((event: DragStartEvent) => {
    setActiveDragId(event.active.id as string);
    setDropIntent(null);
    clearDwellTimer();
    dwellTargetRef.current = null;
  }, [clearDwellTimer]);

  const handleDragCancel = useCallback(() => {
    setActiveDragId(null);
    setDropIntent(null);
    clearDwellTimer();
    dwellTargetRef.current = null;
  }, [clearDwellTimer]);

  const parentDropId = "drop:parent";

  // Collision detection: closestCenter is the most reliable for sortable lists.
  // Folder nesting is handled by dwell timer in handleDragMove, not collision detection.
  const collisionDetection: CollisionDetection = useCallback(
    (args) => {
      // Try pointerWithin first for parentDropId precision
      const pointerCollisions = pointerWithin(args);
      if (pointerCollisions.length > 0) return pointerCollisions;
      return closestCenter(args);
    },
    [],
  );

  // Handle drag move: default to reorder, switch to moveInto after dwelling on a folder
  const handleDragMove = useCallback(
    (event: DragMoveEvent) => {
      const { active, over } = event;
      if (!over || active.id === over.id) {
        setDropIntent(null);
        clearDwellTimer();
        dwellTargetRef.current = null;
        return;
      }

      const overId = over.id as string;
      const activeId = active.id as string;

      // Check if over parent drop zone — no intent, DroppableFolder's isOver handles visual
      if (overId === parentDropId) {
        setDropIntent(null);
        clearDwellTimer();
        dwellTargetRef.current = null;
        return;
      }

      // Determine insert position based on which half of the item the pointer is in
      const getInsertPosition = (): 'before' | 'after' => {
        if (!over.rect || !event.activatorEvent) return 'after';
        const rect = over.rect;
        const pointerY = (event.activatorEvent as PointerEvent).clientY + (event.delta?.y || 0);
        const progress = (pointerY - rect.top) / rect.height;
        return progress < 0.5 ? 'before' : 'after';
      };

      // Check if target is a folder
      const targetItem = allDocuments.find((d) => d.id === overId);
      const isTargetFolder = (targetItem?.is_folder || targetItem?.type === "folder") && activeId !== overId;

      if (isTargetFolder) {
        // If already dwelling on this folder and moveInto is set, keep it
        if (dwellTargetRef.current === overId && dropIntent?.type === 'moveInto' && dropIntent.targetId === overId) {
          return;
        }

        // If this is a new folder target, start dwell timer
        if (dwellTargetRef.current !== overId) {
          clearDwellTimer();
          dwellTargetRef.current = overId;

          // Show reorder intent immediately
          setDropIntent({ type: 'reorder', targetId: overId, insertPosition: getInsertPosition() });

          // After 300ms, switch to moveInto (nest into folder)
          dwellTimerRef.current = setTimeout(() => {
            setDropIntent({ type: 'moveInto', targetId: overId });
          }, 300);
          return;
        }

        // Still on same folder but timer hasn't fired yet — update insert position
        if (dropIntent?.type !== 'moveInto') {
          setDropIntent({ type: 'reorder', targetId: overId, insertPosition: getInsertPosition() });
        }
        return;
      }

      // Not a folder — clear dwell timer and set reorder
      clearDwellTimer();
      dwellTargetRef.current = null;
      setDropIntent({ type: 'reorder', targetId: overId, insertPosition: getInsertPosition() });
    },
    [allDocuments, parentDropId, clearDwellTimer, dropIntent],
  );

  const handleDragEnd = useCallback(
    (event: DragEndEvent) => {
      const currentDropIntent = dropIntent; // Capture before clearing
      setActiveDragId(null);
      setDropIntent(null);
      clearDwellTimer();
      dwellTargetRef.current = null;
      const { active, over } = event;
      if (!over || active.id === over.id) return;

      const overId = over.id as string;
      const activeId = active.id as string;
      const activeItem = allDocuments.find((d) => d.id === activeId);
      const insertPosition = currentDropIntent?.insertPosition || 'after';

      // Check if dropped on "back to parent" zone
      if (overId === parentDropId) {
        const parentFolderId = breadcrumbs.length >= 2
          ? breadcrumbs[breadcrumbs.length - 2]?.id
          : undefined;
        moveDocument(activeId, parentFolderId);
        return;
      }

      // Check if dropIntent says moveInto a folder (set by handleDragMove zone detection)
      if (currentDropIntent?.type === 'moveInto' && currentDropIntent.targetId !== activeId) {
        moveDocument(activeId, currentDropIntent.targetId);
        return;
      }

      // Check if dragged item is from an expanded folder (has parent_id, at root level)
      if (activeItem?.parent_id && !currentFolderId) {
        // Move to root first, then reorder to the correct position
        // Find where the item should be inserted based on the drop target
        const targetIndex = documents.findIndex((d) => d.id === overId);
        if (targetIndex !== -1) {
          // Calculate the new position in the documents array
          const newOrder = documents.map(d => d.id).filter(id => id !== activeId);
          const insertIndex = insertPosition === 'before' ? targetIndex : targetIndex + 1;
          newOrder.splice(insertIndex, 0, activeId);

          // Move to root and reorder
          moveDocument(activeId, undefined);
          if (sortBy !== "manual") {
            setSortBy("manual");
            setSortDirection("asc");
          }
          reorderDocuments(undefined, newOrder);
        } else {
          // Fallback: just move to root
          moveDocument(activeId, undefined);
        }
        return;
      }

      // Otherwise it's a reorder within the current list
      if (!canReorder) return;

      // Helper to reorder with insert position
      const reorderWithPosition = (items: typeof documents, activeId: string, targetId: string) => {
        const oldIndex = items.findIndex((d) => d.id === activeId);
        let targetIndex = items.findIndex((d) => d.id === targetId);
        if (oldIndex === -1 || targetIndex === -1) return null;

        const reordered = [...items];
        const [moved] = reordered.splice(oldIndex, 1);

        // Adjust target index after removal
        if (oldIndex < targetIndex) targetIndex--;

        // Insert before or after based on position
        const insertIndex = insertPosition === 'before' ? targetIndex : targetIndex + 1;
        reordered.splice(insertIndex, 0, moved);

        return reordered;
      };

      // Reorder within the unified documents list
      const reordered = reorderWithPosition(documents, activeId, overId);
      if (!reordered) return;

      if (sortBy !== "manual") {
        setSortBy("manual");
        setSortDirection("asc");
      }
      reorderDocuments(currentFolderId, reordered.map((d) => d.id));
    },
    [
      canReorder,
      documents,
      allDocuments,
      sortBy,
      setSortBy,
      setSortDirection,
      currentFolderId,
      reorderDocuments,
      moveDocument,
      breadcrumbs,
      clearDwellTimer,
      dropIntent,
    ],
  );

  // Compute selected note from raw state (avoids infinite loop from calling getter in selector)
  const selectedNote = useMemo(() => {
    if (!selectedNoteId) return null;
    for (const docs of Object.values(documentsByFolder)) {
      const found = docs.find((d) => d.id === selectedNoteId);
      if (found) return found;
    }
    return null;
  }, [selectedNoteId, documentsByFolder]);

  // Check if the selected item is an image file
  const isSelectedImage = selectedNote ? getFileMimeType(selectedNote).startsWith("image/") : false;

  // Resolve image URL for inline viewer
  const [inlineImageUrl, setInlineImageUrl] = useState<string | null>(null);
  useEffect(() => {
    if (!selectedNote || !isSelectedImage) {
      setInlineImageUrl(null);
      return;
    }
    let cancelled = false;
    const resolve = async () => {
      let url = selectedNote.preview_url || selectedNote.file_url;
      if (!url && selectedNote.file_id) {
        try {
          const result = await getFileDownloadUrl(selectedNote.file_id);
          url = result.url;
        } catch (err) {
          console.error("Failed to resolve image URL:", err);
        }
      }
      if (!cancelled && url) setInlineImageUrl(url);
    };
    void resolve();
    return () => { cancelled = true; };
  }, [selectedNote?.id, isSelectedImage]);

  // Determine if the selected note is editable (owned or write/admin permission)
  const isNoteEditable = useMemo(() => {
    if (!selectedNote) return true;
    // Owner can always edit
    if (selectedNote.user_id === currentUser?.id) return true;
    // Check shared permission
    const shared = sharedWithMe.find((s) => s.resource_id === selectedNote.id);
    if (shared) return shared.permission === 'write' || shared.permission === 'admin';
    // Default: allow (workspace member editing through normal RLS)
    return true;
  }, [selectedNote, currentUser?.id, sharedWithMe]);

  // Local UI state (not data)
  const [noteContent, setNoteContent] = useState("");
  const [noteTitle, setNoteTitle] = useState("");
  const [lastSavedAt, setLastSavedAt] = useState<string | null>(null);
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("saved");
  const saveTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const latestSaveRequestIdRef = useRef(0);
  // Refs to hold current values for debounced save (avoids stale closures)
  const noteTitleRef = useRef(noteTitle);
  const noteContentRef = useRef(noteContent);
  // Track which note's content we've loaded to prevent re-syncing on store updates
  const lastLoadedNoteIdRef = useRef<string | null>(null);
  // Track notes awaiting content hydration after loading stripped persisted cache
  const awaitingHydrationNoteIdRef = useRef<string | null>(null);
  const [isHydratingNote, setIsHydratingNote] = useState(false);
  // Track when we're in the process of creating a new note (to skip loading "Untitled")
  const isCreatingNoteRef = useRef(false);
  const [isCreatingNote, setIsCreatingNote] = useState(false);
  const [shouldFocusTitle, setShouldFocusTitle] = useState(false);
  const [isDragOver, setIsDragOver] = useState(false);
  const [noteEditor, setNoteEditor] = useState<Editor | null>(null);
  const [noteFullWidthMap, setNoteFullWidthMap] = useState<Record<string, boolean>>({});
  const isNoteFullWidth = selectedNoteId ? (noteFullWidthMap[selectedNoteId] ?? false) : false;
  const toggleNoteFullWidth = () => {
    if (!selectedNoteId) return;
    setNoteFullWidthMap(prev => ({ ...prev, [selectedNoteId]: !isNoteFullWidth }));
  };
  const [isToolbarHidden, setIsToolbarHidden] = useState(false);
  // State for "Move to folder" menu
  const [moveMenuDocId, setMoveMenuDocId] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);
  const titleInputRef = useRef<HTMLTextAreaElement>(null);
  const hasUnsyncedSelectedNote = !!(selectedNoteId && pendingEdits[selectedNoteId]);
  const effectiveSaveStatus: SaveStatus =
    saveStatus === "error"
      ? "error"
      : saveStatus === "saving"
        ? "saving"
        : hasUnsyncedSelectedNote
          ? "unsaved"
          : "saved";

  // Preview states
  const [previewImage, setPreviewImage] = useState<{
    url: string;
    title: string;
  } | null>(null);
  const [previewPdf, setPreviewPdf] = useState<{
    url: string;
    title: string;
  } | null>(null);
  const [previewDocx, setPreviewDocx] = useState<{
    url: string;
    title: string;
  } | null>(null);
  const [previewPptx, setPreviewPptx] = useState<{
    url: string;
    title: string;
  } | null>(null);
  const [previewVideo, setPreviewVideo] = useState<{
    url: string;
    title: string;
  } | null>(null);
  const [previewAudio, setPreviewAudio] = useState<{
    url: string;
    title: string;
  } | null>(null);
  const [previewXlsx, setPreviewXlsx] = useState<{
    url: string;
    title: string;
  } | null>(null);

  // Delete confirmation modal state
  const [deleteTarget, setDeleteTarget] = useState<Document | null>(null);
  // Permission error modal state
  const [permissionError, setPermissionError] = useState<string | null>(null);

  // Item menu state
  const [openItemMenuId, setOpenItemMenuId] = useState<string | null>(null);
  const [editingItemId, setEditingItemId] = useState<string | null>(null);
  const [editingItemName, setEditingItemName] = useState("");
  const itemMenuRefs = useRef<Map<string, HTMLButtonElement>>(new Map());

  // New button dropdown state
  const [showNewMenu, setShowNewMenu] = useState(false);
  const newButtonRef = useRef<HTMLButtonElement>(null);

  // Settings dropdown state
  const [showSettingsDropdown, setShowSettingsDropdown] = useState(false);
  const settingsButtonRef = useRef<HTMLButtonElement>(null);

  // Sort dropdown state
  const [showSortMenu, setShowSortMenu] = useState(false);
  const sortButtonRef = useRef<HTMLButtonElement>(null);

  // State for expanded folders in sidebar
  const [expandedFolders, setExpandedFolders] = useState<Set<string>>(new Set());

  // Sidebar section expansion states
  const [searchParams, setSearchParams] = useSearchParams();
  const [showMyFiles, setShowMyFiles] = useState(true);
  const [showShared, setShowShared] = useState(true);

  // Filter shared items to only show file-type resources
  const sharedFileItems = useMemo(() =>
    sharedWithMe.filter((item) =>
      ['document', 'folder', 'file'].includes(item.resource_type)
    ),
    [sharedWithMe],
  );

  const renameDocument = useFilesStore((state) => state.renameDocument);

  // Sync store's renamingId with local editingItemId (handles temp ID -> real ID transition)
  useEffect(() => {
    if (renamingId && renamingId !== editingItemId) {
      setEditingItemId(renamingId);
    } else if (!renamingId && editingItemId?.startsWith('temp-folder-')) {
      // renamingId was cleared (e.g., on error), clear local state too
      setEditingItemId(null);
      setEditingItemName("");
    }
  }, [renamingId, editingItemId]);

  const handleRenameItem = (doc: Document) => {
    setOpenItemMenuId(null);
    setEditingItemId(doc.id);
    setEditingItemName(doc.title);
  };

  const handleRenameSubmit = (docId: string) => {
    const trimmed = editingItemName.trim();
    // Clear editing state immediately for instant feedback
    setEditingItemId(null);
    setEditingItemName("");
    setRenamingId(null);
    // Fire off rename in background (store does optimistic update)
    // Use "New Folder" as default if input is empty
    const finalName = trimmed || "New Folder";
    renameDocument(docId, finalName);
  };

  const handleRenameKeyDown = (e: React.KeyboardEvent, docId: string) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleRenameSubmit(docId);
    } else if (e.key === "Escape") {
      e.preventDefault();
      const trimmed = editingItemName.trim();
      // If escaping with empty name on a new folder, delete it
      if (!trimmed && (docId.startsWith('temp-folder-') || renamingId === docId)) {
        removeDocument(docId);
      }
      setEditingItemId(null);
      setEditingItemName("");
      setRenamingId(null);
    }
  };

  // Toggle folder expansion in sidebar
  const handleFolderClick = (folder: Document) => {
    setExpandedFolders((prev) => {
      const next = new Set(prev);
      if (next.has(folder.id)) {
        next.delete(folder.id);
      } else {
        next.add(folder.id);
        // Fetch folder contents if not already loaded
        if (!documentsByFolder[folder.id]) {
          fetchDocuments(folder.id);
        }
      }
      return next;
    });
  };

  // Open folder as current context (used for creating items inside it)
  const handleOpenFolder = (folder: Document) => {
    navigateToFolder(folder.id, folder.title);
    navigate(`/workspace/${workspaceId}/files/${folder.id}`);
  };

  // Get all folders for "Move to" menu (exclude current folder and the item itself)
  const allFolders = useMemo(() => {
    const folders: Document[] = [];
    for (const docs of Object.values(documentsByFolder)) {
      for (const d of docs) {
        if ((d.is_folder || d.type === 'folder') && !d.id.startsWith('temp-')) {
          if (!folders.some(f => f.id === d.id)) folders.push(d);
        }
      }
    }
    return folders;
  }, [documentsByFolder]);

  // Toggle sort — click same column flips direction, click new column switches to it desc
  const handleSortToggle = (column: "name" | "date" | "size" | "manual") => {
    if (column === "manual") {
      setSortBy("manual");
      setSortDirection("asc");
      return;
    }

    if (sortBy === column) {
      setSortDirection(sortDirection === "asc" ? "desc" : "asc");
    } else {
      setSortBy(column);
      setSortDirection("desc");
    }
  };

  // Handle move document to folder
  const handleMoveToFolder = async (docId: string, targetFolderId: string | undefined) => {
    setMoveMenuDocId(null);
    setOpenItemMenuId(null);
    await moveDocument(docId, targetFolderId);
  };

  // Initialize store with workspace app ID
  useEffect(() => {
    if (filesApp?.id) {
      setWorkspaceAppId(filesApp.id);
    }
  }, [filesApp?.id, setWorkspaceAppId]);

  // Fetch shared-with-me items on mount + when switching to shared view
  useEffect(() => {
    fetchSharedWithMe();
  }, [fetchSharedWithMe]);


  // Auto-open shared document when arriving via share link (?shared=true)
  const sharedParamHandledRef = useRef(false);
  useEffect(() => {
    if (sharedParamHandledRef.current) return;
    if (searchParams.get('shared') !== 'true' || !urlDocumentId) return;
    sharedParamHandledRef.current = true;

    // Clean up the query param from the URL
    searchParams.delete('shared');
    setSearchParams(searchParams, { replace: true });

    // Load and select the shared document
    void (async () => {
      await loadSharedDocument(urlDocumentId);
      setSelectedNote(urlDocumentId);
    })();
  }, [searchParams, setSearchParams, urlDocumentId, loadSharedDocument, setSelectedNote]);

  // Sync URL documentId to store on mount or param change.
  // On mount (initial render), we haven't synced yet so we must treat it as a
  // change.  After that, only sync when the URL actually changes (user
  // navigation), not when documentsByFolder changes (which could be from
  // creating a note).  This prevents the URL->store sync from overwriting
  // programmatic selections.
  const prevUrlDocumentIdRef = useRef<string | undefined>(undefined);
  useEffect(() => {
    if (!urlDocumentId) {
      prevUrlDocumentIdRef.current = urlDocumentId;
      return;
    }

    const isInitialSync = prevUrlDocumentIdRef.current === undefined;
    const urlActuallyChanged = urlDocumentId !== prevUrlDocumentIdRef.current;
    prevUrlDocumentIdRef.current = urlDocumentId;

    if (!isInitialSync && !urlActuallyChanged) return;

    const allDocs = Object.values(documentsByFolder).flat();
    const doc = allDocs.find(d => d.id === urlDocumentId);
    if (doc) {
      if (doc.is_folder || doc.type === 'folder') {
        if (currentFolderId !== urlDocumentId) {
          navigateToFolder(urlDocumentId, doc.title);
        }
      } else {
        // Notes, images, and other file-backed documents open in the main panel
        if (selectedNoteId !== urlDocumentId) {
          setSelectedNote(urlDocumentId);
        }
      }
    } else if (isInitialSync && !selectedNoteId) {
      // Documents haven't loaded yet but we have a URL param — set the
      // selection eagerly so the UI doesn't flash "No note selected".
      setSelectedNote(urlDocumentId);
    }
  }, [urlDocumentId, documentsByFolder, currentFolderId, navigateToFolder, selectedNoteId, setSelectedNote]);

  // If URL has a documentId but it's not in loaded data (and we're done loading),
  // try to load it directly — handles direct links to docs user may or may not have access to.
  const attemptedDocLoadRef = useRef<string | null>(null);
  useEffect(() => {
    if (!urlDocumentId || isLoading) return;
    // Already found or already attempted
    const allDocs = Object.values(documentsByFolder).flat();
    if (allDocs.some(d => d.id === urlDocumentId)) return;
    if (attemptedDocLoadRef.current === urlDocumentId) return;
    // Skip if arriving via ?shared=true (handled by the shared param effect)
    if (searchParams.get('shared') === 'true') return;

    attemptedDocLoadRef.current = urlDocumentId;
    void (async () => {
      const doc = await loadSharedDocument(urlDocumentId);
      if (doc) {
        setSelectedNote(urlDocumentId);
      }
    })();
  }, [urlDocumentId, isLoading, documentsByFolder, searchParams, loadSharedDocument, setSelectedNote]);

  // Clear accessDenied when navigating away from the denied document
  useEffect(() => {
    if (accessDenied && urlDocumentId !== accessDenied.resourceId) {
      clearAccessDenied();
    }
  }, [urlDocumentId, accessDenied, clearAccessDenied]);

  // When landing without a documentId, silently update URL from persisted selection
  useEffect(() => {
    if (!urlDocumentId && workspaceId) {
      if (selectedNoteId) {
        window.history.replaceState(null, '', `/workspace/${workspaceId}/files/${selectedNoteId}`);
      } else if (currentFolderId) {
        window.history.replaceState(null, '', `/workspace/${workspaceId}/files/${currentFolderId}`);
      }
    }
  }, [urlDocumentId, selectedNoteId, currentFolderId, workspaceId]);

  // Keep URL in sync when store changes programmatically.
  // Track whether the store has had a chance to hydrate from the URL so we
  // don't strip the documentId on mount before data has loaded.
  const hasHydratedFromUrl = useRef(false);
  useEffect(() => {
    if (!workspaceId) return;
    if (selectedNoteId && selectedNoteId !== urlDocumentId) {
      hasHydratedFromUrl.current = true;
      window.history.replaceState(null, '', `/workspace/${workspaceId}/files/${selectedNoteId}`);
    } else if (!selectedNoteId && currentFolderId && currentFolderId !== urlDocumentId) {
      hasHydratedFromUrl.current = true;
      window.history.replaceState(null, '', `/workspace/${workspaceId}/files/${currentFolderId}`);
    } else if (!selectedNoteId && !currentFolderId && hasHydratedFromUrl.current) {
      // Only strip the documentId from the URL after the store has hydrated at
      // least once.  Without this guard the effect fires on mount (before data
      // loads) and wipes the URL param the user refreshed on.
      window.history.replaceState(null, '', `/workspace/${workspaceId}/files`);
    }
  }, [selectedNoteId, currentFolderId, workspaceId]);

  // Auto-select the first non-folder item when opening files with nothing selected
  useEffect(() => {
    if (selectedNoteId || urlDocumentId || currentFolderId) return;
    if (documents.length === 0) return;
    const firstSelectable = documents.find(
      (d) => !d.is_folder && d.type !== "folder" && isNote(d),
    );
    if (firstSelectable) {
      setSelectedNote(firstSelectable.id);
    }
  }, [documents, selectedNoteId, urlDocumentId, currentFolderId, setSelectedNote]);

  // Keep refs in sync with state for debounced save
  useEffect(() => {
    noteTitleRef.current = noteTitle;
  }, [noteTitle]);

  useEffect(() => {
    noteContentRef.current = noteContent;
  }, [noteContent]);

  // Track editing state to pause background fetches
  useEffect(() => {
    if (selectedNoteId) {
      setActivelyEditing(true);
    }
    return () => {
      setActivelyEditing(false);
    };
  }, [selectedNoteId, setActivelyEditing]);

  // Load note content only when note ID changes (not on every store update)
  useEffect(() => {
    if (selectedNote && selectedNote.id !== lastLoadedNoteIdRef.current) {
      lastLoadedNoteIdRef.current = selectedNote.id;
      awaitingHydrationNoteIdRef.current = null;
      setIsHydratingNote(false);
      // Reset lastSavedAt to the note's updated_at when switching notes
      setLastSavedAt(selectedNote.updated_at || null);
      setSaveStatus('saved');
      // Skip loading title/content if we're creating a new note (already set to empty)
      if (!isCreatingNoteRef.current) {
        setNoteTitle(selectedNote.title);
        // Content may be stripped from persisted cache to save localStorage space.
        // If missing, fetch it from the API on demand.
        if (selectedNote.content != null) {
          setNoteContent(selectedNote.content);
        } else if (isNote(selectedNote)) {
          setNoteContent("");
          setIsHydratingNote(true);
          awaitingHydrationNoteIdRef.current = selectedNote.id;
          void hydrateNoteContent(selectedNote.id);
        } else {
          setNoteContent("");
        }
      }
    } else if (
      selectedNote &&
      awaitingHydrationNoteIdRef.current === selectedNote.id &&
      selectedNote.content != null
    ) {
      // Apply hydrated content when it arrives, unless user already has unsynced local edits.
      if (!useFilesStore.getState().pendingEdits[selectedNote.id]) {
        setNoteContent(selectedNote.content);
      }
      setIsHydratingNote(false);
      awaitingHydrationNoteIdRef.current = null;
    } else if (!selectedNote) {
      lastLoadedNoteIdRef.current = null;
      awaitingHydrationNoteIdRef.current = null;
      setLastSavedAt(null);
      setSaveStatus('saved');
    }
  }, [selectedNote, hydrateNoteContent]);

  // Auto-save note content
  const saveNote = useCallback(
    async (noteId: string, title: string, content: string) => {
      const requestId = ++latestSaveRequestIdRef.current;
      setSaveStatus("saving");
      try {
        await storeSaveNote(noteId, title, content);
        if (requestId !== latestSaveRequestIdRef.current) return;

        const hasPending = !!useFilesStore.getState().pendingEdits[noteId];
        if (hasPending) {
          setSaveStatus("unsaved");
          return;
        }

        // Update local timestamp immediately so the UI shows current time
        setLastSavedAt(new Date().toISOString());
        setSaveStatus("saved");
      } catch (err) {
        if (requestId === latestSaveRequestIdRef.current) {
          setSaveStatus("error");
        }
        throw err;
      }
    },
    [storeSaveNote],
  );

  // Flush selected note's pending save through normal API flow (clears pendingEdits on success).
  const flushPendingSave = useCallback(() => {
    if (!selectedNoteId) return;

    const pendingEdit = useFilesStore.getState().pendingEdits[selectedNoteId];
    if (!pendingEdit) return;

    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
      saveTimeoutRef.current = null;
    }

    void saveNote(selectedNoteId, pendingEdit.title, pendingEdit.content).catch(
      () => {
        // Status is set by saveNote; no extra handling needed.
      },
    );
  }, [selectedNoteId, saveNote]);

  // Save on page leave / tab close — uses keepalive fetch to survive unload
  useEffect(() => {
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      if (saveTimeoutRef.current) {
        clearTimeout(saveTimeoutRef.current);
        saveTimeoutRef.current = null;
      }

      const hasPending = flushAllPendingWithKeepalive();
      if (!hasPending) return;

      // Show browser "Leave site?" dialog only when there are truly unsynced edits.
      event.preventDefault();
      event.returnValue = true;
    };
    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => window.removeEventListener("beforeunload", handleBeforeUnload);
  }, []);

  // Flush pending save when tab/app visibility changes.
  useEffect(() => {
    const handleVisibilityChange = () => {
      if (document.visibilityState === "hidden") {
        if (saveTimeoutRef.current) {
          clearTimeout(saveTimeoutRef.current);
          saveTimeoutRef.current = null;
        }
        flushAllPendingWithKeepalive();
      } else if (document.visibilityState === "visible") {
        // Retry with normal authenticated flow so pendingEdits can be cleared.
        void flushAllPendingEdits();
      }
    };
    document.addEventListener("visibilitychange", handleVisibilityChange);
    return () => document.removeEventListener("visibilitychange", handleVisibilityChange);
  }, [flushAllPendingEdits]);

  // Keep status in sync when unsynced state changes after replay/flush.
  useEffect(() => {
    if (!selectedNoteId) return;
    if (saveStatus === "saving") return;

    // Keep explicit error state while there are still local unsynced edits.
    if (hasUnsyncedSelectedNote) {
      if (saveStatus === "saved") {
        setSaveStatus("unsaved");
      }
      return;
    }

    // Once pending edits clear, recover status even if last attempt had errored.
    if (saveStatus !== "saved") {
      setSaveStatus("saved");
    }
  }, [selectedNoteId, hasUnsyncedSelectedNote, saveStatus]);

  const handleNoteContentChange = useCallback(
    (markdown: string) => {
      setNoteContent(markdown);
      setSaveStatus("unsaved");

      // Immediately update store (optimistic) - ensures edits survive page refresh
      if (selectedNoteId) {
        updateNoteOptimistic(selectedNoteId, noteTitleRef.current, markdown);
      }

      // Debounced API save - uses refs to get latest values
      if (saveTimeoutRef.current) {
        clearTimeout(saveTimeoutRef.current);
      }

      if (selectedNoteId) {
        saveTimeoutRef.current = setTimeout(() => {
          void saveNote(
            selectedNoteId,
            noteTitleRef.current,
            noteContentRef.current,
          ).catch(() => {
            // Status is set by saveNote; no extra handling needed.
          });
          saveTimeoutRef.current = null;
        }, SAVE_DEBOUNCE_MS);
      }
    },
    [selectedNoteId, saveNote, updateNoteOptimistic],
  );

  const handleNoteTitleChange = useCallback(
    (newTitle: string) => {
      setNoteTitle(newTitle);
      setSaveStatus("unsaved");

      // Immediately update store (optimistic) - ensures edits survive page refresh
      if (selectedNoteId) {
        updateNoteOptimistic(selectedNoteId, newTitle, noteContentRef.current);
      }

      // Debounced API save - uses refs to get latest values
      if (saveTimeoutRef.current) {
        clearTimeout(saveTimeoutRef.current);
      }

      if (selectedNoteId) {
        saveTimeoutRef.current = setTimeout(() => {
          void saveNote(
            selectedNoteId,
            noteTitleRef.current,
            noteContentRef.current,
          ).catch(() => {
            // Status is set by saveNote; no extra handling needed.
          });
          saveTimeoutRef.current = null;
        }, SAVE_DEBOUNCE_MS);
      }
    },
    [selectedNoteId, saveNote, updateNoteOptimistic],
  );

  // Auto-resize title textarea when content changes
  useEffect(() => {
    const ta = titleInputRef.current;
    if (ta) {
      ta.style.height = 'auto';
      ta.style.height = ta.scrollHeight + 'px';
    }
  }, [noteTitle]);

  // Focus title input when creating a new note
  // Depends on selectedNote so it retries once the note editor renders
  useEffect(() => {
    if (shouldFocusTitle && selectedNote) {
      // Use requestAnimationFrame to ensure DOM has updated before focusing
      requestAnimationFrame(() => {
        titleInputRef.current?.focus();
        titleInputRef.current?.select();
      });
      setShouldFocusTitle(false);
    }
  }, [shouldFocusTitle, selectedNote]);

  // Hide toolbar when typing, show on mouse movement (Notion-style)
  useEffect(() => {
    if (!noteEditor || noteEditor.isDestroyed) return;

    let editorElement: HTMLElement;
    try {
      editorElement = noteEditor.view.dom;
    } catch {
      return;
    }

    const handleKeyDown = (e: KeyboardEvent) => {
      // Only hide for actual typing keys, not modifiers or navigation
      const isModifier = e.metaKey || e.ctrlKey || e.altKey;
      const isNavigation = ['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight', 'Home', 'End', 'PageUp', 'PageDown'].includes(e.key);

      if (!isModifier && !isNavigation && e.key.length === 1) {
        setIsToolbarHidden(true);
      }
    };

    editorElement.addEventListener('keydown', handleKeyDown);

    return () => {
      editorElement.removeEventListener('keydown', handleKeyDown);
    };
  }, [noteEditor]);

  // Show toolbar on mouse movement
  useEffect(() => {
    const handleMouseMove = () => {
      if (isToolbarHidden) {
        setIsToolbarHidden(false);
      }
    };

    document.addEventListener('mousemove', handleMouseMove);
    return () => {
      document.removeEventListener('mousemove', handleMouseMove);
    };
  }, [isToolbarHidden]);

  const handleItemClick = async (doc: Document) => {
    // Handle folders
    if (doc.is_folder || doc.type === "folder") {
      navigate(`/workspace/${workspaceId}/files/${doc.id}`);
      return;
    }

    // Handle notes - open in side panel
    if (isNote(doc)) {
      // Flush pending save from previous note before switching
      flushPendingSave();
      // Reset lastLoadedNoteIdRef to allow loading new note content
      lastLoadedNoteIdRef.current = null;
      navigate(`/workspace/${workspaceId}/files/${doc.id}`);
      // Content will be loaded by the effect watching selectedNote
      return;
    }

    // Handle files (uploaded files)
    const mimeType = getFileMimeType(doc);

    // Images - open inline in main panel (same as notes)
    if (mimeType.startsWith("image/")) {
      flushPendingSave();
      lastLoadedNoteIdRef.current = null;
      navigate(`/workspace/${workspaceId}/files/${doc.id}`);
      return;
    }

    // PDFs - show PDF viewer modal
    if (mimeType === "application/pdf") {
      try {
        let pdfUrl = doc.file_url;
        if (!pdfUrl && doc.file_id) {
          const { url } = await getFileDownloadUrl(doc.file_id);
          pdfUrl = url;
        }
        if (pdfUrl) {
          setPreviewPdf({ url: pdfUrl, title: doc.title });
        }
      } catch (err) {
        console.error("Failed to load PDF:", err);
      }
      return;
    }

    // DOCX - show DOCX viewer modal
    if (
      mimeType ===
      "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    ) {
      try {
        let docxUrl = doc.file_url;
        if (!docxUrl && doc.file_id) {
          const { url } = await getFileDownloadUrl(doc.file_id);
          docxUrl = url;
        }
        if (docxUrl) {
          setPreviewDocx({ url: docxUrl, title: doc.title });
        }
      } catch (err) {
        console.error("Failed to load DOCX:", err);
      }
      return;
    }

    // PPTX - show PPTX viewer modal
    if (
      mimeType ===
      "application/vnd.openxmlformats-officedocument.presentationml.presentation"
    ) {
      try {
        let pptxUrl = doc.file_url;
        if (!pptxUrl && doc.file_id) {
          const { url } = await getFileDownloadUrl(doc.file_id);
          pptxUrl = url;
        }
        if (pptxUrl) {
          setPreviewPptx({ url: pptxUrl, title: doc.title });
        }
      } catch (err) {
        console.error("Failed to load PPTX:", err);
      }
      return;
    }

    // XLSX/XLS - show spreadsheet viewer modal
    if (
      mimeType ===
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" ||
      mimeType === "application/vnd.ms-excel" ||
      mimeType === "text/csv" ||
      mimeType === "text/tab-separated-values"
    ) {
      try {
        let xlsxUrl = doc.file_url;
        if (!xlsxUrl && doc.file_id) {
          const { url } = await getFileDownloadUrl(doc.file_id);
          xlsxUrl = url;
        }
        if (xlsxUrl) {
          setPreviewXlsx({ url: xlsxUrl, title: doc.title });
        }
      } catch (err) {
        console.error("Failed to load spreadsheet:", err);
      }
      return;
    }

    // Video - show video player modal
    if (mimeType.startsWith("video/")) {
      try {
        let videoUrl = doc.file_url;
        if (!videoUrl && doc.file_id) {
          const { url } = await getFileDownloadUrl(doc.file_id);
          videoUrl = url;
        }
        if (videoUrl) {
          setPreviewVideo({ url: videoUrl, title: doc.title });
        }
      } catch (err) {
        console.error("Failed to load video:", err);
      }
      return;
    }

    // Audio - show audio player modal
    if (mimeType.startsWith("audio/")) {
      try {
        let audioUrl = doc.file_url;
        if (!audioUrl && doc.file_id) {
          const { url } = await getFileDownloadUrl(doc.file_id);
          audioUrl = url;
        }
        if (audioUrl) {
          setPreviewAudio({ url: audioUrl, title: doc.title });
        }
      } catch (err) {
        console.error("Failed to load audio:", err);
      }
      return;
    }

    // Other files - download/open in new tab
    if (doc.file_url) {
      window.open(doc.file_url, "_blank");
    } else if (doc.file_id) {
      try {
        const { url } = await getFileDownloadUrl(doc.file_id);
        window.open(url, "_blank");
      } catch (err) {
        console.error("Failed to get download URL:", err);
      }
    }
  };

  const handleCreateNote = async () => {
    setIsCreatingNote(true);
    isCreatingNoteRef.current = true;

    // Set up UI optimistically before API call for instant feedback
    setNoteTitle("");
    setNoteContent("");
    setShouldFocusTitle(true);

    try {
      const newNote = await addNote(currentFolderId);
      if (newNote) {
        // Update ref so subsequent loads work correctly
        lastLoadedNoteIdRef.current = newNote.id;
      }
    } finally {
      isCreatingNoteRef.current = false;
      setIsCreatingNote(false);
    }
  };

  const handleCreateFolder = async () => {
    const newFolder = await addFolder(currentFolderId);
    if (newFolder) {
      // Auto-enter rename mode for the new folder with empty input (placeholder shows)
      setEditingItemId(newFolder.id);
      setEditingItemName("");
    }
  };

  const handleDeleteClick = (doc: Document, e: React.MouseEvent) => {
    e.stopPropagation();
    setOpenItemMenuId(null);

    // Check if user has permission to delete (owner, or workspace admin/owner)
    const isOwner = doc.user_id === currentUser?.id;
    const isWorkspaceAdminOrOwner = workspace?.role === 'admin' || workspace?.role === 'owner';
    if (doc.user_id && currentUser?.id && !isOwner && !isWorkspaceAdminOrOwner) {
      setPermissionError("You don't have permission to delete this item. Only the owner or a workspace admin can delete it.");
      return;
    }

    setDeleteTarget(doc);
  };

  const handleDeleteConfirm = async () => {
    if (!deleteTarget) return;

    // Capture the ID and close modal immediately for responsive UX
    const docId = deleteTarget.id;
    setDeleteTarget(null);

    // Cancel any pending auto-save to prevent saving to deleted doc
    if (saveTimeoutRef.current) {
      clearTimeout(saveTimeoutRef.current);
      saveTimeoutRef.current = null;
    }

    await removeDocument(docId);
  };

  const handleFileUpload = async (files: FileList | null) => {
    if (!files || files.length === 0) return;
    await uploadFiles(files, currentFolderId);
  };

  // Upload an image dropped/pasted into the note editor, return the public URL
  const handleEditorImageUpload = useCallback(async (file: File): Promise<string | null> => {
    const appId = filesApp?.id;
    if (!appId) return null;
    try {
      const uploadInfo = await getPresignedUploadUrl({
        workspaceAppId: appId,
        filename: file.name || `image-${Date.now()}.png`,
        contentType: file.type || 'image/png',
        fileSize: file.size,
        createDocument: false,
      });
      await fetch(uploadInfo.upload_url, {
        method: 'PUT',
        headers: uploadInfo.headers,
        body: file,
      });
      await confirmFileUpload(uploadInfo.file_id, {
        workspaceAppId: appId,
        createDocument: false,
      });
      return uploadInfo.public_url;
    } catch (err) {
      console.error('Failed to upload image:', err);
      return null;
    }
  }, [filesApp?.id]);

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(false);
    handleFileUpload(e.dataTransfer.files);
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragOver(true);
  };

  const handleDragLeave = () => {
    setIsDragOver(false);
  };

  const closeImagePreview = () => {
    setPreviewImage(null);
  };

  if (!filesApp) {
    return (
      <div className="flex-1 flex items-center justify-center bg-bg-shell">
        <div className="text-center">
          <HugeiconsIcon
            icon={Folder01Icon}
            size={48}
            className="mx-auto mb-4 text-text-tertiary"
          />
          <p className="text-text-secondary">Files app not configured</p>
          <p className="text-sm text-text-tertiary mt-1">
            Add a Files app to your workspace to get started
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      <input
        ref={fileInputRef}
        type="file"
        multiple
        className="hidden"
        onChange={(e) => handleFileUpload(e.target.files)}
      />

      {/* Main content container */}
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Left panel - File list */}
        <div
          className={`w-[212px] shrink-0 flex flex-col overflow-hidden ${SIDEBAR.bg} border-r border-black/5 ${isDragOver ? "ring-2 ring-inset ring-brand-primary" : ""}`}
          onDrop={handleDrop}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
        >
          {/* Header */}
          <div className="h-12 flex items-center justify-between pl-4 pr-2 shrink-0 relative">
            <h2 className="text-base font-semibold text-text-body">Files</h2>
            <button
              ref={newButtonRef}
              onClick={() => setShowNewMenu(!showNewMenu)}
              className="p-1 rounded bg-white border border-black/10 hover:border-black/20 text-text-secondary hover:text-text-body transition-colors focus-visible:ring-2 focus-visible:ring-brand-primary"
              title="New"
              aria-label="New file or folder"
            >
              <HugeiconsIcon icon={Add01Icon} size={16} aria-hidden="true" />
            </button>
            <Dropdown
              isOpen={showNewMenu}
              onClose={() => setShowNewMenu(false)}
              trigger={newButtonRef}
            >
              <button
                onClick={() => {
                  handleCreateNote();
                  setShowNewMenu(false);
                }}
                disabled={isCreatingNote}
                className="w-[calc(100%-8px)] mx-1 px-2 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray rounded-md flex items-center gap-2 disabled:opacity-50 focus-visible:bg-bg-gray focus-visible:outline-none"
              >
                <HugeiconsIcon icon={Add01Icon} size={14} aria-hidden="true" />
                New Note
              </button>
              <button
                onClick={() => {
                  handleCreateFolder();
                  setShowNewMenu(false);
                }}
                className="w-[calc(100%-8px)] mx-1 px-2 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray rounded-md flex items-center gap-2 focus-visible:bg-bg-gray focus-visible:outline-none"
              >
                <HugeiconsIcon icon={FolderAddIcon} size={14} aria-hidden="true" />
                New Folder
              </button>
              <button
                onClick={() => {
                  fileInputRef.current?.click();
                  setShowNewMenu(false);
                }}
                disabled={isUploading}
                className="w-[calc(100%-8px)] mx-1 px-2 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray rounded-md flex items-center gap-2 disabled:opacity-50 focus-visible:bg-bg-gray focus-visible:outline-none"
              >
                <HugeiconsIcon icon={Upload04Icon} size={14} aria-hidden="true" />
                Upload File
              </button>
            </Dropdown>
          </div>

          {/* Upload progress */}
          {isUploading && (
            <div className="px-3 py-2 border-b border-border-gray">
              <div className="flex items-center gap-2">
                <div className="flex-1 h-1 bg-bg-gray rounded-full overflow-hidden">
                  <div
                    className="h-full bg-brand-primary transition-all duration-300"
                    style={{ width: `${uploadProgress}%` }}
                  />
                </div>
                <span className="text-xs text-text-secondary">
                  {uploadProgress}%
                </span>
              </div>
            </div>
          )}

          {/* Scrollable content area */}
          <div className="flex-1 overflow-y-auto">

          {/* My Files section header */}
          <div className="space-y-0.5">
            <div
              role="button"
              tabIndex={0}
              onClick={() => setShowMyFiles(!showMyFiles)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  setShowMyFiles(!showMyFiles);
                }
              }}
              aria-expanded={showMyFiles}
              className="flex items-center gap-1 px-4 py-1.5 text-xs font-medium text-text-tertiary cursor-pointer group"
            >
              <span>My Files</span>
              <ChevronRightIcon className={`w-3 h-3 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-all ${showMyFiles ? 'rotate-90' : ''}`} aria-hidden="true" />
            </div>
          </div>

          {showMyFiles && (
          <>
          {/* Sort controls */}
          <div className="px-2 relative">
            <button
              ref={sortButtonRef}
              onClick={() => setShowSortMenu(!showSortMenu)}
              className="w-full flex items-center gap-2 px-2 h-[32px] rounded-md text-sm text-text-tertiary hover:text-text-body hover:bg-black/5 transition-colors focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-brand-primary"
              title="Sort by"
              aria-label="Sort files"
            >
              <ChevronUpDownIcon className="w-3.5 h-3.5 shrink-0" aria-hidden="true" />
              <span className="capitalize">
                {sortBy === "manual" ? "Manual" : `${sortBy === "size" ? "File size" : sortBy} ${sortDirection === "asc" ? "↑" : "↓"}`}
              </span>
            </button>
            <Dropdown
              isOpen={showSortMenu}
              onClose={() => setShowSortMenu(false)}
              trigger={sortButtonRef}
            >
              <div className="px-1.5 py-1">
              {(["manual", "date", "name", "size"] as const).map((col) => (
                <button
                  key={col}
                  onClick={() => {
                    handleSortToggle(col);
                    setShowSortMenu(false);
                  }}
                  className={`w-full px-3 py-1.5 text-left text-sm rounded-md hover:bg-bg-gray flex items-center justify-between ${
                    sortBy === col ? 'text-text-body font-medium' : 'text-text-secondary'
                  }`}
                >
                  <span className="capitalize">{col === "size" ? "File size" : col}</span>
                  {sortBy === col && col !== "manual" && (
                    <span className="text-xs">
                      {sortDirection === "asc" ? "↑" : "↓"}
                    </span>
                  )}
                </button>
              ))}
              </div>
            </Dropdown>
          </div>

          {/* Breadcrumbs - show when inside a folder */}
          {currentFolderId && breadcrumbs.length > 1 && (
            <div className="pl-5 pr-3 py-1.5 flex items-center gap-1 text-xs text-text-tertiary">
              <button
                onClick={() => {
                  navigateToBreadcrumb(0);
                  navigate(`/workspace/${workspaceId}/files`);
                }}
                className="hover:text-text-body transition-colors flex items-center gap-1"
              >
                <ArrowUturnLeftIcon className="w-3 h-3" />
                All
              </button>
              {breadcrumbs.slice(1).map((crumb, i) => (
                <span key={crumb.id || i} className="flex items-center gap-1">
                  <ChevronRightIcon className="w-2.5 h-2.5" />
                  <button
                    onClick={() => {
                      navigateToBreadcrumb(i + 1);
                      navigate(`/workspace/${workspaceId}/files${crumb.id ? `/${crumb.id}` : ''}`);
                    }}
                    className={`hover:text-text-body transition-colors truncate max-w-[100px] ${
                      i === breadcrumbs.length - 2 ? 'text-text-secondary font-medium' : ''
                    }`}
                  >
                    {crumb.title}
                  </button>
                </span>
              ))}
            </div>
          )}

          {/* Document list */}
          <div>
            <AnimatePresence mode="wait" initial={false}>
              {isLoading ? (
                <motion.div
                  key="loading"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.1 }}
                  className="px-2 pt-1 space-y-0.5"
                >
                  {[...Array(8)].map((_, i) => (
                    <div key={i} className="flex items-center gap-2 px-2 h-[32px]">
                      <div className="w-4 h-4 bg-gray-200 rounded animate-pulse" />
                      <div className="flex-1 h-3 bg-gray-200 rounded animate-pulse" style={{ width: `${55 + (i * 7) % 30}%` }} />
                    </div>
                  ))}
                </motion.div>
              ) : error ? (
                <motion.div
                  key="error"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.1 }}
                  className="p-4 text-center text-sm text-red-500"
                >
                  {error}
                </motion.div>
              ) : documents.length === 0 ? (
                <motion.div
                  key="empty"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.1 }}
                >
                  <div className="p-6 text-center text-text-tertiary">
                    <HugeiconsIcon
                      icon={File02Icon}
                      size={32}
                      className="mx-auto mb-2 opacity-50"
                    />
                    <p className="text-sm">
                      {searchQuery ? "No matches" : "No notes yet"}
                    </p>
                  </div>
                </motion.div>
              ) : (
                <motion.div
                  key={`content-${workspaceId}`}
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.15 }}
                >
                  <DndContext
                    sensors={sensors}
                    collisionDetection={collisionDetection}
                    onDragStart={handleDragStart}
                    onDragMove={handleDragMove}
                    onDragCancel={handleDragCancel}
                    onDragEnd={handleDragEnd}
                  >
                  {/* Parent drop zone — always rendered when inside a folder to
                      keep layout stable (avoids shifting items when drag starts,
                      which would invalidate dnd-kit's measured rects). */}
                  {currentFolderId && (
                    <DroppableFolder id={parentDropId}>
                      {({ isOver }) => (
                        <div className={`mx-2 mb-1 px-3 h-[32px] rounded-md text-xs flex items-center gap-2 border border-dashed transition-colors ${
                          !activeDragId
                            ? "opacity-0 pointer-events-none"
                            : isOver
                              ? "border-brand-primary bg-brand-primary/10 text-brand-primary"
                              : "border-border-gray text-text-tertiary"
                        }`}>
                          <ArrowUturnLeftIcon className="w-3.5 h-3.5" />
                          {breadcrumbs.length >= 3 ? "Move to parent folder" : "Move to root"}
                        </div>
                      )}
                    </DroppableFolder>
                  )}

                  <SortableContext
                    items={documents.map((d) => d.id)}
                    strategy={verticalListSortingStrategy}
                  >
                  {/* All items — folders, notes, files in unified sort order */}
                  <div className="px-2 space-y-0.5">
                    <AnimatePresence initial={false}>
                      {documents.map((item, index) => {
                        const itemIsFolder = item.is_folder || item.type === "folder";
                        const isExpanded = itemIsFolder && expandedFolders.has(item.id);
                        const folderContents = itemIsFolder ? (documentsByFolder[item.id] || []) : [];

                        // Show highlight when folder will receive the dropped item (nest mode)
                        const showMoveIntoHighlight = itemIsFolder && dropIntent?.type === 'moveInto' && dropIntent.targetId === item.id;

                        // Find the dragged item's index to check if reorder would actually change anything
                        const draggedIndex = activeDragId ? documents.findIndex(d => d.id === activeDragId) : -1;

                        const showDropLineBefore = dropIntent?.type === 'reorder' &&
                          dropIntent.targetId === item.id &&
                          dropIntent.insertPosition === 'before' &&
                          activeDragId !== item.id &&
                          draggedIndex !== index - 1;
                        const showDropLineAfter = dropIntent?.type === 'reorder' &&
                          dropIntent.targetId === item.id &&
                          dropIntent.insertPosition === 'after' &&
                          activeDragId !== item.id &&
                          draggedIndex !== index + 1;

                        return (
                        <div key={(item as unknown as { _stableKey?: string })._stableKey || item.id}>
                          {showDropLineBefore && (
                            <div className="h-0.5 bg-brand-primary rounded-full mx-1 my-0.5" />
                          )}
                          <SortableDocItem id={item.id}>
                            {({ dragAttributes, dragListeners }) => (
                              itemIsFolder ? (
                                /* ── Folder row — no DroppableFolder wrapper needed;
                                       folder nesting is handled by handleDragMove zone detection ── */
                                    <div
                                      {...(editingItemId !== item.id ? dragAttributes : {})}
                                      {...(editingItemId !== item.id ? dragListeners : {})}
                                      role="button"
                                      tabIndex={0}
                                      onClick={() =>
                                        editingItemId !== item.id &&
                                        handleFolderClick(item)
                                      }
                                      onKeyDown={(e) => {
                                        if (e.key === "Enter" || e.key === " ") {
                                          if (editingItemId !== item.id) {
                                            e.preventDefault();
                                            handleFolderClick(item);
                                          }
                                        }
                                      }}
                                      className={`w-full flex items-center pr-3 h-[32px] rounded-md text-sm transition-colors group cursor-pointer active:cursor-grabbing ${
                                        showMoveIntoHighlight
                                          ? "bg-brand-primary/15"
                                          : `${SIDEBAR.item} hover:bg-black/5`
                                      }`}
                                    >
                                      <span className="shrink-0 ml-3 mr-2 relative w-4 h-4">
                                        <HugeiconsIcon icon={Folder01Icon} size={16} className={`absolute inset-0 transition-opacity ${isExpanded ? "opacity-0" : "group-hover:opacity-0"}`} />
                                        <ChevronRightIcon className={`w-4 h-4 absolute inset-0 transition-all duration-150 ${isExpanded ? "opacity-100 rotate-90" : "opacity-0 group-hover:opacity-100"}`} />
                                      </span>
                                      {editingItemId === item.id ? (
                                        <input
                                          type="text"
                                          value={editingItemName}
                                          onChange={(e) => setEditingItemName(e.target.value)}
                                          onKeyDown={(e) => handleRenameKeyDown(e, item.id)}
                                          onBlur={() => handleRenameSubmit(item.id)}
                                          onClick={(e) => e.stopPropagation()}
                                          autoFocus
                                          placeholder="New Folder"
                                          className="flex-1 text-sm bg-transparent px-1 py-0.5 focus:outline-none placeholder:text-text-tertiary"
                                        />
                                      ) : (
                                        <span className="flex-1 text-left truncate">
                                          {item.title}
                                        </span>
                                      )}
                                      {/* Three-dot menu + count overlay in same position */}
                                      {editingItemId !== item.id && (
                                      <div className="relative shrink-0">
                                        <button
                                          ref={(el) => { if (el) itemMenuRefs.current.set(item.id, el); }}
                                          onClick={(e) => {
                                            e.stopPropagation();
                                            setOpenItemMenuId(openItemMenuId === item.id ? null : item.id);
                                          }}
                                          className="p-0.5 rounded text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors opacity-0 group-hover:opacity-100"
                                          title="More options"
                                        >
                                          <EllipsisHorizontalIcon className="w-3 h-3" />
                                        </button>
                                        {folderContents.length > 0 && (
                                          <span className="absolute inset-0 flex items-center justify-center text-xs text-text-tertiary tabular-nums pointer-events-none group-hover:hidden">
                                            {folderContents.length}
                                          </span>
                                        )}
                                        <Dropdown
                                          isOpen={openItemMenuId === item.id}
                                          onClose={() => setOpenItemMenuId(null)}
                                          trigger={{ current: itemMenuRefs.current.get(item.id) || null }}
                                        >
                                          <button
                                            onClick={(e) => { e.stopPropagation(); handleRenameItem(item); }}
                                            className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                                          >
                                            <HugeiconsIcon icon={PencilEdit01Icon} size={14} />
                                            Rename
                                          </button>
                                          <button
                                            onClick={(e) => handleDeleteClick(item, e)}
                                            className="w-full px-3 py-1.5 text-left text-sm text-red-500 hover:bg-bg-gray flex items-center gap-2"
                                          >
                                            <HugeiconsIcon icon={Delete02Icon} size={14} />
                                            Delete
                                          </button>
                                        </Dropdown>
                                      </div>
                                      )}
                                    </div>
                              ) : (
                                /* ── Note / file row ── */
                                <div
                                  {...(canReorder ? dragAttributes : {})}
                                  {...(canReorder ? dragListeners : {})}
                                  onClick={() => editingItemId !== item.id && handleItemClick(item)}
                                  className={`w-full flex items-center pr-3 h-[32px] rounded-md text-sm transition-colors group cursor-pointer active:cursor-grabbing ${
                                    selectedNoteId === item.id
                                      ? SIDEBAR.selected
                                      : `${SIDEBAR.item} hover:bg-black/5`
                                  }`}
                                >
                                  <span className="shrink-0 ml-3 mr-2">
                                    {getFileIcon(item, "w-4 h-4")}
                                  </span>
                                  {editingItemId === item.id ? (
                                    <input
                                      type="text"
                                      value={editingItemName}
                                      onChange={(e) => setEditingItemName(e.target.value)}
                                      onKeyDown={(e) => handleRenameKeyDown(e, item.id)}
                                      onBlur={() => handleRenameSubmit(item.id)}
                                      onClick={(e) => e.stopPropagation()}
                                      autoFocus
                                      className="flex-1 text-sm text-text-body bg-transparent border-0 outline-none p-0 m-0"
                                    />
                                  ) : (
                                    <span className="flex-1 text-left truncate">
                                      {item.title || "Untitled"}
                                    </span>
                                  )}
                                  {editingItemId !== item.id && (
                                    <div
                                      className={`flex items-center shrink-0 relative ${
                                        selectedNoteId === item.id
                                          ? "opacity-100"
                                          : "opacity-0 group-hover:opacity-100"
                                      }`}
                                    >
                                      <button
                                        ref={(el) => { if (el) itemMenuRefs.current.set(item.id, el); }}
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          setOpenItemMenuId(openItemMenuId === item.id ? null : item.id);
                                        }}
                                        className="p-0.5 rounded text-text-tertiary hover:text-text-body hover:bg-bg-gray-light transition-colors"
                                      >
                                        <EllipsisHorizontalIcon className="w-3 h-3" />
                                      </button>
                                      <Dropdown
                                        isOpen={openItemMenuId === item.id}
                                        onClose={() => setOpenItemMenuId(null)}
                                        trigger={{ current: itemMenuRefs.current.get(item.id) || null }}
                                      >
                                        <button
                                          onClick={(e) => { e.stopPropagation(); handleRenameItem(item); }}
                                          className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                                        >
                                          <HugeiconsIcon icon={PencilEdit01Icon} size={14} />
                                          Rename
                                        </button>
                                        {!item.is_folder && item.type !== 'folder' && !item.file_id && (
                                          <button
                                            onClick={(e) => {
                                              e.stopPropagation();
                                              setOpenItemMenuId(null);
                                              duplicateDocument(item.id);
                                            }}
                                            className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                                          >
                                            <HugeiconsIcon icon={Copy01Icon} size={14} />
                                            Duplicate
                                          </button>
                                        )}
                                        {allFolders.length > 0 && (
                                          <div className="relative">
                                            <button
                                              onClick={(e) => {
                                                e.stopPropagation();
                                                setMoveMenuDocId(moveMenuDocId === item.id ? null : item.id);
                                              }}
                                              className="w-full px-3 py-1.5 text-left text-sm text-text-body hover:bg-bg-gray flex items-center gap-2"
                                            >
                                              <HugeiconsIcon icon={FolderExportIcon} size={14} />
                                              Move to...
                                            </button>
                                            {moveMenuDocId === item.id && (
                                              <div className="border-t border-border-gray py-1">
                                                {currentFolderId && (
                                                  <button
                                                    onClick={(e) => { e.stopPropagation(); handleMoveToFolder(item.id, undefined); }}
                                                    className="w-full px-3 py-1.5 text-left text-xs text-text-secondary hover:bg-bg-gray flex items-center gap-2"
                                                  >
                                                    <ArrowUturnLeftIcon className="w-3 h-3" />
                                                    Root
                                                  </button>
                                                )}
                                                {allFolders
                                                  .filter(f => f.id !== currentFolderId && f.id !== item.parent_id)
                                                  .map(f => (
                                                    <button
                                                      key={f.id}
                                                      onClick={(e) => { e.stopPropagation(); handleMoveToFolder(item.id, f.id); }}
                                                      className="w-full px-3 py-1.5 text-left text-xs text-text-secondary hover:bg-bg-gray flex items-center gap-2 truncate"
                                                    >
                                                      <HugeiconsIcon icon={Folder01Icon} size={12} className="text-yellow-500 shrink-0" />
                                                      {f.title}
                                                    </button>
                                                  ))}
                                              </div>
                                            )}
                                          </div>
                                        )}
                                        <button
                                          onClick={(e) => handleDeleteClick(item, e)}
                                          className="w-full px-3 py-1.5 text-left text-sm text-red-500 hover:bg-bg-gray flex items-center gap-2"
                                        >
                                          <HugeiconsIcon icon={Delete02Icon} size={14} />
                                          Delete
                                        </button>
                                      </Dropdown>
                                    </div>
                                  )}
                                </div>
                              )
                            )}
                          </SortableDocItem>

                          {/* Expanded folder contents */}
                          {itemIsFolder && (
                            <AnimatePresence>
                              {isExpanded && folderContents.length > 0 && (
                                <motion.div
                                  initial={{ height: 0, opacity: 0 }}
                                  animate={{ height: "auto", opacity: 1 }}
                                  exit={{ height: 0, opacity: 0 }}
                                  transition={{ duration: 0.15 }}
                                  className="overflow-hidden"
                                >
                                  <NestedFolderContents
                                    items={folderContents}
                                    depth={1}
                                    expandedFolders={expandedFolders}
                                    documentsByFolder={documentsByFolder}
                                    selectedNoteId={selectedNoteId}
                                    onFolderToggle={handleFolderClick}
                                    onFolderOpen={handleOpenFolder}
                                    onItemClick={handleItemClick}
                                    onRename={handleRenameItem}
                                    onDelete={handleDeleteClick}
                                    editingItemId={editingItemId}
                                    editingItemName={editingItemName}
                                    setEditingItemName={setEditingItemName}
                                    onRenameKeyDown={handleRenameKeyDown}
                                    onRenameSubmit={handleRenameSubmit}
                                  />
                                </motion.div>
                              )}
                            </AnimatePresence>
                          )}

                          {showDropLineAfter && (
                            <div className="h-0.5 bg-brand-primary rounded-full mx-1 my-0.5" />
                          )}
                        </div>
                        );
                      })}
                    </AnimatePresence>
                  </div>
                  </SortableContext>
                  {/* Drag overlay — shows dragged item floating under cursor */}
                  <DragOverlay dropAnimation={null}>
                    {activeDragItem && (
                      <div className="flex items-center gap-2 px-2 h-[32px] rounded-md text-sm bg-bg-mini-app shadow-md">
                        {(activeDragItem.is_folder || activeDragItem.type === "folder") ? (
                          <HugeiconsIcon icon={Folder01Icon} size={16} className="text-text-tertiary shrink-0" />
                        ) : (
                          <span className="shrink-0">{getFileIcon(activeDragItem, "w-4 h-4")}</span>
                        )}
                        <span className="truncate">{activeDragItem.title || "Untitled"}</span>
                      </div>
                    )}
                  </DragOverlay>
                  </DndContext>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
          </>
          )}

          {/* Shared with me section header */}
          <div className="space-y-0.5 mt-1">
            <div
              role="button"
              tabIndex={0}
              onClick={() => setShowShared(!showShared)}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  setShowShared(!showShared);
                }
              }}
              aria-expanded={showShared}
              className="flex items-center gap-1 px-4 py-1.5 text-xs font-medium text-text-tertiary cursor-pointer group"
            >
              <span>Shared with me</span>
              <ChevronRightIcon className={`w-3 h-3 flex-shrink-0 opacity-0 group-hover:opacity-100 transition-all ${showShared ? 'rotate-90' : ''}`} aria-hidden="true" />
              
            </div>
          </div>

          {showShared && (
            <div>
              {isLoadingSharedWithMe ? (
                <div className="px-2 pt-1 space-y-0.5">
                  {[...Array(5)].map((_, i) => (
                    <div key={i} className="flex items-center gap-2 px-2 h-[32px]">
                      <div className="w-4 h-4 bg-gray-200 rounded animate-pulse" />
                      <div className="flex-1 h-3 bg-gray-200 rounded animate-pulse" style={{ width: `${55 + (i * 7) % 30}%` }} />
                    </div>
                  ))}
                </div>
              ) : sharedFileItems.length === 0 ? (
                <div className="p-6 text-center text-text-tertiary">
                  <UsersIcon className="w-8 h-8 mx-auto mb-2 opacity-50" />
                  <p className="text-sm">Nothing shared with you yet</p>
                </div>
              ) : (
                <div className="px-2 space-y-0.5">
                  {sharedFileItems.map((item) => {
                    const isFolder = item.resource_type === 'folder';
                    const isFile = item.resource_type === 'file';
                    const isActive = selectedNoteId === item.resource_id;
                    return (
                      <button
                        key={item.permission_id}
                        onClick={async () => {
                          await loadSharedDocument(item.resource_id);
                          setSelectedNote(item.resource_id);
                          navigate(`/workspace/${workspaceId}/files/${item.resource_id}`);
                        }}
                        className={`w-full flex items-center gap-2 px-3 h-[40px] rounded-md text-sm transition-colors group ${
                          isActive
                            ? 'bg-black/8 text-text-body font-medium'
                            : `${SIDEBAR.item} hover:bg-black/5`
                        }`}
                      >
                        {isFolder ? (
                          <HugeiconsIcon icon={Folder01Icon} size={15} className="shrink-0" />
                        ) : isFile ? (
                          <HugeiconsIcon icon={File01Icon} size={15} className="shrink-0" />
                        ) : (
                          <HugeiconsIcon icon={File02Icon} size={15} className="shrink-0" />
                        )}
                        <div className="flex-1 min-w-0 text-left">
                          <span className="block truncate">{item.title || 'Untitled'}</span>
                          {item.workspace_name && (
                            <span className="block text-[10px] text-text-tertiary truncate">
                              {item.workspace_name}
                            </span>
                          )}
                        </div>
                        <span className={`shrink-0 text-[10px] px-1.5 py-0.5 rounded-full ${
                          item.permission === 'admin' ? 'bg-purple-100 text-purple-600' :
                          item.permission === 'write' ? 'bg-blue-100 text-blue-600' :
                          'bg-gray-100 text-gray-500'
                        }`}>
                          {item.permission}
                        </span>
                      </button>
                    );
                  })}
                </div>
              )}
            </div>
          )}

          </div>
          {/* Drag overlay */}
          {isDragOver && (
            <div className="absolute inset-0 bg-brand-primary/10 flex items-center justify-center pointer-events-none rounded-lg m-1">
              <div className="bg-white rounded-lg shadow-lg px-4 py-3 text-center">
                <HugeiconsIcon
                  icon={Upload04Icon}
                  size={24}
                  className="mx-auto mb-1 text-brand-primary"
                />
                <p className="text-sm font-medium text-text-body">
                  Drop to upload
                </p>
              </div>
            </div>
          )}
        </div>

        {/* Right panel - Note editor + Chat */}
        <div className="flex-1 flex min-w-0 overflow-hidden">
          <div className="flex-1 flex flex-col min-w-0 overflow-hidden bg-white">
          {selectedNote && isSelectedImage ? (
            <>
              {/* Image viewer header */}
              <div className="h-12 shrink-0 flex items-center justify-between pl-6 pr-3 border-b border-border-gray">
                <span className="text-sm font-medium text-text-body truncate">
                  {selectedNote.title}
                </span>
                <div className="flex items-center gap-1">
                  {inlineImageUrl && (
                    <a
                      href={inlineImageUrl}
                      download={selectedNote.title}
                      className="p-2 rounded-lg hover:bg-bg-gray transition-colors"
                      title="Download"
                    >
                      <HugeiconsIcon icon={Download04Icon} size={18} className="text-text-tertiary" />
                    </a>
                  )}
                  <HeaderButtons onSettingsClick={() => setShowSettingsDropdown(!showSettingsDropdown)} settingsButtonRef={settingsButtonRef} />
                </div>
              </div>
              {/* Image display */}
              <div className="flex-1 flex items-center justify-center overflow-hidden bg-bg-gray/30 p-6 relative">
                {inlineImageUrl ? (
                  <img
                    src={inlineImageUrl}
                    alt={selectedNote.title}
                    className="max-w-full max-h-full object-contain rounded-lg cursor-zoom-in"
                    onClick={() => setPreviewImage({ url: inlineImageUrl, title: selectedNote.title })}
                  />
                ) : (
                  <div className="animate-pulse w-48 h-48 bg-bg-gray rounded-lg" />
                )}
              </div>
            </>
          ) : selectedNote ? (
            <>
              {/* Toolbar row */}
              <div
                className={`h-12 shrink-0 relative transition-opacity duration-200 ${
                  isToolbarHidden ? "opacity-0" : "opacity-100"
                }`}
              >
                {/* Formatting toolbar + save status aligned with content width */}
                <div className={`h-full pr-28 ${isNoteFullWidth ? "w-full" : "w-[min(800px,90%)] mx-auto"}`}>
                  <div className="h-full flex items-center justify-between px-6">
                    <Suspense
                      fallback={
                        <div className="h-10 bg-bg-gray" />
                      }
                    >
                      <NoteToolbar
                        editor={noteEditor}
                        isHidden={false}
                      />
                    </Suspense>
                    {/* Save status - right-aligned with content edge */}
                    <div className="flex items-center gap-2 shrink-0">
                      <div className="flex items-center gap-2 text-xs text-text-tertiary">
                        <span>{formatDateRelative(lastSavedAt || selectedNote.updated_at)}</span>
                        {isNoteEditable && effectiveSaveStatus === 'saving' && <span>· Saving...</span>}
                        {isNoteEditable && effectiveSaveStatus === 'unsaved' && <span className="text-text-secondary">· Unsaved</span>}
                        {isNoteEditable && effectiveSaveStatus === 'error' && <span className="text-red-500">· Save failed</span>}
                        {isNoteEditable && effectiveSaveStatus === 'saved' && <span>· Saved</span>}
                      </div>
                      {!isNoteEditable && (
                        <span className="text-xs text-text-tertiary bg-bg-gray px-2 py-1 rounded-md">
                          Read only
                        </span>
                      )}
                    </div>
                  </div>
                </div>
                {/* Header buttons - always pinned to top right */}
                <div className="absolute right-3 top-0 h-full flex items-center">
                  <HeaderButtons onSettingsClick={() => setShowSettingsDropdown(!showSettingsDropdown)} settingsButtonRef={settingsButtonRef} />
                </div>
              </div>

              {/* Content area below toolbar - relative for overlay panels */}
              <div className="flex-1 overflow-hidden relative">
              {/* Scrollable area - full width so scrollbar is on the right edge */}
              <div className="flex-1 h-full overflow-y-auto">
                <div className={isNoteFullWidth ? "w-full" : "w-[min(800px,90%)] mx-auto"}>
                  {/* Note title */}
                  <div className="px-6 pt-4 pb-1">
                    <textarea
                      ref={titleInputRef}
                      rows={1}
                      value={noteTitle}
                      readOnly={!isNoteEditable}
                      onChange={(e) => {
                        if (!isNoteEditable) return;
                        handleNoteTitleChange(e.target.value);
                        // Auto-resize
                        e.target.style.height = 'auto';
                        e.target.style.height = e.target.scrollHeight + 'px';
                      }}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") {
                          e.preventDefault();
                          noteEditor?.commands.focus('start');
                        }
                        if (e.key === "ArrowDown" && noteEditor) {
                          const ta = e.currentTarget;
                          const cursorPos = ta.selectionStart ?? 0;
                          // Check if cursor is on the last line
                          const textBeforeCursor = ta.value.slice(0, cursorPos);
                          const textAfterCursor = ta.value.slice(cursorPos);
                          // If there's a newline after cursor position (visual wrap), let default handle it
                          // We use a canvas to measure if cursor is on the last visual line
                          const lastLineBreak = textBeforeCursor.lastIndexOf('\n');
                          const hasLineAfter = textAfterCursor.includes('\n');
                          // Simple check: if cursor is at the end or no more visual lines below
                          if (!hasLineAfter) {
                            e.preventDefault();
                            // Measure pixel offset of cursor in title using a hidden span
                            const measure = document.createElement('span');
                            measure.style.cssText = window.getComputedStyle(ta).cssText;
                            measure.style.position = 'absolute';
                            measure.style.visibility = 'hidden';
                            measure.style.whiteSpace = 'pre';
                            const lineText = lastLineBreak >= 0 ? textBeforeCursor.slice(lastLineBreak + 1) : textBeforeCursor;
                            measure.textContent = lineText;
                            document.body.appendChild(measure);
                            const targetX = measure.getBoundingClientRect().width;
                            document.body.removeChild(measure);
                            // Focus editor then find closest position at that X offset
                            noteEditor.commands.focus('start');
                            const view = noteEditor.view;
                            const firstLine = view.coordsAtPos(1);
                            const pos = view.posAtCoords({ left: view.dom.getBoundingClientRect().left + targetX, top: firstLine.top + 1 });
                            if (pos) {
                              view.dispatch(view.state.tr.setSelection(
                                (view.state.selection.constructor as typeof import('@tiptap/pm/state').Selection).near(view.state.doc.resolve(pos.pos))
                              ));
                            }
                          }
                        }
                        if (e.key === "ArrowRight") {
                          const ta = e.currentTarget;
                          if (ta.selectionStart === ta.value.length && ta.selectionEnd === ta.value.length) {
                            e.preventDefault();
                            noteEditor?.commands.focus('start');
                          }
                        }
                      }}
                      className={`w-full text-[28px] font-semibold text-text-body bg-transparent border-0 focus:outline-none placeholder:text-text-tertiary resize-none overflow-hidden text-balance ${!isNoteEditable ? 'cursor-default' : ''}`}
                      placeholder="Untitled"
                      style={{ height: 'auto' }}
                    />
                  </div>

                  {/* Note editor */}
                  <div
                    className="px-6 pb-4"
                    onBlur={() => flushPendingSave()}
                  >
                    {isHydratingNote ? (
                      <div className="space-y-3 py-2 animate-pulse">
                        <div className="h-4 bg-bg-gray rounded w-full" />
                        <div className="h-4 bg-bg-gray rounded w-[90%]" />
                        <div className="h-4 bg-bg-gray rounded w-[75%]" />
                        <div className="h-4 bg-bg-gray rounded w-[85%]" />
                        <div className="h-4 bg-bg-gray rounded w-[60%]" />
                      </div>
                    ) : (
                      <Suspense
                        fallback={
                          <div className="py-2 text-sm text-text-secondary">
                            Loading editor...
                          </div>
                        }
                      >
                        <NoteEditor
                          content={noteContent}
                          onChange={handleNoteContentChange}
                          placeholder="Start writing..."
                          onEditorReady={setNoteEditor}
                          editable={isNoteEditable}
                          onImageUpload={handleEditorImageUpload}
                          onBackspaceAtStart={() => {
                            titleInputRef.current?.focus();
                            // Place cursor at end of title
                            const len = titleInputRef.current?.value.length ?? 0;
                            titleInputRef.current?.setSelectionRange(len, len);
                          }}
                          onArrowUpAtStart={(pixelX: number) => {
                            const input = titleInputRef.current;
                            if (!input) return;
                            input.focus();
                            // Binary search for the character position matching pixelX
                            const text = input.value;
                            const measure = document.createElement('span');
                            const style = window.getComputedStyle(input);
                            measure.style.font = style.font;
                            measure.style.fontSize = style.fontSize;
                            measure.style.fontWeight = style.fontWeight;
                            measure.style.fontFamily = style.fontFamily;
                            measure.style.letterSpacing = style.letterSpacing;
                            measure.style.position = 'absolute';
                            measure.style.visibility = 'hidden';
                            measure.style.whiteSpace = 'pre';
                            document.body.appendChild(measure);
                            let best = text.length;
                            for (let i = 0; i <= text.length; i++) {
                              measure.textContent = text.slice(0, i);
                              if (measure.getBoundingClientRect().width >= pixelX) {
                                best = i;
                                break;
                              }
                            }
                            document.body.removeChild(measure);
                            input.setSelectionRange(best, best);
                          }}
                        />
                      </Suspense>
                    )}
                  </div>
                </div>
              </div>
              </div>
            </>
          ) : (
            <div className="flex-1 flex flex-col">
              {/* Header with buttons */}
              <div className="h-12 shrink-0 flex items-center justify-end pl-6 pr-3 border-b border-border-gray">
                <HeaderButtons onSettingsClick={() => setShowSettingsDropdown(!showSettingsDropdown)} settingsButtonRef={settingsButtonRef} />
              </div>
              {/* Empty state - relative for overlay panels */}
              <div className="flex-1 flex items-center justify-center relative">
                {accessDenied && urlDocumentId === accessDenied.resourceId ? (
                  <RequestAccessCard
                    resourceType={accessDenied.resourceType}
                    resourceId={accessDenied.resourceId}
                    isAuthenticated={true}
                  />
                ) : selectedNoteId || urlDocumentId ? (
                  /* Document is selected but data hasn't loaded yet — show skeleton */
                  <div className="w-full max-w-3xl mx-auto px-10 pt-10 animate-pulse">
                    <div className="h-8 bg-bg-gray rounded w-1/3 mb-6" />
                    <div className="space-y-3">
                      <div className="h-4 bg-bg-gray rounded w-full" />
                      <div className="h-4 bg-bg-gray rounded w-5/6" />
                      <div className="h-4 bg-bg-gray rounded w-4/6" />
                    </div>
                  </div>
                ) : (
                  <div className="text-center">
                    <HugeiconsIcon
                      icon={File02Icon}
                      size={48}
                      className="mx-auto mb-4 text-text-tertiary opacity-50"
                    />
                    <p className="text-lg font-medium text-text-secondary">
                      No Note Selected
                    </p>
                    <p className="text-sm text-text-tertiary mt-1">
                      Select a note or create a new one
                    </p>
                    <button
                      onClick={handleCreateNote}
                      disabled={isCreatingNote}
                      className="mt-4 flex items-center gap-2 px-4 py-2 text-sm font-medium text-text-light bg-brand-primary hover:opacity-90 rounded-lg transition-opacity disabled:opacity-50 mx-auto"
                    >
                      <HugeiconsIcon icon={Add01Icon} size={16} />
                      New Note
                    </button>
                  </div>
                )}
              </div>
            </div>
          )}
          </div>
          <VersionHistoryPanel
            documentId={selectedNoteId}
            saveStatus={effectiveSaveStatus}
            onRestore={(content, title) => {
              setNoteContent(content);
              setNoteTitle(title);
              if (noteEditor) {
                noteEditor.commands.setContent(content, { contentType: 'markdown' });
              }
            }}
          />
        </div>

        {/* Image Preview Modal */}
        {createPortal(
          <AnimatePresence>
            {previewImage && (
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.15 }}
                onClick={closeImagePreview}
                style={{
                  position: "fixed",
                  inset: 0,
                  zIndex: 50,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  backgroundColor: "rgba(0, 0, 0, 0.8)",
                  padding: 24,
                }}
              >
                <button
                  onClick={closeImagePreview}
                  style={{
                    position: "absolute",
                    top: 16,
                    right: 16,
                    width: 40,
                    height: 40,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: "white",
                    background: "rgba(0, 0, 0, 0.5)",
                    border: "none",
                    borderRadius: 8,
                    cursor: "pointer",
                  }}
                >
                  <HugeiconsIcon icon={Cancel01Icon} size={24} />
                </button>
                <a
                  href={previewImage.url}
                  download={previewImage.title}
                  onClick={(e) => e.stopPropagation()}
                  style={{
                    position: "absolute",
                    top: 16,
                    right: 72,
                    width: 40,
                    height: 40,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    color: "white",
                    background: "rgba(0, 0, 0, 0.5)",
                    border: "none",
                    borderRadius: 8,
                    cursor: "pointer",
                    textDecoration: "none",
                  }}
                >
                  <HugeiconsIcon icon={Download04Icon} size={24} />
                </a>
                <motion.img
                  initial={{ opacity: 0, scale: 0.9 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, scale: 0.9 }}
                  transition={{ duration: 0.15 }}
                  src={previewImage.url}
                  alt={previewImage.title}
                  onClick={(e) => e.stopPropagation()}
                  style={{
                    maxWidth: "100%",
                    maxHeight: "100%",
                    objectFit: "contain",
                    borderRadius: 8,
                    boxShadow: "0 25px 50px -12px rgb(0 0 0 / 0.5)",
                  }}
                />
              </motion.div>
            )}
          </AnimatePresence>,
          document.body,
        )}

      {/* PDF Viewer Modal */}
      {previewPdf && (
        <Suspense fallback={<div className="fixed inset-0 z-50 bg-black/70" />}>
          <PdfViewer
            url={previewPdf.url}
            title={previewPdf.title}
            onClose={() => setPreviewPdf(null)}
          />
        </Suspense>
      )}

      {/* DOCX Viewer Modal */}
      {previewDocx && (
        <Suspense fallback={<div className="fixed inset-0 z-50 bg-black/70" />}>
          <DocxViewer
            url={previewDocx.url}
            title={previewDocx.title}
            onClose={() => setPreviewDocx(null)}
          />
        </Suspense>
      )}

      {/* PPTX Viewer Modal */}
      {previewPptx && (
        <Suspense fallback={<div className="fixed inset-0 z-50 bg-black/70" />}>
          <PptxViewer
            url={previewPptx.url}
            title={previewPptx.title}
            onClose={() => setPreviewPptx(null)}
          />
        </Suspense>
      )}

      {/* Video Viewer Modal */}
      {previewVideo && (
        <Suspense fallback={<div className="fixed inset-0 z-50 bg-black/70" />}>
          <VideoViewer
            url={previewVideo.url}
            title={previewVideo.title}
            onClose={() => setPreviewVideo(null)}
          />
        </Suspense>
      )}

      {/* Audio Viewer Modal */}
      {previewAudio && (
        <Suspense fallback={<div className="fixed inset-0 z-50 bg-black/70" />}>
          <AudioViewer
            url={previewAudio.url}
            title={previewAudio.title}
            onClose={() => setPreviewAudio(null)}
          />
        </Suspense>
      )}

      {/* XLSX Viewer Modal */}
      {previewXlsx && (
        <Suspense fallback={<div className="fixed inset-0 z-50 bg-black/70" />}>
          <XlsxViewer
            url={previewXlsx.url}
            title={previewXlsx.title}
            onClose={() => setPreviewXlsx(null)}
          />
        </Suspense>
      )}

      {/* Delete Confirmation Modal */}
      <ConfirmModal
        isOpen={!!deleteTarget}
        title={
          deleteTarget?.is_folder || deleteTarget?.type === "folder"
            ? "Delete Folder"
            : "Delete Note"
        }
        message={`Are you sure you want to delete "${deleteTarget?.title || "Untitled"}"? This action cannot be undone.`}
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirm}
        onCancel={() => setDeleteTarget(null)}
      />

      {/* Permission Error Modal */}
      <ConfirmModal
        isOpen={!!permissionError}
        title="Cannot Delete"
        message={permissionError || ""}
        variant="warning"
        confirmLabel="OK"
        hideCancelButton
        onConfirm={() => setPermissionError(null)}
        onCancel={() => setPermissionError(null)}
      />

      {/* Files Settings Dropdown */}
      <FilesSettingsModal
        isOpen={showSettingsDropdown}
        onClose={() => setShowSettingsDropdown(false)}
        trigger={settingsButtonRef}
        fileId={selectedNote?.id}
        isFullWidth={isNoteFullWidth}
        onToggleFullWidth={toggleNoteFullWidth}
        onDownload={() => {
          if (!noteEditor) return;
          const md = noteEditor.getMarkdown();
          const blob = new Blob([md], { type: "text/markdown" });
          const url = URL.createObjectURL(blob);
          const a = document.createElement("a");
          a.href = url;
          a.download = `${noteTitle || "Untitled"}.md`;
          a.click();
          URL.revokeObjectURL(url);
        }}
        onOpenVersionHistory={toggleVersionHistory}
        onRename={() => {
          titleInputRef.current?.focus();
          titleInputRef.current?.select();
        }}
        onDuplicate={selectedNote ? () => duplicateDocument(selectedNote.id) : undefined}
        onMove={selectedNote && allFolders.length > 0 ? () => setMoveMenuDocId(selectedNote.id) : undefined}
        onDelete={selectedNote ? () => setDeleteTarget(selectedNote) : undefined}
      />
      </div>
    </div>
  );
}
