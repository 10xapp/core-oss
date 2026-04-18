import { useCallback, useEffect, useState, useRef, useMemo } from "react";
import { useParams, useNavigate } from "react-router-dom";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import { useProjectsStore } from "../../stores/projectsStore";
import { useProjectBoards, usePrefetchBoards } from "../../hooks/queries/useProjects";
import { updateProjectBoard, deleteProjectBoard } from "../../api/client";
import { useQueryClient } from "@tanstack/react-query";
import { projectKeys } from "../../hooks/queries/keys";
import ProjectSidebar from "./components/ProjectSidebar";
import KanbanBoard from "./components/KanbanBoard";
import CreateProjectModal from "./components/CreateProjectModal";
import ProjectsFilterBar from "./components/ProjectsFilterBar";
import ProjectsListView from "./components/ProjectsListView";
import ProjectsSettingsModal from "./components/ProjectsSettingsModal";
import ProjectsSettingsDropdown from "./components/ProjectsSettingsDropdown";
import { HeaderButtons } from "../MiniAppHeader";
import { Columns3 } from "lucide-react";
import { Icon } from "../ui/Icon";

export default function ProjectsView() {
  const { workspaceId, boardId: urlBoardId } = useParams<{ workspaceId: string; boardId?: string }>();
  const navigate = useNavigate();
  const workspaces = useWorkspaceStore((state) => state.workspaces);
  const workspace = workspaces.find((w) => w.id === workspaceId);
  const projectsApp = workspace?.apps.find((app) => app.type === "projects");

  const workspaceAppId = useProjectsStore((state) => state.workspaceAppId);
  const setWorkspaceAppId = useProjectsStore(
    (state) => state.setWorkspaceAppId,
  );
  const setWorkspaceId = useProjectsStore((state) => state.setWorkspaceId);
  const activeProjectId = useProjectsStore((state) => state.activeProjectId);
  const setActiveProject = useProjectsStore((state) => state.setActiveProject);
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [showSettingsModal, setShowSettingsModal] = useState(false);
  const [settingsModalTab, setSettingsModalTab] = useState<"board" | "app">("board");
  const [showSettingsDropdown, setShowSettingsDropdown] = useState(false);
  const settingsButtonRef = useRef<HTMLButtonElement>(null);
  const [viewMode, setViewMode] = useState<"kanban" | "list">("kanban");
  const queryClient = useQueryClient();

  // React Query: fetch boards for auto-select
  const { data: boards = [] } = useProjectBoards(workspaceAppId);
  const prefetchBoards = usePrefetchBoards();
  const hasPrefetchedRef = useRef<string | null>(null);

  // Get active board name
  const activeBoard = useMemo(
    () => boards.find((b) => b.id === activeProjectId),
    [boards, activeProjectId]
  );

  // Prefetch ALL boards when entering projects view for instant switching
  useEffect(() => {
    if (workspaceAppId && boards.length > 0 && hasPrefetchedRef.current !== workspaceAppId) {
      hasPrefetchedRef.current = workspaceAppId;
      // Prefetch in background - don't await
      prefetchBoards(workspaceAppId);
    }
  }, [workspaceAppId, boards.length, prefetchBoards]);

  // Initialize workspace app context
  useEffect(() => {
    if (projectsApp?.id) {
      setWorkspaceAppId(projectsApp.id);
    }
  }, [projectsApp?.id, setWorkspaceAppId]);

  // Initialize workspace context for member fetching
  useEffect(() => {
    if (workspaceId) {
      setWorkspaceId(workspaceId);
    }
  }, [workspaceId, setWorkspaceId]);

  // Sync URL boardId to store
  useEffect(() => {
    if (urlBoardId && urlBoardId !== activeProjectId) {
      setActiveProject(urlBoardId);
    }
  }, [urlBoardId]);

  // Auto-select first board when boards are loaded and no board is selected
  useEffect(() => {
    if (!urlBoardId && !activeProjectId && boards.length > 0) {
      setActiveProject(boards[0].id);
    }
  }, [urlBoardId, activeProjectId, boards, setActiveProject]);

  // When landing without a boardId, silently update URL from persisted store selection
  useEffect(() => {
    if (!urlBoardId && activeProjectId && workspaceId) {
      window.history.replaceState(null, '', `/workspace/${workspaceId}/projects/${activeProjectId}`);
    }
  }, [urlBoardId, activeProjectId, workspaceId]);

  // Keep URL in sync when store changes programmatically (e.g. auto-select after fetch)
  useEffect(() => {
    if (activeProjectId && activeProjectId !== urlBoardId && workspaceId) {
      window.history.replaceState(null, '', `/workspace/${workspaceId}/projects/${activeProjectId}`);
    }
  }, [activeProjectId, workspaceId]);

  const navigateToProject = useCallback((boardId: string) => {
    navigate(`/workspace/${workspaceId}/projects/${boardId}`);
  }, [navigate, workspaceId]);

  const handleSaveBoardSettings = useCallback(async (name: string, description: string) => {
    if (!activeProjectId) return;
    await updateProjectBoard(activeProjectId, { name, description });
    queryClient.invalidateQueries({ queryKey: projectKeys.boards(workspaceAppId ?? '') });
  }, [activeProjectId, workspaceAppId, queryClient]);

  const handleDeleteBoard = useCallback(async () => {
    if (!activeProjectId || !workspaceId) return;
    await deleteProjectBoard(activeProjectId);
    queryClient.invalidateQueries({ queryKey: projectKeys.boards(workspaceAppId ?? '') });
    setActiveProject(null);
    navigate(`/workspace/${workspaceId}/projects`);
  }, [activeProjectId, workspaceAppId, workspaceId, queryClient, navigate, setActiveProject]);

  if (!workspace) {
    return (
      <div className="flex-1 flex items-center justify-center bg-background-primary">
        <p className="text-text-tertiary">Workspace not found</p>
      </div>
    );
  }

  return (
    <div className="flex-1 flex h-full min-w-0 overflow-hidden">
      {/* Main content container - light bg with rounded corners */}
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Sidebar */}
        <div className="w-[212px] shrink-0 flex flex-col overflow-hidden">
          <ProjectSidebar onCreateClick={() => setShowCreateModal(true)} onSelectProject={navigateToProject} />
        </div>

        {/* Board Content */}
        <div className="flex-1 flex flex-col min-w-0 overflow-hidden bg-white rounded-r-lg">
          {activeProjectId ? (
            <>
              {/* Board Header */}
              <div className="h-12 flex items-center justify-between pl-5 pr-3 border-b border-border-gray">
                <div className="flex items-center gap-2">
                  <Icon icon={Columns3} size={18} className="text-text-body" />
                  <h1 className="text-base font-semibold text-text-body">
                    {activeBoard?.name || "Project Board"}
                  </h1>
                </div>
                <HeaderButtons
                  onSettingsClick={() => setShowSettingsDropdown(prev => !prev)}
                  settingsButtonRef={settingsButtonRef}
                />
              </div>
              {/* Filter bar + content area with overlay panels */}
              <div className="flex-1 flex flex-col overflow-hidden relative">
                <ProjectsFilterBar viewMode={viewMode} setViewMode={setViewMode} />
                <div className="flex-1 overflow-hidden">
                  {viewMode === "kanban" ? <KanbanBoard /> : <ProjectsListView />}
                </div>
              </div>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center relative">
              <div className="text-center">
                <p className="text-text-tertiary mb-4">No projects yet</p>
                <button
                  onClick={() => setShowCreateModal(true)}
                  className="px-4 py-2 bg-brand-primary text-white rounded-md hover:opacity-90 transition-opacity"
                >
                  Create your first project
                </button>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Create Project Modal */}
      {showCreateModal && (
        <CreateProjectModal
          isOpen={showCreateModal}
          onClose={() => setShowCreateModal(false)}
          onCreated={navigateToProject}
        />
      )}

      {/* Settings Dropdown */}
      <ProjectsSettingsDropdown
        isOpen={showSettingsDropdown}
        onClose={() => setShowSettingsDropdown(false)}
        trigger={settingsButtonRef}
        onBoardSettings={activeProjectId ? () => { setSettingsModalTab("board"); setShowSettingsModal(true); } : undefined}
        onAppSettings={() => { setSettingsModalTab("app"); setShowSettingsModal(true); }}
        onDelete={activeProjectId ? handleDeleteBoard : undefined}
      />

      {/* Projects Settings Modal */}
      <ProjectsSettingsModal
        isOpen={showSettingsModal}
        onClose={() => setShowSettingsModal(false)}
        boardId={activeProjectId}
        boardName={activeBoard?.name || ""}
        boardDescription={activeBoard?.description || ""}
        initialTab={settingsModalTab}
        onSave={handleSaveBoardSettings}
        onDelete={handleDeleteBoard}
      />
    </div>
  );
}
