import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { useNavigate, useLocation } from "react-router-dom";
import { avatarGradient } from "../../utils/avatarGradient";
import { motion, AnimatePresence } from "motion/react";
import { useKeyboardNavigation } from "../../hooks/useKeyboardNavigation";
import { useViewPrefetch } from "../../hooks/useViewPrefetch";
import { usePresenceStore } from "../../stores/presenceStore";
import {
  Folder,
  MessageCircle,
  Users,
  Settings,
  ChevronDown,
  Pin,
  Plus,
  Trash2,
  LogOut,
  LogIn,
  LayoutDashboard,
  Camera,
  Mail,
  Calendar,
  Brain,
  Code,
  ExternalLink,
} from "lucide-react";
import { Icon, type LucideIcon } from "../ui/Icon";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import { useProductStore } from "../../stores/productStore";
import { useAuthStore } from "../../stores/authStore";
import { useConversationStore } from "../../stores/conversationStore";
import { useMessagesStore } from "../../stores/messagesStore";
import { useEmailStore } from "../../stores/emailStore";
import { copyTextToClipboard } from "../../lib/clipboard";
import Modal from "../Modal/Modal";
import Dropdown from "../Dropdown/Dropdown";
import {
  getWorkspaceMembers,
  getWorkspaceInvitations,
  createWorkspaceInvitation,
  getWorkspaceInvitationShareLink,
  revokeWorkspaceInvitation,
  removeWorkspaceMember,
  updateWorkspaceMemberRole,
  getPresignedUploadUrl,
  confirmFileUpload,
  reorderWorkspaceApps,
  type WorkspaceInvitation,
  type WorkspaceMember,
} from "../../api/client";
import { resolveUploadMimeType } from "../../utils/uploadMime";

import SettingsView from "../Settings/SettingsView";

interface NavItem {
  id: string;
  name: string;
  icon: LucideIcon;
  path: string;
  type?: string;
}


// Available mini app types
const availableAppTypes = [
  {
    type: "chat",
    name: "Chat",
    icon: MessageCircle,
  },
  {
    type: "messages",
    name: "Messages",
    icon: Users,
  },
  {
    type: "files",
    name: "Files",
    icon: Folder,
  },
  {
    type: "dashboard",
    name: "Personal",
    icon: LayoutDashboard,
  },
  {
    type: "projects",
    name: "Projects",
    icon: Pin,
  },
  {
    type: "agents",
    name: "Agents",
    icon: Brain,
  },
  {
    type: "email",
    name: "Email",
    icon: Mail,
  },
  {
    type: "calendar",
    name: "Calendar",
    icon: Calendar,
  },
];

// Map app types to their icons
const appIcons: Record<string, LucideIcon> = {
  chat: MessageCircle,
  files: Folder,
  team: Users,
  messages: Users,
  projects: Pin,
  dashboard: LayoutDashboard,
  agents: Brain,
  email: Mail,
  calendar: Calendar,
};

// Generate workspace-specific path for an app
const getAppPath = (workspaceId: string, appType: string) =>
  `/workspace/${workspaceId}/${appType}`;

type ModalType = "createWorkspace" | "workspaceSettings" | "dashboardSettings" | null;
type WorkspaceSettingsTab = "members" | "apps" | "settings";


function SelfPresenceAvatar({ avatarUrl, name, fallbackLetter, userId }: {
  avatarUrl?: string | null;
  name: string;
  fallbackLetter: string;
  userId?: string;
}) {
  const onlineUserIds = usePresenceStore((s) => s.onlineUserIds);
  const isOnline = userId ? onlineUserIds.has(userId) : false;

  return (
    <div className="relative">
      {avatarUrl ? (
        <img src={avatarUrl} alt={name} className="w-8 h-8 rounded-full object-cover" />
      ) : (
        <div
          className="w-8 h-8 rounded-full flex items-center justify-center"
          style={{ background: avatarGradient(name) }}
        >
          <span className="text-xs font-medium text-white">{fallbackLetter}</span>
        </div>
      )}
      {isOnline && (
        <span className="absolute -bottom-0.5 -right-0.5 w-2.5 h-2.5 bg-green-500 rounded-full border-2 border-white" />
      )}
    </div>
  );
}

export default function Sidebar() {
  const navigate = useNavigate();
  const location = useLocation();
  const setActiveConversationId = useConversationStore(
    (s) => s.setActiveConversationId
  );
  const unreadCounts = useMessagesStore((s) => s.unreadCounts);
  const workspaceCache = useMessagesStore((s) => s.workspaceCache);
  const setWorkspaceAppId = useMessagesStore((s) => s.setWorkspaceAppId);
  const [openMenu, setOpenMenu] = useState<string | null>(null);
  // 'dashboard' shows core apps + mini apps, workspace ID shows only that workspace's mini apps
  const [selectedView, setSelectedView] = useState<"dashboard" | string>(
    "dashboard",
  );

  // Modal state
  const [showSettings, setShowSettings] = useState(false);
  const [modalType, setModalType] = useState<ModalType>(null);
  const [workspaceSettingsTab, setWorkspaceSettingsTab] = useState<WorkspaceSettingsTab>("members");
  const [selectedWorkspaceId, setSelectedWorkspaceId] = useState<string | null>(
    null,
  );
  const [formName, setFormName] = useState("");
  const [formEmoji, setFormEmoji] = useState("");
  const [formIconUrl, setFormIconUrl] = useState<string | null>(null);
  const [formIconR2Key, setFormIconR2Key] = useState<string | null>(null);
  const [isUploadingIcon, setIsUploadingIcon] = useState(false);
  const iconInputRef = useRef<HTMLInputElement>(null);
  const [editingApps, setEditingApps] = useState<
    { id: string; type: string; name: string }[]
  >([]);
  const [isCreatingWorkspace, setIsCreatingWorkspace] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  const [showDangerZone, setShowDangerZone] = useState(false);

  // Workspace members state
  const [members, setMembers] = useState<
    (WorkspaceMember & { email?: string; name?: string; avatar_url?: string })[]
  >([]);
  const [membersLoading, setMembersLoading] = useState(false);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<"member" | "admin">("member");
  const [inviteError, setInviteError] = useState("");
  const [inviteSuccess, setInviteSuccess] = useState("");
  const [inviting, setInviting] = useState(false);
  const [pendingInvites, setPendingInvites] = useState<WorkspaceInvitation[]>([]);
  const [pendingInvitesLoading, setPendingInvitesLoading] = useState(false);
  const [inviteActionBusyId, setInviteActionBusyId] = useState<string | null>(null);
  const membersCache = useRef<
    Map<string, (WorkspaceMember & { email?: string; name?: string; avatar_url?: string })[]>
  >(new Map());

  // Refs for dropdown triggers
  const menuButtonRefs = useRef<Map<string, HTMLElement>>(new Map());

  const {
    workspaces,
    activeWorkspaceId,
    setActiveWorkspace,
    addWorkspace,
    removeWorkspace,
    updateWorkspace,
    addMiniApp,
    removeMiniApp,
    recordSessionApp,
    getSessionApp,
  } = useWorkspaceStore();
  const { isAuthenticated, user, userProfile, signOut } = useAuthStore();
  const { activeProductType, setActiveProductType } = useProductStore();
  const { activeZone, setActiveZone } = useKeyboardNavigation();
  const { prefetchView } = useViewPrefetch();

  // Sync product type from URL when navigating directly
  useEffect(() => {
    if (location.pathname.startsWith('/builder')) {
      if (activeProductType !== 'ai_builder') setActiveProductType('ai_builder');
    } else if (location.pathname.startsWith('/sites')) {
      if (activeProductType !== 'website_builder') setActiveProductType('website_builder');
    } else {
      if (activeProductType !== 'workspace') setActiveProductType('workspace');
    }
  }, [location.pathname]); // eslint-disable-line react-hooks/exhaustive-deps

  // Preload all workspace images to avoid loading delay when switching
  useEffect(() => {
    workspaces.forEach((ws) => {
      if (ws.icon_url) {
        const img = new Image();
        img.src = ws.icon_url;
      }
    });
  }, [workspaces]);

  const loadPendingInvites = useCallback(async (workspaceId: string) => {
    setPendingInvitesLoading(true);
    try {
      const invitations = await getWorkspaceInvitations(workspaceId);
      setPendingInvites(
        invitations.filter((invitation) => invitation.status === "pending"),
      );
    } catch (err) {
      console.error("Failed to fetch workspace invitations:", err);
      setPendingInvites([]);
    } finally {
      setPendingInvitesLoading(false);
    }
  }, []);

  // Get target workspace based on selectedView for both display and keyboard nav
  const targetWorkspaceForNav = useMemo(() => {
    if (selectedView === "dashboard") {
      return workspaces.find((w) => w.isDefault) || workspaces[0];
    }
    return workspaces.find((w) => w.id === selectedView);
  }, [selectedView, workspaces]);

  // Build apps list for keyboard navigation
  const miniAppsForNav = useMemo(() => {
    if (!targetWorkspaceForNav) return [];
    const topLevel = ['chat', 'email', 'calendar'];
    const pinnedTypes = ['email', 'calendar'];
    const workspaceApps = (targetWorkspaceForNav.apps || [])
      .filter((app) => appIcons[app.type] && !pinnedTypes.includes(app.type))
      .map((app) => ({
        id: app.id,
        path: targetWorkspaceForNav.isDefault && topLevel.includes(app.type)
          ? `/${app.type}`
          : getAppPath(targetWorkspaceForNav.id, app.type),
        type: app.type,
      }));
    const pinnedApps = pinnedTypes.map((type) => ({
      id: type,
      path: `/${type}`,
      type,
    }));
    return [...workspaceApps, ...pinnedApps];
  }, [targetWorkspaceForNav]);

  // All sidebar nav items come from workspace apps
  const sidebarNavItems = useMemo(() => {
    return miniAppsForNav.map((app) => ({
      id: app.id,
      path: app.path,
      type: "app",
    }));
  }, [miniAppsForNav]);

  const currentNavIndex = useMemo(() => {
    const currentPath = location.pathname;
    const index = sidebarNavItems.findIndex((item) => {
      // Match both direct paths and workspace-prefixed paths
      return (
        currentPath === item.path ||
        currentPath.startsWith(item.path + "/") ||
        currentPath.match(new RegExp(`/workspace/[^/]+/${item.id}(/|$)`))
      );
    });
    return index >= 0 ? index : 0;
  }, [location.pathname, sidebarNavItems]);

  const sidebarContainerRef = useRef<HTMLDivElement>(null);

  const handleSidebarKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (sidebarNavItems.length === 0) return;
      if (openMenu) return;
      if (activeZone !== "main-sidebar") return;

      if (e.key === "ArrowDown") {
        e.preventDefault();
        const nextIndex =
          currentNavIndex < sidebarNavItems.length - 1
            ? currentNavIndex + 1
            : 0;
        const item = sidebarNavItems[nextIndex];
        if (item.id === "chat") setActiveConversationId(null);
        navigate(item.path);
      } else if (e.key === "ArrowUp") {
        e.preventDefault();
        const prevIndex =
          currentNavIndex > 0
            ? currentNavIndex - 1
            : sidebarNavItems.length - 1;
        const item = sidebarNavItems[prevIndex];
        if (item.id === "chat") setActiveConversationId(null);
        navigate(item.path);
      } else if (e.key === "ArrowRight") {
        e.preventDefault();
        setActiveZone("app-sidebar");
      }
    },
    [
      sidebarNavItems,
      currentNavIndex,
      navigate,
      openMenu,
      setActiveConversationId,
      setActiveZone,
      activeZone,
    ],
  );

  // Don't steal focus when compose modal is open
  const composeIsOpen = useEmailStore((s) => s.compose.isOpen);

  useEffect(() => {
    // Don't steal focus when compose modal is open - user is typing an email
    if (composeIsOpen) return;
    if (activeZone === "main-sidebar") {
      sidebarContainerRef.current?.focus();
    }
  }, [activeZone, composeIsOpen]);

  const openWorkspaceSettingsModal = async (workspaceId: string, tab: WorkspaceSettingsTab = "members") => {
    const workspace = workspaces.find((w) => w.id === workspaceId);
    if (workspace) {
      setSelectedWorkspaceId(workspaceId);
      setFormName(workspace.name);
      setFormEmoji(workspace.emoji || "");
      setFormIconUrl(workspace.icon_url || null);
      setFormIconR2Key(workspace.icon_r2_key || null);
      setWorkspaceSettingsTab(tab);
      setModalType("workspaceSettings");
      setOpenMenu(null);

      // Load apps for editing
      const apps = (workspace?.apps || []).map((a) => ({
        id: a.id,
        type: a.type,
        name: a.name,
      }));
      setEditingApps(apps);

      // Load pending invites
      void loadPendingInvites(workspaceId);

      // Load members
      const cached = membersCache.current.get(workspaceId);
      if (cached) {
        setMembers(cached);
        return;
      }

      setMembersLoading(true);
      setMembers([]);
      try {
        const data = await getWorkspaceMembers(workspaceId);
        const memberData = data as (WorkspaceMember & {
          email?: string;
          name?: string;
          avatar_url?: string;
        })[];
        setMembers(memberData);
        membersCache.current.set(workspaceId, memberData);
      } catch (err) {
        console.error("Failed to fetch members:", err);
      } finally {
        setMembersLoading(false);
      }
    }
  };

  const moveAppUp = (index: number) => {
    if (index === 0) return;
    setEditingApps((prev) => {
      const newApps = [...prev];
      [newApps[index - 1], newApps[index]] = [
        newApps[index],
        newApps[index - 1],
      ];
      return newApps;
    });
  };

  const moveAppDown = (index: number) => {
    setEditingApps((prev) => {
      if (index === prev.length - 1) return prev;
      const newApps = [...prev];
      [newApps[index], newApps[index + 1]] = [
        newApps[index + 1],
        newApps[index],
      ];
      return newApps;
    });
  };

  const removeAppFromEditing = (appId: string) => {
    setEditingApps((prev) => prev.filter((a) => a.id !== appId));
  };

  const addAppToEditing = (appType: string) => {
    const appInfo = availableAppTypes.find((a) => a.type === appType);
    if (appInfo) {
      const tempId = `temp-${Date.now()}`;
      setEditingApps((prev) => [
        ...prev,
        { id: tempId, type: appType, name: appInfo.name },
      ]);
    }
  };

  const closeModal = () => {
    setModalType(null);
    setSelectedWorkspaceId(null);
    setFormName("");
    setFormEmoji("");
    setFormIconUrl(null);
    setEditingApps([]);
    setIsCreatingWorkspace(false);
    setIsSaving(false);
    setSaveError(null);
    setMembers([]);
    setInviteEmail("");
    setInviteRole("member");
    setInviteError("");
    setInviteSuccess("");
    setPendingInvites([]);
    setPendingInvitesLoading(false);
    setInviteActionBusyId(null);
    setShowDangerZone(false);
  };

  const handleCreateWorkspace = async () => {
    if (!formName.trim() || isCreatingWorkspace) return;
    setIsCreatingWorkspace(true);
    try {
      const workspace = await addWorkspace(formName.trim(), formEmoji);
      closeModal();
      // Navigate to the new workspace
      setSelectedView(workspace.id);
      setActiveWorkspace(workspace.id);
      const firstAppType = workspace.apps[0]?.type ?? "messages";
      navigate(getAppPath(workspace.id, firstAppType));
    } catch (err) {
      console.error("Failed to create workspace:", err);
    } finally {
      setIsCreatingWorkspace(false);
    }
  };

  const handleEditWorkspace = async () => {
    if (!formName.trim() || !selectedWorkspaceId) return;
    try {
      const workspace = workspaces.find((w) => w.id === selectedWorkspaceId);
      const hadIcon = workspace?.icon_r2_key || workspace?.icon_url;
      const shouldClearIcon = hadIcon && !formIconR2Key && !formIconUrl;

      await updateWorkspace(selectedWorkspaceId, {
        name: formName.trim(),
        emoji: formEmoji,
        icon_r2_key: formIconR2Key || undefined,
        clear_icon: shouldClearIcon || undefined,
      });
      closeModal();
    } catch (err) {
      console.error("Failed to update workspace:", err);
    }
  };

  const handleIconUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    const mimeType = resolveUploadMimeType(file);
    if (!mimeType.startsWith("image/")) {
      console.error("Invalid file type");
      return;
    }

    setIsUploadingIcon(true);
    try {
      const uploadInfo = await getPresignedUploadUrl({
        workspaceId: selectedWorkspaceId || undefined,
        filename: file.name,
        contentType: mimeType,
        fileSize: file.size,
        createDocument: false,
      });

      const uploadResponse = await fetch(uploadInfo.upload_url, {
        method: "PUT",
        headers: uploadInfo.headers,
        body: file,
      });

      if (!uploadResponse.ok) {
        throw new Error("Upload failed");
      }

      const confirmResult = await confirmFileUpload(uploadInfo.file_id, {
        createDocument: false,
      });

      setFormIconR2Key(confirmResult.file.r2_key || uploadInfo.r2_key);
      setFormIconUrl(confirmResult.file.public_url || uploadInfo.public_url);
      setFormEmoji("");
    } catch (err) {
      console.error("Failed to upload icon:", err);
    } finally {
      setIsUploadingIcon(false);
      if (e.target) e.target.value = "";
    }
  };

  const handleInviteMember = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!selectedWorkspaceId || !inviteEmail.trim()) return;

    setInviteError("");
    setInviteSuccess("");
    setInviting(true);

    try {
      await createWorkspaceInvitation(
        selectedWorkspaceId,
        inviteEmail.trim(),
        inviteRole,
      );
      await loadPendingInvites(selectedWorkspaceId);
      const invitedEmail = inviteEmail.trim();
      setInviteEmail("");
      setInviteRole("member");
      setInviteSuccess(`Invitation sent to ${invitedEmail} as ${inviteRole}`);
      setTimeout(() => setInviteSuccess(""), 3000);
    } catch (err) {
      setInviteError(
        err instanceof Error ? err.message : "Failed to send invitation",
      );
    } finally {
      setInviting(false);
    }
  };

  const handleResendInvite = async (invitation: WorkspaceInvitation) => {
    if (!selectedWorkspaceId) return;

    setInviteError("");
    setInviteSuccess("");
    setInviteActionBusyId(invitation.id);

    try {
      await createWorkspaceInvitation(
        selectedWorkspaceId,
        invitation.email,
        invitation.role,
      );
      await loadPendingInvites(selectedWorkspaceId);
      setInviteSuccess(`Invitation resent to ${invitation.email}`);
      setTimeout(() => setInviteSuccess(""), 3000);
    } catch (err) {
      setInviteError(
        err instanceof Error ? err.message : "Failed to resend invitation",
      );
    } finally {
      setInviteActionBusyId(null);
    }
  };

  const handleCopyInviteLink = async (invitationId: string) => {
    setInviteError("");
    setInviteSuccess("");
    setInviteActionBusyId(invitationId);

    try {
      const result = await getWorkspaceInvitationShareLink(invitationId);
      await copyTextToClipboard(result.invite_url);
      setInviteSuccess("Invite link copied");
      setTimeout(() => setInviteSuccess(""), 3000);
    } catch (err) {
      setInviteError(
        err instanceof Error ? err.message : "Failed to copy invite link",
      );
    } finally {
      setInviteActionBusyId(null);
    }
  };

  const handleRevokeInvite = async (invitationId: string) => {
    if (!selectedWorkspaceId) return;

    setInviteError("");
    setInviteSuccess("");
    setInviteActionBusyId(invitationId);

    try {
      await revokeWorkspaceInvitation(invitationId);
      await loadPendingInvites(selectedWorkspaceId);
      setInviteSuccess("Invitation revoked");
      setTimeout(() => setInviteSuccess(""), 3000);
    } catch (err) {
      setInviteError(
        err instanceof Error ? err.message : "Failed to revoke invitation",
      );
    } finally {
      setInviteActionBusyId(null);
    }
  };

  const handleRemoveMember = async (memberId: string, memberUserId: string) => {
    if (!selectedWorkspaceId) return;

    try {
      await removeWorkspaceMember(selectedWorkspaceId, memberUserId);
      const updatedMembers = members.filter((m) => m.id !== memberId);
      setMembers(updatedMembers);
      membersCache.current.set(selectedWorkspaceId, updatedMembers);

      // Refresh DMs — backend cleans up empty DM channels with removed user
      useMessagesStore.getState().fetchDMs();
    } catch (err) {
      console.error("Failed to remove member:", err);
    }
  };

  const handleMemberRoleChange = async (member: { id: string; user_id: string; email?: string; name?: string }, newRole: 'member' | 'admin') => {
    if (!selectedWorkspaceId) return;
    try {
      const updated = await updateWorkspaceMemberRole(selectedWorkspaceId, member.user_id, newRole);
      const updatedMembers = members.map((m) =>
        m.id === member.id ? { ...m, role: updated.role } : m
      );
      setMembers(updatedMembers);
      membersCache.current.set(selectedWorkspaceId, updatedMembers);
    } catch (err) {
      console.error("Failed to update role:", err);
    }
  };

  useEffect(() => {
    if (!location.pathname.startsWith("/chat/")) {
      setActiveConversationId(null);
    }
  }, [location.pathname, setActiveConversationId]);

  // Track previous location to detect actual URL changes vs selectedView changes
  // Initialize to null so first mount triggers sync from URL
  const prevLocationRef = useRef<string | null>(null);

  // Sync selectedView with current route to handle direct navigation, back/forward buttons
  // Also record the current app for session-based workspace switching
  useEffect(() => {
    const isInitialMount = prevLocationRef.current === null;
    const locationChanged = isInitialMount || prevLocationRef.current !== location.pathname;
    prevLocationRef.current = location.pathname;

    const workspaceMatch = location.pathname.match(/^\/workspace\/([^/]+)\/([^/]+)/);
    if (workspaceMatch) {
      const workspaceId = workspaceMatch[1];
      const appType = workspaceMatch[2];
      const isDefaultWs = workspaces.find((w) => w.id === workspaceId)?.isDefault;

      // Record the app type for this workspace (for session-based navigation)
      // This includes core apps when accessed via workspace URL
      recordSessionApp(workspaceId, appType);

      // Only sync selectedView when LOCATION changed (browser nav, direct URL)
      // Don't sync when selectedView changed (user clicking workspace selector)
      // This prevents the effect from reverting intentional selectedView changes
      if (locationChanged) {
        if (isDefaultWs) {
          // Default workspace apps should stay in dashboard mode
          if (selectedView !== "dashboard") {
            setSelectedView("dashboard");
          }
        } else if (selectedView !== workspaceId) {
          setSelectedView(workspaceId);
          setActiveWorkspace(workspaceId);
        }
      }
    } else {
      // Handle top-level app URLs (e.g., /chat, /email, /calendar)
      // Record them for the currently active workspace
      const appMatch = location.pathname.match(/^\/(chat|email|calendar|messages|files|projects|agents)(?:\/|$)/);
      if (appMatch && activeWorkspaceId) {
        recordSessionApp(activeWorkspaceId, appMatch[1]);
      }
    }
  }, [location.pathname, selectedView, setActiveWorkspace, workspaces, recordSessionApp, activeWorkspaceId]);

  const isActivePath = (path: string) =>
    location.pathname === path || location.pathname.startsWith(path + "/");

  // Get current workspace for display
  const currentWorkspace =
    selectedView === "dashboard"
      ? null
      : workspaces.find((w) => w.id === selectedView);

  // Get target workspace for mini apps
  const targetWorkspace =
    selectedView === "dashboard"
      ? workspaces.find((w) => w.isDefault) || workspaces[0]
      : workspaces.find((w) => w.id === selectedView);

  const currentMemberRole = useMemo(() => {
    if (!user?.id) return null;
    return members.find((member) => member.user_id === user.id)?.role || null;
  }, [members, user?.id]);

  const editingWorkspace = workspaces.find((w) => w.id === selectedWorkspaceId);
  const canManageWorkspaceMembers =
    currentMemberRole === "owner" ||
    currentMemberRole === "admin" ||
    editingWorkspace?.role === "owner" ||
    editingWorkspace?.role === "admin";

  // Apps with top-level routes (for default workspace shorthand URLs)
  const topLevelAppTypes = ['chat', 'email', 'calendar'];

  const displayMiniApps: NavItem[] = targetWorkspace
    ? (targetWorkspace.apps || [])
        .filter((app) => appIcons[app.type])
        .map((app) => ({
          id: app.id,
          name: app.name,
          icon: appIcons[app.type],
          path: targetWorkspace.isDefault && topLevelAppTypes.includes(app.type)
            ? `/${app.type}`
            : getAppPath(targetWorkspace.id, app.type),
          type: app.type,
        }))
    : [];

  // Icon button styles (dark sidebar)
  const iconBtn =
    "w-10 h-10 flex items-center justify-center rounded-lg transition-all relative outline-none focus:outline-none";
  const iconBtnActive = "bg-black/10 text-text-body";
  const iconBtnInactive =
    "text-[#323232] hover:bg-black/5";

  return (
    <>
      <div
        ref={sidebarContainerRef}
        tabIndex={0}
        onKeyDown={handleSidebarKeyDown}
        onFocus={() => setActiveZone("main-sidebar")}
        className="w-16 shrink-0 bg-[#E3E3E5] text-text-secondary h-full flex flex-col items-center pb-3 outline-none pt-3"
      >
        {/* Product & Workspace Selector */}
        <div className="relative mb-4">
          <button
            ref={(el) => {
              if (el) menuButtonRefs.current.set("view-selector", el);
            }}
            onClick={() =>
              setOpenMenu(openMenu === "view-selector" ? null : "view-selector")
            }
            title={
              activeProductType === 'ai_builder'
                ? "AI App Builder"
                : activeProductType === 'website_builder'
                ? "Website Builder"
                : selectedView === "dashboard"
                ? "Personal"
                : currentWorkspace?.name || "Select workspace"
            }
            className={`w-10 h-10 flex items-center justify-center rounded-lg transition-all ${
              openMenu === "view-selector"
                ? "bg-black/10"
                : "hover:bg-black/5"
            }`}
          >
            <AnimatePresence mode="wait">
              <motion.div
                key={activeProductType === 'workspace' ? selectedView : activeProductType}
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                exit={{ opacity: 0 }}
                transition={{ duration: 0.08 }}
                className={`w-9 h-9 flex items-center justify-center rounded-lg overflow-hidden ${
                  activeProductType === 'workspace' && selectedView === "dashboard" ? "bg-white shadow-sm" : ""
                }`}
              >
                {activeProductType === 'ai_builder' ? (
                  <Icon icon={Code} size={20} className="text-text-body" />
                ) : activeProductType === 'website_builder' ? (
                  <Icon icon={ExternalLink} size={20} className="text-text-body" />
                ) : selectedView === "dashboard" ? (
                  <img
                    src="/CoreLogo.png"
                    alt="Personal"
                    className="w-5 h-5"
                  />
                ) : currentWorkspace?.icon_url ? (
                  <img
                    src={currentWorkspace.icon_url}
                    alt={currentWorkspace.name}
                    className="w-full h-full object-cover"
                  />
                ) : (
                  <span className="w-full h-full flex items-center justify-center bg-gradient-to-br from-blue-500 to-purple-600 text-white font-semibold text-sm">
                    {currentWorkspace?.name.charAt(0).toUpperCase()}
                  </span>
                )}
              </motion.div>
            </AnimatePresence>
          </button>
          {/* Unread indicator - shows when OTHER workspaces have unreads */}
          {(() => {
            const hasUnreadsInOtherWorkspaces = workspaces.some((ws) => {
              // Skip the currently selected workspace
              if (selectedView !== "dashboard" && ws.id === selectedView)
                return false;

              const messagesApp = ws.apps?.find(
                (app) => app.type === "messages",
              );
              const wsCache = messagesApp
                ? workspaceCache[messagesApp.id]
                : null;
              if (!wsCache) return false;

              const channelIds = [
                ...(wsCache.channels || []),
                ...(wsCache.dms || []),
              ].map((c) => c.id);
              return channelIds.some((id) => unreadCounts[id] > 0);
            });

            return hasUnreadsInOtherWorkspaces ? (
              <span className="absolute -top-0.5 -right-0.5 w-2.5 h-2.5 bg-red-500 rounded-full border-[1.5px] border-[#E3E3E5]" />
            ) : null;
          })()}
          <Dropdown
            isOpen={openMenu === "view-selector"}
            onClose={() => { setOpenMenu(null); }}
            trigger={{
              current: menuButtonRefs.current.get("view-selector") || null,
            }}
          >
            <div className="p-1.5 flex flex-col gap-0.5">
              {/* Dashboard + Workspace options */}
              <button
                onClick={() => {
                  setSelectedView("dashboard");
                  const defaultWs = workspaces.find((w) => w.isDefault);
                  setActiveWorkspace(defaultWs?.id ?? null);
                  setActiveProductType('workspace');
                  setOpenMenu(null);
                  setTimeout(() => {
                    const sessionApp = defaultWs ? getSessionApp(defaultWs.id) : null;
                    const coreApps = ["chat", "email", "calendar"];
                    if (sessionApp && defaultWs) {
                      if (coreApps.includes(sessionApp)) {
                        navigate(`/${sessionApp}`);
                      } else {
                        navigate(`/workspace/${defaultWs.id}/${sessionApp}`);
                      }
                    } else {
                      navigate("/chat");
                    }
                    sidebarContainerRef.current?.focus();
                  }, 0);
                }}
                className={`w-full px-2.5 py-2 text-left text-sm flex items-center gap-2.5 rounded-lg ${
                  selectedView === "dashboard" && activeProductType === 'workspace'
                    ? "bg-bg-gray"
                    : "hover:bg-bg-gray"
                }`}
              >
                <img src="/CoreLogo.png" alt="Personal" className="w-5 h-5" />
                <span className="font-semibold">Personal</span>
              </button>
              <div className="mx-1 my-0.5 border-t border-border-light" />
              {workspaces
                .filter((ws) => !ws.isDefault)
                .map((ws) => {
                  const messagesApp = ws.apps?.find(
                    (app) => app.type === "messages",
                  );
                  const wsCache = messagesApp
                    ? workspaceCache[messagesApp.id]
                    : null;
                  const wsChannelIds = wsCache
                    ? [...(wsCache.channels || []), ...(wsCache.dms || [])].map(
                        (c) => c.id,
                      )
                    : [];
                  const hasUnreads = wsChannelIds.some(
                    (id) => unreadCounts[id] > 0,
                  );

                  return (
                    <button
                      key={ws.id}
                      onClick={() => {
                        setSelectedView(ws.id);
                        setActiveWorkspace(ws.id);
                        setOpenMenu(null);
                        // Determine which app to navigate to:
                        // 1. Check session app for this workspace (includes core apps)
                        // 2. If on a core app, navigate to workspace-scoped version
                        // 3. Otherwise, match current app type or fall back to first app
                        const messagesApp = ws.apps?.find(
                          (app) => app.type === "messages",
                        );
                        if (messagesApp) setWorkspaceAppId(messagesApp.id);

                        const coreApps = ["chat", "email", "calendar"];

                        // Detect the current app type from the URL
                        const coreAppMatch = location.pathname.match(/^\/(chat|email|calendar)(?:\/|$)/) ||
                          location.pathname.match(/^\/workspace\/[^/]+\/(chat|email|calendar)(?:\/|$)/);
                        const workspaceAppMatch = location.pathname.match(/^\/workspace\/[^/]+\/([^/]+)/);
                        const currentAppType = coreAppMatch?.[1] || workspaceAppMatch?.[1];

                        if (currentAppType) {
                          // Stay on the same app type in the new workspace
                          if (coreApps.includes(currentAppType)) {
                            navigate(`/workspace/${ws.id}/${currentAppType}`);
                          } else {
                            const targetApp = ws.apps?.find((app) => app.type === currentAppType);
                            if (targetApp) {
                              navigate(getAppPath(ws.id, targetApp.type));
                            } else if (ws.apps && ws.apps.length > 0) {
                              navigate(getAppPath(ws.id, ws.apps[0].type));
                            }
                          }
                        } else {
                          // No current app — fall back to session app or first app
                          const sessionApp = getSessionApp(ws.id);
                          if (sessionApp) {
                            if (coreApps.includes(sessionApp)) {
                              navigate(`/workspace/${ws.id}/${sessionApp}`);
                            } else {
                              navigate(getAppPath(ws.id, sessionApp));
                            }
                          } else if (ws.apps && ws.apps.length > 0) {
                            navigate(getAppPath(ws.id, ws.apps[0].type));
                          }
                        }
                        // Focus sidebar for immediate keyboard nav
                        setTimeout(
                          () => sidebarContainerRef.current?.focus(),
                          0,
                        );
                      }}
                      className={`w-full px-2.5 py-2 text-left text-sm flex items-center gap-2.5 rounded-lg ${
                        selectedView === ws.id
                          ? "bg-bg-gray"
                          : "hover:bg-bg-gray"
                      }`}
                    >
                      {ws.icon_url ? (
                        <img
                          src={ws.icon_url}
                          alt={ws.name}
                          className="w-5 h-5 rounded object-cover"
                        />
                      ) : (
                        <span className="w-5 h-5 flex items-center justify-center text-xs font-medium bg-blue-600 text-white rounded">
                          {ws.name.charAt(0).toUpperCase()}
                        </span>
                      )}
                      <span className="flex-1 font-semibold">{ws.name}</span>
                      {ws.isShared && (
                        <span className="text-xs uppercase tracking-wide text-text-tertiary border border-border-gray rounded px-1.5 py-0.5">
                          Shared
                        </span>
                      )}
                      {hasUnreads && (
                        <span className="w-2 h-2 rounded-full bg-red-500" />
                      )}
                    </button>
                  );
                })}
            </div>

            <div className="mx-1.5 border-t border-border-light" />

            <div className="p-1.5">
              <button
                onClick={() => {
                  setFormName("");
                  setFormEmoji("");
                  setModalType("createWorkspace");
                  setOpenMenu(null);
                }}
                className="w-full px-2.5 py-2 text-left text-sm flex items-center gap-2.5 text-text-secondary hover:bg-bg-gray rounded-lg"
              >
                <Icon icon={Plus} size={18} />
                <span>New workspace</span>
              </button>
            </div>
          </Dropdown>
        </div>


        {/* Navigation Icons - Workspace apps */}
        {activeProductType === 'workspace' && (
        <div className="flex-1 flex flex-col items-center gap-2 overflow-y-auto px-1 pt-1 -mx-1 -mt-1">
          {/* Workspace-specific mini apps - at top */}
          {displayMiniApps.filter((app) => app.type !== 'email' && app.type !== 'calendar').map((app) => {
            // Check both the direct path and any workspace-prefixed variant
            const isItemActive = isActivePath(app.path) || !!(
              targetWorkspace && location.pathname.match(new RegExp(`/workspace/[^/]+/${app.type}(/|$)`))
            );
            const hasUnread =
              app.type === "messages" &&
              (() => {
                const cache = workspaceCache[app.id];
                if (!cache) return false;
                const channelIds = [
                  ...(cache.channels || []),
                  ...(cache.dms || []),
                ].map((c) => c.id);
                return channelIds.some((id) => unreadCounts[id] > 0);
              })();

            return (
              <div key={app.id} className="relative">
                <button
                  onClick={() => {
                    if (app.type === 'chat') setActiveConversationId(null);
                    navigate(app.path);
                  }}
                  onMouseEnter={() => prefetchView(app.type || app.id, app.id, targetWorkspace?.id)}
                  title={app.name}
                  className={`${iconBtn} ${isItemActive ? iconBtnActive : iconBtnInactive}`}
                >
                  <Icon icon={app.icon} size={20} active={isItemActive} />
                </button>
                {hasUnread && (
                  <span className="absolute -top-0.5 -right-0.5 w-3 h-3 bg-red-500 rounded-full border-2 border-[#E3E3E5]" />
                )}
              </div>
            );
          })}


        </div>
        )}

        {/* AI Builder sidebar content */}
        {activeProductType === 'ai_builder' && (
          <div className="flex-1 flex flex-col items-center gap-2 overflow-y-auto px-1 pt-1">
            <button
              onClick={() => navigate('/builder')}
              title="AI Apps"
              className={`${iconBtn} ${location.pathname.startsWith('/builder') ? iconBtnActive : iconBtnInactive}`}
            >
              <Icon icon={Code} size={20} />
            </button>
          </div>
        )}

        {/* Website Builder sidebar content */}
        {activeProductType === 'website_builder' && (
          <div className="flex-1 flex flex-col items-center gap-2 overflow-y-auto px-1 pt-1">
            <button
              onClick={() => navigate('/sites')}
              title="My Sites"
              className={`${iconBtn} ${location.pathname.startsWith('/sites') ? iconBtnActive : iconBtnInactive}`}
            >
              <Icon icon={ExternalLink} size={20} />
            </button>
          </div>
        )}

        {/* Bottom section */}
        <div className="flex flex-col items-center gap-2 mt-2">
          {/* Pinned: Email & Calendar — always visible */}
          {(() => {
            const isEmailActive = isActivePath('/email') || !!location.pathname.match(/\/workspace\/[^/]+\/email(\/|$)/);
            const isCalendarActive = isActivePath('/calendar') || !!location.pathname.match(/\/workspace\/[^/]+\/calendar(\/|$)/);
            return (
              <>
                <button
                  onClick={() => navigate(targetWorkspace && !targetWorkspace.isDefault ? `/workspace/${targetWorkspace.id}/email` : '/email')}
                  onMouseEnter={() => prefetchView('email', 'email', targetWorkspace?.id)}
                  title="Email"
                  className={`${iconBtn} ${isEmailActive ? iconBtnActive : iconBtnInactive}`}
                >
                  <Icon icon={Mail} size={20} active={isEmailActive} />
                </button>
                <button
                  onClick={() => navigate(targetWorkspace && !targetWorkspace.isDefault ? `/workspace/${targetWorkspace.id}/calendar` : '/calendar')}
                  onMouseEnter={() => prefetchView('calendar', 'calendar', targetWorkspace?.id)}
                  title="Calendar"
                  className={`${iconBtn} ${isCalendarActive ? iconBtnActive : iconBtnInactive}`}
                >
                  <Icon icon={Calendar} size={20} active={isCalendarActive} />
                </button>
              </>
            );
          })()}
          {/* Settings - workspace only */}
          {activeProductType === 'workspace' && targetWorkspace && (
            <button
              onClick={() => openWorkspaceSettingsModal(targetWorkspace.id, "members")}
              title="Workspace Settings"
              className={`${iconBtn} ${iconBtnInactive}`}
            >
              <Icon icon={Settings} size={20} />
            </button>
          )}

          {/* User Profile */}
          <div className="relative">
            <button
              ref={(el) => {
                if (el) menuButtonRefs.current.set("user-menu", el);
              }}
              onClick={() =>
                setOpenMenu(openMenu === "user-menu" ? null : "user-menu")
              }
              title={isAuthenticated && user?.email ? user.email : "Guest"}
              className={`${iconBtn} ${openMenu === "user-menu" ? iconBtnActive : iconBtnInactive}`}
            >
              <SelfPresenceAvatar
                avatarUrl={userProfile?.avatar_url}
                name={userProfile?.name || user?.email || "User"}
                fallbackLetter={isAuthenticated && user?.email ? user.email.charAt(0).toUpperCase() : "G"}
                userId={user?.id}
              />
            </button>
            <Dropdown
              isOpen={openMenu === "user-menu"}
              onClose={() => setOpenMenu(null)}
              trigger={{
                current: menuButtonRefs.current.get("user-menu") || null,
              }}
              position="top"
            >
              <div className="p-1.5 flex flex-col gap-0.5">
                <button
                  onClick={() => {
                    setOpenMenu(null);
                    setShowSettings(true);
                  }}
                  className="w-full px-3 py-2 text-left text-sm flex items-center gap-2.5 hover:bg-bg-gray rounded-lg"
                >
                  <Icon icon={Settings} size={20} />
                  User Settings
                </button>
                {isAuthenticated ? (
                  <button
                    onClick={() => {
                      setOpenMenu(null);
                      signOut();
                    }}
                    className="w-full px-3 py-2 text-left text-sm flex items-center gap-2.5 hover:bg-bg-gray rounded-lg"
                  >
                    <Icon icon={LogOut} size={20} />
                    Log out
                  </button>
                ) : (
                  <button
                    onClick={() => {
                      setOpenMenu(null);
                      setShowSettings(true);
                    }}
                    className="w-full px-3 py-2 text-left text-sm flex items-center gap-2.5 hover:bg-bg-gray rounded-lg"
                  >
                    <Icon icon={LogIn} size={20} />
                    Sign in
                  </button>
                )}
              </div>
            </Dropdown>
          </div>
        </div>
      </div>

      {/* Create Workspace Modal */}
      <Modal
        isOpen={modalType === "createWorkspace"}
        onClose={closeModal}
        title="Create Workspace"
      >
        <div className="flex flex-col gap-4">
          <div>
            <label className="block text-xs text-text-secondary mb-1.5">
              Name
            </label>
            <input
              type="text"
              value={formName}
              onChange={(e) => setFormName(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && !isCreatingWorkspace && handleCreateWorkspace()}
              placeholder="Workspace name"
              autoFocus
              className="w-full bg-bg-gray border border-border-light rounded-lg px-3 py-2 text-sm outline-none focus:border-border-gray"
            />
          </div>
          <div className="flex justify-end gap-2 pt-2">
            <button
              onClick={closeModal}
              className="px-4 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg"
            >
              Cancel
            </button>
            <button
              onClick={handleCreateWorkspace}
              disabled={!formName.trim() || isCreatingWorkspace}
              className="px-4 py-2 text-sm bg-brand-primary text-white rounded-lg disabled:opacity-50"
            >
              {isCreatingWorkspace ? "Creating..." : "Create"}
            </button>
          </div>
        </div>
      </Modal>

      {/* Unified Workspace Settings Modal */}
      <Modal
        isOpen={modalType === "workspaceSettings"}
        onClose={closeModal}
        title={(() => {
          const ws = workspaces.find((w) => w.id === selectedWorkspaceId);
          return ws ? `${ws.name} Workspace` : "Workspace Settings";
        })()}
        size="md"
      >
        <div className="flex flex-col" style={{ minHeight: 420 }}>
          {/* Tabs */}
          <div className="flex gap-4 mb-4">
            <button
              onClick={() => setWorkspaceSettingsTab("members")}
              className={`pb-2 text-sm font-medium transition-colors relative ${
                workspaceSettingsTab === "members"
                  ? "text-gray-900"
                  : "text-gray-500 hover:text-gray-700"
              }`}
            >
              Members
              {workspaceSettingsTab === "members" && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gray-900 rounded-full" />
              )}
            </button>
            <button
              onClick={() => setWorkspaceSettingsTab("apps")}
              className={`pb-2 text-sm font-medium transition-colors relative ${
                workspaceSettingsTab === "apps"
                  ? "text-gray-900"
                  : "text-gray-500 hover:text-gray-700"
              }`}
            >
              Apps
              {workspaceSettingsTab === "apps" && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gray-900 rounded-full" />
              )}
            </button>
            <button
              onClick={() => setWorkspaceSettingsTab("settings")}
              className={`pb-2 text-sm font-medium transition-colors relative ${
                workspaceSettingsTab === "settings"
                  ? "text-gray-900"
                  : "text-gray-500 hover:text-gray-700"
              }`}
            >
              Workspace Settings
              {workspaceSettingsTab === "settings" && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-gray-900 rounded-full" />
              )}
            </button>
          </div>

          {/* Members Tab */}
          {workspaceSettingsTab === "members" && (
            <div className="flex flex-col flex-1">
              {/* Scrollable members content */}
              <div className="flex-1 overflow-y-auto space-y-4" style={{ maxHeight: 320 }}>
              {canManageWorkspaceMembers ? (
                <>
                  <label className="block text-xs text-text-secondary">
                    Invite members
                  </label>
                  <form onSubmit={handleInviteMember} className="flex gap-2">
                    <input
                      type="email"
                      value={inviteEmail}
                      onChange={(e) => setInviteEmail(e.target.value)}
                      placeholder="Enter email to invite"
                      className="flex-1 bg-white border border-border-gray rounded-lg px-3 py-2.5 text-xs outline-none focus:border-text-tertiary"
                    />
                    <select
                      value={inviteRole}
                      onChange={(e) => setInviteRole(e.target.value as "member" | "admin")}
                      className="bg-white border border-border-gray rounded-lg px-2 py-2.5 text-xs outline-none focus:border-text-tertiary"
                      aria-label="Invite role"
                    >
                      <option value="member">Member</option>
                      <option value="admin">Admin</option>
                    </select>
                    <button
                      type="submit"
                      disabled={inviting || !inviteEmail.trim()}
                      className="px-4 py-2 text-sm bg-black text-white rounded-lg disabled:opacity-50"
                    >
                      {inviting ? "Sending..." : "Invite"}
                    </button>
                  </form>
                </>
              ) : (
                <p className="text-xs text-text-secondary">
                  Only workspace admins and owners can manage invites.
                </p>
              )}
              {inviteError && (
                <p className="text-xs text-red-500">{inviteError}</p>
              )}
              {inviteSuccess && (
                <p className="text-xs text-green-500">{inviteSuccess}</p>
              )}

              <div>
                <p className="text-[11px] uppercase tracking-wide text-text-tertiary mb-1.5">
                  Pending invites
                </p>
                {pendingInvitesLoading ? (
                  <p className="text-xs text-text-secondary">Loading...</p>
                ) : pendingInvites.length === 0 ? (
                  <p className="text-xs text-text-secondary">No pending invites</p>
                ) : (
                  <div className="flex flex-col gap-1.5">
                    {pendingInvites.map((invitation) => (
                      <div
                        key={invitation.id}
                        className="flex items-center justify-between px-3 py-2"
                      >
                        <div className="min-w-0">
                          <p className="text-sm truncate">{invitation.email}</p>
                          <p className="text-[10px] text-text-secondary capitalize">
                            {invitation.role} • Pending
                          </p>
                        </div>
                        <div className="flex items-center gap-1.5">
                          {canManageWorkspaceMembers && (
                            <>
                              <button
                                onClick={() => handleCopyInviteLink(invitation.id)}
                                disabled={inviteActionBusyId === invitation.id}
                                className="text-xs px-2 py-1 rounded bg-bg-gray hover:bg-bg-gray/80 disabled:opacity-60"
                              >
                                Copy link
                              </button>
                              <button
                                onClick={() => handleResendInvite(invitation)}
                                disabled={inviteActionBusyId === invitation.id}
                                className="text-xs px-2 py-1 rounded bg-bg-gray hover:bg-bg-gray/80 disabled:opacity-60"
                              >
                                Resend
                              </button>
                              <button
                                onClick={() => handleRevokeInvite(invitation.id)}
                                disabled={inviteActionBusyId === invitation.id}
                                className="text-xs px-2 py-1 rounded text-red-500 hover:bg-red-50 disabled:opacity-60"
                              >
                                Revoke
                              </button>
                            </>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>

              <div>
                <p className="text-[11px] uppercase tracking-wide text-text-tertiary mb-1.5">
                  Current members
                </p>
                <div className="flex flex-col gap-1.5">
                  {membersLoading ? (
                    <p className="text-xs text-text-secondary">Loading...</p>
                  ) : members.length === 0 ? (
                    <p className="text-xs text-text-secondary">No members</p>
                  ) : (
                    members.map((member) => {
                      const isSelf = member.user_id === user?.id;
                      const isOwner = member.role === "owner";
                      const canRemove = canManageWorkspaceMembers && !isOwner && !isSelf;
                      const canChangeRole =
                        !isSelf &&
                        !isOwner &&
                        (currentMemberRole === "owner" ||
                          (currentMemberRole === "admin" && member.role === "member"));

                      return (
                      <div
                        key={member.id}
                        className="group flex items-center justify-between px-0 py-2"
                      >
                        <div className="flex items-center gap-3 min-w-0">
                          {member.avatar_url ? (
                            <img
                              src={member.avatar_url}
                              alt={member.name || member.email || "User"}
                              className="w-7 h-7 rounded-full object-cover shrink-0"
                              onError={(e) => {
                                e.currentTarget.style.display = 'none';
                                e.currentTarget.nextElementSibling?.classList.remove('hidden');
                              }}
                            />
                          ) : null}
                          <div className={`w-7 h-7 rounded-full bg-gray-200 flex items-center justify-center shrink-0 ${member.avatar_url ? 'hidden' : ''}`}>
                            <span className="text-xs text-gray-500 font-medium">
                              {(member.name || member.email || "?").charAt(0).toUpperCase()}
                            </span>
                          </div>
                          <div className="relative group/member min-w-0">
                            <p className="text-sm truncate">
                              {member.name || member.email || member.user_id}
                              {isSelf && <span className="text-text-tertiary ml-1">(you)</span>}
                            </p>
                            {canChangeRole ? (
                              <select
                                value={member.role}
                                onChange={(e) => void handleMemberRoleChange(member, e.target.value as 'member' | 'admin')}
                                className="text-[10px] text-text-secondary capitalize bg-transparent border-none p-0 cursor-pointer hover:text-text-body focus:outline-none"
                              >
                                <option value="member">Member</option>
                                <option value="admin">Admin</option>
                              </select>
                            ) : (
                              <p className="text-[10px] text-text-secondary capitalize">
                                {member.role}
                              </p>
                            )}
                            {member.name && member.email && (
                              <div className="absolute left-0 top-full mt-1 px-2 py-1 bg-gray-800 text-white text-xs rounded opacity-0 group-hover/member:opacity-100 transition-opacity pointer-events-none whitespace-nowrap z-50">
                                {member.email}
                              </div>
                            )}
                          </div>
                        </div>
                        {canRemove && (
                          <button
                            onClick={() =>
                              handleRemoveMember(member.id, member.user_id)
                            }
                            className="text-xs text-red-500 hover:bg-red-50 px-2 py-1 rounded opacity-0 group-hover:opacity-100 transition-opacity shrink-0"
                          >
                            Remove
                          </button>
                        )}
                      </div>
                      );
                    })
                  )}
                </div>
              </div>
              </div>
              {/* Footer spacer to match Apps tab */}
              <div className="pt-4 mt-auto border-t border-border-light" />
            </div>
          )}

          {/* Apps Tab */}
          {workspaceSettingsTab === "apps" && (
            <div className="flex flex-col flex-1">
              {/* Scrollable apps content */}
              <div className="flex-1 overflow-y-auto space-y-4" style={{ maxHeight: 320 }}>
                {editingApps.length > 0 && (
                  <div>
                    <label className="block text-xs text-text-secondary mb-2">
                      Current Apps
                    </label>
                    <div className="flex flex-col gap-0.5">
                      {editingApps.map((app, index) => {
                        const appTypeInfo = availableAppTypes.find(
                          (a) => a.type === app.type,
                        );
                        return (
                          <div
                            key={app.id}
                            className="flex items-center gap-2 px-3 py-2 rounded-md hover:bg-bg-gray transition-colors"
                          >
                            <span className="w-7 h-7 flex items-center justify-center text-text-secondary">
                              {appTypeInfo?.icon && <Icon icon={appTypeInfo.icon} size={18} />}
                            </span>
                            <span className="flex-1 text-sm">{app.name}</span>
                            <button
                              onClick={() => moveAppUp(index)}
                              disabled={index === 0}
                              className="w-6 h-6 flex items-center justify-center text-text-tertiary disabled:opacity-30 hover:bg-bg-gray rounded"
                            >
                              <Icon
                                icon={ChevronDown}
                                size={14}
                                style={{ transform: "rotate(180deg)" }}
                              />
                            </button>
                            <button
                              onClick={() => moveAppDown(index)}
                              disabled={index === editingApps.length - 1}
                              className="w-6 h-6 flex items-center justify-center text-text-tertiary disabled:opacity-30 hover:bg-bg-gray rounded"
                            >
                              <Icon icon={ChevronDown} size={14} />
                            </button>
                            <button
                              onClick={() => removeAppFromEditing(app.id)}
                              className="w-6 h-6 flex items-center justify-center text-red-500 hover:bg-red-50 rounded"
                            >
                              <Icon icon={Trash2} size={14} />
                            </button>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {availableAppTypes.filter(
                  (app) => !editingApps.some((e) => e.type === app.type),
                ).length > 0 && (
                  <div>
                    <label className="block text-xs text-text-secondary mb-2">
                      Add Apps
                    </label>
                    <div className="flex flex-col gap-0.5">
                    {availableAppTypes
                      .filter(
                        (app) => !editingApps.some((e) => e.type === app.type),
                      )
                      .map((app) => (
                        <button
                          key={app.type}
                          onClick={() => addAppToEditing(app.type)}
                          className="w-full flex items-center gap-2 px-3 py-2 text-sm rounded-md hover:bg-bg-gray text-left transition-colors"
                        >
                          <span className="w-7 h-7 flex items-center justify-center text-text-secondary">
                            <Icon icon={app.icon} size={18} />
                          </span>
                          <span className="flex-1">{app.name}</span>
                          <Icon
                            icon={Plus}
                            size={16}
                            className="text-text-tertiary"
                          />
                        </button>
                      ))}
                    </div>
                  </div>
                )}
              </div>

              {saveError && (
                <p className="text-xs text-red-500 px-3 py-2 bg-red-50 rounded-lg mt-4">
                  {saveError}
                </p>
              )}

              <div className="flex justify-end gap-2 pt-4 mt-auto border-t border-border-light">
                <button
                  onClick={closeModal}
                  className="px-4 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg"
                >
                  Cancel
                </button>
                <button
                  onClick={async () => {
                    if (!selectedWorkspaceId || isSaving) return;
                    setIsSaving(true);
                    setSaveError(null);

                    try {
                      const workspace = workspaces.find(
                        (w) => w.id === selectedWorkspaceId,
                      );
                      const currentAppIds = (workspace?.apps || []).map(
                        (a) => a.id,
                      );
                      const editingAppIds = editingApps
                        .filter((a) => !a.id.startsWith("temp-"))
                        .map((a) => a.id);

                      for (const appId of currentAppIds) {
                        if (!editingAppIds.includes(appId)) {
                          await removeMiniApp(selectedWorkspaceId, appId);
                        }
                      }

                      for (const app of editingApps) {
                        if (app.id.startsWith("temp-")) {
                          await addMiniApp(selectedWorkspaceId, app.type);
                        }
                      }

                      // Persist reorder: match editingApps order to real app IDs by app_type
                      const updatedWorkspace = useWorkspaceStore
                        .getState()
                        .workspaces.find((w) => w.id === selectedWorkspaceId);
                      if (updatedWorkspace) {
                        const typeToPosition = new Map(
                          editingApps.map((a, i) => [a.type, i]),
                        );
                        const appPositions = updatedWorkspace.apps
                          .filter((a) => typeToPosition.has(a.type))
                          .map((a) => ({
                            id: a.id,
                            position: typeToPosition.get(a.type)!,
                          }));
                        if (appPositions.length > 0) {
                          await reorderWorkspaceApps(
                            selectedWorkspaceId,
                            appPositions,
                          );
                          // Update local store to reflect new order
                          useWorkspaceStore.setState((state) => ({
                            workspaces: state.workspaces.map((w) => {
                              if (w.id !== selectedWorkspaceId) return w;
                              const sorted = [...w.apps].sort(
                                (a, b) =>
                                  (typeToPosition.get(a.type) ?? 0) -
                                  (typeToPosition.get(b.type) ?? 0),
                              );
                              return { ...w, apps: sorted };
                            }),
                          }));
                        }
                      }

                      closeModal();
                    } catch (err) {
                      console.error("Failed to save apps:", err);
                      setSaveError("Failed to save changes.");
                      setIsSaving(false);
                    }
                  }}
                  disabled={isSaving}
                  className="px-4 py-2 text-sm bg-brand-primary text-white rounded-lg disabled:opacity-50"
                >
                  {isSaving ? "Saving..." : "Save"}
                </button>
              </div>
            </div>
          )}

          {/* Workspace Settings Tab */}
          {workspaceSettingsTab === "settings" && (
            <div className="flex flex-col gap-5 flex-1">
              {/* Workspace Title */}
              <div>
                <label className="block text-xs text-text-secondary mb-2">
                  Workspace Title
                </label>
                <div className="flex items-center gap-3">
                <input
                  ref={iconInputRef}
                  type="file"
                  accept="image/*"
                  onChange={handleIconUpload}
                  className="hidden"
                />
                <button
                  onClick={() => iconInputRef.current?.click()}
                  disabled={isUploadingIcon}
                  className="group/icon relative w-[38px] h-[38px] flex items-center justify-center rounded-lg bg-bg-gray overflow-hidden shrink-0 cursor-pointer"
                >
                  {formIconUrl ? (
                    <img
                      src={formIconUrl}
                      alt="Icon"
                      className="w-full h-full object-cover"
                    />
                  ) : (
                    <span className="text-base font-medium text-text-secondary">
                      {formName.charAt(0).toUpperCase()}
                    </span>
                  )}
                  <div className="absolute inset-0 bg-black/50 flex items-center justify-center opacity-0 group-hover/icon:opacity-100 transition-opacity">
                    <Icon icon={Camera} size={16} className="text-white" />
                  </div>
                </button>
                <input
                  type="text"
                  value={formName}
                  onChange={(e) => setFormName(e.target.value)}
                  onKeyDown={(e) => e.key === "Enter" && handleEditWorkspace()}
                  placeholder="Workspace name"
                  className="flex-1 bg-white border border-border-gray rounded-lg px-3 py-2.5 text-xs outline-none focus:border-text-tertiary"
                />
                </div>
              </div>

              {/* Workspace Info */}
              {editingWorkspace?.created_at && (
                <div className="text-xs text-text-tertiary">
                  Created {new Date(editingWorkspace.created_at).toLocaleDateString(undefined, {
                    year: 'numeric',
                    month: 'long',
                    day: 'numeric'
                  })}
                </div>
              )}

              {/* Collapsible Danger Zone */}
              {selectedWorkspaceId && (
                <div className="border-t border-border-light pt-4">
                  <button
                    onClick={() => setShowDangerZone(!showDangerZone)}
                    className="flex items-center gap-2 text-xs text-text-tertiary hover:text-text-secondary transition-colors"
                  >
                    <Icon
                      icon={ChevronDown}
                      size={12}
                      style={{
                        transform: showDangerZone ? "rotate(0deg)" : "rotate(-90deg)",
                        transition: "transform 0.15s ease",
                      }}
                    />
                    Advanced options
                  </button>
                  {showDangerZone && (
                    <div className="mt-3">
                      <p className="text-xs text-text-tertiary mb-3">
                        Deleting this workspace will permanently remove all data associated with it. This action cannot be undone.
                      </p>
                      <button
                        onClick={async () => {
                          if (
                            confirm("Delete this workspace? This cannot be undone.")
                          ) {
                            try {
                              await removeWorkspace(selectedWorkspaceId);
                              closeModal();
                              setSelectedView("dashboard");
                              navigate("/chat");
                            } catch (err) {
                              console.error("Failed to delete workspace:", err);
                            }
                          }
                        }}
                        className="px-3 py-1.5 text-xs text-red-600 border border-red-200 rounded-md hover:bg-red-50 transition-colors"
                      >
                        Delete workspace
                      </button>
                    </div>
                  )}
                </div>
              )}

              {/* Actions */}
              <div className="flex justify-end gap-2 pt-4 mt-auto border-t border-border-light">
                <button
                  onClick={closeModal}
                  className="px-4 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg"
                >
                  Cancel
                </button>
                <button
                  onClick={handleEditWorkspace}
                  disabled={!formName.trim()}
                  className="px-4 py-2 text-sm bg-brand-primary text-white rounded-lg disabled:opacity-50"
                >
                  Save
                </button>
              </div>
            </div>
          )}
        </div>
      </Modal>

      {/* Dashboard Settings Modal - reorder & remove dashboards */}
      <Modal
        isOpen={modalType === "dashboardSettings"}
        onClose={closeModal}
        title="Manage Dashboards"
      >
        <div className="flex flex-col gap-4">
          <p className="text-xs text-text-secondary">
            Drag to reorder or remove dashboards from your sidebar.
          </p>

          <div className="flex flex-col gap-0.5">
            {/* Workspaces */}
            {workspaces
              .filter((ws) => !ws.isDefault)
              .map((ws) => (
                <div
                  key={ws.id}
                  className="flex items-center gap-2 px-3 py-2 rounded-md hover:bg-bg-gray transition-colors"
                >
                  {ws.icon_url ? (
                    <img
                      src={ws.icon_url}
                      alt={ws.name}
                      className="w-5 h-5 rounded object-cover shrink-0"
                    />
                  ) : (
                    <span className="w-5 h-5 flex items-center justify-center text-xs font-medium bg-gradient-to-br from-blue-500 to-purple-600 text-white rounded shrink-0">
                      {ws.name.charAt(0).toUpperCase()}
                    </span>
                  )}
                  <span className="flex-1 text-sm">{ws.name}</span>
                  <span className="text-[10px] text-text-tertiary uppercase tracking-wide">Workspace</span>
                  <button
                    onClick={async () => {
                      if (confirm(`Delete "${ws.name}"? This cannot be undone.`)) {
                        try {
                          await removeWorkspace(ws.id);
                          if (selectedView === ws.id) {
                            setSelectedView("dashboard");
                            navigate("/chat");
                          }
                        } catch (err) {
                          console.error("Failed to delete workspace:", err);
                        }
                      }
                    }}
                    className="w-6 h-6 flex items-center justify-center text-red-500 hover:bg-red-50 rounded shrink-0"
                  >
                    <Icon icon={Trash2} size={14} />
                  </button>
                </div>
              ))}

            {/* AI App Builder and Website Builder are core product types — always present, not removable */}
          </div>

          <div className="flex justify-end pt-2">
            <button
              onClick={closeModal}
              className="px-4 py-2 text-sm text-text-secondary hover:bg-bg-gray rounded-lg"
            >
              Done
            </button>
          </div>
        </div>
      </Modal>

      <SettingsView isOpen={showSettings} onClose={() => setShowSettings(false)} />
    </>
  );
}
