import { lazy, Suspense, useEffect, useRef } from "react";
import {
  BrowserRouter,
  Routes,
  Route,
  useLocation,
  useNavigate,
  Navigate,
} from "react-router-dom";
import { PersistQueryClientProvider } from "@tanstack/react-query-persist-client";
import { ReactQueryDevtools } from "@tanstack/react-query-devtools";
import { Toaster, toast } from "sonner";
import { queryClient, getQueryPersister } from "./lib/queryClient";
import { resolvePostSignupInvitations } from "./api/client";
import { useAuthStore } from "./stores/authStore";
import { useNotificationStore } from "./stores/notificationStore";
import { useWorkspaceStore } from "./stores/workspaceStore";
import { useProductStore } from "./stores/productStore";
import { KeyboardNavigationProvider } from "./hooks/useKeyboardNavigation";
import { useGlobalRealtime } from "./hooks/useGlobalRealtime";
import { useAppPreloader } from "./hooks/useAppPreloader";
import { useWorkspacePresence } from "./hooks/useWorkspacePresence";
import { useResumeRevalidation } from "./hooks/useResumeRevalidation";
import { Sentry, setSentryUser } from "./lib/sentry";
import { identifyUser, resetUser, trackPageView } from "./lib/posthog";
import { FeatureErrorBoundary } from "./components/ui/FeatureErrorBoundary";

const OAuthCallback = lazy(() => import("./components/OAuthCallback"));
const InviteAcceptPage = lazy(() => import("./pages/InviteAcceptPage"));
const ShareLinkResolver = lazy(() => import("./components/ShareLinkResolver"));
const Sidebar = lazy(() => import("./components/Sidebar/Sidebar"));
const ChatPanel = lazy(() => import("./components/ChatPanel/ChatPanel"));
const LandingPage = lazy(() => import("./components/Landing/LandingPage"));
const ChatView = lazy(() => import("./components/Chat/ChatView"));
const EmailView = lazy(() => import("./components/Email/EmailView"));
const CalendarView = lazy(() => import("./components/Calendar/CalendarView"));
const TeamView = lazy(() => import("./components/Team/TeamView"));
const MembersView = lazy(() => import("./components/Members/MembersView"));
const MessagesView = lazy(() => import("./components/Messages/MessagesView"));
const FilesView = lazy(() => import("./components/Files/FilesView"));
const ProjectsView = lazy(() => import("./components/Projects/ProjectsView"));
const AgentsView = lazy(() => import("./components/Agents/AgentsView"));
const DashboardView = lazy(
  () => import("./components/Dashboard/DashboardView"),
);
const AIBuilderView = lazy(() => import("./components/ai-app-builder/AIBuilderView"));
const WebsiteBuilderView = lazy(() => import("./components/WebsiteBuilder/WebsiteBuilderView"));
const OnboardingWizard = lazy(() => import("./components/Onboarding/OnboardingWizard"));
const PENDING_INVITE_TOKEN_KEY = "pending_invite_token";
const PENDING_INVITE_TOKEN_SET_AT_KEY = "pending_invite_token_set_at";
const PENDING_INVITE_TOKEN_MAX_AGE_MS = 60 * 60 * 1000;

const consumePendingInviteToken = (): string | null => {
  try {
    const token = sessionStorage.getItem(PENDING_INVITE_TOKEN_KEY)?.trim();
    const rawSetAt = sessionStorage.getItem(PENDING_INVITE_TOKEN_SET_AT_KEY);
    const setAt = rawSetAt ? Number(rawSetAt) : NaN;

    sessionStorage.removeItem(PENDING_INVITE_TOKEN_KEY);
    sessionStorage.removeItem(PENDING_INVITE_TOKEN_SET_AT_KEY);

    if (!token) return null;
    if (!Number.isFinite(setAt)) return null;
    if (Date.now() - setAt > PENDING_INVITE_TOKEN_MAX_AGE_MS) return null;

    return token;
  } catch {
    return null;
  }
};

function AppErrorFallback({
  error,
  resetError,
}: {
  error: unknown;
  resetError: () => void;
}) {
  return (
    <div className="h-screen w-screen flex items-center justify-center bg-bg-mini-app">
      <div className="flex flex-col items-center gap-4 max-w-md text-center px-6">
        <h1 className="text-xl font-semibold text-text-dark">
          Something went wrong
        </h1>
        {/* TODO: Replace with generic message once error tracking is mature */}
        <p className="text-sm text-text-secondary">
          {error instanceof Error ? error.message : "An unexpected error occurred."}
        </p>
        <div className="flex gap-3 mt-2">
          <button
            onClick={resetError}
            className="px-4 py-2 text-sm font-medium rounded-lg bg-brand-primary text-white hover:opacity-90 transition-opacity"
          >
            Try again
          </button>
          <button
            onClick={() => (window.location.href = "/")}
            className="px-4 py-2 text-sm font-medium rounded-lg border border-border-default text-text-dark hover:bg-bg-hover transition-colors"
          >
            Go home
          </button>
        </div>
      </div>
    </div>
  );
}

function RouteLoading() {
  return (
    <div className="flex-1 flex h-full overflow-hidden">
      {/* Skeleton for mini app content area */}
      <div className="flex-1 flex overflow-hidden bg-white">
        {/* Skeleton sidebar */}
        <div className="w-[212px] shrink-0 bg-gray-50 border-r border-gray-100 p-3">
          {/* Sidebar header */}
          <div className="h-5 w-24 bg-gray-200 rounded animate-pulse mb-4" />
          {/* Sidebar items */}
          <div className="space-y-1">
            {[...Array(8)].map((_, i) => (
              <div key={i} className="flex items-center gap-2 h-[35px] px-2">
                <div className="w-4 h-4 bg-gray-200 rounded animate-pulse" />
                <div className="flex-1 h-3 bg-gray-200 rounded animate-pulse" />
              </div>
            ))}
          </div>
        </div>
        {/* Skeleton main content */}
        <div className="flex-1 p-6">
          {/* Header */}
          <div className="h-6 w-48 bg-gray-100 rounded animate-pulse mb-6" />
          {/* Content blocks */}
          <div className="space-y-6">
            {[...Array(4)].map((_, i) => (
              <div key={i} className="flex gap-3">
                <div className="w-10 h-10 bg-gray-100 rounded-lg animate-pulse shrink-0" />
                <div className="flex-1 space-y-2">
                  <div className="flex items-center gap-2">
                    <div className="h-3.5 w-24 bg-gray-100 rounded animate-pulse" />
                    <div className="h-2.5 w-12 bg-gray-50 rounded animate-pulse" />
                  </div>
                  <div className="h-3 w-full bg-gray-100 rounded animate-pulse" />
                  <div className="h-3 w-3/4 bg-gray-100 rounded animate-pulse" />
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

function SidebarSkeleton() {
  return (
    <div className="w-16 shrink-0 bg-[#E3E3E5] flex flex-col items-center py-3 gap-3">
      {/* Logo placeholder */}
      <div className="w-10 h-10 rounded-xl bg-black/10 animate-pulse" />
      {/* Icon placeholders */}
      <div className="flex-1 flex flex-col items-center gap-2 mt-4">
        {[...Array(6)].map((_, i) => (
          <div key={i} className="w-10 h-10 rounded-lg bg-black/5 animate-pulse" />
        ))}
      </div>
      {/* Bottom icon placeholder */}
      <div className="w-10 h-10 rounded-lg bg-black/5 animate-pulse" />
    </div>
  );
}

// Layout with sidebar for app routes
function AppLayout({ children }: { children: React.ReactNode }) {
  const { isLoading: authLoading } = useAuthStore();

  return (
    <KeyboardNavigationProvider>
      <Toaster position="top-right" richColors />
      <div className="h-screen w-screen overflow-hidden flex bg-[#E3E3E5]">
        {/* Main Sidebar */}
        <Suspense fallback={<SidebarSkeleton />}>
          <Sidebar />
        </Suspense>
        {/* Content area - inset with spacing to show sidebar bg */}
        <div className="flex-1 flex min-w-0 min-h-0 pt-2 pr-2 pb-2 gap-2">
          {/* Main content */}
          <main className="flex-1 flex min-w-0 overflow-hidden rounded-lg bg-[#FCFCFC] border border-border-light shadow-[0_0_1px_rgba(0,0,0,0.25)]">
            {authLoading ? (
              <RouteLoading />
            ) : (
              <FeatureErrorBoundary feature="this view">
                <Suspense fallback={<RouteLoading />}>{children}</Suspense>
              </FeatureErrorBoundary>
            )}
          </main>
          {/* AI Chat Panel - right side */}
          <FeatureErrorBoundary feature="Chat">
            <Suspense fallback={null}>
              <ChatPanel />
            </Suspense>
          </FeatureErrorBoundary>
        </div>
      </div>
    </KeyboardNavigationProvider>
  );
}

// Inner component that uses router hooks
function AppContent() {
  const location = useLocation();
  const navigate = useNavigate();
  const {
    initialize: initAuth,
    isAuthenticated,
    isLoading: authLoading,
    user,
  } = useAuthStore();
  const { fetchInitData, activeWorkspaceId, workspaces, getSessionApp } = useWorkspaceStore();
  const { activeProductType } = useProductStore();
  const isInviteRoute = location.pathname.startsWith("/invite/");
  const isShareLinkRoute = location.pathname.startsWith("/s/");
  const initBootstrappedUserRef = useRef<string | null>(null);
  const postSignupResolvedUserRef = useRef<string | null>(null);
  const inviteRedirectInProgressRef = useRef(false);
  const previousUserIdRef = useRef<string | null>(null);
  const currentUserId = user?.id ?? null;

  // Initialize auth on mount
  useEffect(() => {
    initAuth();
  }, [initAuth]);

  useEffect(() => {
    if (!isAuthenticated || authLoading) return;
    if (isInviteRoute || isShareLinkRoute) return;

    const pendingToken = consumePendingInviteToken();
    if (!pendingToken) return;

    inviteRedirectInProgressRef.current = true;
    void navigate(`/invite/${encodeURIComponent(pendingToken)}`, { replace: true });
  }, [isAuthenticated, authLoading, isInviteRoute, isShareLinkRoute, navigate]);

  useEffect(() => {
    if (isInviteRoute) {
      inviteRedirectInProgressRef.current = false;
    }
  }, [isInviteRoute]);

  useEffect(() => {
    const previousUserId = previousUserIdRef.current;
    if (previousUserId !== null && previousUserId !== currentUserId) {
      useNotificationStore.getState().reset();
    }
    previousUserIdRef.current = currentUserId;
  }, [currentUserId]);

  useEffect(() => {
    if (currentUserId) return;
    initBootstrappedUserRef.current = null;
    postSignupResolvedUserRef.current = null;
  }, [currentUserId]);

  // Fetch init data once per authenticated user (outside invite route)
  useEffect(() => {
    if (!isAuthenticated || authLoading || !currentUserId) return;
    if (isInviteRoute || isShareLinkRoute || inviteRedirectInProgressRef.current) return;
    if (initBootstrappedUserRef.current === currentUserId) return;

    initBootstrappedUserRef.current = currentUserId;
    void fetchInitData();
  }, [isAuthenticated, authLoading, currentUserId, isInviteRoute, isShareLinkRoute, fetchInitData]);

  // Resolve post-signup invites once per authenticated user (outside invite route)
  useEffect(() => {
    if (!isAuthenticated || authLoading || !currentUserId) return;
    if (isInviteRoute || isShareLinkRoute || inviteRedirectInProgressRef.current) return;
    if (postSignupResolvedUserRef.current === currentUserId) return;

    postSignupResolvedUserRef.current = currentUserId;
    void resolvePostSignupInvitations()
      .then(() => {
        const notificationStore = useNotificationStore.getState();
        void notificationStore.fetchNotifications();
        void notificationStore.fetchUnreadCount();
      })
      .catch((err) => {
        console.error("Failed to resolve post-signup invitations:", err);
      });
  }, [isAuthenticated, authLoading, currentUserId, isInviteRoute, isShareLinkRoute]);

  // Global realtime: messages notifications, file changes, channel changes
  useGlobalRealtime(!(isInviteRoute || isShareLinkRoute));

  // Revalidate Zustand-backed data when the app resumes
  useResumeRevalidation(!(isInviteRoute || isShareLinkRoute));

  // Preload all app data for instant switching
  useAppPreloader(!(isInviteRoute || isShareLinkRoute));

  // Track user presence across the entire app (not just messages page)
  useWorkspacePresence(
    isInviteRoute || isShareLinkRoute ? undefined : (activeWorkspaceId ?? undefined),
    isInviteRoute || isShareLinkRoute ? [] : workspaces.map((workspace) => workspace.id),
  );

  // Sync user context to Sentry for error attribution
  const userProfile = useAuthStore((s) => s.userProfile);
  const onboardingCompletedAt = useAuthStore((s) => s.onboardingCompletedAt);
  useEffect(() => {
    if (isAuthenticated && userProfile) {
      setSentryUser({ id: userProfile.id, email: userProfile.email, name: userProfile.name });
    } else {
      setSentryUser(null);
    }
  }, [isAuthenticated, userProfile]);

  // Sync user context to PostHog for analytics attribution
  useEffect(() => {
    if (isAuthenticated && userProfile) {
      identifyUser({ id: userProfile.id, email: userProfile.email, name: userProfile.name });
    } else {
      resetUser();
    }
  }, [isAuthenticated, userProfile]);

  // Track page views on route change
  useEffect(() => {
    trackPageView(location.pathname);
  }, [location.pathname]);

  // Monitor network connectivity and show toast when offline
  useEffect(() => {
    const handleOffline = () => {
      toast.error("You're offline", {
        description: "Check your internet connection",
        duration: Infinity,
        id: "offline-toast",
      });
    };

    const handleOnline = () => {
      toast.dismiss("offline-toast");
      toast.success("Back online", { duration: 3000 });
    };

    window.addEventListener("offline", handleOffline);
    window.addEventListener("online", handleOnline);

    // Show toast if already offline on mount
    if (!navigator.onLine) {
      handleOffline();
    }

    return () => {
      window.removeEventListener("offline", handleOffline);
      window.removeEventListener("online", handleOnline);
    };
  }, []);

  // OAuth callback route — rendered in popup without auth guard
  if (location.pathname === "/oauth/callback") {
    return (
      <Suspense fallback={<RouteLoading />}>
        <OAuthCallback />
      </Suspense>
    );
  }

  // Invite deep-link route — accessible both before and after auth
  if (location.pathname.startsWith("/invite/")) {
    return (
      <Suspense fallback={<RouteLoading />}>
        <InviteAcceptPage />
      </Suspense>
    );
  }

  // Share link route — accessible both before and after auth
  if (location.pathname.startsWith("/s/")) {
    return (
      <Suspense fallback={<RouteLoading />}>
        <ShareLinkResolver />
      </Suspense>
    );
  }

  // Onboarding route — full-screen, no sidebar
  if (location.pathname === "/onboarding") {
    if (!isAuthenticated && !authLoading) {
      return <Navigate to="/" replace />;
    }
    return (
      <Suspense
        fallback={
          <div className="h-screen w-screen flex items-center justify-center bg-white" />
        }
      >
        <OnboardingWizard />
      </Suspense>
    );
  }

  // While authenticated but onboarding status not yet known, show a blank screen
  // to prevent flashing the app before redirecting new users to onboarding.
  // undefined = not yet loaded; null = loaded, not completed; string = completed.
  if (isAuthenticated && !authLoading && onboardingCompletedAt === undefined && !isInviteRoute && !isShareLinkRoute) {
    return <div className="h-screen w-screen bg-white" />;
  }

  // Check if user needs onboarding (status loaded and not completed)
  const needsOnboarding = isAuthenticated && onboardingCompletedAt === null;

  // Landing page logic - handle auth state before rendering
  if (location.pathname === "/") {
    // If authenticated (from persisted state or verified), redirect immediately
    // This makes the redirect instant without waiting for async verification
    if (isAuthenticated) {
      // Redirect to onboarding if not completed
      if (needsOnboarding) {
        return <Navigate to="/onboarding" replace />;
      }

      // Route based on active product type
      if (activeProductType === 'ai_builder') {
        return <Navigate to="/builder" replace />;
      }
      if (activeProductType === 'website_builder') {
        return <Navigate to="/sites" replace />;
      }

      // Resume the workspace + app the user was last on
      const topLevelApps = ['chat', 'email', 'calendar'];
      const wsId = activeWorkspaceId;
      const ws = wsId ? workspaces.find((w) => w.id === wsId) : null;
      const isDefault = ws?.isDefault ?? true;
      const lastApp = wsId ? getSessionApp(wsId) : null;
      const app = lastApp || 'chat';

      if (!isDefault && wsId) {
        return <Navigate to={`/workspace/${wsId}/${app}`} replace />;
      }

      // Default workspace — use top-level route if available
      if (topLevelApps.includes(app)) {
        return <Navigate to={`/${app}`} replace />;
      }
      return <Navigate to="/chat" replace />;
    }
    // If still loading and no persisted auth state, show loading
    if (authLoading) {
      return (
        <div className="h-screen w-screen flex items-center justify-center bg-white">
          <div className="w-8 h-8 border-2 border-gray-200 border-t-gray-600 rounded-full animate-spin" />
        </div>
      );
    }
    // Not authenticated, show landing page
    return (
      <Suspense fallback={
        <div className="h-screen w-screen flex items-center justify-center bg-white">
          <div className="w-8 h-8 border-2 border-gray-200 border-t-gray-600 rounded-full animate-spin" />
        </div>
      }>
        <LandingPage />
      </Suspense>
    );
  }

  // Protected routes - redirect to landing if definitely not authenticated
  // Only redirect when: authLoading is false AND isAuthenticated is false
  // This prevents kicking out users during connection blips (they still have persisted auth state)
  if (!authLoading && !isAuthenticated && !isShareLinkRoute) {
    return <Navigate to="/" replace />;
  }

  // Redirect to onboarding if not completed (guard all protected routes)
  if (needsOnboarding) {
    return <Navigate to="/onboarding" replace />;
  }

  // Custom-layout apps render without the sidebar
  if (location.pathname === "/builder" || location.pathname.startsWith("/builder/")) {
    return (
      <FeatureErrorBoundary feature="AI Builder">
        <Suspense fallback={<div className="h-screen w-screen bg-white" />}>
          <Toaster position="top-right" richColors />
          <Routes>
            <Route path="/builder" element={<AIBuilderView />} />
            <Route path="/builder/:projectId" element={<AIBuilderView />} />
            <Route path="/builder/:projectId/preview" element={<AIBuilderView />} />
          </Routes>
        </Suspense>
      </FeatureErrorBoundary>
    );
  }

  // All other routes use the app layout with sidebar
  return (
    <AppLayout>
      <Routes>
        {/* Core apps (not workspace-specific) */}
        <Route path="/chat" element={<ChatView />} />
        <Route path="/chat/:conversationId" element={<ChatView />} />
        <Route path="/email" element={<EmailView />} />
        <Route path="/calendar" element={<CalendarView />} />

        {/* Workspace-specific apps */}
        <Route
          path="/workspace/:workspaceId"
          element={
            <div className="p-8 text-text-secondary">
              Workspace View - Coming soon
            </div>
          }
        />
        <Route path="/workspace/:workspaceId/chat" element={<ChatView />} />
        <Route path="/workspace/:workspaceId/chat/:conversationId" element={<ChatView />} />
        <Route path="/workspace/:workspaceId/email" element={<EmailView />} />
        <Route path="/workspace/:workspaceId/calendar" element={<CalendarView />} />
        <Route path="/workspace/:workspaceId/team" element={<TeamView />} />
        <Route path="/workspace/:workspaceId/members" element={<MembersView />} />
        <Route path="/workspace/:workspaceId/messages" element={<MessagesView />} />
        <Route path="/workspace/:workspaceId/messages/:channelId" element={<MessagesView />} />
        <Route path="/workspace/:workspaceId/files" element={<FilesView />} />
        <Route path="/workspace/:workspaceId/files/:documentId" element={<FilesView />} />
        <Route path="/workspace/:workspaceId/dashboard" element={<DashboardView />} />
        <Route path="/workspace/:workspaceId/projects" element={<ProjectsView />} />
        <Route path="/workspace/:workspaceId/projects/:boardId" element={<ProjectsView />} />
        <Route path="/workspace/:workspaceId/agents" element={<AgentsView />} />
        <Route path="/workspace/:workspaceId/agents/:agentId" element={<AgentsView />} />

        {/* Website / Linktree Builder */}
        <Route path="/sites" element={<WebsiteBuilderView />} />
        <Route path="/sites/:siteId" element={<WebsiteBuilderView />} />
      </Routes>
    </AppLayout>
  );
}

function App() {
  return (
    <Sentry.ErrorBoundary fallback={AppErrorFallback}>
      <PersistQueryClientProvider
        client={queryClient}
        persistOptions={{
          persister: getQueryPersister()!,
          maxAge: 24 * 60 * 60 * 1000, // 24 hours - cache persists for a day
          buster: 'v2', // Bump to clear old cache without email exclusions
        }}
      >
        <BrowserRouter>
          <AppContent />
        </BrowserRouter>
        {import.meta.env.DEV && <ReactQueryDevtools initialIsOpen={false} />}
      </PersistQueryClientProvider>
    </Sentry.ErrorBoundary>
  );
}

export default App;
