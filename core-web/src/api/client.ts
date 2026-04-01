import { useAuthStore, type UserProfile } from '../stores/authStore';
import { API_BASE } from '../lib/apiBase';
import { captureException } from '../lib/sentry';
import { trackEvent } from '../lib/posthog';
import { supabase } from '../lib/supabase';

// Singleton refresh promise to deduplicate concurrent refresh attempts
let refreshPromise: Promise<string | null> | null = null;
let authFailureHandled = false;

export class AuthExpiredError extends Error {
  status: number;

  constructor(message = 'Session expired. Please sign in again.') {
    super(message);
    this.name = 'AuthExpiredError';
    this.status = 401;
  }
}

async function refreshAccessToken(): Promise<string | null> {
  if (refreshPromise) return refreshPromise;
  refreshPromise = (async () => {
    try {
      const { data, error } = await supabase.auth.refreshSession();
      if (error || !data.session) return null;
      return data.session.access_token;
    } catch {
      return null;
    } finally {
      // Keep promise around briefly so concurrent callers get the same result
      setTimeout(() => { refreshPromise = null; }, 1000);
    }
  })();
  return refreshPromise;
}

async function ensureFreshToken(): Promise<string | null> {
  const session = useAuthStore.getState().session;
  if (!session) return null;
  authFailureHandled = false;

  // If token expires within 60 seconds, proactively refresh
  const expiresAt = session.expires_at; // unix seconds
  const now = Math.floor(Date.now() / 1000);
  if (expiresAt && expiresAt - now > 60) {
    return session.access_token;
  }

  // Token is expired or about to expire — refresh it
  const refreshed = await refreshAccessToken();
  return refreshed ?? session.access_token;
}

async function forceRefreshToken(): Promise<string | null> {
  refreshPromise = null;
  return refreshAccessToken();
}

async function handleTerminalAuthFailure(): Promise<void> {
  if (authFailureHandled) return;
  authFailureHandled = true;

  try {
    await useAuthStore.getState().signOut();
  } catch (error) {
    console.error('Failed to sign out after auth failure:', error);
    useAuthStore.setState({
      user: null,
      session: null,
      userProfile: null,
      isAuthenticated: false,
      onboardingCompletedAt: undefined,
      isLoading: false,
    });
  }

  if (typeof window !== 'undefined' && window.location.pathname !== '/') {
    window.location.assign('/');
  }
}

async function makeRequest(endpoint: string, options: RequestInit, token: string | null): Promise<Response> {
  return fetch(`${API_BASE}${endpoint}`, {
    ...options,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(options.headers || {}),
    },
  });
}

export async function api<T>(
  endpoint: string,
  options: RequestInit = {}
): Promise<T> {
  // Proactively refresh if token is expired or about to expire
  let token = await ensureFreshToken();
  let response = await makeRequest(endpoint, options, token);

  // If still 401 (e.g., token was just revoked), force one more refresh and retry
  if (response.status === 401) {
    token = await forceRefreshToken();
    if (token) {
      response = await makeRequest(endpoint, options, token);
    }
  }

  if (response.status === 401) {
    await handleTerminalAuthFailure();
    throw new AuthExpiredError();
  }

  if (!response.ok) {
    // Try to extract error detail from response
    let errorMessage = `API error: ${response.status}`;
    try {
      const errorData = await response.json();
      if (errorData.detail) {
        // Handle FastAPI validation errors (array of objects)
        if (Array.isArray(errorData.detail)) {
          errorMessage = errorData.detail
            .map((err: { msg?: string; message?: string }) => err.msg || err.message || JSON.stringify(err))
            .join(', ');
        } else if (typeof errorData.detail === 'string') {
          errorMessage = errorData.detail;
        } else {
          errorMessage = JSON.stringify(errorData.detail);
        }
      } else if (errorData.message) {
        errorMessage = errorData.message;
      }
    } catch {
      // Ignore JSON parse errors
    }
    const error = new Error(errorMessage) as Error & { status?: number };
    error.status = response.status;
    if (response.status >= 500) {
      captureException(error, { endpoint, status: response.status, method: options.method ?? 'GET' });
    }
    throw error;
  }

  // Handle empty responses (e.g., 204 No Content from DELETE operations)
  const contentLength = response.headers.get('content-length');
  if (response.status === 204 || contentLength === '0') {
    return {} as T;
  }

  // Try to parse JSON, return empty object if body is empty
  const text = await response.text();
  if (!text) {
    return {} as T;
  }
  return JSON.parse(text);
}

// ============================================================================
// Product Types
// ============================================================================

export type ProductType = 'workspace' | 'ai_builder' | 'website_builder';

// ============================================================================
// Workspace Types
// ============================================================================

export interface Workspace {
  id: string;
  name: string;
  owner_id: string;
  is_default: boolean;
  created_at: string;
  updated_at: string;
  role?: string; // User's role in this workspace
  is_shared?: boolean; // Shared workspace flag
  emoji?: string; // Emoji icon for workspace
  icon_r2_key?: string; // R2 key for workspace icon (stored in DB)
  icon_url?: string; // Generated signed proxy URL (from API response)
}

export interface WorkspaceApp {
  id: string;
  workspace_id: string;
  app_type: 'chat' | 'team' | 'files' | 'messages' | 'dashboard' | 'projects' | 'email' | 'calendar' | 'agents';
  is_public: boolean;
  position: number;
  config: Record<string, unknown>;
  created_at: string;
}

export interface WorkspaceMember {
  id: string;
  workspace_id: string;
  user_id: string;
  role: 'owner' | 'admin' | 'member';
  joined_at: string;
  email?: string;
  name?: string;
  avatar_url?: string;
}

export interface WorkspaceInvitation {
  id: string;
  workspace_id: string;
  email: string;
  role: 'member' | 'admin';
  status: 'pending' | 'accepted' | 'declined' | 'revoked' | 'expired';
  token: string;
  expires_at: string;
  invited_by_user_id?: string | null;
  accepted_by_user_id?: string | null;
  accepted_at?: string | null;
  declined_at?: string | null;
  revoked_at?: string | null;
  last_email_sent_at?: string | null;
  last_email_error?: string | null;
  created_at?: string;
  updated_at?: string;
  workspace_name?: string;
  inviter_name?: string;
  recipient_user_exists?: boolean;
}

export interface WorkspaceInvitationActionResult {
  invitation: WorkspaceInvitation;
  already_processed: boolean;
  membership_created?: boolean;
}

export interface WorkspaceInvitationShareLink {
  invitation_id: string;
  invite_url: string;
  expires_at?: string | null;
}

// ============================================================================
// Workspace API Functions
// ============================================================================

export async function getWorkspaces(): Promise<Workspace[]> {
  const response = await api<{ workspaces: Workspace[]; count: number }>('/workspaces');
  return response.workspaces;
}

// Batched init data for cold start — single round-trip for all bootstrap data
export interface InitData {
  workspaces: (Workspace & { apps: WorkspaceApp[] })[];
  channels_by_app: Record<string, Channel[]>;
  dms_by_app: Record<string, DMChannel[]>;
  unread_counts: Record<string, number>;
  onboarding_completed_at?: string | null;
}

export async function getInitData(): Promise<InitData> {
  return api<InitData>('/me/init');
}

export async function getWorkspace(id: string): Promise<Workspace> {
  const response = await api<{ workspace: Workspace }>(`/workspaces/${id}`);
  return response.workspace;
}

export async function getDefaultWorkspace(): Promise<Workspace> {
  const response = await api<{ workspace: Workspace }>('/workspaces/default');
  return response.workspace;
}

export async function createWorkspace(
  name: string,
  createDefaultApps: boolean = true
): Promise<{ workspace: Workspace; welcome_note_id?: string }> {
  const response = await api<{ workspace: Workspace; welcome_note_id?: string }>('/workspaces', {
    method: 'POST',
    body: JSON.stringify({ name, create_default_apps: createDefaultApps }),
  });
  trackEvent('workspace_created');
  return response;
}

export async function updateWorkspace(
  id: string,
  updates: { name?: string; emoji?: string; icon_r2_key?: string; clear_icon?: boolean }
): Promise<Workspace> {
  const response = await api<{ workspace: Workspace }>(`/workspaces/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
  return response.workspace;
}

export async function deleteWorkspace(id: string): Promise<void> {
  await api(`/workspaces/${id}`, { method: 'DELETE' });
}

// ============================================================================
// Sharing & Permissions Types & API Functions
// ============================================================================

export interface Permission {
  id: string;
  workspace_id?: string;
  resource_type: string;
  resource_id: string;
  grantee_type: 'user' | 'link' | 'public';
  grantee_id?: string;
  permission: 'read' | 'write' | 'admin';
  link_token?: string | null;
  granted_by?: string | null;
  created_at?: string;
  expires_at?: string | null;
  grantee?: {
    id: string;
    email: string;
    name?: string;
    avatar_url?: string;
  };
}

export type PermissionLevel = Permission['permission'];

export interface ShareLink {
  id: string;
  link_token: string;
  link_slug?: string | null;
  resource_type: string;
  resource_id: string;
  permission: PermissionLevel;
  granted_by?: string;
  created_at?: string;
  expires_at?: string;
  url: string;
}

export interface ResolvedLink {
  resource_type: string;
  resource_id: string;
  workspace_id?: string;
  workspace_app_id?: string;
  app_type?: string;
  title?: string;
  permission: PermissionLevel;
}

export interface PublicSharedDocument {
  id: string;
  title?: string;
  content?: string;
  created_at?: string;
  updated_at?: string;
  thumb_url?: string;
  preview_url?: string;
  file_url?: string;
}

export interface PublicSharedFile {
  id: string;
  filename?: string;
  content_type?: string;
  file_size?: number;
  created_at?: string;
  download_url: string;
}

export interface PublicSharedResource {
  resource_type: string;
  resource_id: string;
  permission: PermissionLevel;
  shared_by?: {
    name?: string;
    avatar_url?: string;
  };
  document?: PublicSharedDocument;
  file?: PublicSharedFile;
}

export interface ShareRequest {
  resource_type: string;
  resource_id: string;
  grantee_email: string;
  permission?: 'read' | 'write' | 'admin';
}

export interface BatchShareRequest {
  resource_type: string;
  resource_id: string;
  grants: Array<{ email: string; permission?: 'read' | 'write' | 'admin' }>;
}

export interface SharedResource {
  permission_id: string;
  permission: 'read' | 'write' | 'admin';
  resource_type: string;
  resource_id: string;
  workspace_id?: string;
  workspace_name?: string;
  title?: string;
  workspace_app_id?: string;
  app_type?: string;
  created_at?: string;
}

export interface SharedWithMeParams {
  workspace_id?: string;
  resource_type?: string;
  limit?: number;
  offset?: number;
}

export interface SharedWithMeResponse {
  items: SharedResource[];
  count: number;
}

export interface AccessRequest {
  id: string;
  resource_type: string;
  resource_id: string;
  workspace_id?: string | null;
  requester_id: string;
  status: 'pending' | 'approved' | 'denied';
  message?: string | null;
  reviewed_by?: string | null;
  created_at?: string;
  resolved_at?: string | null;
  requester?: UserSearchResult;
  resource_title?: string | null;
}

export interface AccessRequestCreate {
  resource_type: string;
  resource_id: string;
  message?: string;
}

export interface AccessRequestResolve {
  status: 'approved' | 'denied';
  permission?: 'read' | 'write' | 'admin';
}

export interface UserSearchResult {
  id: string;
  email: string;
  name?: string;
  avatar_url?: string;
}

export async function shareResource(data: ShareRequest): Promise<Permission> {
  return api<Permission>('/permissions/share', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function batchShare(data: BatchShareRequest): Promise<Permission[]> {
  return api<Permission[]>('/permissions/share/batch', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function revokeShare(permissionId: string): Promise<void> {
  await api(`/permissions/share/${permissionId}`, { method: 'DELETE' });
}

export async function updateShare(permissionId: string, permission: PermissionLevel): Promise<Permission> {
  return api<Permission>(`/permissions/share/${permissionId}`, {
    method: 'PATCH',
    body: JSON.stringify({ permission }),
  });
}

export async function createShareLink(
  resourceType: string,
  resourceId: string,
  permission: PermissionLevel,
  slug?: string | null
): Promise<ShareLink> {
  const normalizedSlug = slug?.trim().toLowerCase();
  return api<ShareLink>('/permissions/link', {
    method: 'POST',
    body: JSON.stringify({
      resource_type: resourceType,
      resource_id: resourceId,
      permission,
      ...(normalizedSlug ? { slug: normalizedSlug } : {}),
    }),
  });
}

export async function updateShareLinkSlug(linkId: string, slug: string | null): Promise<ShareLink> {
  const normalizedSlug = slug?.trim().toLowerCase() || null;
  return api<ShareLink>(`/permissions/link/${linkId}/slug`, {
    method: 'PATCH',
    body: JSON.stringify({ slug: normalizedSlug }),
  });
}

export async function checkShareLinkSlugAvailability(slug: string): Promise<{
  slug: string;
  available: boolean;
  reason?: string | null;
}> {
  return api<{ slug: string; available: boolean; reason?: string | null }>(
    `/permissions/link/slug-availability?slug=${encodeURIComponent(slug)}`
  );
}

export async function revokeShareLink(token: string): Promise<void> {
  await api(`/permissions/link/${token}`, { method: 'DELETE' });
}

export async function getResourceLinks(
  resourceType: string,
  resourceId: string
): Promise<{ links: ShareLink[] }> {
  return api<{ links: ShareLink[] }>(`/permissions/links/${resourceType}/${resourceId}`);
}

export async function resolveShareLink(token: string): Promise<ResolvedLink> {
  return api<ResolvedLink>(`/permissions/resolve-link/${token}`, { method: 'POST' });
}

export async function getPublicSharedResource(token: string): Promise<PublicSharedResource> {
  const response = await fetch(`${API_BASE}/public/shared/${encodeURIComponent(token)}`, {
    headers: {
      'Content-Type': 'application/json',
    },
  });

  if (!response.ok) {
    let errorMessage = `API error: ${response.status}`;
    try {
      const errorData = await response.json();
      if (errorData.detail) {
        errorMessage = typeof errorData.detail === 'string' ? errorData.detail : JSON.stringify(errorData.detail);
      } else if (errorData.message) {
        errorMessage = errorData.message;
      }
    } catch {
      // Ignore parse failures.
    }
    const error = new Error(errorMessage) as Error & { status?: number };
    error.status = response.status;
    throw error;
  }

  return response.json();
}

export async function getResourceShares(resourceType: string, resourceId: string): Promise<{
  shares: Permission[];
  members: WorkspaceMember[];
}> {
  return api<{ shares: Permission[]; members: WorkspaceMember[] }>(
    `/permissions/resource/${resourceType}/${resourceId}`
  );
}

export async function getSharedWithMe(params: SharedWithMeParams = {}): Promise<SharedWithMeResponse> {
  const searchParams = new URLSearchParams();
  if (params.workspace_id) searchParams.append('workspace_id', params.workspace_id);
  if (params.resource_type) searchParams.append('resource_type', params.resource_type);
  if (typeof params.limit === 'number') searchParams.append('limit', String(params.limit));
  if (typeof params.offset === 'number') searchParams.append('offset', String(params.offset));
  const query = searchParams.toString();
  return api<SharedWithMeResponse>(`/permissions/shared-with-me${query ? `?${query}` : ''}`);
}

export async function requestAccess(data: AccessRequestCreate): Promise<AccessRequest> {
  return api<AccessRequest>('/access-requests', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function getPendingAccessRequests(): Promise<AccessRequest[]> {
  return api<AccessRequest[]>('/access-requests/pending');
}

export async function resolveAccessRequest(requestId: string, data: AccessRequestResolve): Promise<AccessRequest> {
  return api<AccessRequest>(`/access-requests/${requestId}`, {
    method: 'PATCH',
    body: JSON.stringify(data),
  });
}

export async function searchUsersForSharing(query: string): Promise<UserSearchResult[]> {
  const result = await api<{ users: UserSearchResult[]; count: number }>(
    `/permissions/users/search?q=${encodeURIComponent(query)}`
  );
  return result.users;
}

// ============================================================================
// Workspace Apps API Functions
// ============================================================================

export async function getWorkspaceApps(workspaceId: string): Promise<WorkspaceApp[]> {
  const response = await api<{ apps: WorkspaceApp[]; count: number }>(
    `/workspaces/${workspaceId}/apps`
  );
  return response.apps;
}

export async function createWorkspaceApp(
  workspaceId: string,
  appType: string
): Promise<WorkspaceApp> {
  const response = await api<{ app: WorkspaceApp }>(
    `/workspaces/${workspaceId}/apps`,
    {
      method: 'POST',
      body: JSON.stringify({ app_type: appType }),
    }
  );
  return response.app;
}

export async function updateWorkspaceApp(
  workspaceId: string,
  appId: string,
  updates: { is_public?: boolean; position?: number; config?: Record<string, unknown> }
): Promise<WorkspaceApp> {
  const response = await api<{ app: WorkspaceApp }>(
    `/workspaces/${workspaceId}/apps/${appId}`,
    {
      method: 'PATCH',
      body: JSON.stringify(updates),
    }
  );
  return response.app;
}

export async function deleteWorkspaceApp(
  workspaceId: string,
  appId: string
): Promise<void> {
  await api(`/workspaces/${workspaceId}/apps/${appId}`, { method: 'DELETE' });
}

export async function reorderWorkspaceApps(
  workspaceId: string,
  appPositions: { id: string; position: number }[]
): Promise<{ message: string; updated_count: number }> {
  return api(`/workspaces/${workspaceId}/apps/reorder`, {
    method: 'POST',
    body: JSON.stringify({ app_positions: appPositions }),
  });
}

// ============================================================================
// Workspace Members API Functions
// ============================================================================

export async function getWorkspaceMembers(workspaceId: string): Promise<WorkspaceMember[]> {
  const response = await api<{ members: WorkspaceMember[]; count: number }>(
    `/workspaces/${workspaceId}/members`
  );
  return response.members;
}

export async function addWorkspaceMember(
  workspaceId: string,
  email: string,
  role: 'member' | 'admin' = 'member'
): Promise<WorkspaceMember & { email?: string; name?: string }> {
  const response = await api<{ member: WorkspaceMember & { email?: string; name?: string } }>(
    `/workspaces/${workspaceId}/members`,
    {
      method: 'POST',
      body: JSON.stringify({ email, role }),
    }
  );
  return response.member;
}

export async function removeWorkspaceMember(
  workspaceId: string,
  userId: string
): Promise<void> {
  await api(`/workspaces/${workspaceId}/members/${userId}`, { method: 'DELETE' });
}

export async function updateWorkspaceMemberRole(
  workspaceId: string,
  userId: string,
  role: 'member' | 'admin'
): Promise<WorkspaceMember> {
  const response = await api<{ member: WorkspaceMember }>(
    `/workspaces/${workspaceId}/members/${userId}`,
    { method: 'PATCH', body: JSON.stringify({ role }) }
  );
  return response.member;
}

export async function getWorkspaceInvitations(workspaceId: string): Promise<WorkspaceInvitation[]> {
  const response = await api<{ invitations: WorkspaceInvitation[]; count: number }>(
    `/workspaces/${workspaceId}/invitations`
  );
  return response.invitations;
}

export async function createWorkspaceInvitation(
  workspaceId: string,
  email: string,
  role: 'member' | 'admin' = 'member'
): Promise<WorkspaceInvitation> {
  const response = await api<{ invitation: WorkspaceInvitation }>(
    `/workspaces/${workspaceId}/invitations`,
    {
      method: 'POST',
      body: JSON.stringify({ email, role }),
    }
  );
  return response.invitation;
}

export async function revokeWorkspaceInvitation(
  invitationId: string
): Promise<WorkspaceInvitationActionResult> {
  return api<WorkspaceInvitationActionResult>(`/workspaces/invitations/${invitationId}/revoke`, {
    method: 'POST',
  });
}

export async function acceptWorkspaceInvitation(
  invitationId: string
): Promise<WorkspaceInvitationActionResult> {
  return api<WorkspaceInvitationActionResult>(`/workspaces/invitations/${invitationId}/accept`, {
    method: 'POST',
  });
}

export async function declineWorkspaceInvitation(
  invitationId: string
): Promise<WorkspaceInvitationActionResult> {
  return api<WorkspaceInvitationActionResult>(`/workspaces/invitations/${invitationId}/decline`, {
    method: 'POST',
  });
}

export async function acceptWorkspaceInvitationByToken(
  token: string
): Promise<WorkspaceInvitationActionResult> {
  return api<WorkspaceInvitationActionResult>('/workspaces/invitations/accept-by-token', {
    method: 'POST',
    body: JSON.stringify({ token }),
  });
}

export async function resolvePostSignupInvitations(): Promise<{
  pending_invitations: WorkspaceInvitation[];
  count: number;
}> {
  return api('/auth/post-signup', { method: 'POST' });
}

export async function getWorkspaceInvitationShareLink(
  invitationId: string
): Promise<WorkspaceInvitationShareLink> {
  return api<WorkspaceInvitationShareLink>(`/workspaces/invitations/${invitationId}/share-link`);
}

// ============================================================================
// Email Account Types & API Functions (Multi-Account)
// ============================================================================

export interface EmailAccount {
  id: string;
  provider: 'google' | 'microsoft';
  provider_email: string;
  provider_name?: string;
  provider_avatar?: string;
  is_primary: boolean;
  account_order: number;
  is_active: boolean;
}

export interface OAuthConfig {
  google_client_id: string;
  microsoft_client_id?: string;
  supported_providers: string[];
}

export async function getOAuthConfig(): Promise<OAuthConfig> {
  return api('/auth/oauth-config');
}

export async function getEmailAccounts(): Promise<{ accounts: EmailAccount[] }> {
  return api('/auth/email-accounts');
}

export async function addEmailAccount(data: {
  provider: string;
  server_auth_code: string;
  code_verifier?: string;
  redirect_uri?: string;
  scopes: string[];
}): Promise<EmailAccount> {
  return api('/auth/email-accounts', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function removeEmailAccount(accountId: string): Promise<void> {
  await api(`/auth/email-accounts/${accountId}`, { method: 'DELETE' });
}

export async function updateEmailAccountOrder(accountId: string, accountOrder: number): Promise<void> {
  await api(`/auth/email-accounts/${accountId}`, {
    method: 'PATCH',
    body: JSON.stringify({ account_order: accountOrder }),
  });
}

// ============================================================================
// Email Types & API Functions
// ============================================================================

export interface Email {
  id: string;
  gmail_draft_id?: string;
  thread_id: string;
  subject: string;
  snippet: string;
  from_email: string;
  from_name?: string;
  to_emails: string[];
  cc_emails?: string[];
  date: string;
  is_read: boolean;
  is_starred: boolean;
  label_ids: string[];
  has_attachments: boolean;
  body_text?: string;
  body_html?: string;
  account_email?: string;
  account_provider?: string;
  source?: 'local' | 'remote';
  connection_id?: string;
  message_count?: number;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  raw_item?: any;
}

export interface EmailAttachment {
  attachmentId: string;
  filename: string;
  mimeType: string;
  size: number;
}

export interface EmailWithAttachments extends Email {
  attachments?: EmailAttachment[];
}

export interface ThreadResponse {
  thread_id: string;
  emails: EmailWithAttachments[];
}

export interface AttachmentDownloadResponse {
  attachment: {
    attachmentId: string;
    data: string;
    size: number;
  };
}

export interface EmailCounts {
  inbox_unread: number;
  drafts_count: number;
  unified: boolean;
  per_account: {
    id: string;
    email: string;
    provider: string;
    inbox_unread: number;
    drafts_count: number;
  }[];
}

export interface EmailAccountStatus {
  connectionId: string;
  email: string;
  provider: string;
  avatar?: string;
  lastSynced?: string;
}

// Parse "Name <email>" format into separate name and email
function parseFromField(from: string): { from_email: string; from_name?: string } {
  if (!from) return { from_email: '' };
  const match = from.match(/^(.+?)\s*<(.+?)>$/);
  if (match) {
    return { from_name: match[1].trim(), from_email: match[2].trim() };
  }
  // Just an email address
  return { from_email: from.trim() };
}

// raw_item.id is only a Gmail draft ID when the payload is a draft wrapper.
// For plain message payloads, raw_item.id is a Gmail message ID.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function extractGmailDraftId(rawItem: any): string | undefined {
  if (!rawItem || typeof rawItem !== 'object') return undefined;
  if (typeof rawItem.gmail_draft_id === 'string' && rawItem.gmail_draft_id) {
    return rawItem.gmail_draft_id;
  }
  if (rawItem.message && typeof rawItem.id === 'string' && rawItem.id) {
    return rawItem.id;
  }
  return undefined;
}

export async function getEmails(options?: {
  maxResults?: number;
  offset?: number;
  labelIds?: string[];
  query?: string;
  accountIds?: string[];
}): Promise<{ emails: Email[]; count: number; unified: boolean; hasMore: boolean; accountsStatus?: EmailAccountStatus[] }> {
  const params = new URLSearchParams();
  if (options?.maxResults) params.append('max_results', String(options.maxResults));
  if (options?.offset) params.append('offset', String(options.offset));
  if (options?.labelIds) options.labelIds.forEach(id => params.append('label_ids', id));
  if (options?.query) params.append('query', options.query);
  if (options?.accountIds) options.accountIds.forEach(id => params.append('account_ids', id));

  const queryString = params.toString();
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const response = await api<{ emails: any[]; count: number; unified: boolean; has_more: boolean; accounts_status?: any[] }>(
    `/email/messages${queryString ? `?${queryString}` : ''}`
  );

  // Transform backend response to match frontend Email interface
  const emails: Email[] = response.emails.map(e => {
    const { from_email, from_name } = parseFromField(e.from || '');
    return {
      id: e.external_id || e.id || '',
      gmail_draft_id: e.gmail_draft_id || extractGmailDraftId(e.raw_item),
      thread_id: e.thread_id || '',
      subject: e.subject || '',
      snippet: e.snippet || '',
      from_email,
      from_name,
      to_emails: Array.isArray(e.to) ? e.to : (e.to ? [e.to] : []),
      cc_emails: Array.isArray(e.cc) ? e.cc : (e.cc ? [e.cc] : []),
      date: e.received_at || e.date || '',
      is_read: !e.is_unread,
      is_starred: e.is_starred || false,
      label_ids: e.labels || e.label_ids || [],
      has_attachments: e.has_attachments || false,
      body_text: e.body_text || e.body || '',
      body_html: e.body_html || '',
      account_email: e.account_email,
      account_provider: e.account_provider,
      connection_id: e.ext_connection_id || e.connection_id,
      message_count: e.message_count,
    };
  });

  // Map account status for filtering UI
  const accountsStatus = response.accounts_status?.map(acc => ({
    connectionId: acc.connection_id,
    email: acc.email,
    provider: acc.provider,
    avatar: acc.avatar,
    lastSynced: acc.last_synced,
  }));

  return {
    emails,
    count: response.count,
    unified: response.unified,
    hasMore: response.has_more ?? emails.length >= (options?.maxResults ?? 50),
    accountsStatus,
  };
}

// Search emails via backend (local DB + provider APIs)
export interface SearchEmailsResponse {
  emails: Email[];
  count: number;
  local_count: number;
  remote_count: number;
  provider_errors?: Record<string, string>;
  has_provider_errors: boolean;
}

export async function searchEmails(options: {
  query: string;
  account_ids?: string[];
  provider_search?: boolean;
  max_results?: number;
}): Promise<SearchEmailsResponse> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const response = await api<any>('/email/search', {
    method: 'POST',
    body: JSON.stringify({
      query: options.query,
      account_ids: options.account_ids,
      provider_search: options.provider_search ?? true,
      max_results: options.max_results ?? 25,
    }),
  });

  const emails: Email[] = (response.emails || []).map((e: any) => {
    const { from_email, from_name } = parseFromField(e.from || '');
    return {
      id: e.external_id || e.id || '',
      gmail_draft_id: e.gmail_draft_id || extractGmailDraftId(e.raw_item),
      thread_id: e.thread_id || '',
      subject: e.subject || '',
      snippet: e.snippet || '',
      from_email,
      from_name,
      to_emails: Array.isArray(e.to) ? e.to : (e.to ? [e.to] : []),
      cc_emails: Array.isArray(e.cc) ? e.cc : (e.cc ? [e.cc] : []),
      date: e.received_at || e.date || '',
      is_read: e.is_read ?? !e.is_unread,
      is_starred: e.is_starred || false,
      label_ids: e.labels || e.label_ids || [],
      has_attachments: e.has_attachments || false,
      body_text: e.body_text || e.body || '',
      body_html: e.body_html || '',
      account_email: e.account_email,
      account_provider: e.account_provider,
      source: e.source || 'local',
      connection_id: e.ext_connection_id || e.connection_id,
    };
  });

  trackEvent('email_searched');
  return {
    emails,
    count: response.count ?? emails.length,
    local_count: response.local_count ?? 0,
    remote_count: response.remote_count ?? 0,
    provider_errors: response.provider_errors,
    has_provider_errors: response.has_provider_errors ?? false,
  };
}

// Fetch a remote-only email and sync it to local DB
export async function fetchRemoteEmail(options: {
  external_id: string;
  connection_id: string;
}): Promise<{ success: boolean; email?: Email; error?: string }> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const response = await api<any>('/email/fetch-remote', {
      method: 'POST',
      body: JSON.stringify(options),
    });

    const e = response.email || response;
    const { from_email, from_name } = parseFromField(e.from || e.sender || '');

    const email: Email = {
      id: e.id || e.external_id || options.external_id,
      gmail_draft_id: e.gmail_draft_id || extractGmailDraftId(e.raw_item),
      thread_id: e.thread_id || '',
      subject: e.subject || '',
      snippet: e.snippet || '',
      from_email,
      from_name,
      to_emails: Array.isArray(e.to) ? e.to : (e.to ? [e.to] : []),
      cc_emails: Array.isArray(e.cc) ? e.cc : (e.cc ? [e.cc] : []),
      date: e.date || e.received_at || '',
      is_read: e.is_read ?? !e.is_unread,
      is_starred: e.is_starred || false,
      label_ids: e.labels || e.label_ids || [],
      has_attachments: e.has_attachments || false,
      body_text: e.body_plain || e.body_text || e.body || '',
      body_html: e.body_html || '',
      account_email: e.account_email,
      account_provider: e.account_provider,
      source: 'local',
      connection_id: e.ext_connection_id || e.connection_id,
    };

    return { success: true, email };
  } catch (err) {
    return {
      success: false,
      error: err instanceof Error ? err.message : 'Failed to fetch remote email',
    };
  }
}

export async function getEmailCounts(accountIds?: string[]): Promise<EmailCounts> {
  const params = new URLSearchParams();
  if (accountIds && accountIds.length > 0) {
    accountIds.forEach((id) => params.append('account_ids', id));
  }
  const qs = params.toString();
  return api(`/email/counts${qs ? `?${qs}` : ''}`);
}

export async function getEmailDetails(emailId: string): Promise<{ email: Email }> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const response = await api<any>(`/email/messages/${emailId}`);
  // The actual email data is nested under 'email' key from backend
  const e = response.email || response;
  const { from_email, from_name } = parseFromField(e.from || e.sender || '');

  const email: Email = {
    id: e.id || e.external_id || emailId,
    gmail_draft_id: e.gmail_draft_id || extractGmailDraftId(e.raw_item),
    thread_id: e.thread_id || '',
    subject: e.subject || '',
    snippet: e.snippet || '',
    from_email,
    from_name,
    to_emails: Array.isArray(e.to) ? e.to : (e.to ? [e.to] : []),
    cc_emails: Array.isArray(e.cc) ? e.cc : (e.cc ? [e.cc] : []),
    date: e.date || e.received_at || '',
    is_read: e.is_read ?? !e.is_unread,
    is_starred: e.is_starred || false,
    label_ids: e.labels || e.label_ids || [],
    has_attachments: e.has_attachments || false,
    // Backend returns body_plain and body_html
    body_text: e.body_plain || e.body_text || e.body || '',
    body_html: e.body_html || '',
    account_email: e.account_email,
    account_provider: e.account_provider,
    connection_id: e.ext_connection_id || e.connection_id,
    raw_item: e.raw_item,
  };

  return { email };
}

export async function getThreadDetails(threadId: string): Promise<ThreadResponse> {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const response = await api<any>(`/email/threads/${threadId}`);
  const threadData = response.thread || response;

  // Transform each email to have proper from_email/from_name fields
  // The backend returns 'from' or 'sender' as unparsed string like "John Doe <john@example.com>"
  // For sent emails, 'from' may be empty - use account_email as fallback (it's the user's own email)
  const emails: EmailWithAttachments[] = (threadData.emails || []).map((e: Record<string, unknown>) => {
    const fromRaw = (e.from as string) || (e.sender as string) || '';
    let { from_email, from_name } = parseFromField(fromRaw);

    // If from is empty, use account_email as fallback (for sent emails)
    if (!from_email && e.account_email) {
      from_email = e.account_email as string;
    }

    // Parse to/cc recipients - backend returns string or array
    const toEmails = Array.isArray(e.to)
      ? e.to
      : Array.isArray(e.to_recipients)
        ? e.to_recipients
        : typeof e.to === 'string'
          ? e.to.split(',').map((s: string) => s.trim()).filter(Boolean)
          : typeof e.to_recipients === 'string'
            ? e.to_recipients.split(',').map((s: string) => s.trim()).filter(Boolean)
            : [];

    const ccEmails = Array.isArray(e.cc)
      ? e.cc
      : Array.isArray(e.cc_recipients)
        ? e.cc_recipients
        : typeof e.cc === 'string'
          ? e.cc.split(',').map((s: string) => s.trim()).filter(Boolean)
          : typeof e.cc_recipients === 'string'
            ? e.cc_recipients.split(',').map((s: string) => s.trim()).filter(Boolean)
            : [];

    // Handle body content - sometimes body_text contains HTML, body_html is empty
    let bodyText = (e.body_plain || e.body_text || e.body || '') as string;
    let bodyHtml = (e.body_html || '') as string;

    // If body_html is empty but body_text looks like HTML, swap them
    if (!bodyHtml && bodyText && bodyText.trim().startsWith('<')) {
      bodyHtml = bodyText;
      bodyText = ''; // Will extract text from HTML if needed
    }

    return {
      id: (e.id || e.external_id) as string,
      thread_id: (e.thread_id || threadId) as string,
      subject: (e.subject || '') as string,
      snippet: (e.snippet || '') as string,
      from_email,
      from_name,
      to_emails: toEmails as string[],
      cc_emails: ccEmails as string[],
      date: (e.date || e.received_at || '') as string,
      is_read: e.is_read !== undefined ? e.is_read as boolean : !(e.is_unread as boolean),
      is_starred: (e.is_starred || false) as boolean,
      label_ids: (e.labels || e.label_ids || []) as string[],
      has_attachments: (e.has_attachments || false) as boolean,
      body_text: bodyText,
      body_html: bodyHtml,
      account_email: e.account_email as string | undefined,
      account_provider: e.account_provider as string | undefined,
      connection_id: (e.ext_connection_id || e.connection_id) as string | undefined,
      attachments: e.attachments as EmailAttachment[] | undefined,
    };
  });

  return {
    thread_id: threadId,
    emails,
  };
}

export async function downloadEmailAttachment(
  emailId: string,
  attachmentId: string
): Promise<{ blob: Blob; filename: string }> {
  const response = await api<AttachmentDownloadResponse>(
    `/email/messages/${emailId}/attachments/${attachmentId}`
  );

  try {
    // Decode base64url to binary
    const base64Data = response.attachment.data
      .replace(/-/g, '+')
      .replace(/_/g, '/');

    const binaryString = atob(base64Data);
    const bytes = new Uint8Array(binaryString.length);
    for (let i = 0; i < binaryString.length; i++) {
      bytes[i] = binaryString.charCodeAt(i);
    }

    const blob = new Blob([bytes]);

    return {
      blob,
      filename: attachmentId,
    };
  } catch (err) {
    console.error('Attachment download failed:', err);
    throw new Error('Failed to download attachment. Please try again.');
  }
}

export async function markEmailRead(emailId: string): Promise<void> {
  await api(`/email/messages/${emailId}/mark-read`, { method: 'POST' });
}

export async function markEmailUnread(emailId: string): Promise<void> {
  await api(`/email/messages/${emailId}/mark-unread`, { method: 'POST' });
}

export async function archiveEmail(emailId: string): Promise<void> {
  await api(`/email/messages/${emailId}/archive`, { method: 'POST' });
}

export async function deleteEmail(emailId: string): Promise<void> {
  await api(`/email/messages/${emailId}`, { method: 'DELETE' });
}

export async function restoreEmail(emailId: string): Promise<void> {
  await api(`/email/messages/${emailId}/restore`, { method: 'POST' });
}

export async function syncEmails(): Promise<{ new_emails: number; updated_emails: number }> {
  return api('/email/sync', { method: 'POST' });
}

// Send email
export interface EmailAttachmentUpload {
  filename: string;
  content: string; // Base64-encoded
  mime_type: string;
}

export interface SendEmailRequest {
  to: string[];
  cc?: string[];
  bcc?: string[];
  subject: string;
  body: string;       // Plain text body
  body_html?: string; // HTML body (optional)
  account_id?: string;
  attachments?: EmailAttachmentUpload[];
  // Reply threading fields
  in_reply_to?: string;  // Message-ID of email being replied to
  thread_id?: string;    // Thread ID for grouping
  references?: string;   // Message-ID chain for threading
}

export async function sendEmail(data: SendEmailRequest): Promise<{ id: string; thread_id: string }> {
  const payload = {
    to: data.to.join(', '),
    cc: data.cc?.length ? data.cc : undefined,
    bcc: data.bcc?.length ? data.bcc : undefined,
    subject: data.subject,
    body: data.body,
    html_body: data.body_html,
    from_account_id: data.account_id,
    in_reply_to: data.in_reply_to,
    thread_id: data.thread_id,
    references: data.references,
    attachments: data.attachments?.length ? data.attachments : undefined,
  };

  const result = await api<{ id: string; thread_id: string }>('/email/send', {
    method: 'POST',
    body: JSON.stringify(payload),
  });
  trackEvent('email_sent', { is_reply: !!data.in_reply_to, has_attachments: !!data.attachments?.length });
  return result;
}

// Draft management
export interface SaveDraftRequest {
  to?: string[];
  cc?: string[];
  bcc?: string[];
  subject?: string;
  body_html?: string;
  body_text?: string;
  account_id?: string;
}

export async function saveDraft(data: SaveDraftRequest): Promise<{ draft_id: string }> {
  // Backend draft endpoints expect MIME-style fields:
  // - to: comma-separated string
  // - body/html_body: plain + html content
  const response = await api<{ draft?: { id?: string } }>('/email/drafts', {
    method: 'POST',
    body: JSON.stringify({
      to: data.to?.length ? data.to.join(', ') : undefined,
      cc: data.cc?.length ? data.cc : undefined,
      bcc: data.bcc?.length ? data.bcc : undefined,
      subject: data.subject,
      body: data.body_text ?? '',
      html_body: data.body_html,
      account_id: data.account_id,
    }),
  });
  return { draft_id: response?.draft?.id || '' };
}

export async function updateDraft(draftId: string, data: SaveDraftRequest): Promise<{ draft_id: string }> {
  const response = await api<{ draft?: { id?: string } }>(`/email/drafts/${draftId}`, {
    method: 'PUT',
    body: JSON.stringify({
      to: data.to?.length ? data.to.join(', ') : undefined,
      cc: data.cc?.length ? data.cc : undefined,
      bcc: data.bcc?.length ? data.bcc : undefined,
      subject: data.subject,
      body: data.body_text,
      html_body: data.body_html,
    }),
  });
  return { draft_id: response?.draft?.id || draftId };
}

export async function deleteDraft(draftId: string): Promise<void> {
  await api(`/email/drafts/${draftId}`, { method: 'DELETE' });
}

export async function sendDraft(draftId: string): Promise<Record<string, unknown>> {
  return api(`/email/drafts/${draftId}/send`, { method: 'POST' });
}

// ============================================================================
// Calendar Types & API Functions
// ============================================================================

export interface CalendarEvent {
  id: string;
  title: string;
  description?: string;
  start_time: string;
  end_time: string;
  all_day: boolean;
  location?: string;
  status: 'confirmed' | 'tentative' | 'cancelled';
  account_email?: string;
  account_provider?: string;
  google_event_id?: string;
  recurrence?: string[];
  attendees?: { email: string; display_name?: string; response_status: string }[];
  meeting_link?: string;
  is_organizer?: boolean;
  organizer_email?: string;
}

export interface CalendarEventsResponse {
  events: CalendarEvent[];
  unified: boolean;
  accounts_status?: { id: string; email: string; provider: string }[];
}

export async function getCalendarEvents(accountIds?: string[]): Promise<CalendarEventsResponse> {
  const params = accountIds?.length ? `?account_ids=${accountIds.join(',')}` : '';
  return api(`/calendar/events${params}`);
}

export async function getTodayEvents(accountIds?: string[]): Promise<CalendarEventsResponse> {
  const params = accountIds?.length ? `?account_ids=${accountIds.join(',')}` : '';
  return api(`/calendar/events/today${params}`);
}

export async function createCalendarEvent(event: {
  title: string;
  description?: string;
  start_time: string;
  end_time: string;
  all_day?: boolean;
  location?: string;
  meeting_link?: string;
  add_google_meet?: boolean;
}): Promise<CalendarEvent> {
  const response = await api<any>('/calendar/events', {
    method: 'POST',
    body: JSON.stringify(event),
  });
  trackEvent('calendar_event_created');
  // Handle both direct event response and wrapped response
  return response.event || response;
}

export async function updateCalendarEvent(
  eventId: string,
  updates: Partial<CalendarEvent>
): Promise<CalendarEvent> {
  const result = await api<CalendarEvent>(`/calendar/events/${eventId}`, {
    method: 'PUT',
    body: JSON.stringify(updates),
  });
  trackEvent('calendar_event_updated');
  return result;
}

export async function deleteCalendarEvent(eventId: string): Promise<void> {
  await api(`/calendar/events/${eventId}`, { method: 'DELETE' });
  trackEvent('calendar_event_deleted');
}

export type CalendarResponseStatus = 'accepted' | 'declined' | 'tentative';

export async function respondToCalendarEvent(
  eventId: string,
  responseStatus: CalendarResponseStatus
): Promise<{ id: string; response_status: string; synced_to_google: boolean }> {
  const result = await api<{ id: string; response_status: string; synced_to_google: boolean }>(
    `/calendar/events/${eventId}/rsvp`,
    {
      method: 'POST',
      body: JSON.stringify({ response_status: responseStatus }),
    }
  );
  trackEvent('calendar_event_rsvp', { response_status: responseStatus });
  return result;
}

export async function syncCalendar(): Promise<{ synced: number }> {
  return api('/calendar/sync', { method: 'POST' });
}

// ============================================================================
// Conversation Types & API Functions
// ============================================================================

// Conversation types
export interface Conversation {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
}

export interface Message {
  id: string;
  conversation_id: string;
  role: 'user' | 'assistant';
  content: string;
  content_parts?: ContentPart[];
  created_at: string;
}

export interface ContentPart {
  id: string;
  type: 'text' | 'display' | 'action' | 'sources' | 'attachment' | 'source_ref' | 'email_ref' | 'cal_ref' | 'tool_result' | 'reasoning' | 'tool_call';
  data: Record<string, unknown>;
}

// Streaming event types
export interface StreamEvent {
  type: 'content' | 'display' | 'action' | 'sources' | 'done' | 'error' | 'ping' | 'status' | 'tool_call';
  id?: string;
  delta?: string;
  display_type?: string;
  items?: unknown[];
  total_count?: number;
  action?: string;
  status?: string;
  data?: Record<string, unknown>;
  description?: string;
  message?: string;
  sources?: Source[];
  message_id?: string;
  error?: string;
  // tool_call event fields
  phase?: 'start' | 'end';
  name?: string;
  args?: Record<string, unknown>;
  duration_ms?: number;
}

export interface Source {
  title: string;
  url: string;
  domain?: string;
  favicon?: string;
}

// API functions
export async function getConversations(): Promise<Conversation[]> {
  return api<Conversation[]>('/chat/conversations');
}

export async function createConversation(title?: string): Promise<Conversation> {
  const result = await api<Conversation>('/chat/conversations', {
    method: 'POST',
    body: JSON.stringify({ title: title || 'New Conversation' }),
  });
  trackEvent('chat_conversation_created');
  return result;
}

export async function deleteConversation(id: string): Promise<void> {
  await api(`/chat/conversations/${id}`, { method: 'DELETE' });
}

export async function renameConversation(id: string, title: string): Promise<Conversation> {
  return api<Conversation>(`/chat/conversations/${id}`, {
    method: 'PATCH',
    body: JSON.stringify({ title }),
  });
}

export async function getMessages(conversationId: string): Promise<Message[]> {
  return api<Message[]>(`/chat/conversations/${conversationId}/messages`);
}

export async function executeAction(messageId: string, actionId: string): Promise<{ status: string; result?: Record<string, unknown> }> {
  return api<{ status: string; result?: Record<string, unknown> }>(`/chat/messages/${messageId}/actions/${actionId}/execute`, {
    method: 'PATCH',
  });
}

export async function* streamMessage(
  conversationId: string,
  content: string,
  options?: {
    timezone?: string;
    attachmentIds?: string[];
    workspaceIds?: string[];
  }
): AsyncGenerator<StreamEvent> {
  trackEvent('chat_message_sent');

  const buildStreamRequest = (authToken: string | null) => fetch(`${API_BASE}/chat/conversations/${conversationId}/messages`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(authToken ? { Authorization: `Bearer ${authToken}` } : {}),
    },
    body: JSON.stringify({
      content,
      timezone: options?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone,
      ...(options?.attachmentIds?.length ? { attachment_ids: options.attachmentIds } : {}),
      ...(options?.workspaceIds?.length ? { workspace_ids: options.workspaceIds } : {}),
    }),
  });

  const t0 = performance.now();
  let token = await ensureFreshToken();
  let response = await buildStreamRequest(token);
  console.log(`⏱ [stream] fetch responded (TTFB): ${(performance.now() - t0).toFixed(0)}ms`);

  // If still 401, force refresh and retry
  if (response.status === 401) {
    token = await forceRefreshToken();
    if (token) {
      response = await buildStreamRequest(token);
    }
  }

  if (response.status === 401) {
    await handleTerminalAuthFailure();
    throw new AuthExpiredError();
  }

  if (!response.ok) {
    const error = new Error(`API error: ${response.status}`);
    if (response.status >= 500) {
      captureException(error, { endpoint: `/chat/conversations/${conversationId}/messages`, status: response.status, method: 'POST' });
    }
    throw error;
  }

  const reader = response.body?.getReader();
  if (!reader) throw new Error('No response body');

  const decoder = new TextDecoder();
  let buffer = '';
  let firstChunk = true;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    if (firstChunk) { console.log(`⏱ [stream] first chunk: ${(performance.now() - t0).toFixed(0)}ms`); firstChunk = false; }

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (line.trim()) {
        try {
          const event = JSON.parse(line) as StreamEvent;
          yield event;
        } catch {
          // Skip malformed lines
        }
      }
    }
  }

  // Process any remaining buffer
  if (buffer.trim()) {
    try {
      const event = JSON.parse(buffer) as StreamEvent;
      yield event;
    } catch {
      // Skip malformed data
    }
  }
}

export async function* regenerateMessage(
  conversationId: string,
  messageId: string,
  options?: {
    timezone?: string;
    workspaceIds?: string[];
  }
): AsyncGenerator<StreamEvent> {
  trackEvent('chat_message_regenerated');

  const buildRequest = (authToken: string | null) => fetch(`${API_BASE}/chat/conversations/${conversationId}/messages/${messageId}/regenerate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(authToken ? { Authorization: `Bearer ${authToken}` } : {}),
    },
    body: JSON.stringify({
      timezone: options?.timezone || Intl.DateTimeFormat().resolvedOptions().timeZone,
      ...(options?.workspaceIds?.length ? { workspace_ids: options.workspaceIds } : {}),
    }),
  });

  const token = await ensureFreshToken();
  let response = await buildRequest(token);

  if (response.status === 401) {
    refreshPromise = null;
    const { data } = await supabase.auth.refreshSession().catch(() => ({ data: { session: null } }));
    if (data?.session) {
      response = await buildRequest(data.session.access_token);
    }
  }

  if (!response.ok) {
    const error = new Error(`API error: ${response.status}`);
    if (response.status >= 500) {
      captureException(error, { endpoint: `/chat/conversations/${conversationId}/messages/${messageId}/regenerate`, status: response.status, method: 'POST' });
    }
    throw error;
  }

  const reader = response.body?.getReader();
  if (!reader) throw new Error('No response body');

  const decoder = new TextDecoder();
  let buffer = '';

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;

    buffer += decoder.decode(value, { stream: true });
    const lines = buffer.split('\n');
    buffer = lines.pop() || '';

    for (const line of lines) {
      if (line.trim()) {
        try {
          yield JSON.parse(line) as StreamEvent;
        } catch {
          // Skip malformed lines
        }
      }
    }
  }

  if (buffer.trim()) {
    try {
      yield JSON.parse(buffer) as StreamEvent;
    } catch {
      // Skip malformed data
    }
  }
}

export async function deleteMessage(conversationId: string, messageId: string): Promise<void> {
  await api(`/chat/conversations/${conversationId}/messages/${messageId}`, { method: 'DELETE' });
}

// ============================================================================
// Chat Attachment Types & API Functions
// ============================================================================

export interface ChatAttachmentUploadResponse {
  attachment_id: string;
  original: { upload_url: string; r2_key: string };
  thumbnail: { upload_url: string; r2_key: string };
  expires_at: string;
}

export interface ChatAttachmentMetadata {
  id: string;
  conversation_id: string;
  filename: string;
  mime_type: string;
  file_size: number;
  width?: number;
  height?: number;
  r2_key: string;
  thumbnail_r2_key?: string;
  status: string;
  created_at: string;
}

export async function getChatAttachmentUploadUrl(data: {
  conversationId: string;
  filename: string;
  contentType: string;
  fileSize: number;
  thumbnailSize: number;
  width?: number;
  height?: number;
}): Promise<ChatAttachmentUploadResponse> {
  return api('/chat/attachments/upload-url', {
    method: 'POST',
    body: JSON.stringify({
      conversation_id: data.conversationId,
      filename: data.filename,
      content_type: data.contentType,
      file_size: data.fileSize,
      thumbnail_size: data.thumbnailSize,
      width: data.width,
      height: data.height,
    }),
  });
}

export async function confirmChatAttachment(attachmentId: string): Promise<{ attachment: ChatAttachmentMetadata }> {
  return api(`/chat/attachments/${attachmentId}/confirm`, { method: 'POST' });
}

export async function getChatAttachmentUrl(attachmentId: string, thumbnail = false): Promise<{ url: string; expires_in: number }> {
  const params = thumbnail ? '?thumbnail=true' : '';
  return api(`/chat/attachments/${attachmentId}/url${params}`);
}

export async function deleteChatAttachment(attachmentId: string): Promise<void> {
  await api(`/chat/attachments/${attachmentId}`, { method: 'DELETE' });
}

// ============================================================================
// Documents & Files Types & API Functions
// ============================================================================

export interface Document {
  id: string;
  user_id: string;
  workspace_app_id?: string;
  title: string;
  content?: string;
  icon?: string;
  cover_image?: string;
  type?: 'folder' | 'note' | 'file';  // Document type: folder, note (editable), or file (uploaded)
  is_folder: boolean;  // Kept for backwards compatibility
  parent_id?: string;
  position: number;
  tags?: string[];
  is_archived: boolean;
  is_favorite: boolean;
  is_public: boolean;
  public_id?: string;
  file_id?: string;
  file_url?: string;
  file_type?: string;  // Deprecated - use file.file_type
  file_size?: number;  // Deprecated - use file.file_size
  thumb_url?: string;  // CDN thumbnail URL (image proxy)
  preview_url?: string;  // CDN preview URL (image proxy)
  file?: {  // Nested file object from join
    id: string;
    filename: string;
    file_type: string;
    file_size: number;
    r2_key: string;
    status: string;
  };
  created_at: string;
  updated_at: string;
}

export interface FileItem {
  id: string;
  user_id: string;
  workspace_app_id?: string;
  filename: string;
  content_type: string;
  file_size: number;
  r2_key: string;
  public_url?: string;
  created_at: string;
  updated_at: string;
}

export interface PresignedUploadResponse {
  file_id: string;
  upload_url: string;
  r2_key: string;
  public_url: string;
  expires_at: string;
  headers: Record<string, string>;
  workspace_app_id: string;
  parent_id?: string;
  tags?: string[];
  create_document: boolean;
}

export interface DocumentVersion {
  id: string;
  document_id: string;
  title?: string;
  content?: string;
  version_number: number;
  created_by?: string;
  created_at?: string;
}

// Get documents with optional filtering
export async function getDocuments(options?: {
  workspaceAppId?: string;
  parentId?: string;
  includeArchived?: boolean;
  favoritesOnly?: boolean;
  foldersOnly?: boolean;
  documentsOnly?: boolean;
  tags?: string[];
  sortBy?: 'name' | 'type' | 'date' | 'size' | 'position';
  sortDirection?: 'asc' | 'desc';
}): Promise<{ documents: Document[]; count: number }> {
  const params = new URLSearchParams();
  if (options?.workspaceAppId) params.append('workspace_app_id', options.workspaceAppId);
  if (options?.parentId) params.append('parent_id', options.parentId);
  if (options?.includeArchived) params.append('include_archived', 'true');
  if (options?.favoritesOnly) params.append('favorites_only', 'true');
  if (options?.foldersOnly) params.append('folders_only', 'true');
  if (options?.documentsOnly) params.append('documents_only', 'true');
  if (options?.tags?.length) params.append('tags', options.tags.join(','));
  if (options?.sortBy) params.append('sort_by', options.sortBy);
  if (options?.sortDirection) params.append('sort_direction', options.sortDirection);

  const queryString = params.toString();
  return api(`/documents${queryString ? `?${queryString}` : ''}`);
}

// Get a single document by ID
export async function getDocument(documentId: string): Promise<Document> {
  return api<Document>(`/documents/${documentId}`);
}

// Create a new document
export async function createDocument(data: {
  workspaceAppId: string;
  title?: string;
  content?: string;
  icon?: string;
  parentId?: string;
  tags?: string[];
}): Promise<Document> {
  const result = await api<Document>('/documents', {
    method: 'POST',
    body: JSON.stringify({
      workspace_app_id: data.workspaceAppId,
      title: data.title || 'Untitled',
      content: data.content || '',
      icon: data.icon,
      parent_id: data.parentId,
      tags: data.tags || [],
    }),
  });
  trackEvent('document_created', { type: 'note' });
  return result;
}

// Create a new folder
export async function createFolder(data: {
  workspaceAppId: string;
  title?: string;
  parentId?: string;
}): Promise<Document> {
  return api<Document>('/documents/folders', {
    method: 'POST',
    body: JSON.stringify({
      workspace_app_id: data.workspaceAppId,
      title: data.title || 'New Folder',
      parent_id: data.parentId,
    }),
  });
}

// Update a document
export async function updateDocument(
  documentId: string,
  updates: {
    title?: string;
    content?: string;
    icon?: string;
    parentId?: string | null;
    tags?: string[];
    expectedUpdatedAt?: string;
  }
): Promise<Document> {
  return api<Document>(`/documents/${documentId}`, {
    method: 'PATCH',
    body: JSON.stringify({
      title: updates.title,
      content: updates.content,
      icon: updates.icon,
      parent_id: updates.parentId,
      tags: updates.tags,
      expected_updated_at: updates.expectedUpdatedAt,
    }),
  });
}

// Reorder documents
export async function reorderDocuments(
  documentPositions: { id: string; position: number }[]
): Promise<{ documents: Document[]; count: number }> {
  return api<{ documents: Document[]; count: number }>('/documents/reorder', {
    method: 'POST',
    body: JSON.stringify({ document_positions: documentPositions }),
  });
}

// Delete a document
export async function deleteDocument(documentId: string): Promise<void> {
  await api(`/documents/${documentId}`, { method: 'DELETE' });
}

// Archive/unarchive document
export async function archiveDocument(documentId: string): Promise<Document> {
  return api<Document>(`/documents/${documentId}/archive`, {
    method: 'POST',
  });
}

export async function unarchiveDocument(documentId: string): Promise<Document> {
  return api<Document>(`/documents/${documentId}/unarchive`, {
    method: 'POST',
  });
}

// Favorite/unfavorite document
export async function favoriteDocument(documentId: string): Promise<Document> {
  return api<Document>(`/documents/${documentId}/favorite`, {
    method: 'POST',
  });
}

export async function unfavoriteDocument(documentId: string): Promise<Document> {
  return api<Document>(`/documents/${documentId}/unfavorite`, {
    method: 'POST',
  });
}

// Document version history
export async function getDocumentVersions(
  documentId: string
): Promise<{ versions: DocumentVersion[]; count: number }> {
  return api(`/documents/${documentId}/versions`);
}

export async function getDocumentVersion(
  documentId: string,
  versionId: string
): Promise<DocumentVersion> {
  return api(`/documents/${documentId}/versions/${versionId}`);
}

export async function restoreDocumentVersion(
  documentId: string,
  versionId: string
): Promise<Document> {
  return api(`/documents/${documentId}/versions/${versionId}/restore`, {
    method: 'POST',
  });
}

// Get presigned upload URL
export async function getPresignedUploadUrl(data: {
  workspaceAppId?: string;
  workspaceId?: string;
  filename: string;
  contentType: string;
  fileSize: number;
  parentId?: string;
  tags?: string[];
  createDocument?: boolean;
}): Promise<PresignedUploadResponse> {
  return api('/files/upload-url', {
    method: 'POST',
    body: JSON.stringify({
      workspace_app_id: data.workspaceAppId,
      workspace_id: data.workspaceId,
      filename: data.filename,
      content_type: data.contentType,
      file_size: data.fileSize,
      parent_id: data.parentId,
      tags: data.tags,
      create_document: data.createDocument ?? true,
    }),
  });
}

// Confirm file upload
export async function confirmFileUpload(
  fileId: string,
  data?: {
    workspaceAppId?: string;
    parentId?: string;
    tags?: string[];
    createDocument?: boolean;
  }
): Promise<{ file: FileItem; document?: Document }> {
  const result = await api<{ file: FileItem; document?: Document }>(`/files/${fileId}/confirm`, {
    method: 'POST',
    body: JSON.stringify({
      workspace_app_id: data?.workspaceAppId,
      parent_id: data?.parentId,
      tags: data?.tags,
      create_document: data?.createDocument ?? true,
    }),
  });
  trackEvent('file_uploaded');
  return result;
}

// Delete a file
export async function deleteFile(fileId: string): Promise<void> {
  await api(`/files/${fileId}`, { method: 'DELETE' });
}

// Get file download URL
export async function getFileDownloadUrl(fileId: string): Promise<{ url: string }> {
  return api(`/files/${fileId}/url`);
}


// ============================================================================
// Messages Types & API Functions (Workspace Team Messaging)
// ============================================================================

export interface Channel {
  id: string;
  workspace_app_id: string;
  name: string;
  description?: string;
  is_private: boolean;
  created_by: string;
  created_by_user?: {
    id: string;
    email: string;
    name?: string;
    avatar_url?: string;
  };
  created_at: string;
  updated_at: string;
}

export interface ChannelMember {
  user_id: string;
  role: 'owner' | 'moderator' | 'member';
  joined_at: string;
  name?: string;
  avatar_url?: string;
}

export interface ContentBlock {
  type: 'text' | 'mention' | 'file' | 'link_preview' | 'code' | 'quote' | 'embed' | 'shared_message';
  data: Record<string, unknown>;
}

export interface MessageReaction {
  id: string;
  message_id: string;
  user_id: string;
  emoji: string;
  created_at: string;
}

export interface ChannelMessage {
  id: string;
  channel_id: string;
  user_id: string;
  content: string;
  blocks: ContentBlock[];
  is_edited: boolean;
  edited_at?: string;
  thread_parent_id?: string;
  reply_count: number;
  created_at: string;
  user?: {
    id: string;
    email: string;
    name?: string;
    avatar_url?: string;
  };
  agent_id?: string;
  agent?: {
    id: string;
    name: string;
    avatar_url?: string;
  };
  reactions?: MessageReaction[];
}

// Channel API Functions

export async function getChannels(workspaceAppId: string): Promise<{ channels: Channel[]; count: number }> {
  return api(`/workspaces/apps/${workspaceAppId}/channels`);
}

export async function getChannel(channelId: string): Promise<{ channel: Channel }> {
  return api(`/channels/${channelId}`);
}

export async function createChannel(
  workspaceAppId: string,
  data: { name: string; description?: string; is_private?: boolean }
): Promise<{ channel: Channel }> {
  const result = await api<{ channel: Channel }>(`/workspaces/apps/${workspaceAppId}/channels`, {
    method: 'POST',
    body: JSON.stringify(data),
  });
  trackEvent('channel_created');
  return result;
}

export async function updateChannel(
  channelId: string,
  updates: { name?: string; description?: string }
): Promise<{ channel: Channel }> {
  return api(`/channels/${channelId}`, {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
}

export async function deleteChannel(channelId: string): Promise<void> {
  await api(`/channels/${channelId}`, { method: 'DELETE' });
}

// Channel Members API Functions

export async function getChannelMembers(channelId: string): Promise<{ members: ChannelMember[]; count: number }> {
  return api(`/channels/${channelId}/members`);
}

export async function addChannelMember(
  channelId: string,
  userId: string,
  role: string = 'member'
): Promise<{ member: ChannelMember }> {
  return api(`/channels/${channelId}/members`, {
    method: 'POST',
    body: JSON.stringify({ user_id: userId, role }),
  });
}

export async function removeChannelMember(channelId: string, userId: string): Promise<void> {
  await api(`/channels/${channelId}/members/${userId}`, { method: 'DELETE' });
}

// Messages API Functions

export async function getChannelMessages(
  channelId: string,
  options?: { limit?: number; offset?: number; beforeId?: string }
): Promise<{ messages: ChannelMessage[]; count: number; has_more?: boolean }> {
  const params = new URLSearchParams();
  if (options?.limit) params.append('limit', String(options.limit));
  if (options?.offset) params.append('offset', String(options.offset));
  if (options?.beforeId) params.append('before_id', options.beforeId);

  const queryString = params.toString();
  return api(`/channels/${channelId}/messages${queryString ? `?${queryString}` : ''}`);
}

export async function getChannelMessage(messageId: string): Promise<{ message: ChannelMessage }> {
  return api(`/messages/${messageId}`);
}

export async function sendChannelMessage(
  channelId: string,
  blocks: ContentBlock[],
  threadParentId?: string
): Promise<{ message: ChannelMessage }> {
  const result = await api<{ message: ChannelMessage }>(`/channels/${channelId}/messages`, {
    method: 'POST',
    body: JSON.stringify({
      blocks,
      thread_parent_id: threadParentId,
    }),
  });
  trackEvent('message_sent', { is_thread_reply: !!threadParentId });
  return result;
}

export async function updateChannelMessage(
  messageId: string,
  blocks: ContentBlock[]
): Promise<{ message: ChannelMessage }> {
  return api(`/messages/${messageId}`, {
    method: 'PATCH',
    body: JSON.stringify({ blocks }),
  });
}

export async function deleteChannelMessage(messageId: string): Promise<void> {
  await api(`/messages/${messageId}`, { method: 'DELETE' });
}

// Thread Replies

export async function getThreadReplies(
  messageId: string,
  options?: { limit?: number; offset?: number }
): Promise<{ replies: ChannelMessage[]; count: number }> {
  const params = new URLSearchParams();
  if (options?.limit) params.append('limit', String(options.limit));
  if (options?.offset) params.append('offset', String(options.offset));

  const queryString = params.toString();
  return api(`/messages/${messageId}/replies${queryString ? `?${queryString}` : ''}`);
}

// Reactions

export async function addMessageReaction(messageId: string, emoji: string): Promise<{ reaction: MessageReaction }> {
  const result = await api<{ reaction: MessageReaction }>(`/messages/${messageId}/reactions`, {
    method: 'POST',
    body: JSON.stringify({ emoji }),
  });
  trackEvent('reaction_added');
  return result;
}

export async function removeMessageReaction(messageId: string, emoji: string): Promise<void> {
  await api(`/messages/${messageId}/reactions/${encodeURIComponent(emoji)}`, { method: 'DELETE' });
}

// Direct Messages (DMs) API Functions

export interface DMChannel {
  id: string;
  workspace_app_id: string;
  is_dm: boolean;
  dm_participants: string[];
  participants?: {
    id: string;
    email: string;
    name?: string;
    avatar_url?: string;
  }[];
  created_at: string;
  updated_at: string;
}

export async function getUserDMs(workspaceAppId: string): Promise<{ dms: DMChannel[]; count: number }> {
  return api(`/workspaces/apps/${workspaceAppId}/dms`);
}

export async function getOrCreateDM(
  workspaceAppId: string,
  participantIds: string[]
): Promise<{ dm: DMChannel }> {
  return api(`/workspaces/apps/${workspaceAppId}/dms`, {
    method: 'POST',
    body: JSON.stringify({ participant_ids: participantIds }),
  });
}

// Unread Indicators API Functions

export async function getUnreadCounts(workspaceAppId: string): Promise<{ unread_counts: Record<string, number> }> {
  return api(`/workspaces/apps/${workspaceAppId}/unread-counts`);
}

export async function markChannelRead(channelId: string): Promise<{ success: boolean }> {
  return api(`/channels/${channelId}/read`, { method: 'POST' });
}

// User Profile API Functions
// UserProfile type is imported from authStore to avoid circular dependency
export type { UserProfile };

export interface AvatarUploadInitiateResponse {
  upload_url: string;
  r2_key: string;
  public_url: string;
  expires_at: string;
}

export async function getCurrentUserProfile(): Promise<UserProfile> {
  return api('/users/me');
}

export async function initiateAvatarUpload(
  filename: string,
  contentType: string,
  fileSize: number
): Promise<AvatarUploadInitiateResponse> {
  return api('/users/avatar/initiate', {
    method: 'POST',
    body: JSON.stringify({
      filename,
      content_type: contentType,
      file_size: fileSize,
    }),
  });
}

export async function confirmAvatarUpload(r2Key: string): Promise<{ avatar_url: string }> {
  return api('/users/avatar/confirm', {
    method: 'POST',
    body: JSON.stringify({ r2_key: r2Key }),
  });
}

export async function deleteAvatar(): Promise<{ avatar_url: string }> {
  return api('/users/avatar', { method: 'DELETE' });
}

/**
 * Update current user's profile (name, onboarding status)
 */
export async function updateUserProfile(
  updates: { name?: string; onboarding_completed_at?: string }
): Promise<UserProfile> {
  return api<UserProfile>('/users/me', {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
}

/**
 * Upload avatar image - handles the full presigned URL flow
 */
export async function uploadAvatar(file: File): Promise<string> {
  // 1. Get presigned URL
  console.log('1. Getting presigned URL...');
  const { upload_url, r2_key, public_url } = await initiateAvatarUpload(
    file.name,
    file.type,
    file.size
  );
  console.log('2. Got presigned URL:', { r2_key, public_url });

  // 2. Upload directly to R2
  console.log('3. Uploading to R2...');
  const uploadResponse = await fetch(upload_url, {
    method: 'PUT',
    body: file,
    headers: {
      'Content-Type': file.type,
    },
  });

  if (!uploadResponse.ok) {
    const errorText = await uploadResponse.text();
    console.error('R2 upload failed:', uploadResponse.status, errorText);
    throw new Error(`Failed to upload avatar to storage: ${uploadResponse.status}`);
  }
  console.log('4. R2 upload complete');

  // 3. Confirm upload
  console.log('5. Confirming upload...');
  const { avatar_url } = await confirmAvatarUpload(r2_key);
  console.log('6. Upload confirmed:', avatar_url);

  return avatar_url;
}

// ============================================================================
// Projects Types & API Functions (Kanban Boards)
// ============================================================================

export interface ProjectBoard {
  id: string;
  workspace_app_id: string;
  workspace_id: string;
  name: string;
  description?: string;
  key?: string;
  icon?: string;
  color?: string;
  position: number;
  next_issue_number: number;
  created_by?: string;
  created_at?: string;
  updated_at?: string;
}

export interface ProjectState {
  id: string;
  workspace_app_id?: string;
  workspace_id?: string;
  board_id: string;
  name: string;
  color?: string;
  position: number;
  is_done: boolean;
  created_at?: string;
  updated_at?: string;
}

export interface ProjectLabel {
  id: string;
  workspace_app_id?: string;
  workspace_id?: string;
  board_id: string;
  name: string;
  color: string;
  created_by?: string;
  created_at?: string;
  updated_at?: string;
}

export interface ProjectIssueAssignee {
  id: string;
  workspace_app_id?: string;
  workspace_id?: string;
  issue_id: string;
  user_id: string;
  created_at?: string;
}

export interface ProjectIssue {
  id: string;
  workspace_app_id?: string;
  workspace_id?: string;
  board_id: string;
  state_id: string;
  number: number;
  title: string;
  description?: string;
  priority: number; // 0=none, 1=urgent, 2=high, 3=medium, 4=low
  due_at?: string;
  image_r2_keys?: string[]; // R2 keys stored in DB
  image_urls?: string[]; // Generated signed proxy URLs
  label_objects?: ProjectLabel[];
  assignees?: ProjectIssueAssignee[];
  position: number;
  created_by?: string;
  completed_at?: string;
  created_at?: string;
  updated_at?: string;
}

export interface ItemPosition {
  id: string;
  position: number;
}

// --- Issue Comments ---

export interface IssueCommentUser {
  id: string;
  email?: string;
  name?: string;
  avatar_url?: string;
}

export interface CommentReaction {
  id: string;
  comment_id: string;
  user_id: string;
  emoji: string;
  created_at?: string;
}

export interface IssueComment {
  id: string;
  workspace_app_id?: string;
  workspace_id?: string;
  issue_id: string;
  user_id: string;
  content?: string;
  blocks: ContentBlock[];
  is_edited: boolean;
  edited_at?: string;
  created_at?: string;
  user?: IssueCommentUser;
  reactions?: CommentReaction[];
}

// --- Boards ---

export async function getProjectBoards(workspaceAppId: string): Promise<{ boards: ProjectBoard[]; count: number }> {
  return api(`/projects/boards?workspace_app_id=${encodeURIComponent(workspaceAppId)}`);
}

export async function getProjectBoard(boardId: string): Promise<ProjectBoard> {
  return api(`/projects/boards/${boardId}`);
}

export async function createProjectBoard(data: {
  workspace_app_id: string;
  name: string;
  description?: string;
  icon?: string;
  color?: string;
  key?: string;
}): Promise<{ board: ProjectBoard; states: ProjectState[] }> {
  return api('/projects/boards', {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function updateProjectBoard(boardId: string, updates: {
  name?: string;
  description?: string;
  icon?: string;
  color?: string;
  key?: string;
}): Promise<ProjectBoard> {
  return api(`/projects/boards/${boardId}`, {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
}

export async function deleteProjectBoard(boardId: string): Promise<{ status: string }> {
  return api(`/projects/boards/${boardId}`, { method: 'DELETE' });
}

// --- States ---

export async function getProjectStates(boardId: string): Promise<{ states: ProjectState[]; count: number }> {
  return api(`/projects/boards/${boardId}/states`);
}

export async function createProjectState(boardId: string, data: {
  name: string;
  color?: string;
  is_done?: boolean;
}): Promise<ProjectState> {
  return api(`/projects/boards/${boardId}/states`, {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function updateProjectState(stateId: string, updates: {
  name?: string;
  color?: string;
  is_done?: boolean;
}): Promise<ProjectState> {
  const response = await api<ProjectState | { state: ProjectState }>(`/projects/states/${stateId}`, {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
  // Handle both wrapped and unwrapped response formats
  return 'state' in response ? response.state : response;
}

export async function deleteProjectState(stateId: string): Promise<{ status: string }> {
  return api(`/projects/states/${stateId}`, { method: 'DELETE' });
}

export async function reorderProjectStates(boardId: string, items: ItemPosition[]): Promise<{ updated_count: number }> {
  return api(`/projects/boards/${boardId}/states/reorder`, {
    method: 'POST',
    body: JSON.stringify({ items }),
  });
}

// --- Issues ---

export async function getProjectIssues(boardId: string, options?: {
  stateId?: string;
  assigneeId?: string;
  includeDone?: boolean;
}): Promise<{ issues: ProjectIssue[]; count: number }> {
  const params = new URLSearchParams();
  if (options?.stateId) params.append('state_id', options.stateId);
  if (options?.assigneeId) params.append('assignee_user_id', options.assigneeId);
  if (options?.includeDone !== undefined) params.append('include_done', String(options.includeDone));
  const queryString = params.toString();
  return api(`/projects/boards/${boardId}/issues${queryString ? `?${queryString}` : ''}`);
}

export async function getProjectIssue(issueId: string): Promise<ProjectIssue> {
  return api(`/projects/issues/${issueId}`);
}

export async function createProjectIssue(data: {
  board_id: string;
  state_id: string;
  title: string;
  description?: string;
  priority?: number;
  due_at?: string;
  label_ids?: string[];
  assignee_ids?: string[];
}): Promise<ProjectIssue> {
  const result = await api<ProjectIssue>('/projects/issues', {
    method: 'POST',
    body: JSON.stringify(data),
  });
  trackEvent('project_issue_created');
  return result;
}

export async function updateProjectIssue(issueId: string, updates: {
  title?: string;
  description?: string;
  priority?: number;
  due_at?: string;
  clear_due_at?: boolean;
  // Image operations (mutually exclusive)
  add_image_r2_keys?: string[]; // Append images
  remove_image_r2_keys?: string[]; // Remove specific images
  image_r2_keys?: string[]; // Replace all images
  clear_images?: boolean; // Clear all images
  state_id?: string;
  position?: number;
  label_ids?: string[];
  assignee_ids?: string[];
}): Promise<ProjectIssue> {
  return api(`/projects/issues/${issueId}`, {
    method: 'PATCH',
    body: JSON.stringify(updates),
  });
}

export async function moveProjectIssue(issueId: string, targetStateId: string, position: number): Promise<ProjectIssue> {
  const result = await api<ProjectIssue>(`/projects/issues/${issueId}/move`, {
    method: 'POST',
    body: JSON.stringify({ target_state_id: targetStateId, position }),
  });
  trackEvent('project_issue_moved');
  return result;
}

export async function reorderProjectIssues(stateId: string, items: ItemPosition[]): Promise<{ updated_count: number }> {
  return api(`/projects/states/${stateId}/issues/reorder`, {
    method: 'POST',
    body: JSON.stringify({ items }),
  });
}

export async function deleteProjectIssue(issueId: string): Promise<{ status: string }> {
  return api(`/projects/issues/${issueId}`, { method: 'DELETE' });
}

// --- Labels ---

export async function getProjectLabels(boardId: string): Promise<{ labels: ProjectLabel[]; count: number }> {
  return api(`/projects/boards/${boardId}/labels`);
}

export async function createProjectLabel(boardId: string, name: string, color?: string): Promise<ProjectLabel> {
  return api(`/projects/boards/${boardId}/labels`, {
    method: 'POST',
    body: JSON.stringify({ name, color }),
  });
}

export async function addLabelToIssue(issueId: string, labelId: string): Promise<{ id: string; issue_id: string; label_id: string }> {
  return api(`/projects/issues/${issueId}/labels/${labelId}`, { method: 'POST' });
}

export async function removeLabelFromIssue(issueId: string, labelId: string): Promise<{ status: string }> {
  return api(`/projects/issues/${issueId}/labels/${labelId}`, { method: 'DELETE' });
}

// --- Assignees ---

export async function addIssueAssignee(issueId: string, userId: string): Promise<ProjectIssueAssignee> {
  return api(`/projects/issues/${issueId}/assignees`, {
    method: 'POST',
    body: JSON.stringify({ user_id: userId }),
  });
}

export async function removeIssueAssignee(issueId: string, userId: string): Promise<{ status: string }> {
  return api(`/projects/issues/${issueId}/assignees/${userId}`, { method: 'DELETE' });
}

// --- Issue Comments ---

export async function getIssueComments(
  issueId: string,
  options?: { limit?: number; offset?: number }
): Promise<{ comments: IssueComment[]; count: number; total_count: number }> {
  const params = new URLSearchParams();
  if (options?.limit) params.set('limit', String(options.limit));
  if (options?.offset) params.set('offset', String(options.offset));
  const qs = params.toString();
  return api(`/projects/issues/${issueId}/comments${qs ? `?${qs}` : ''}`);
}

export async function createIssueComment(
  issueId: string,
  blocks: ContentBlock[]
): Promise<IssueComment> {
  const result = await api<IssueComment>(`/projects/issues/${issueId}/comments`, {
    method: 'POST',
    body: JSON.stringify({ blocks }),
  });
  trackEvent('project_comment_created');
  return result;
}

export async function updateIssueComment(
  commentId: string,
  blocks: ContentBlock[]
): Promise<IssueComment> {
  return api(`/projects/comments/${commentId}`, {
    method: 'PATCH',
    body: JSON.stringify({ blocks }),
  });
}

export async function deleteIssueComment(commentId: string): Promise<{ status: string }> {
  return api(`/projects/comments/${commentId}`, { method: 'DELETE' });
}

export async function addCommentReaction(
  commentId: string,
  emoji: string
): Promise<CommentReaction> {
  return api(`/projects/comments/${commentId}/reactions`, {
    method: 'POST',
    body: JSON.stringify({ emoji }),
  });
}

export async function removeCommentReaction(
  commentId: string,
  emoji: string
): Promise<{ status: string }> {
  return api(`/projects/comments/${commentId}/reactions/${encodeURIComponent(emoji)}`, {
    method: 'DELETE',
  });
}

// ============================================================================
// Agent Types & API Functions
// ============================================================================

export interface AgentTemplate {
  id: string;
  slug: string;
  name: string;
  description?: string;
  category: string;
  icon_url?: string;
  default_system_prompt: string;
  default_enabled_tools: string[];
  default_config: Record<string, unknown>;
}

export interface AgentInstance {
  id: string;
  workspace_id: string;
  name: string;
  avatar_url?: string;
  status: 'idle' | 'working' | 'error';
  system_prompt: string;
  enabled_tools: string[];
  config: Record<string, unknown>;
  created_by: string;
  created_at: string;
  updated_at: string;
  template_id?: string;
  sandbox_id?: string;
  sandbox_status?: 'off' | 'starting' | 'running' | 'paused' | 'idle' | 'error';
  sandbox_created_at?: string;
  last_active_at?: string;
}

export interface AgentTask {
  id: string;
  agent_id: string;
  workspace_id: string;
  trigger: string;
  status: 'queued' | 'running' | 'completed' | 'failed';
  input: Record<string, unknown>;
  output?: Record<string, unknown>;
  steps: unknown[];
  token_usage: number;
  error?: string;
  started_at?: string;
  completed_at?: string;
  created_at: string;
  sandbox_id?: string;
  model?: string;
  conversation_id?: string;
}

export interface AgentConversation {
  id: string;
  agent_id: string;
  workspace_id: string;
  title: string;
  created_by: string;
  created_at: string;
  updated_at: string;
}

export interface AgentTaskStep {
  id: string;
  task_id: string;
  agent_id: string;
  turn: number;
  step_type: 'thinking' | 'tool_call' | 'tool_result' | 'message' | 'error' | 'log';
  tool_name?: string;
  tool_args?: Record<string, unknown>;
  tool_result?: Record<string, unknown>;
  content?: string;
  token_usage: number;
  duration_ms?: number;
  created_at: string;
}

// Template endpoints
export async function getAgentTemplates(category?: string): Promise<{ templates: AgentTemplate[]; count: number }> {
  const params = category ? `?category=${category}` : '';
  return api(`/agent-templates${params}`);
}

export async function getAgentTemplate(slug: string): Promise<AgentTemplate> {
  return api(`/agent-templates/${slug}`);
}

// Agent endpoints
export async function getWorkspaceAgents(workspaceId: string): Promise<{ agents: AgentInstance[]; count: number }> {
  return api(`/workspaces/${workspaceId}/agents`);
}

export async function getAgent(agentId: string): Promise<AgentInstance> {
  return api(`/agents/${agentId}`);
}

export async function createAgent(workspaceId: string, data: {
  name: string;
  template_slug?: string;
  system_prompt?: string;
  enabled_tools?: string[];
  config?: Record<string, unknown>;
  role?: string;
  backstory?: string;
  objective?: string;
  personality?: string;
}): Promise<AgentInstance> {
  return api(`/workspaces/${workspaceId}/agents`, {
    method: 'POST',
    body: JSON.stringify(data),
  });
}

export async function uploadAgentAvatar(agentId: string, file: File): Promise<AgentInstance> {
  const token = useAuthStore.getState().getAccessToken();
  const formData = new FormData();
  formData.append('file', file);
  const response = await fetch(`${API_BASE}/agents/${agentId}/avatar`, {
    method: 'POST',
    body: formData,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
  });
  if (!response.ok) {
    throw new Error(`Upload failed: ${response.status}`);
  }
  return response.json();
}

export async function updateAgent(agentId: string, data: Partial<Pick<AgentInstance, 'name' | 'system_prompt' | 'enabled_tools' | 'config' | 'avatar_url'>>): Promise<AgentInstance> {
  return api(`/agents/${agentId}`, {
    method: 'PATCH',
    body: JSON.stringify(data),
  });
}

export async function deleteAgent(agentId: string): Promise<{ status: string }> {
  return api(`/agents/${agentId}`, { method: 'DELETE' });
}

export async function pauseAgent(agentId: string): Promise<AgentInstance> {
  return api(`/agents/${agentId}/pause`, { method: 'POST' });
}

export async function resumeAgent(agentId: string): Promise<AgentInstance> {
  return api(`/agents/${agentId}/resume`, { method: 'POST' });
}

export async function invokeAgent(agentId: string, instruction: string, channelId?: string, conversationId?: string): Promise<AgentTask> {
  return api(`/agents/${agentId}/invoke`, {
    method: 'POST',
    body: JSON.stringify({ instruction, channel_id: channelId, conversation_id: conversationId }),
  });
}

export async function getAgentTasks(agentId: string): Promise<{ tasks: AgentTask[]; count: number }> {
  return api(`/agents/${agentId}/tasks`);
}

export async function getTaskSteps(agentId: string, taskId: string): Promise<{ steps: AgentTaskStep[]; count: number }> {
  return api(`/agents/${agentId}/tasks/${taskId}/steps`);
}

// Conversation endpoints
export async function getAgentConversations(agentId: string): Promise<{ conversations: AgentConversation[]; count: number }> {
  return api(`/agents/${agentId}/conversations`);
}

export async function createAgentConversation(agentId: string, title?: string): Promise<AgentConversation> {
  return api(`/agents/${agentId}/conversations`, {
    method: 'POST',
    body: JSON.stringify({ title }),
  });
}

export async function renameAgentConversation(agentId: string, conversationId: string, title: string): Promise<AgentConversation> {
  return api(`/agents/${agentId}/conversations/${conversationId}`, {
    method: 'PATCH',
    body: JSON.stringify({ title }),
  });
}

export async function deleteAgentConversation(agentId: string, conversationId: string): Promise<{ status: string }> {
  return api(`/agents/${agentId}/conversations/${conversationId}`, { method: 'DELETE' });
}

export async function getConversationTasks(agentId: string, conversationId: string): Promise<{ tasks: AgentTask[]; count: number }> {
  return api(`/agents/${agentId}/conversations/${conversationId}/tasks`);
}

// --- Sandbox File Browsing ---

export interface SandboxFile {
  name: string;
  type: "file" | "dir";
  size: number;
}

export async function listSandboxFiles(agentId: string, path: string = "/home/user"): Promise<SandboxFile[]> {
  const result = await api<{ files: SandboxFile[] }>(`/agents/${agentId}/sandbox/files?path=${encodeURIComponent(path)}`);
  return result.files;
}

export async function readSandboxFile(agentId: string, path: string): Promise<string> {
  const result = await api<{ content: string }>(`/agents/${agentId}/sandbox/files/read?path=${encodeURIComponent(path)}`);
  return result.content;
}

// ============================================================================
// AI App Builder
// ============================================================================

export interface BuilderProject {
  id: string;
  user_id: string;
  name: string;
  description: string;
  slug: string;
  platform: 'web' | 'ios' | 'react_native';
  status: 'draft' | 'deployed' | 'archived';
  current_version_id: string | null;
  settings: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface BuilderVersion {
  id: string;
  project_id: string;
  version_number: number;
  conversation_id: string | null;
  file_tree: Record<string, string>;
  prompt: string;
  status: 'generating' | 'ready' | 'error';
  created_at: string;
}

export interface BuilderMessage {
  id: string;
  conversation_id: string;
  role: 'user' | 'assistant';
  content: string;
  content_parts: unknown[];
  version_id: string | null;
  created_at: string;
}

export interface BuilderDeployment {
  id: string;
  project_id: string;
  version_id: string;
  url: string;
  status: 'deploying' | 'live' | 'failed';
  deployed_at: string;
}

export async function getBuilderProjects(): Promise<BuilderProject[]> {
  const result = await api<{ projects: BuilderProject[] }>('/builder/projects');
  return result.projects;
}

export async function createBuilderProject(name: string): Promise<BuilderProject> {
  const result = await api<{ project: BuilderProject }>('/builder/projects', {
    method: 'POST',
    body: JSON.stringify({ name }),
  });
  return result.project;
}

export async function getBuilderProject(id: string): Promise<BuilderProject> {
  const result = await api<{ project: BuilderProject }>(`/builder/projects/${id}`);
  return result.project;
}

export async function updateBuilderProject(id: string, data: Partial<Pick<BuilderProject, 'name' | 'description' | 'slug' | 'settings'>>): Promise<BuilderProject> {
  const result = await api<{ project: BuilderProject }>(`/builder/projects/${id}`, {
    method: 'PATCH',
    body: JSON.stringify(data),
  });
  return result.project;
}

export async function deleteBuilderProject(id: string): Promise<void> {
  await api(`/builder/projects/${id}`, { method: 'DELETE' });
}

export async function getBuilderVersions(projectId: string): Promise<BuilderVersion[]> {
  const result = await api<{ versions: BuilderVersion[] }>(`/builder/projects/${projectId}/versions`);
  return result.versions;
}

export async function getBuilderVersion(projectId: string, versionId: string): Promise<BuilderVersion> {
  const result = await api<{ version: BuilderVersion }>(`/builder/projects/${projectId}/versions/${versionId}`);
  return result.version;
}

export async function getBuilderConversation(projectId: string): Promise<BuilderMessage[]> {
  const result = await api<{ messages: BuilderMessage[] }>(`/builder/projects/${projectId}/conversations`);
  return result.messages;
}

export async function streamBuilderGeneration(projectId: string, message: string): Promise<Response> {
  const token = await ensureFreshToken();
  const response = await fetch(`${API_BASE}/builder/projects/${projectId}/generate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify({ message }),
  });
  if (!response.ok) {
    const errorData = await response.json().catch(() => ({}));
    throw new Error(errorData.detail || `Generation failed: ${response.status}`);
  }
  return response;
}

export async function deployBuilderProject(projectId: string): Promise<BuilderDeployment> {
  const result = await api<{ deployment: BuilderDeployment }>(`/builder/projects/${projectId}/deploy`, {
    method: 'POST',
  });
  return result.deployment;
}

// ============================================================================
// Preferences
// ============================================================================

export async function syncTimezone(): Promise<void> {
  const browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone;
  await api('/preferences', {
    method: 'PATCH',
    body: JSON.stringify({ timezone: browserTimezone }),
  });
}
