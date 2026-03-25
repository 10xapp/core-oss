import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { useAuthStore } from "../../stores/authStore";
import { useWorkspaceStore } from "../../stores/workspaceStore";
import { useMessagesStore } from "../../stores/messagesStore";
import { copyTextToClipboard } from "../../lib/clipboard";
import {
  createWorkspaceInvitation,
  getWorkspaceInvitations,
  getWorkspaceInvitationShareLink,
  getWorkspaceMembers,
  removeWorkspaceMember,
  revokeWorkspaceInvitation,
  updateWorkspaceMemberRole,
  type WorkspaceInvitation,
  type WorkspaceMember,
} from "../../api/client";

type InviteRole = "member" | "admin";

export default function MembersView() {
  const { workspaceId } = useParams<{ workspaceId: string }>();
  const navigate = useNavigate();

  const user = useAuthStore((s) => s.user);
  const workspaces = useWorkspaceStore((s) => s.workspaces);
  const setActiveWorkspace = useWorkspaceStore((s) => s.setActiveWorkspace);

  const [members, setMembers] = useState<WorkspaceMember[]>([]);
  const [pendingInvites, setPendingInvites] = useState<WorkspaceInvitation[]>([]);
  const [loadingMembers, setLoadingMembers] = useState(false);
  const [loadingInvites, setLoadingInvites] = useState(false);
  const [pageError, setPageError] = useState<string | null>(null);
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState<InviteRole>("member");
  const [inviting, setInviting] = useState(false);
  const [actionBusyId, setActionBusyId] = useState<string | null>(null);
  const [feedback, setFeedback] = useState<string | null>(null);
  const feedbackTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  const showFeedback = useCallback((msg: string) => {
    if (feedbackTimer.current) clearTimeout(feedbackTimer.current);
    setFeedback(msg);
    feedbackTimer.current = setTimeout(() => setFeedback(null), 3000);
  }, []);

  useEffect(() => {
    return () => {
      if (feedbackTimer.current) clearTimeout(feedbackTimer.current);
    };
  }, []);

  const workspace = workspaces.find((item) => item.id === workspaceId);

  const currentMemberRole = useMemo(() => {
    if (!user?.id) return null;
    return members.find((member) => member.user_id === user.id)?.role || null;
  }, [members, user?.id]);

  const canManageMembers =
    currentMemberRole === "owner" ||
    currentMemberRole === "admin" ||
    workspace?.role === "owner" ||
    workspace?.role === "admin";

  const loadMembers = useCallback(async () => {
    if (!workspaceId) return;
    setLoadingMembers(true);
    setPageError(null);
    try {
      const data = await getWorkspaceMembers(workspaceId);
      setMembers(data);
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to load workspace members");
      setMembers([]);
    } finally {
      setLoadingMembers(false);
    }
  }, [workspaceId]);

  const loadPendingInvites = useCallback(async () => {
    if (!workspaceId) return;
    setLoadingInvites(true);
    try {
      const invitations = await getWorkspaceInvitations(workspaceId);
      setPendingInvites(
        invitations.filter((invitation) => invitation.status === "pending"),
      );
    } catch (err) {
      setPendingInvites([]);
      setPageError(err instanceof Error ? err.message : "Failed to load invitations");
    } finally {
      setLoadingInvites(false);
    }
  }, [workspaceId]);

  useEffect(() => {
    if (!workspaceId) return;
    setActiveWorkspace(workspaceId);
    void loadMembers();
  }, [workspaceId, setActiveWorkspace, loadMembers]);

  useEffect(() => {
    if (!workspaceId || !canManageMembers) {
      setPendingInvites([]);
      return;
    }
    void loadPendingInvites();
  }, [workspaceId, canManageMembers, loadPendingInvites]);

  const handleInvite = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!workspaceId || !inviteEmail.trim()) return;

    setInviting(true);
    setFeedback(null);
    setPageError(null);
    try {
      const targetEmail = inviteEmail.trim();
      await createWorkspaceInvitation(workspaceId, targetEmail, inviteRole);
      setInviteEmail("");
      setInviteRole("member");
      await loadPendingInvites();
      showFeedback(`Invitation sent to ${targetEmail} as ${inviteRole}`);
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to send invitation");
    } finally {
      setInviting(false);
    }
  };

  const handleResend = async (invitation: WorkspaceInvitation) => {
    if (!workspaceId) return;
    setActionBusyId(invitation.id);
    setFeedback(null);
    setPageError(null);
    try {
      await createWorkspaceInvitation(workspaceId, invitation.email, invitation.role);
      await loadPendingInvites();
      showFeedback(`Invitation resent to ${invitation.email}`);
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to resend invitation");
    } finally {
      setActionBusyId(null);
    }
  };

  const handleCopyInviteLink = async (invitationId: string) => {
    setActionBusyId(invitationId);
    setFeedback(null);
    setPageError(null);
    try {
      const result = await getWorkspaceInvitationShareLink(invitationId);
      await copyTextToClipboard(result.invite_url);
      showFeedback("Invite link copied");
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to copy invite link");
    } finally {
      setActionBusyId(null);
    }
  };

  const handleRevoke = async (invitationId: string) => {
    setActionBusyId(invitationId);
    setFeedback(null);
    setPageError(null);
    try {
      await revokeWorkspaceInvitation(invitationId);
      await loadPendingInvites();
      showFeedback("Invitation revoked");
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to revoke invitation");
    } finally {
      setActionBusyId(null);
    }
  };

  const handleRoleChange = async (member: WorkspaceMember, newRole: 'member' | 'admin') => {
    if (!workspaceId || member.role === newRole) return;
    setActionBusyId(member.user_id);
    setFeedback(null);
    setPageError(null);
    try {
      await updateWorkspaceMemberRole(workspaceId, member.user_id, newRole);
      await loadMembers();
      showFeedback(`${member.email || member.name || "Member"} is now ${newRole}`);
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to update role");
    } finally {
      setActionBusyId(null);
    }
  };

  const handleRemoveMember = async (member: WorkspaceMember) => {
    if (!workspaceId) return;
    setActionBusyId(member.user_id);
    setFeedback(null);
    setPageError(null);
    try {
      await removeWorkspaceMember(workspaceId, member.user_id);
      await loadMembers();
      // Refresh DMs — backend cleans up empty DM channels with removed user
      useMessagesStore.getState().fetchDMs();
      showFeedback("Member removed");
    } catch (err) {
      setPageError(err instanceof Error ? err.message : "Failed to remove member");
    } finally {
      setActionBusyId(null);
    }
  };

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      <div className="flex-1 flex min-w-0 overflow-hidden bg-white relative">
        <div className="flex-1 overflow-y-auto p-6">
          <div className="flex items-center justify-between mb-5">
            <div>
              <h2 className="text-lg font-semibold text-text-body">
                {workspace?.name || "Workspace"}
              </h2>
              <p className="text-sm text-text-secondary">
                Manage members and invitation links.
              </p>
            </div>
            <button
              onClick={() => navigate(-1)}
              className="px-3 py-2 text-sm rounded border border-border-gray hover:bg-bg-gray"
            >
              Back
            </button>
          </div>

          {pageError && (
            <p className="text-sm text-red-500 mb-4">{pageError}</p>
          )}
          {feedback && (
            <p className="text-sm text-green-600 mb-4">{feedback}</p>
          )}

          {canManageMembers ? (
            <div className="mb-6">
              <h3 className="text-sm font-medium text-text-body mb-2">Invite member</h3>
              <form onSubmit={handleInvite} className="flex items-center gap-2">
                <input
                  type="email"
                  value={inviteEmail}
                  onChange={(e) => setInviteEmail(e.target.value)}
                  placeholder="Email address"
                  className="flex-1 border border-border-gray rounded px-3 py-2 text-sm outline-none focus:border-text-tertiary"
                />
                <select
                  value={inviteRole}
                  onChange={(e) => setInviteRole(e.target.value as InviteRole)}
                  className="border border-border-gray rounded px-2 py-2 text-sm"
                  aria-label="Invite role"
                >
                  <option value="member">Member</option>
                  <option value="admin">Admin</option>
                </select>
                <button
                  type="submit"
                  disabled={inviting || !inviteEmail.trim()}
                  className="px-3 py-2 text-sm rounded bg-black text-white disabled:opacity-60"
                >
                  {inviting ? "Sending..." : "Invite"}
                </button>
              </form>
            </div>
          ) : (
            <p className="text-sm text-text-secondary mb-6">
              You can view members, but only admins and owners can manage invitations.
            </p>
          )}

          <div className="mb-7">
            <h3 className="text-sm font-medium text-text-body mb-2">Pending invites</h3>
            {loadingInvites ? (
              <p className="text-sm text-text-secondary">Loading...</p>
            ) : pendingInvites.length === 0 ? (
              <p className="text-sm text-text-secondary">No pending invites</p>
            ) : (
              <div className="space-y-2">
                {pendingInvites.map((invitation) => (
                  <div
                    key={invitation.id}
                    className="flex items-center justify-between border border-border-gray rounded px-3 py-2"
                  >
                    <div className="min-w-0">
                      <p className="text-sm text-text-body truncate">{invitation.email}</p>
                      <p className="text-xs text-text-secondary capitalize">
                        {invitation.role} • pending
                      </p>
                    </div>
                    {canManageMembers && (
                      <div className="flex items-center gap-1.5">
                        <button
                          onClick={() => void handleCopyInviteLink(invitation.id)}
                          disabled={actionBusyId === invitation.id}
                          className="text-xs px-2 py-1 rounded bg-bg-gray hover:bg-bg-gray/80 disabled:opacity-60"
                        >
                          Copy link
                        </button>
                        <button
                          onClick={() => void handleResend(invitation)}
                          disabled={actionBusyId === invitation.id}
                          className="text-xs px-2 py-1 rounded bg-bg-gray hover:bg-bg-gray/80 disabled:opacity-60"
                        >
                          Resend
                        </button>
                        <button
                          onClick={() => void handleRevoke(invitation.id)}
                          disabled={actionBusyId === invitation.id}
                          className="text-xs px-2 py-1 rounded text-red-500 hover:bg-red-50 disabled:opacity-60"
                        >
                          Revoke
                        </button>
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>

          <div>
            <h3 className="text-sm font-medium text-text-body mb-2">Members</h3>
            {loadingMembers ? (
              <p className="text-sm text-text-secondary">Loading...</p>
            ) : members.length === 0 ? (
              <p className="text-sm text-text-secondary">No members found</p>
            ) : (
              <div className="space-y-2">
                {members.map((member) => {
                  const isSelf = member.user_id === user?.id;
                  const isOwner = member.role === "owner";
                  const canRemove = canManageMembers && !isOwner && !isSelf;

                  // Role change permissions:
                  // Owner can promote/demote anyone (except self and other owners)
                  // Admin can promote members to admin (but not demote other admins)
                  // Member can't change roles
                  const canChangeRole =
                    !isSelf &&
                    !isOwner &&
                    (currentMemberRole === "owner" ||
                      (currentMemberRole === "admin" && member.role === "member"));

                  return (
                    <div
                      key={member.id}
                      className="flex items-center justify-between border border-border-gray rounded px-3 py-2"
                    >
                      <div className="min-w-0">
                        <p className="text-sm text-text-body truncate">
                          {member.email || member.name || member.user_id}
                          {isSelf && <span className="text-text-tertiary ml-1">(you)</span>}
                        </p>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        {canChangeRole ? (
                          <select
                            value={member.role}
                            onChange={(e) => void handleRoleChange(member, e.target.value as 'member' | 'admin')}
                            disabled={actionBusyId === member.user_id}
                            className="text-xs border border-border-gray rounded px-1.5 py-1 bg-white disabled:opacity-60"
                          >
                            <option value="member">Member</option>
                            <option value="admin">Admin</option>
                          </select>
                        ) : (
                          <span className="text-xs text-text-secondary capitalize px-1.5 py-1">
                            {member.role}
                          </span>
                        )}
                        {canRemove && (
                          <button
                            onClick={() => void handleRemoveMember(member)}
                            disabled={actionBusyId === member.user_id}
                            className="text-xs px-2 py-1 rounded text-red-500 hover:bg-red-50 disabled:opacity-60"
                          >
                            Remove
                          </button>
                        )}
                      </div>
                    </div>
                  );
                })}
              </div>
            )}
          </div>
        </div>

      </div>
    </div>
  );
}
