import { useEffect, useMemo, useState } from 'react';
import { toast } from 'sonner';
import Modal from '../Modal/Modal';
import { usePermissionStore } from '../../stores/permissionStore';
import PermissionSelect from './PermissionSelect';
import ShareUserRow from './ShareUserRow';
import UserSearchInput from './UserSearchInput';
import PendingRequestRow from './PendingRequestRow';
import LinkSlugEditor from './LinkSlugEditor';
import type { UserSearchResult } from '../../api/client';

export default function ShareModal() {
  const {
    shareModalOpen,
    shareModalResource,
    resourceShares,
    resourceMembers,
    resourceLinks,
    isLoadingShares,
    isLoadingLinks,
    pendingRequests,
    isLoadingPendingRequests,
    pendingRequestsError,
    error,
    linksError,
    closeShareModal,
    fetchResourceShares,
    fetchResourceLinks,
    fetchPendingRequests,
    shareWithUser,
    revokePermission,
    updatePermission,
    createLink,
    revokeLink,
    updateLinkPermission,
    updateLinkSlug,
    approveRequest,
    denyRequest,
  } = usePermissionStore();

  const [email, setEmail] = useState('');
  const [permission, setPermission] = useState<'read' | 'write' | 'admin'>('read');
  const [linkPermission, setLinkPermission] = useState<'read' | 'write' | 'admin'>('read');
  const [linkSlug, setLinkSlug] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isCreatingLink, setIsCreatingLink] = useState(false);
  const [selectedUser, setSelectedUser] = useState<UserSearchResult | null>(null);

  useEffect(() => {
    if (shareModalOpen && shareModalResource) {
      fetchResourceShares(shareModalResource.type, shareModalResource.id);
      fetchResourceLinks(shareModalResource.type, shareModalResource.id);
      fetchPendingRequests();
    }
  }, [shareModalOpen, shareModalResource, fetchResourceShares, fetchResourceLinks, fetchPendingRequests]);

  useEffect(() => {
    if (!shareModalOpen) {
      setEmail('');
      setPermission('read');
      setLinkPermission('read');
      setLinkSlug('');
      setIsSubmitting(false);
      setIsCreatingLink(false);
      setSelectedUser(null);
    }
  }, [shareModalOpen]);

  const title = shareModalResource?.title || 'Share';

  const hasShares = resourceShares.length > 0;
  const membersSorted = useMemo(() => {
    return [...resourceMembers].sort((a, b) => {
      const roleOrder = { owner: 0, admin: 1, member: 2 } as Record<string, number>;
      const aOrder = roleOrder[a.role] ?? 3;
      const bOrder = roleOrder[b.role] ?? 3;
      if (aOrder !== bOrder) return aOrder - bOrder;
      return (a.name || a.email || '').localeCompare(b.name || b.email || '');
    });
  }, [resourceMembers]);

  const pendingForResource = useMemo(() => {
    if (!shareModalResource) return [];
    return pendingRequests.filter((request) => (
      request.resource_id === shareModalResource.id
      && request.resource_type === shareModalResource.type
    ));
  }, [pendingRequests, shareModalResource]);

  const handleShare = async () => {
    if (!shareModalResource || !email.trim()) return;
    setIsSubmitting(true);
    const result = await shareWithUser(
      shareModalResource.type,
      shareModalResource.id,
      email.trim(),
      permission,
      selectedUser,
    );
    setIsSubmitting(false);
    if (result) {
      const label = selectedUser?.name || selectedUser?.email || email.trim();
      toast.success(`Shared with ${label}`);
      setEmail('');
      setSelectedUser(null);
    }
  };

  const handleCreateLink = async () => {
    if (!shareModalResource) return;
    setIsCreatingLink(true);
    const link = await createLink(
      shareModalResource.type,
      shareModalResource.id,
      linkPermission,
      linkSlug || null,
    );
    setIsCreatingLink(false);
    if (link?.url) {
      try {
        await navigator.clipboard.writeText(link.url);
        toast.success('Link copied to clipboard');
      } catch {
        toast.success('Link created');
      }
      setLinkSlug('');
    }
  };

  const handleCopyLink = async (url: string) => {
    try {
      await navigator.clipboard.writeText(url);
      toast.success('Link copied to clipboard');
    } catch {
      toast.error('Failed to copy link');
    }
  };

  const handleApproveRequest = async (requestId: string, permissionValue: 'read' | 'write' | 'admin') => {
    if (!shareModalResource) return;
    const updated = await approveRequest(requestId, permissionValue);
    if (updated) {
      toast.success('Access request approved');
      await fetchResourceShares(shareModalResource.type, shareModalResource.id);
    }
  };

  const handleDenyRequest = async (requestId: string) => {
    const updated = await denyRequest(requestId);
    if (updated) {
      toast.success('Access request denied');
    }
  };

  return (
    <Modal
      isOpen={shareModalOpen}
      onClose={closeShareModal}
      title={`Share "${title}"`}
      size="md"
    >
      <div className="space-y-4">
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <div className="min-w-0 flex-1">
              <UserSearchInput
                value={email}
                onValueChange={(nextValue) => {
                  setEmail(nextValue);
                  setSelectedUser(null);
                }}
                onSelect={(user) => {
                  setEmail(user.email);
                  setSelectedUser(user);
                }}
              />
            </div>
            <PermissionSelect value={permission} onChange={setPermission} />
            <button
              onClick={handleShare}
              disabled={!email.trim() || isSubmitting}
              className="px-4 py-2 text-sm bg-black text-white rounded-md disabled:opacity-50 shrink-0"
            >
              {isSubmitting ? 'Sharing...' : 'Share'}
            </button>
          </div>
          {error && <p className="text-xs text-red-500">{error}</p>}
        </div>

        <div className="border-t border-border-gray pt-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary">People with access</h3>
            {isLoadingShares && <span className="text-xs text-text-secondary">Loading...</span>}
          </div>

          {!isLoadingShares && !hasShares && (
            <p className="text-xs text-text-secondary">No one else has access yet.</p>
          )}

          {!isLoadingShares && hasShares && (
            <div className="divide-y divide-border-gray">
              {resourceShares.map((share) => (
                <ShareUserRow
                  key={share.id}
                  permission={share}
                  onChangePermission={(permissionId, newPermission) =>
                    updatePermission(permissionId, newPermission)
                  }
                  onRevoke={(permissionId) => revokePermission(permissionId)}
                />
              ))}
            </div>
          )}
        </div>

        {membersSorted.length > 0 && (
          <div className="border-t border-border-gray pt-3">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary mb-2">Workspace access</h3>
            <div className="space-y-2">
              {membersSorted.map((member) => (
                <div key={member.id} className="flex items-center justify-between">
                  <div className="min-w-0">
                    <p className="text-sm text-text-body truncate">{member.name || member.email || 'Member'}</p>
                    {member.email && (
                      <p className="text-xs text-text-secondary truncate">{member.email}</p>
                    )}
                  </div>
                  <span className="text-xs text-text-secondary capitalize">{member.role}</span>
                </div>
              ))}
            </div>
          </div>
        )}

        <div className="border-t border-border-gray pt-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary">Pending requests</h3>
            {isLoadingPendingRequests && <span className="text-xs text-text-secondary">Loading...</span>}
          </div>

          {pendingRequestsError && (
            <p className="text-xs text-red-500 mb-2">{pendingRequestsError}</p>
          )}

          {!isLoadingPendingRequests && !pendingRequestsError && pendingForResource.length === 0 && (
            <p className="text-xs text-text-secondary">No pending requests.</p>
          )}

          {pendingForResource.length > 0 && (
            <div className="divide-y divide-border-gray">
              {pendingForResource.map((request) => (
                <PendingRequestRow
                  key={request.id}
                  request={request}
                  onApprove={handleApproveRequest}
                  onDeny={handleDenyRequest}
                />
              ))}
            </div>
          )}
        </div>

        <div className="border-t border-border-gray pt-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary">Link sharing</h3>
            {isLoadingLinks && <span className="text-xs text-text-secondary">Loading...</span>}
          </div>

          <div className="flex items-center gap-2">
            <input
              value={linkSlug}
              onChange={(event) => setLinkSlug(event.target.value)}
              placeholder="custom-link (optional)"
              className="min-w-0 flex-1 px-2.5 py-1.5 text-xs border border-border-gray rounded-md"
            />
            <PermissionSelect value={linkPermission} onChange={setLinkPermission} size="sm" />
            <button
              onClick={handleCreateLink}
              disabled={isCreatingLink}
              className="px-3 py-1.5 text-xs rounded bg-black text-white disabled:opacity-60"
            >
              {isCreatingLink ? 'Creating...' : 'Create link'}
            </button>
          </div>

          {linksError && (
            <p className="text-xs text-red-500 mt-2">{linksError}</p>
          )}

          {resourceLinks.length > 0 && (
            <div className="mt-3 space-y-2">
              {resourceLinks.map((link) => (
                <div key={link.id} className="flex items-center justify-between gap-2 p-2 border border-border-gray rounded-md">
                  <div className="min-w-0 flex-1">
                    <LinkSlugEditor
                      link={link}
                      onUpdateSlug={(slug) => updateLinkSlug(link.id, slug)}
                    />
                    <p className="text-[11px] text-text-secondary">Anyone with the link</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <PermissionSelect
                      value={link.permission}
                      onChange={(value) => updateLinkPermission(link.id, value)}
                      size="sm"
                    />
                    <button
                      onClick={() => handleCopyLink(link.url)}
                      className="px-2.5 py-1 text-xs rounded border border-border-gray hover:bg-bg-gray"
                    >
                      Copy
                    </button>
                    <button
                      onClick={() => revokeLink(link.link_token)}
                      className="px-2.5 py-1 text-xs rounded border border-border-gray hover:bg-bg-gray text-red-600"
                    >
                      Revoke
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </Modal>
  );
}
