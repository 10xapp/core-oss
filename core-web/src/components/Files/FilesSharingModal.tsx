import { useState, useEffect, useMemo } from "react";
import { toast } from "sonner";
import Modal from "../Modal/Modal";
import { usePermissionStore } from "../../stores/permissionStore";
import PermissionSelect from "../ShareModal/PermissionSelect";
import ShareUserRow from "../ShareModal/ShareUserRow";
import UserSearchInput from "../ShareModal/UserSearchInput";
import PendingRequestRow from "../ShareModal/PendingRequestRow";
import type { UserSearchResult } from "../../api/client";

interface FilesSharingModalProps {
  isOpen: boolean;
  onClose: () => void;
  fileId: string;
}

export default function FilesSharingModal({
  isOpen,
  onClose,
  fileId,
}: FilesSharingModalProps) {
  const [email, setEmail] = useState("");
  const [permission, setPermission] = useState<"read" | "write" | "admin">("read");
  const [linkPermission, setLinkPermission] = useState<"read" | "write" | "admin">("read");
  const [linkSlug, setLinkSlug] = useState("");
  const [linkDirty, setLinkDirty] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [isCreatingLink, setIsCreatingLink] = useState(false);
  const [selectedUser, setSelectedUser] = useState<UserSearchResult | null>(null);

  const {
    resourceShares,
    resourceMembers,
    resourceLinks,
    isLoadingShares,
    isLoadingLinks,
    pendingRequests,
    isLoadingPendingRequests,
    pendingRequestsError,
    error: shareError,
    linksError,
    fetchResourceShares,
    fetchResourceLinks,
    fetchPendingRequests,
    shareWithUser,
    revokePermission,
    updatePermission,
    createLink,
    approveRequest,
    denyRequest,
  } = usePermissionStore();

  const hasShares = resourceShares.length > 0;
  const membersSorted = useMemo(() => {
    return [...resourceMembers].sort((a, b) => {
      const roleOrder = { owner: 0, admin: 1, member: 2 } as Record<string, number>;
      const aOrder = roleOrder[a.role] ?? 3;
      const bOrder = roleOrder[b.role] ?? 3;
      if (aOrder !== bOrder) return aOrder - bOrder;
      return (a.name || a.email || "").localeCompare(b.name || b.email || "");
    });
  }, [resourceMembers]);

  const pendingForResource = useMemo(() => {
    if (!fileId) return [];
    return pendingRequests.filter(
      (request) => request.resource_id === fileId && request.resource_type === "document"
    );
  }, [pendingRequests, fileId]);

  // Reset form when modal opens
  useEffect(() => {
    if (isOpen) {
      setEmail("");
      setPermission("read");
      setLinkPermission("read");
      setLinkSlug("");
      setSelectedUser(null);
      setLinkDirty(false);
    }
  }, [isOpen]);

  // Fetch sharing data when modal opens
  useEffect(() => {
    if (isOpen && fileId) {
      fetchResourceShares("document", fileId);
      fetchResourceLinks("document", fileId);
      fetchPendingRequests();
    }
  }, [isOpen, fileId, fetchResourceShares, fetchResourceLinks, fetchPendingRequests]);

  // Populate slug input when links are loaded
  useEffect(() => {
    if (resourceLinks.length > 0 && !linkDirty) {
      const first = resourceLinks[0];
      setLinkSlug(first.link_slug || first.link_token);
      setLinkPermission(first.permission);
    }
  }, [resourceLinks, linkDirty]);

  const handleShare = async () => {
    if (!fileId || !email.trim()) return;
    setIsSubmitting(true);
    const result = await shareWithUser("document", fileId, email.trim(), permission, selectedUser);
    setIsSubmitting(false);
    if (result) {
      const label = selectedUser?.name || selectedUser?.email || email.trim();
      toast.success(`Shared with ${label}`);
      setEmail("");
      setSelectedUser(null);
    }
  };

  const activeLink = resourceLinks.length > 0 && !linkDirty ? resourceLinks[0] : null;

  const handleCreateLink = async () => {
    if (!fileId) return;
    setIsCreatingLink(true);
    const link = await createLink("document", fileId, linkPermission, linkSlug || null);
    setIsCreatingLink(false);
    if (link?.url) {
      setLinkSlug(link.link_slug || link.link_token);
      setLinkDirty(false);
      try {
        await navigator.clipboard.writeText(link.url);
        toast.success("Link copied to clipboard");
      } catch {
        toast.success("Link created");
      }
    }
  };

  const handleCopyActiveLink = async () => {
    if (!activeLink) return;
    try {
      await navigator.clipboard.writeText(activeLink.url);
      toast.success("Link copied to clipboard");
    } catch {
      toast.error("Failed to copy link");
    }
  };

  const handleApproveRequest = async (
    requestId: string,
    permissionValue: "read" | "write" | "admin"
  ) => {
    const updated = await approveRequest(requestId, permissionValue);
    if (updated) {
      toast.success("Access request approved");
      await fetchResourceShares("document", fileId);
    }
  };

  const handleDenyRequest = async (requestId: string) => {
    const updated = await denyRequest(requestId);
    if (updated) {
      toast.success("Access request denied");
    }
  };

  return (
    <Modal isOpen={isOpen} onClose={onClose} title="Sharing" size="md">
      <div className="space-y-4 -mt-1">
        {/* Share with user */}
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
              {isSubmitting ? "Sharing..." : "Share"}
            </button>
          </div>
          {shareError && <p className="text-xs text-red-500">{shareError}</p>}
        </div>

        {/* People with access */}
        <div className="border-t border-border-gray pt-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary">
              People with access
            </h3>
            {isLoadingShares && (
              <span className="text-xs text-text-secondary">Loading...</span>
            )}
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

        {/* Workspace access */}
        {membersSorted.length > 0 && (
          <div className="border-t border-border-gray pt-3">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary mb-2">
              Workspace access
            </h3>
            <div className="space-y-2">
              {membersSorted.map((member) => (
                <div key={member.id} className="flex items-center justify-between">
                  <div className="min-w-0">
                    <p className="text-sm text-text-body truncate">
                      {member.name || member.email || "Member"}
                    </p>
                    {member.email && (
                      <p className="text-xs text-text-secondary truncate">
                        {member.email}
                      </p>
                    )}
                  </div>
                  <span className="text-xs text-text-secondary capitalize">
                    {member.role}
                  </span>
                </div>
              ))}
            </div>
          </div>
        )}

        {/* Pending requests */}
        <div className="border-t border-border-gray pt-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary">
              Pending requests
            </h3>
            {isLoadingPendingRequests && (
              <span className="text-xs text-text-secondary">Loading...</span>
            )}
          </div>

          {pendingRequestsError && (
            <p className="text-xs text-red-500 mb-2">{pendingRequestsError}</p>
          )}

          {!isLoadingPendingRequests &&
            !pendingRequestsError &&
            pendingForResource.length === 0 && (
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

        {/* Link sharing */}
        <div className="border-t border-border-gray pt-3">
          <div className="flex items-center justify-between mb-2">
            <h3 className="text-xs uppercase tracking-wide text-text-tertiary">
              Link sharing
            </h3>
            {isLoadingLinks && (
              <span className="text-xs text-text-secondary">Loading...</span>
            )}
          </div>

          <div className="flex items-center gap-2">
            <div className="min-w-0 flex-1 flex items-center bg-white border border-border-gray rounded-md overflow-hidden focus-within:border-text-tertiary">
              <span className="pl-3 text-sm text-text-secondary shrink-0">{`${window.location.origin}/s/`}</span>
              <input
                value={linkSlug}
                onChange={(event) => {
                  setLinkSlug(event.target.value);
                  setLinkDirty(true);
                }}
                placeholder="custom-link (optional)"
                className="min-w-0 flex-1 py-2 pr-3 text-sm outline-none"
              />
            </div>
            <PermissionSelect
              value={linkPermission}
              onChange={setLinkPermission}
            />
            {activeLink ? (
              <button
                onClick={handleCopyActiveLink}
                className="px-4 py-2 text-sm rounded-md bg-black text-white shrink-0"
              >
                Copy link
              </button>
            ) : (
              <button
                onClick={handleCreateLink}
                disabled={isCreatingLink}
                className="px-4 py-2 text-sm rounded-md bg-black text-white disabled:opacity-60 shrink-0"
              >
                {isCreatingLink ? "Creating..." : "Create link"}
              </button>
            )}
          </div>

          {linksError && <p className="text-xs text-red-500 mt-2">{linksError}</p>}
        </div>
      </div>
    </Modal>
  );
}
