import { useState } from 'react';
import type { AccessRequest, PermissionLevel } from '../../api/client';
import PermissionSelect from './PermissionSelect';
import { avatarGradient } from '../../utils/avatarGradient';

interface PendingRequestRowProps {
  request: AccessRequest;
  onApprove: (requestId: string, permission: PermissionLevel) => Promise<void> | void;
  onDeny: (requestId: string) => Promise<void> | void;
}

function getRelativeTime(dateStr?: string): string {
  if (!dateStr) return 'just now';
  const now = Date.now();
  const date = new Date(dateStr).getTime();
  const diffMs = now - date;
  const diffMins = Math.floor(diffMs / 60000);
  if (diffMins < 1) return 'now';
  if (diffMins < 60) return `${diffMins}m`;
  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h`;
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays < 7) return `${diffDays}d`;
  return new Date(dateStr).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

export default function PendingRequestRow({ request, onApprove, onDeny }: PendingRequestRowProps) {
  const [permission, setPermission] = useState<PermissionLevel>('read');
  const [approving, setApproving] = useState(false);
  const [denying, setDenying] = useState(false);

  const requester = request.requester;
  const name = requester?.name || requester?.email || 'User';
  const email = requester?.email || '';
  const initials = name
    .split(' ')
    .map((part) => part[0])
    .join('')
    .toUpperCase()
    .slice(0, 2);

  const handleApprove = async () => {
    if (approving || denying) return;
    setApproving(true);
    try {
      await onApprove(request.id, permission);
    } finally {
      setApproving(false);
    }
  };

  const handleDeny = async () => {
    if (approving || denying) return;
    setDenying(true);
    try {
      await onDeny(request.id);
    } finally {
      setDenying(false);
    }
  };

  return (
    <div className="flex items-start justify-between gap-3 py-3">
      <div className="flex items-start gap-3 min-w-0">
        {requester?.avatar_url ? (
          <img
            src={requester.avatar_url}
            alt={name}
            className="w-9 h-9 rounded-full object-cover"
          />
        ) : (
          <div className="w-9 h-9 rounded-full flex items-center justify-center text-xs font-semibold text-white" style={{ background: avatarGradient(name) }}>
            {initials}
          </div>
        )}
        <div className="min-w-0">
          <p className="text-sm font-medium text-text-body truncate">{name}</p>
          {email && <p className="text-xs text-text-secondary truncate">{email}</p>}
          {request.message && (
            <p className="text-xs text-text-tertiary mt-1 line-clamp-2">“{request.message}”</p>
          )}
          <span className="text-[11px] text-text-tertiary mt-1 block">
            {getRelativeTime(request.created_at)}
          </span>
        </div>
      </div>

      <div className="flex items-center gap-2">
        <PermissionSelect value={permission} onChange={setPermission} size="sm" />
        <button
          onClick={handleApprove}
          disabled={approving || denying}
          className="px-2.5 py-1 text-xs rounded bg-black text-white disabled:opacity-60"
        >
          {approving ? 'Approving...' : 'Approve'}
        </button>
        <button
          onClick={handleDeny}
          disabled={approving || denying}
          className="px-2.5 py-1 text-xs rounded border border-border-gray hover:bg-bg-gray disabled:opacity-60"
        >
          {denying ? 'Denying...' : 'Deny'}
        </button>
      </div>
    </div>
  );
}
