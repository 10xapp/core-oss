import { useProjectsStore } from '../../../stores/projectsStore';
import { useProjectMembers } from '../../../hooks/queries/useProjects';
import { avatarGradient } from '../../../utils/avatarGradient';

interface StackedAvatarsProps {
  userIds: string[];
  maxVisible?: number;
  size?: 'sm' | 'md';
}

const SIZE_CLASSES = {
  sm: {
    avatar: 'w-5 h-5',
    text: 'text-[8px]',
    overlap: '-ml-1.5',
    ring: 'ring-2 ring-white',
    badge: 'w-5 h-5 text-[8px]',
  },
  md: {
    avatar: 'w-6 h-6',
    text: 'text-[10px]',
    overlap: '-ml-2',
    ring: 'ring-2 ring-white',
    badge: 'w-6 h-6 text-[9px]',
  },
};

export default function StackedAvatars({ userIds, maxVisible = 3, size = 'sm' }: StackedAvatarsProps) {
  const workspaceId = useProjectsStore((state) => state.workspaceId);
  const { data: members = [] } = useProjectMembers(workspaceId);
  const getMemberByUserId = (userId: string) => members.find((m) => m.user_id === userId);
  const s = SIZE_CLASSES[size];

  const visibleIds = userIds.slice(0, maxVisible);
  const overflow = userIds.length - maxVisible;

  const getInitials = (member: { name?: string; email?: string }) => {
    if (member.name) {
      return member.name.split(' ').map((n) => n[0]).join('').toUpperCase().slice(0, 2);
    }
    return (member.email?.[0] || '?').toUpperCase();
  };

  return (
    <div className="flex items-center">
      {visibleIds.map((userId, index) => {
        const member = getMemberByUserId(userId);
        return (
          <div
            key={userId}
            className={`${s.avatar} rounded-full flex items-center justify-center flex-shrink-0 ${s.ring} overflow-hidden ${
              index > 0 ? s.overlap : ''
            }`}
            style={{ zIndex: visibleIds.length - index, ...(!member?.avatar_url ? { background: avatarGradient(member?.name || member?.email || userId) } : {}) }}
            title={member ? (member.name || member.email || 'Unknown') : 'Unknown'}
          >
            {member?.avatar_url ? (
              <img
                src={member.avatar_url}
                alt={member.name || member.email || 'Unknown'}
                className="w-full h-full object-cover"
              />
            ) : (
              <span className={`${s.text} font-medium text-white`}>
                {member ? getInitials(member) : '?'}
              </span>
            )}
          </div>
        );
      })}
      {overflow > 0 && (
        <div
          className={`${s.badge} rounded-full bg-gray-200 flex items-center justify-center flex-shrink-0 ${s.ring} ${s.overlap}`}
          style={{ zIndex: 0 }}
        >
          <span className="font-medium text-gray-600">+{overflow}</span>
        </div>
      )}
    </div>
  );
}
