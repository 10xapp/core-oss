import { useState, useRef, useEffect, useMemo, useCallback } from 'react';
import { createPortal } from 'react-dom';
import { avatarGradient } from '../../../utils/avatarGradient';
import { motion, AnimatePresence } from 'motion/react';
import { CheckIcon } from '@heroicons/react/24/outline';
import { HugeiconsIcon } from '@hugeicons/react';
import { UserIcon } from '@hugeicons-pro/core-stroke-standard';
import { useProjectsStore } from '../../../stores/projectsStore';
import {
  useProjectMembers,
  useAddAssignee,
  useRemoveAssignee,
  type ProjectIssueAssignee,
  type WorkspaceMember,
} from '../../../hooks/queries/useProjects';

interface AssigneePickerProps {
  issueId: string;
  boardId?: string;
  currentAssignees: ProjectIssueAssignee[];
  buttonClassName?: string;
  emptyState?: 'default' | 'icon-dash' | 'icon-only';
  compact?: boolean;
}

const MAX_ASSIGNEES = 10;

export default function AssigneePicker({ issueId, boardId, currentAssignees, buttonClassName = '', emptyState = 'default', compact = false }: AssigneePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [search, setSearch] = useState('');
  const [dropdownPosition, setDropdownPosition] = useState({ top: 0, left: 0, openAbove: false });
  const containerRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // UI state from store
  const workspaceId = useProjectsStore((state) => state.workspaceId);

  // React Query data
  const { data: members = [] } = useProjectMembers(workspaceId);

  // Helper to get member by user_id
  const membersByUserId = useMemo(() => {
    const map = new Map<string, WorkspaceMember>();
    members.forEach((m) => map.set(m.user_id, m));
    return map;
  }, [members]);

  const getMemberByUserId = useCallback(
    (userId: string) => membersByUserId.get(userId),
    [membersByUserId]
  );

  // React Query mutations
  const addAssigneeMutation = useAddAssignee(boardId ?? null);
  const removeAssigneeMutation = useRemoveAssignee(boardId ?? null);

  const calculateDropdownPosition = () => {
    if (buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      const dropdownWidth = 240; // w-60 approx width (15rem = 240px)
      const dropdownHeight = 280; // approximate max height of dropdown

      let left = rect.left;
      // Keep dropdown within viewport horizontally
      if (left + dropdownWidth > window.innerWidth - 8) {
        left = window.innerWidth - dropdownWidth - 8;
      }
      if (left < 8) left = 8;

      // Check if there's enough space below
      const spaceBelow = window.innerHeight - rect.bottom;
      const spaceAbove = rect.top;
      const openAbove = spaceBelow < dropdownHeight && spaceAbove > spaceBelow;

      setDropdownPosition({
        top: openAbove ? rect.top - 4 : rect.bottom + 4,
        left,
        openAbove,
      });
    }
  };

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      const clickedInContainer = containerRef.current?.contains(target);
      const clickedInDropdown = dropdownRef.current?.contains(target);

      if (!clickedInContainer && !clickedInDropdown) {
        setIsOpen(false);
        setSearch('');
      }
    };

    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => {
        document.removeEventListener('mousedown', handleClickOutside);
      };
    }
  }, [isOpen]);

  // Prevent scrolling when dropdown is open
  useEffect(() => {
    if (!isOpen) return;

    const preventScroll = (e: Event) => {
      // Allow scrolling inside the dropdown itself
      if (dropdownRef.current?.contains(e.target as Node)) return;
      e.preventDefault();
    };

    // Use wheel and touchmove to prevent scroll
    document.addEventListener('wheel', preventScroll, { passive: false });
    document.addEventListener('touchmove', preventScroll, { passive: false });

    return () => {
      document.removeEventListener('wheel', preventScroll);
      document.removeEventListener('touchmove', preventScroll);
    };
  }, [isOpen]);

  const assignedUserIds = new Set(currentAssignees.map((a) => a.user_id));
  const atMax = currentAssignees.length >= MAX_ASSIGNEES;

  const filteredMembers = members.filter((m) => {
    if (!search) return true;
    const s = search.toLowerCase();
    return (m.name?.toLowerCase().includes(s) || m.email?.toLowerCase().includes(s));
  });

  const getInitials = (member: { name?: string; email?: string }) => {
    if (member.name) {
      return member.name.split(' ').map((n) => n[0]).join('').toUpperCase().slice(0, 2);
    }
    return (member.email?.[0] || '?').toUpperCase();
  };

  const getDisplayName = (member: { name?: string; email?: string }) => {
    return member.name || member.email || 'Unknown';
  };

  const handleToggle = (userId: string) => {
    if (assignedUserIds.has(userId)) {
      removeAssigneeMutation.mutate({ issueId, userId });
    } else if (!atMax) {
      addAssigneeMutation.mutate({ issueId, userId });
    }
  };

  return (
    <div className="relative" ref={containerRef}>
      <button
        ref={buttonRef}
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          if (!isOpen) {
            calculateDropdownPosition();
          }
          setIsOpen(!isOpen);
        }}
        className={`flex items-center gap-2.5 px-3 py-2.5 text-[13px] rounded-xl transition-all text-left ${buttonClassName}`}
      >
        <div className={`flex items-center gap-1 min-w-0 overflow-hidden ${!(compact && emptyState === 'icon-only' && currentAssignees.length === 0) ? 'flex-1' : ''}`}>
          <div className="flex items-center min-h-5">
            <AnimatePresence mode="popLayout">
              {currentAssignees.length === 0 ? (
                <motion.div
                  key="empty-state"
                  initial={{ opacity: 0 }}
                  animate={{ opacity: 1 }}
                  exit={{ opacity: 0 }}
                  transition={{ duration: 0.15 }}
                  className={`flex items-center gap-1 text-gray-400 ${emptyState === 'icon-dash' || emptyState === 'icon-only' ? '' : 'gap-1.5'}`}
                >
                  <HugeiconsIcon icon={UserIcon} size={compact ? 12 : 14} />
                  {emptyState !== 'icon-only' && (
                    <span className={compact ? 'text-[11px]' : ''}>{emptyState === 'icon-dash' ? '–' : 'No assignees'}</span>
                  )}
                </motion.div>
              ) : (
                currentAssignees.slice(0, compact ? 3 : 3).map((a, index) => {
                  const member = getMemberByUserId(a.user_id);
                  return (
                    <motion.div
                      key={a.user_id}
                      initial={{ opacity: 0, scale: 0.8 }}
                      animate={{ opacity: 1, scale: 1 }}
                      exit={{ opacity: 0, scale: 0.8 }}
                      transition={{
                        duration: 0.15,
                        ease: 'easeOut'
                      }}
                      className={`${compact ? 'w-5 h-5' : 'w-5 h-5'} rounded-full bg-white flex items-center justify-center shrink-0 border border-gray-100/60 ${index > 0 ? '-ml-1.5' : ''}`}
                      title={member ? getDisplayName(member) : 'Unknown'}
                    >
                      {member?.avatar_url ? (
                        <img
                          src={member.avatar_url}
                          alt={getDisplayName(member)}
                          className="w-full h-full rounded-full object-cover"
                        />
                      ) : (
                        <div className="w-full h-full rounded-full flex items-center justify-center" style={{ background: avatarGradient(member?.name || member?.email || '?') }}>
                          <span className="text-[8px] font-medium text-white">
                            {member ? getInitials(member) : '?'}
                          </span>
                        </div>
                      )}
                    </motion.div>
                  );
                })
              )}
            </AnimatePresence>
          </div>
          {currentAssignees.length > 0 && !compact && (
            <span className="text-gray-700 truncate ml-1">
              {currentAssignees.length === 1
                ? (() => {
                    const member = getMemberByUserId(currentAssignees[0].user_id);
                    return member ? getDisplayName(member) : 'Unknown';
                  })()
                : `${currentAssignees.length} assignees`}
            </span>
          )}
          {currentAssignees.length > 3 && compact && (
            <span className="text-[11px] text-gray-400 ml-0.5">+{currentAssignees.length - 3}</span>
          )}
        </div>
      </button>

      {isOpen && createPortal(
        <div
          ref={dropdownRef}
          onClick={(e) => e.stopPropagation()}
          className="fixed z-10000 w-60 bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden"
          style={{
            top: dropdownPosition.top,
            left: dropdownPosition.left,
            transform: dropdownPosition.openAbove ? 'translateY(-100%)' : undefined,
          }}
        >
          <div className="p-2">
            <input
              type="text"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Search members..."
              className="w-full px-2.5 py-1.5 text-[12px] text-gray-700 bg-transparent border-0 rounded-lg focus:outline-none placeholder:text-gray-400"
              autoFocus
            />
          </div>
          {atMax && (
            <div className="px-3 py-1.5 text-[11px] text-amber-600 bg-amber-50">
              Maximum {MAX_ASSIGNEES} assignees reached
            </div>
          )}
          <div className="max-h-48 overflow-y-auto p-1.5 flex flex-col gap-0.5 [&::-webkit-scrollbar]:hidden" style={{ scrollbarWidth: 'none', msOverflowStyle: 'none' }}>
            {filteredMembers.map((member) => {
              const isAssigned = assignedUserIds.has(member.user_id);
              const isDisabled = !isAssigned && atMax;
              return (
                <button
                  key={member.id}
                  type="button"
                  onClick={(e) => {
                    e.stopPropagation();
                    handleToggle(member.user_id);
                  }}
                  disabled={isDisabled}
                  className={`w-full flex items-center gap-2.5 px-2.5 py-2 text-[12px] transition-colors rounded-lg ${
                    isDisabled
                      ? 'opacity-40 cursor-not-allowed'
                      : 'hover:bg-gray-50'
                  } ${isAssigned ? 'bg-gray-50' : ''}`}
                >
                  {member.avatar_url ? (
                    <img
                      src={member.avatar_url}
                      alt={getDisplayName(member)}
                      className="w-5 h-5 rounded-full object-cover shrink-0"
                    />
                  ) : (
                    <div className="w-5 h-5 rounded-full flex items-center justify-center shrink-0" style={{ background: avatarGradient(member.name || member.email || '?') }}>
                      <span className="text-[10px] font-medium text-white">
                        {getInitials(member)}
                      </span>
                    </div>
                  )}
                  <div className="flex-1 text-left min-w-0">
                    <div className="text-gray-700 font-medium truncate">
                      {getDisplayName(member)}
                    </div>
                    {member.name && member.email && (
                      <div className="text-[11px] text-gray-400 truncate">
                        {member.email}
                      </div>
                    )}
                  </div>
                  {isAssigned && (
                    <CheckIcon className="w-4 h-4 text-gray-900 shrink-0" />
                  )}
                </button>
              );
            })}
            {filteredMembers.length === 0 && (
              <div className="px-3 py-3 text-[12px] text-gray-400 text-center">
                No members found
              </div>
            )}
          </div>
        </div>,
        document.body
      )}
    </div>
  );
}
