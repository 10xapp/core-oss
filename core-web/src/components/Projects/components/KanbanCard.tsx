import { useSortable } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { useState, useRef, useEffect, memo, useMemo } from 'react';
import { Flag } from '@phosphor-icons/react';
import { differenceInCalendarDays } from 'date-fns';
import { useProjectsStore } from '../../../stores/projectsStore';
import { useUpdateIssue, useDeleteIssue, type ProjectIssue } from '../../../hooks/queries/useProjects';
import { lastDragEndTime } from './KanbanBoard';
import ConfirmModal from './ConfirmModal';
import AssigneePicker from './AssigneePicker';
import DatePicker from '../../ui/DatePicker';
import { PROJECT_PRIORITY_OPTIONS } from './priorityOptions';

interface KanbanCardProps {
  card: ProjectIssue;
  isDragging?: boolean;
  isOverlay?: boolean;
  onCardClick?: (cardId: string) => void;
  isDragActive?: boolean;
}

// Memoized label tag to avoid inline style object recreation
const LabelTag = memo(function LabelTag({ color, name }: { color: string; name: string }) {
  const style = useMemo(() => ({
    backgroundColor: color + '1F',
    color: color,
  }), [color]);

  const dotStyle = useMemo(() => ({ backgroundColor: color }), [color]);

  return (
    <span
      className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[11px] font-medium"
      style={style}
    >
      <span className="w-1.5 h-1.5 rounded-full shrink-0" style={dotStyle} />
      {name}
    </span>
  );
});

const KanbanCard = memo(function KanbanCard({
  card,
  isDragging = false,
  isOverlay = false,
  onCardClick,
}: KanbanCardProps) {
  const { attributes, listeners, setNodeRef, transform, transition } =
    useSortable({
      id: card.id,
      data: {
        type: 'card',
        card,
        columnId: card.state_id,
      },
    });

  // React Query mutations
  const updateIssue = useUpdateIssue(card.board_id);
  const deleteIssue = useDeleteIssue(card.board_id);

  // UI state from store
  const setSelectedCard = useProjectsStore((state) => state.setSelectedCard);

  const [showPriorityDropdown, setShowPriorityDropdown] = useState(false);
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(false);
  const priorityDropdownRef = useRef<HTMLDivElement>(null);

  // Close priority dropdown on outside click
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (priorityDropdownRef.current && !priorityDropdownRef.current.contains(e.target as Node)) {
        setShowPriorityDropdown(false);
      }
    };
    if (showPriorityDropdown) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [showPriorityDropdown]);

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
  };

  const handleDeleteConfirm = () => {
    deleteIssue.mutate(card.id);
    setShowDeleteConfirm(false);
  };

  const handleCardClick = () => {
    // Skip click if a drag just ended (within last 300ms)
    if (Date.now() - lastDragEndTime < 300) return;
    setSelectedCard(card.id);
    onCardClick?.(card.id);
  };

  const handlePriorityClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    setShowPriorityDropdown(!showPriorityDropdown);
  };

  const handlePriorityChange = (newPriority: number) => {
    setShowPriorityDropdown(false);
    updateIssue.mutate({ issueId: card.id, updates: { priority: newPriority } });
  };

  const handleDueDateChange = (newDate: string) => {
    if (newDate) {
      updateIssue.mutate({ issueId: card.id, updates: { due_at: newDate } });
    } else {
      updateIssue.mutate({ issueId: card.id, updates: { clear_due_at: true } });
    }
  };

  const priorityConfig =
    PROJECT_PRIORITY_OPTIONS.find((option) => option.value === card.priority) ?? null;

  const cardClasses = [
    'group relative bg-white px-4 py-3 rounded-lg mb-2 cursor-default active:cursor-grabbing',
    'hover:shadow-[0_2px_8px_rgba(0,0,0,0.06)]',
    'border border-gray-100/60',
    'transition-[box-shadow,opacity] duration-200 ease-out',
    isDragging && !isOverlay && 'opacity-30',
    isOverlay && 'shadow-[0_20px_40px_rgba(0,0,0,0.12)] scale-[1.02] bg-white',
  ]
    .filter(Boolean)
    .join(' ');

  return (
    <div
      ref={setNodeRef}
      style={style}
      {...attributes}
      {...listeners}
      onClick={handleCardClick}
      className={cardClasses}
    >
      {/* Title */}
      <h4 className="font-medium text-[13px] text-gray-900 leading-snug line-clamp-2 mb-3">
        {card.title}
      </h4>

      {/* Labels */}
      {card.label_objects && card.label_objects.length > 0 && (
        <div className="flex flex-wrap gap-1.5 mb-3">
          {card.label_objects.slice(0, 3).map((label) => (
            <LabelTag key={label.id} color={label.color} name={label.name} />
          ))}
          {card.label_objects.length > 3 && (
            <span className="text-[11px] text-gray-400">
              +{card.label_objects.length - 3}
            </span>
          )}
        </div>
      )}

      {/* Metadata row: Priority, Due Date, Assignee */}
      <div className="flex items-center gap-1 -ml-1">
        {/* Priority flag with dropdown */}
        <div className="relative" ref={priorityDropdownRef}>
          <button
            onClick={handlePriorityClick}
            className={`flex items-center justify-center gap-1 min-h-[28px] min-w-[28px] px-1.5 rounded-md transition-colors text-[11px] font-medium ${
              card.priority > 0
                ? `${priorityConfig?.color} ${priorityConfig?.bg}`
                : 'text-gray-400 hover:bg-gray-50'
            }`}
            title="Set priority"
          >
            <Flag size={14} weight={card.priority > 0 ? 'fill' : 'regular'} />
            {card.priority > 0 && <span>P{card.priority}</span>}
          </button>
          {showPriorityDropdown && (
            <div className="absolute top-full left-0 mt-1 p-1.5 bg-white rounded-lg shadow-lg border border-gray-100 z-20">
              <div className="flex gap-1">
                {/* No priority option */}
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    handlePriorityChange(0);
                  }}
                  className={`flex items-center justify-center w-7 py-1 rounded-md transition-all text-[11px] font-medium text-gray-400 ${
                    card.priority === 0 ? 'bg-gray-100' : 'opacity-50 hover:opacity-100'
                  }`}
                  title="No priority"
                >
                  <span>–</span>
                </button>
                {PROJECT_PRIORITY_OPTIONS.filter((option) => option.value > 0).map((option) => (
                  <button
                    key={option.value}
                    onClick={(e) => {
                      e.stopPropagation();
                      handlePriorityChange(option.value);
                    }}
                    className={`flex items-center gap-1 px-1.5 py-1 rounded-md transition-all text-[11px] font-medium ${option.color} ${
                      card.priority === option.value ? option.bg : 'opacity-50 hover:opacity-100'
                    }`}
                    title={option.label}
                  >
                    <span>P{option.value}</span>
                    <Flag size={12} weight="fill" />
                  </button>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Due Date */}
        <DatePicker
          value={card.due_at || null}
          onChange={handleDueDateChange}
          placeholder=""
          showQuickActions={true}
          showClearButton={false}
          showRelativeDate={true}
          clickToClear
          buttonClassName="!min-h-[28px] !min-w-[28px] !px-1.5 !py-0 !text-[11px] !gap-1 hover:bg-gray-50 !rounded-md !justify-center"
        />

        {/* Assignee */}
        <AssigneePicker
          issueId={card.id}
          boardId={card.board_id}
          currentAssignees={card.assignees || []}
          buttonClassName="!min-h-[28px] !min-w-[28px] !px-1.5 !py-0 !text-[11px] !gap-1 hover:bg-gray-50 !rounded-md !justify-center"
          emptyState="icon-only"
          compact
        />

        {/* Days since created */}
        {card.created_at && (
          <span className="ml-auto text-[11px] text-gray-400 tabular-nums">
            {differenceInCalendarDays(new Date(), new Date(card.created_at))}d
          </span>
        )}
      </div>

      {/* Delete Confirmation Modal */}
      <ConfirmModal
        isOpen={showDeleteConfirm}
        title="Delete card"
        message={`Are you sure you want to delete "${card.title}"? This action cannot be undone.`}
        confirmLabel="Delete"
        onConfirm={handleDeleteConfirm}
        onCancel={() => setShowDeleteConfirm(false)}
      />
    </div>
  );
});

export default KanbanCard;
