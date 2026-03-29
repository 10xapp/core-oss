import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { CheckIcon } from '@heroicons/react/24/outline';
import { Tag, Plus } from 'lucide-react';
import { Icon } from '../../ui/Icon';
import { useProjectBoard, useCreateLabel, useToggleIssueLabel, type ProjectLabel } from '../../../hooks/queries/useProjects';

interface LabelPickerProps {
  issueId: string;
  boardId: string;
  currentLabels: ProjectLabel[];
  buttonClassName?: string;
  emptyState?: 'default' | 'icon-dash';
}

const PRESET_COLORS = [
  '#B60205', '#D93F0B', '#E99695', '#F9D0C4',
  '#0E8A16', '#006B75', '#1D76DB', '#0075CA',
  '#5319E7', '#D4C5F9', '#BFD4F2', '#FBCA04',
  '#6B7280', '#E5E7EB',
];

export default function LabelPicker({ issueId, boardId, currentLabels, buttonClassName = '', emptyState = 'default' }: LabelPickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [showCreate, setShowCreate] = useState(false);
  const [newName, setNewName] = useState('');
  const [newColor, setNewColor] = useState(PRESET_COLORS[0]);
  const [dropdownPosition, setDropdownPosition] = useState({ top: 0, left: 0, openAbove: false });
  const buttonRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // React Query data
  const { data: boardData } = useProjectBoard(boardId);
  const boardLabels = boardData?.labels ?? [];

  // React Query mutations
  const createLabelMutation = useCreateLabel(boardId);
  const toggleLabelMutation = useToggleIssueLabel(boardId);

  const calculateDropdownPosition = () => {
    if (buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      const dropdownWidth = 224; // w-56 = 14rem = 224px
      const dropdownHeight = 280; // approximate max height

      let left = rect.left;
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
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        buttonRef.current &&
        !buttonRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
        setShowCreate(false);
        setNewName('');
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

    document.addEventListener('wheel', preventScroll, { passive: false });
    document.addEventListener('touchmove', preventScroll, { passive: false });

    return () => {
      document.removeEventListener('wheel', preventScroll);
      document.removeEventListener('touchmove', preventScroll);
    };
  }, [isOpen]);

  const currentLabelIds = new Set(currentLabels.map((l) => l.id));

  const handleToggle = (labelId: string) => {
    const hasLabel = currentLabelIds.has(labelId);
    toggleLabelMutation.mutate({ issueId, labelId, hasLabel });
  };

  const handleCreate = async () => {
    if (!newName.trim()) return;
    try {
      const label = await createLabelMutation.mutateAsync({ name: newName.trim(), color: newColor });
      if (label) {
        toggleLabelMutation.mutate({ issueId, labelId: label.id, hasLabel: false });
        setNewName('');
        setShowCreate(false);
      }
    } catch (err) {
      console.error('Failed to create label:', err);
    }
  };

  return (
    <>
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
        className={`w-full flex items-center gap-2 px-3 py-2.5 text-[13px] rounded-xl transition-all text-left ${buttonClassName}`}
      >
        {currentLabels.length > 0 ? (
          <div className="flex flex-wrap gap-1 flex-1 min-w-0">
            {currentLabels.map((label) => (
              <span
                key={label.id}
                className="inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md text-[11px] font-medium"
                style={{
                  backgroundColor: label.color + '1F',
                  color: label.color,
                }}
              >
                <span
                  className="w-1.5 h-1.5 rounded-full shrink-0"
                  style={{ backgroundColor: label.color }}
                />
                {label.name}
              </span>
            ))}
          </div>
        ) : (
          <div className={`flex items-center gap-1.5 text-gray-400 flex-1 ${emptyState === 'icon-dash' ? 'pl-0.5' : ''}`}>
            <Icon icon={Tag} size={14} />
            <span>{emptyState === 'icon-dash' ? '—' : 'Add label'}</span>
          </div>
        )}
      </button>

      {isOpen && createPortal(
        <div
          ref={dropdownRef}
          className="fixed z-10000 w-56 bg-white rounded-xl shadow-lg border border-gray-100 overflow-hidden"
          style={{
            top: dropdownPosition.top,
            left: dropdownPosition.left,
            transform: dropdownPosition.openAbove ? 'translateY(-100%)' : undefined,
          }}
        >
          <div className="max-h-48 overflow-y-auto py-1">
            {boardLabels.length === 0 && !showCreate && (
              <div className="px-3 py-3 text-[12px] text-gray-400 text-center">
                No labels yet
              </div>
            )}
            {boardLabels.map((label) => (
              <button
                key={label.id}
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  handleToggle(label.id);
                }}
                className="w-full flex items-center gap-2.5 px-3 py-2 text-[12px] hover:bg-gray-50 transition-colors"
              >
                <span
                  className="w-3 h-3 rounded-full shrink-0"
                  style={{ backgroundColor: label.color }}
                />
                <span className="flex-1 text-left text-gray-700 font-medium truncate">
                  {label.name}
                </span>
                {currentLabelIds.has(label.id) && (
                  <CheckIcon className="w-4 h-4 text-gray-900 shrink-0" />
                )}
              </button>
            ))}
          </div>

          {/* Create new label */}
          <div className="border-t border-gray-100">
            {showCreate ? (
              <div 
                className="p-2 space-y-2"
                onClick={(e) => e.stopPropagation()}
              >
                <input
                  type="text"
                  value={newName}
                  onChange={(e) => setNewName(e.target.value)}
                  placeholder="Label name"
                  className="w-full px-2.5 py-1.5 text-[12px] text-gray-700 bg-gray-50 border-0 rounded-lg focus:outline-none focus:ring-1 focus:ring-gray-200 placeholder:text-gray-400"
                  autoFocus
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') handleCreate();
                    if (e.key === 'Escape') {
                      setShowCreate(false);
                      setNewName('');
                    }
                  }}
                />
                <div className="flex flex-wrap gap-1">
                  {PRESET_COLORS.map((color) => (
                    <button
                      key={color}
                      type="button"
                      onClick={() => setNewColor(color)}
                      className={`w-5 h-5 rounded-full transition-all ${
                        newColor === color ? 'ring-2 ring-offset-1 ring-gray-400 scale-110' : 'hover:scale-110'
                      }`}
                      style={{ backgroundColor: color }}
                    />
                  ))}
                </div>
                <div className="flex gap-1.5">
                  <button
                    type="button"
                    onClick={() => {
                      setShowCreate(false);
                      setNewName('');
                    }}
                    className="flex-1 px-2 py-1 text-[11px] font-medium text-gray-500 hover:bg-gray-50 rounded-lg transition-colors"
                  >
                    Cancel
                  </button>
                  <button
                    type="button"
                    onClick={handleCreate}
                    disabled={!newName.trim()}
                    className="flex-1 px-2 py-1 text-[11px] font-medium bg-gray-900 text-white rounded-lg hover:bg-gray-800 disabled:opacity-40 transition-all"
                  >
                    Create
                  </button>
                </div>
              </div>
            ) : (
              <button
                type="button"
                onClick={(e) => {
                  e.stopPropagation();
                  setShowCreate(true);
                }}
                className="w-full flex items-center gap-2 px-3 py-2 text-[12px] text-gray-500 hover:bg-gray-50 transition-colors"
              >
                <Icon icon={Plus} size={14} />
                Create label
              </button>
            )}
          </div>
        </div>,
        document.body
      )}
    </>
  );
}
