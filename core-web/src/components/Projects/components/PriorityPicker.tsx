import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { Flag } from '@phosphor-icons/react';
import { CheckIcon } from '@heroicons/react/24/outline';
import { useUpdateIssue } from '../../../hooks/queries/useProjects';
import { PROJECT_PRIORITY_OPTIONS } from './priorityOptions';

interface PriorityPickerProps {
  issueId: string;
  boardId?: string;
  priority: number;
  buttonClassName?: string;
}

export default function PriorityPicker({ issueId, boardId, priority, buttonClassName = '' }: PriorityPickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [dropdownPosition, setDropdownPosition] = useState({ top: 0, left: 0 });
  const buttonRef = useRef<HTMLButtonElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // React Query mutations
  const updateIssue = useUpdateIssue(boardId ?? null);

  const currentOption =
    PROJECT_PRIORITY_OPTIONS.find((p) => p.value === priority) || PROJECT_PRIORITY_OPTIONS[0];

  const handleUpdate = (value: number) => {
    setIsOpen(false);
    if (value === priority) return;
    updateIssue.mutate({ issueId, updates: { priority: value } });
  };

  useEffect(() => {
    if (isOpen && buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      const dropdownWidth = 160;
      
      let left = rect.left;
      if (left + dropdownWidth > window.innerWidth - 8) {
        left = window.innerWidth - dropdownWidth - 8;
      }
      if (left < 8) left = 8;

      setDropdownPosition({
        top: rect.bottom + 4,
        left,
      });
    }
  }, [isOpen]);

  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (
        dropdownRef.current && 
        !dropdownRef.current.contains(e.target as Node) &&
        buttonRef.current &&
        !buttonRef.current.contains(e.target as Node)
      ) {
        setIsOpen(false);
      }
    };
    if (isOpen) {
      document.addEventListener('mousedown', handleClickOutside);
      return () => document.removeEventListener('mousedown', handleClickOutside);
    }
  }, [isOpen]);

  return (
    <>
      <button
        ref={buttonRef}
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setIsOpen(!isOpen);
        }}
        className={`flex items-center gap-1.5 px-2 py-1.5 rounded-md transition-colors ${buttonClassName}`}
      >
        <Flag size={14} weight="fill" className={currentOption.color} />
        <span className={`text-sm ${priority === 0 ? 'text-gray-500' : 'text-gray-700'}`}>
           {priority > 0 ? `P${priority}` : '—'}
        </span>
      </button>

      {isOpen && createPortal(
        <div
          ref={dropdownRef}
          className="fixed z-[100] bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden py-1 w-40"
          style={{ top: dropdownPosition.top, left: dropdownPosition.left }}
        >
          {PROJECT_PRIORITY_OPTIONS.map((option) => (
            <button
              key={option.value}
              onClick={(e) => {
                e.stopPropagation();
                handleUpdate(option.value);
              }}
              className="w-full flex items-center gap-2 px-3 py-2 text-sm hover:bg-gray-50 transition-colors text-left"
            >
              <Flag size={14} weight="fill" className={option.color} />
              <span className="flex-1 text-gray-700">{option.label}</span>
              {priority === option.value && (
                <CheckIcon className="w-4 h-4 text-gray-900 shrink-0" />
              )}
            </button>
          ))}
        </div>,
        document.body
      )}
    </>
  );
}
