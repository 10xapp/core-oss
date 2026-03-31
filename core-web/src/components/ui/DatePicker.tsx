import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import {
  format,
  startOfMonth,
  endOfMonth,
  startOfWeek,
  endOfWeek,
  addDays,
  addMonths,
  subMonths,
  isSameMonth,
  isSameDay,
  isToday,
} from 'date-fns';
import { ChevronLeftIcon, ChevronRightIcon, XMarkIcon } from '@heroicons/react/24/outline';
import { Calendar } from 'lucide-react';
import { Icon } from './Icon';
import { getRelativeDate } from '../../utils/dateUtils';

interface DatePickerProps {
  value: string | Date | null; // YYYY-MM-DD string or Date object
  onChange: (value: string) => void; // Always returns YYYY-MM-DD format
  placeholder?: string;
  showQuickActions?: boolean;
  showClearButton?: boolean;
  showRelativeDate?: boolean;
  showIcon?: boolean;
  clickToClear?: boolean; // When true, clicking on a date clears it and hover shows X icon
  className?: string;
  buttonClassName?: string;
  dropdownClassName?: string;
  align?: 'left' | 'right';
}

export default function DatePicker({
  value,
  onChange,
  placeholder = 'Select date',
  showQuickActions = true,
  showClearButton = true,
  showRelativeDate = true,
  showIcon = true,
  clickToClear = false,
  className = '',
  buttonClassName = '',
  dropdownClassName = '',
  align = 'left',
}: DatePickerProps) {
  const [isOpen, setIsOpen] = useState(false);
  const [isHovered, setIsHovered] = useState(false);

  // Parse date string to local Date (avoids UTC timezone issues)
  const parseLocalDate = (dateStr: string): Date => {
    // Handle ISO datetime strings (e.g., 2025-01-20T00:00:00.000Z)
    const datePart = dateStr.split('T')[0];
    const [year, month, day] = datePart.split('-').map(Number);
    return new Date(year, month - 1, day);
  };

  // Normalize value to string format (YYYY-MM-DD only)
  const normalizedValue = value
    ? typeof value === 'string'
      ? value.split('T')[0] // Strip time portion from ISO datetime strings
      : format(value, 'yyyy-MM-dd')
    : '';

  const [currentMonth, setCurrentMonth] = useState(() =>
    normalizedValue ? parseLocalDate(normalizedValue) : new Date()
  );
  const [dropdownPosition, setDropdownPosition] = useState({ top: 0, left: 0, openAbove: false });
  const containerRef = useRef<HTMLDivElement>(null);
  const buttonRef = useRef<HTMLDivElement>(null);
  const dropdownRef = useRef<HTMLDivElement>(null);

  const selectedDate = normalizedValue ? parseLocalDate(normalizedValue) : null;

  // Calculate dropdown position synchronously before opening
  const calculateDropdownPosition = () => {
    if (buttonRef.current) {
      const rect = buttonRef.current.getBoundingClientRect();
      const dropdownWidth = 280; // Approximate width of dropdown
      const dropdownHeight = 320; // Approximate height of dropdown

      let left = rect.left;
      if (align === 'right') {
        left = rect.right - dropdownWidth;
      }

      // Keep dropdown within viewport horizontally
      if (left + dropdownWidth > window.innerWidth - 8) {
        left = window.innerWidth - dropdownWidth - 8;
      }
      if (left < 8) {
        left = 8;
      }

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

  // Close on outside click
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      const clickedInContainer = containerRef.current?.contains(target);
      const clickedInDropdown = dropdownRef.current?.contains(target);
      if (!clickedInContainer && !clickedInDropdown) {
        setIsOpen(false);
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

  // Update currentMonth when value changes externally
  useEffect(() => {
    if (normalizedValue) {
      setCurrentMonth(parseLocalDate(normalizedValue));
    }
  }, [normalizedValue]);

  const handleDateClick = (date: Date) => {
    onChange(format(date, 'yyyy-MM-dd'));
    setIsOpen(false);
  };

  const handleClear = (e: React.MouseEvent) => {
    e.stopPropagation();
    onChange('');
    setIsOpen(false);
  };

  const renderHeader = () => (
    <div className="flex items-center justify-between px-2 py-1.5 border-b border-gray-100">
      <button
        type="button"
        onClick={() => setCurrentMonth(subMonths(currentMonth, 1))}
        className="p-1 hover:bg-gray-100 rounded-md transition-colors"
      >
        <ChevronLeftIcon className="w-3.5 h-3.5 text-gray-500" />
      </button>
      <span className="text-xs font-medium text-gray-900">
        {format(currentMonth, 'MMM yyyy')}
      </span>
      <button
        type="button"
        onClick={() => setCurrentMonth(addMonths(currentMonth, 1))}
        className="p-1 hover:bg-gray-100 rounded-md transition-colors"
      >
        <ChevronRightIcon className="w-3.5 h-3.5 text-gray-500" />
      </button>
    </div>
  );

  const renderDays = () => {
    const days = ['S', 'M', 'T', 'W', 'T', 'F', 'S'];
    return (
      <div className="grid grid-cols-7 gap-0 px-2 pt-1">
        {days.map((day, i) => (
          <div
            key={i}
            className="w-7 text-[10px] font-medium text-gray-400 text-center py-0.5"
          >
            {day}
          </div>
        ))}
      </div>
    );
  };

  const renderCells = () => {
    const monthStart = startOfMonth(currentMonth);
    const monthEnd = endOfMonth(monthStart);
    const startDate = startOfWeek(monthStart);
    const endDate = endOfWeek(monthEnd);

    const rows = [];
    let days = [];
    let day = startDate;

    while (day <= endDate) {
      for (let i = 0; i < 7; i++) {
        const cloneDay = day;
        const isSelected = selectedDate && isSameDay(day, selectedDate);
        const isCurrentMonth = isSameMonth(day, monthStart);
        const isTodayDate = isToday(day);

        days.push(
          <button
            key={day.toString()}
            type="button"
            onClick={() => handleDateClick(cloneDay)}
            className={`
              w-7 h-7 text-[11px] rounded-lg transition-all
              ${!isCurrentMonth ? 'text-gray-300' : 'text-gray-700'}
              ${isSelected ? 'bg-gray-900 text-white' : 'hover:bg-gray-100'}
              ${isTodayDate && !isSelected ? 'font-semibold ring-1 ring-gray-300' : ''}
            `}
          >
            {format(day, 'd')}
          </button>
        );
        day = addDays(day, 1);
      }
      rows.push(
        <div key={day.toString()} className="grid grid-cols-7 gap-0 px-2">
          {days}
        </div>
      );
      days = [];
    }
    return <div className="py-0.5">{rows}</div>;
  };

  const relativeInfo = normalizedValue ? getRelativeDate(normalizedValue) : null;

  const getDisplayText = () => {
    if (!normalizedValue) return null;
    if (showRelativeDate && relativeInfo) {
      return relativeInfo.text;
    }
    return format(parseLocalDate(normalizedValue), 'MMM d');
  };

  const getTextColor = () => {
    if (!relativeInfo) return 'text-gray-500';
    if (relativeInfo.isOverdue) return 'text-red-500';
    if (relativeInfo.isSoon) return 'text-amber-600';
    return 'text-gray-500';
  };

  const handleButtonClick = (e: React.MouseEvent) => {
    e.stopPropagation();
    // If clickToClear is enabled and there's a value, clear it
    if (clickToClear && normalizedValue) {
      onChange('');
      return;
    }
    // Otherwise, open/close the picker
    if (!isOpen) {
      calculateDropdownPosition();
    }
    setIsOpen(!isOpen);
  };

  // Determine which icon to show
  const showXIcon = clickToClear && normalizedValue && isHovered;

  return (
    <div className={`relative ${className}`} ref={containerRef}>
      <div
        ref={buttonRef}
        onClick={handleButtonClick}
        onMouseEnter={() => setIsHovered(true)}
        onMouseLeave={() => setIsHovered(false)}
        className={`flex items-center gap-2 px-3 py-2 text-[13px] rounded-lg transition-all text-left cursor-pointer ${buttonClassName}`}
      >
        {showIcon && (
          showXIcon ? (
            <XMarkIcon className="w-4 h-4 text-gray-400 shrink-0" />
          ) : (
            <Icon icon={Calendar} size={16} className="text-gray-400 shrink-0" />
          )
        )}
        {normalizedValue ? (
          <span className={getTextColor()}>
            {getDisplayText()}
          </span>
        ) : placeholder ? (
          <span className="text-gray-400">{placeholder}</span>
        ) : null}
        {normalizedValue && showClearButton && !clickToClear && (
          <button
            type="button"
            onClick={handleClear}
            className="p-0.5 hover:bg-gray-200 rounded transition-colors"
          >
            <XMarkIcon className="w-3.5 h-3.5 text-gray-400" />
          </button>
        )}
      </div>

      {isOpen && createPortal(
        <div
          ref={dropdownRef}
          data-datepicker-portal
          onClick={(e) => e.stopPropagation()}
          className={`fixed z-10000 bg-white rounded-lg shadow-lg border border-gray-200 overflow-hidden ${dropdownClassName}`}
          style={{
            top: dropdownPosition.top,
            left: dropdownPosition.left,
            transform: dropdownPosition.openAbove ? 'translateY(-100%)' : undefined,
          }}
        >
          {renderHeader()}
          {renderDays()}
          {renderCells()}

          {showQuickActions && (
            <div className="border-t border-gray-100 px-2 py-1.5 flex gap-0.5">
              <button
                type="button"
                onClick={() => handleDateClick(new Date())}
                className="flex-1 px-1 py-1 text-[10px] font-medium text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
              >
                Today
              </button>
              <button
                type="button"
                onClick={() => handleDateClick(addDays(new Date(), 1))}
                className="flex-1 px-1 py-1 text-[10px] font-medium text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
              >
                Tomorrow
              </button>
              <button
                type="button"
                onClick={() => handleDateClick(addDays(new Date(), 7))}
                className="flex-1 px-1 py-1 text-[10px] font-medium text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
              >
                +1 week
              </button>
            </div>
          )}
        </div>,
        document.body
      )}
    </div>
  );
}
