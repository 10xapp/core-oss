import { useEffect, useRef, useCallback, useState } from 'react';
import { createPortal } from 'react-dom';
import { motion } from 'motion/react';
import { Calendar, MapPin, Video, FileEdit } from 'lucide-react';
import { Icon } from '../../ui/Icon';
import { createCalendarEvent } from '../../../api/client';
import { useCalendarStore } from '../../../stores/calendarStore';
import { eventCreationFlags } from '../utils/eventCreationFlags';
import DatePicker from '../../ui/DatePicker';

const POPOVER_WIDTH = 280;
const POPOVER_GAP = 12;
const clampNumber = (value: number, min: number, max: number) => Math.min(max, Math.max(min, value));

interface InvertedSpinInputProps {
  value: number;
  displayValue?: string;
  min: number;
  max: number;
  step?: number;
  onChange: (value: number) => void;
  className?: string;
}

function InvertedSpinInput({
  value,
  displayValue,
  min,
  max,
  step = 1,
  onChange,
  className = ''
}: InvertedSpinInputProps) {
  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const parsed = parseInt(e.target.value, 10);
    if (Number.isNaN(parsed)) return;
    onChange(clampNumber(parsed, min, max));
  };

  const stepBy = (delta: number) => {
    onChange(clampNumber(value + delta, min, max));
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'ArrowUp') {
      e.preventDefault();
      stepBy(-step);
    } else if (e.key === 'ArrowDown') {
      e.preventDefault();
      stepBy(step);
    }
  };

  return (
    <div className="relative inline-flex items-stretch">
      <input
        type="text"
        inputMode="numeric"
        pattern="[0-9]*"
        value={displayValue ?? String(value)}
        onChange={handleInputChange}
        onKeyDown={handleKeyDown}
        className={`w-10 px-1 py-0.5 pr-4 border border-gray-300 rounded text-gray-900 text-center ${className}`}
      />
      <div className="absolute right-0 top-0 h-full flex flex-col">
        <button
          type="button"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => stepBy(-step)}
          aria-label="Decrease value"
          className="h-1/2 w-4 text-gray-500 hover:text-gray-700 leading-none flex items-center justify-center"
        >
          <svg viewBox="0 0 20 20" className="w-3 h-3" aria-hidden="true">
            <path d="M5.5 12.5L10 8l4.5 4.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
        <button
          type="button"
          onMouseDown={(e) => e.preventDefault()}
          onClick={() => stepBy(step)}
          aria-label="Increase value"
          className="h-1/2 w-4 text-gray-500 hover:text-gray-700 leading-none flex items-center justify-center"
        >
          <svg viewBox="0 0 20 20" className="w-3 h-3" aria-hidden="true">
            <path d="M5.5 7.5L10 12l4.5-4.5" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </button>
      </div>
    </div>
  );
}

export default function NewEventPopover() {
  const pendingEvent = useCalendarStore((state) => state.pendingEvent);
  const updatePendingEventTitle = useCalendarStore((state) => state.updatePendingEventTitle);
  const updatePendingEventTime = useCalendarStore((state) => state.updatePendingEventTime);
  const updatePendingEventEndTime = useCalendarStore((state) => state.updatePendingEventEndTime);
  const updatePendingEventDescription = useCalendarStore((state) => state.updatePendingEventDescription);
  const updatePendingEventLocation = useCalendarStore((state) => state.updatePendingEventLocation);
  const togglePendingEventGoogleMeet = useCalendarStore((state) => state.togglePendingEventGoogleMeet);
  const cancelCreatingEvent = useCalendarStore((state) => state.cancelCreatingEvent);
  const updatePendingEventEndDate = useCalendarStore((state) => state.updatePendingEventEndDate);
  const updatePendingEventAllDay = useCalendarStore((state) => state.updatePendingEventAllDay);
  const addEvent = useCalendarStore((state) => state.addEvent);
  const replaceEvent = useCalendarStore((state) => state.replaceEvent);
  const removeEvent = useCalendarStore((state) => state.removeEvent);

  const inputRef = useRef<HTMLInputElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);
  const isSavingRef = useRef(false);
  const initialScrollYRef = useRef(0);
  const [isEditingTime, setIsEditingTime] = useState(false);

  // Track if popover is open (not the full pendingEvent to avoid effect re-runs on title change)
  const isOpen = !!pendingEvent;

  // Calculate position synchronously in render (like EventPopover) to avoid flash
  const positionRef = useRef<{ left: number; top: number; positionRight: boolean } | null>(null);

  if (isOpen && pendingEvent) {
    const triggerRect = pendingEvent.triggerRect;
    const isValidRect = !(triggerRect.width === 0 && triggerRect.height === 0 && triggerRect.x === 0 && triggerRect.y === 0);

    // Calculate position once we have a valid triggerRect, and keep it stable
    if (isValidRect && !positionRef.current) {
      isSavingRef.current = false;
      initialScrollYRef.current = window.scrollY;

      const spaceOnRight = window.innerWidth - triggerRect.right - POPOVER_GAP;
      const spaceOnLeft = triggerRect.left - POPOVER_GAP;
      const preferRight = spaceOnRight >= POPOVER_WIDTH;
      const canFitLeft = spaceOnLeft >= POPOVER_WIDTH;
      const positionRight = preferRight || !canFitLeft;

      let left: number;
      if (positionRight) {
        left = triggerRect.right + POPOVER_GAP;
      } else {
        left = triggerRect.left - POPOVER_WIDTH - POPOVER_GAP;
      }

      let top = triggerRect.top;
      const popoverHeight = 140;
      const minTop = 20;
      const maxTop = window.innerHeight - popoverHeight - 20;
      top = Math.max(minTop, Math.min(top, maxTop));

      positionRef.current = { left, top, positionRight };
    }
  } else {
    positionRef.current = null;
  }

  const position = positionRef.current;

  // Focus input when popover first appears
  const hasFocusedRef = useRef(false);
  useEffect(() => {
    if (position && !hasFocusedRef.current) {
      hasFocusedRef.current = true;
      // Use requestAnimationFrame to focus after the portal renders
      requestAnimationFrame(() => {
        inputRef.current?.focus();
      });
    }
    if (!isOpen) {
      hasFocusedRef.current = false;
    }
  }, [position, isOpen]);


  // Save event - matches CreateEventModal pattern exactly
  const saveEvent = useCallback(async () => {
    if (isSavingRef.current) return; // Prevent double-save

    const currentPending = useCalendarStore.getState().pendingEvent;
    if (!currentPending || !currentPending.title.trim()) {
      cancelCreatingEvent();
      return;
    }

    isSavingRef.current = true;

    const { date, hour, minute = 0, endDate, endHour, endMinute = 0, title, isAllDay, description, location, add_google_meet } = currentPending;
    const dateObj = date instanceof Date ? date : new Date(date);

    // Helper to format timezone offset as ±HH:MM
    const getTimezoneOffset = (d: Date) => {
      const offset = -d.getTimezoneOffset();
      const sign = offset >= 0 ? '+' : '-';
      const absOffset = Math.abs(offset);
      const hours = String(Math.floor(absOffset / 60)).padStart(2, '0');
      const minutes = String(absOffset % 60).padStart(2, '0');
      return `${sign}${hours}:${minutes}`;
    };

    const tzOffset = getTimezoneOffset(dateObj);

    let start_time: string;
    let end_time: string;

    if (isAllDay) {
      // All-day event: start at midnight, end at 23:59:59
      const year = dateObj.getFullYear();
      const month = String(dateObj.getMonth() + 1).padStart(2, '0');
      const day = String(dateObj.getDate()).padStart(2, '0');
      start_time = `${year}-${month}-${day}T00:00:00${tzOffset}`;
      end_time = `${year}-${month}-${day}T23:59:59${tzOffset}`;
    } else {
      // Determine end time - use drag-to-create values if available, otherwise default to 1 hour
      const finalEndHour = endHour !== undefined ? endHour : hour + 1;
      const finalEndMinute = endMinute !== undefined ? endMinute : 0;

      // Handle multi-day events: if end time is past midnight, use next day
      const startDateObj = dateObj;
      // Use explicit endDate if provided, otherwise calculate based on endHour
      const endDateObj = endDate ? new Date(endDate) : new Date(dateObj);

      // Check if end time wraps to next day (either via 23+ hours or via endHour < hour)
      let actualEndHour = finalEndHour;
      const endTimeWrapsToNextDay =
        finalEndHour > 23 ||
        (finalEndHour < hour) ||
        (finalEndHour === hour && finalEndMinute < minute);

      // Only auto-adjust date if no explicit endDate was set
      if (!endDate && endTimeWrapsToNextDay) {
        // If hour exceeds 23, wrap to next day (e.g., 25 -> 1)
        if (actualEndHour > 23) {
          actualEndHour = actualEndHour - 24;
        }
        endDateObj.setDate(endDateObj.getDate() + 1);
      }

      // Format start date
      const startYear = startDateObj.getFullYear();
      const startMonth = String(startDateObj.getMonth() + 1).padStart(2, '0');
      const startDay = String(startDateObj.getDate()).padStart(2, '0');
      const startHourStr = String(hour).padStart(2, '0');
      const startMinuteStr = String(minute).padStart(2, '0');

      // Format end date
      const endYear = endDateObj.getFullYear();
      const endMonth = String(endDateObj.getMonth() + 1).padStart(2, '0');
      const endDay = String(endDateObj.getDate()).padStart(2, '0');
      const endHourStr = String(actualEndHour).padStart(2, '0');
      const endMinuteStr = String(finalEndMinute).padStart(2, '0');

      start_time = `${startYear}-${startMonth}-${startDay}T${startHourStr}:${startMinuteStr}:00${tzOffset}`;
      end_time = `${endYear}-${endMonth}-${endDay}T${endHourStr}:${endMinuteStr}:00${tzOffset}`;
    }

    // Create optimistic event with temporary ID
    const tempId = `temp-${Date.now()}`;
    const optimisticEvent = {
      id: tempId,
      title: title.trim(),
      description: description?.trim() || undefined,
      location: location?.trim() || undefined,
      start_time,
      end_time,
      all_day: isAllDay || false,
      status: 'confirmed' as const,
    };

    // Add optimistic event and close popover immediately (matches CreateEventModal)
    addEvent(optimisticEvent);
    cancelCreatingEvent(); // This clears pendingEvent, closing the popover

    try {
      const createdEvent = await createCalendarEvent({
        title: title.trim(),
        description: description?.trim() || undefined,
        location: location?.trim() || undefined,
        start_time,
        end_time,
        all_day: isAllDay || false,
        add_google_meet: add_google_meet || undefined,
      });
      console.log('Event created successfully:', createdEvent);
      // Replace optimistic event with the real one from server
      replaceEvent(tempId, createdEvent);
    } catch (err) {
      // Remove optimistic event on failure
      removeEvent(tempId);
      console.error('Failed to create event:', err);
    }
  }, [addEvent, replaceEvent, removeEvent, cancelCreatingEvent]);

  // Lock scrolling while popover is open
  useEffect(() => {
    if (!isOpen) return;
    const scrollContainer = document.getElementById('calendar-scroll-container');
    if (!scrollContainer) return;

    const preventScroll = (e: WheelEvent) => {
      e.preventDefault();
    };

    scrollContainer.addEventListener('wheel', preventScroll, { passive: false });
    return () => {
      scrollContainer.removeEventListener('wheel', preventScroll);
    };
  }, [isOpen]);

  // Handle click outside and escape - only depend on isOpen, not the full pendingEvent
  useEffect(() => {
    if (!isOpen) return;

    const handleClickOutside = (e: MouseEvent) => {
      const target = e.target as Node;
      // DatePicker dropdown is portaled to document.body — not inside popoverRef
      if (
        target instanceof Element &&
        target.closest("[data-datepicker-portal]")
      ) {
        return;
      }
      if (popoverRef.current && !popoverRef.current.contains(target)) {
        // Get fresh state from store
        const currentPending = useCalendarStore.getState().pendingEvent;
        if (currentPending?.title.trim()) {
          saveEvent();
        } else {
          eventCreationFlags.pendingEventIsClosing = true;
          cancelCreatingEvent();
          setTimeout(() => { eventCreationFlags.pendingEventIsClosing = false; }, 100);
        }
      }
    };

    const handleEscape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') {
        eventCreationFlags.pendingEventIsClosing = true;
        cancelCreatingEvent();
        setTimeout(() => { eventCreationFlags.pendingEventIsClosing = false; }, 100);
      }
    };

    // Delay adding click listener to prevent immediate close
    const timer = setTimeout(() => {
      document.addEventListener('mousedown', handleClickOutside);
    }, 0);
    document.addEventListener('keydown', handleEscape);

    return () => {
      clearTimeout(timer);
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [isOpen, cancelCreatingEvent, saveEvent]);

  if (!pendingEvent || !position) return null;

  const { date, hour, title } = pendingEvent;

  const formatDisplayDate = () => {
    return date.toLocaleDateString('en-US', {
      weekday: 'long',
      month: 'long',
      day: 'numeric'
    });
  };

  const formatTime = () => {
    if (pendingEvent.isAllDay) {
      return 'All day';
    }
    const startMinute = pendingEvent.minute || 0;
    const actualEndHour = pendingEvent.endHour ?? hour + 1;
    const actualEndMinute = pendingEvent.endMinute ?? 0;

    const startHour12 = hour % 12 || 12;
    const endHour12 = actualEndHour % 12 || 12;
    const startPeriod = hour < 12 ? 'AM' : 'PM';
    const endPeriod = actualEndHour < 12 ? 'AM' : 'PM';

    const startMinuteStr = String(startMinute).padStart(2, '0');
    const endMinuteStr = String(actualEndMinute).padStart(2, '0');

    return `${startHour12}:${startMinuteStr} ${startPeriod} – ${endHour12}:${endMinuteStr} ${endPeriod}`;
  };

  // Handle Enter key to save event from any input field
  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      e.stopPropagation();
      if (title.trim()) {
        saveEvent();
      }
    }
  };

  // Handle title-specific keydown (includes backspace to dismiss)
  const handleTitleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      e.preventDefault();
      e.stopPropagation();
      if (title.trim()) {
        saveEvent();
      }
    } else if (e.key === 'Backspace') {
      // If title is already empty, dismiss the form
      if (!title || title.length === 0) {
        e.preventDefault();
        e.stopPropagation();
        eventCreationFlags.pendingEventIsClosing = true;
        cancelCreatingEvent();
        setTimeout(() => { eventCreationFlags.pendingEventIsClosing = false; }, 100);
      }
    }
  };

  return createPortal(
    <motion.div
      ref={popoverRef}
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      transition={{
        duration: 0.15,
        ease: [0.4, 0, 0.2, 1]
      }}
      style={{
        position: 'fixed',
        top: position.top,
        left: position.left,
        width: POPOVER_WIDTH,
        zIndex: 9999
      }}
      className="bg-white rounded-xl overflow-hidden border border-border-gray shadow-lg"
      onClick={(e) => e.stopPropagation()}
    >
      <div className="p-4">
        {/* Title input */}
        <input
          ref={inputRef}
          type="text"
          value={title}
          onChange={(e) => updatePendingEventTitle(e.target.value)}
          onKeyDown={handleTitleKeyDown}
          placeholder="Add title"
          className="w-full text-base font-semibold text-gray-900 bg-transparent border-none outline-none placeholder:text-gray-400 mb-3"
          autoComplete="off"
        />

        {/* Date and time */}
        <div className="flex items-start gap-2.5 mb-3 pb-3 border-b border-gray-200">
          <Icon icon={Calendar} size={16} className="text-gray-400 mt-0.5 shrink-0" />
          <div className="flex-1 min-w-0">
            <p className="text-xs text-gray-500 mb-1">{formatDisplayDate()}</p>

            {!pendingEvent.isAllDay && isEditingTime ? (
              <div className="space-y-2">
                <div className="flex gap-2 items-center text-xs">
                  <label className="text-gray-500 w-8">Start</label>
                  <InvertedSpinInput
                    min={1}
                    max={12}
                    value={(() => {
                      const h = pendingEvent.hour || 0;
                      return h === 0 ? 12 : h > 12 ? h - 12 : h;
                    })()}
                    onChange={(val) => {
                      const isPM = (pendingEvent.hour || 0) >= 12;
                      let hour24 = val === 12 ? 0 : val;
                      if (isPM) hour24 += 12;
                      updatePendingEventTime(hour24, pendingEvent.minute);
                    }}
                  />
                  <span className="text-gray-400">:</span>
                  <InvertedSpinInput
                    min={0}
                    max={59}
                    step={15}
                    value={pendingEvent.minute || 0}
                    displayValue={String(pendingEvent.minute || 0).padStart(2, '0')}
                    onChange={(val) => updatePendingEventTime(pendingEvent.hour, val)}
                  />
                  <select
                    value={(pendingEvent.hour || 0) >= 12 ? 'PM' : 'AM'}
                    onChange={(e) => {
                      const isPM = e.target.value === 'PM';
                      let h = pendingEvent.hour || 0;
                      const wasAM = h < 12;
                      if (isPM && wasAM) h += 12;
                      else if (!isPM && !wasAM) h -= 12;
                      updatePendingEventTime(h, pendingEvent.minute);
                    }}
                    className="px-1 py-0.5 border border-gray-300 rounded text-gray-900 text-xs"
                  >
                    <option value="AM">AM</option>
                    <option value="PM">PM</option>
                  </select>
                </div>
                <div className="flex gap-2 items-center text-xs">
                  <label className="text-gray-500 w-8">End</label>
                  <InvertedSpinInput
                    min={1}
                    max={12}
                    value={(() => {
                      const h = pendingEvent.endHour ?? (pendingEvent.hour + 1);
                      return h === 0 ? 12 : h > 12 ? h - 12 : h;
                    })()}
                    onChange={(val) => {
                      const currentEndHour = pendingEvent.endHour ?? (pendingEvent.hour + 1);
                      const isPM = currentEndHour >= 12;
                      let hour24 = val === 12 ? 0 : val;
                      if (isPM) hour24 += 12;
                      updatePendingEventEndTime(hour24, pendingEvent.endMinute);
                    }}
                  />
                  <span className="text-gray-400">:</span>
                  <InvertedSpinInput
                    min={0}
                    max={59}
                    step={15}
                    value={pendingEvent.endMinute || 0}
                    displayValue={String(pendingEvent.endMinute || 0).padStart(2, '0')}
                    onChange={(val) => updatePendingEventEndTime(pendingEvent.endHour ?? (pendingEvent.hour + 1), val)}
                  />
                  <select
                    value={(pendingEvent.endHour ?? (pendingEvent.hour + 1)) >= 12 ? 'PM' : 'AM'}
                    onChange={(e) => {
                      const isPM = e.target.value === 'PM';
                      let h = pendingEvent.endHour ?? (pendingEvent.hour + 1);
                      const wasAM = h < 12;
                      if (isPM && wasAM) h += 12;
                      else if (!isPM && !wasAM) h -= 12;
                      updatePendingEventEndTime(h, pendingEvent.endMinute);
                    }}
                    className="px-1 py-0.5 border border-gray-300 rounded text-gray-900 text-xs"
                  >
                    <option value="AM">AM</option>
                    <option value="PM">PM</option>
                  </select>
                </div>
                {/* End date picker */}
                <div className="flex gap-2 items-center text-xs mt-2 pt-2 border-t border-gray-200">
                  <label className="text-gray-500 w-8">End Date</label>
                  <DatePicker
                    value={pendingEvent.endDate || pendingEvent.date}
                    onChange={(dateStr) => {
                      const [year, month, day] = dateStr.split('-');
                      const newDate = new Date(parseInt(year), parseInt(month) - 1, parseInt(day));
                      updatePendingEventEndDate(newDate);
                    }}
                    showQuickActions={false}
                    showClearButton={false}
                    showRelativeDate={false}
                    buttonClassName="flex-1 px-2 py-1 text-xs min-w-0"
                  />
                </div>
                <button
                  onClick={() => setIsEditingTime(false)}
                  className="text-xs text-blue-600 hover:text-blue-700 mt-2"
                >
                  Done
                </button>
              </div>
            ) : !pendingEvent.isAllDay ? (
              <button
                onClick={() => setIsEditingTime(true)}
                className="text-sm text-gray-900 hover:text-blue-600 hover:bg-blue-50 px-2 py-1 rounded transition-colors font-medium"
              >
                {formatTime()}
              </button>
            ) : null}

            {/* All day toggle */}
            <label className="flex items-center gap-2 mt-2 cursor-pointer">
              <input
                type="checkbox"
                checked={pendingEvent.isAllDay || false}
                onChange={(e) => updatePendingEventAllDay(e.target.checked)}
                className="w-4 h-4 rounded-md border-gray-300 accent-black focus:ring-gray-500"
              />
              <span className="text-sm text-gray-700">All day</span>
            </label>
          </div>
        </div>

        {/* Google Meet toggle */}
        {pendingEvent?.add_google_meet ? (
          <button
            type="button"
            onClick={() => togglePendingEventGoogleMeet(false)}
            className="flex items-center gap-2.5 mb-2 w-full text-left px-0 py-1"
          >
            <Icon icon={Video} size={16} className="text-blue-600 shrink-0" />
            <span className="text-sm text-blue-600 underline underline-offset-2">meet.google.com/...</span>
          </button>
        ) : (
          <button
            type="button"
            onClick={() => togglePendingEventGoogleMeet(true)}
            className="flex items-center gap-2.5 mb-2 w-full text-left px-0 py-1"
          >
            <Icon icon={Video} size={16} className="text-gray-400 shrink-0" />
            <span className="text-sm text-gray-400">Add Google Meet</span>
          </button>
        )}

        {/* Location */}
        <div className="flex items-center gap-2.5 mb-3">
          <Icon icon={MapPin} size={16} className="text-gray-400 shrink-0" />
          <input
            type="text"
            value={pendingEvent?.location || ''}
            onChange={(e) => updatePendingEventLocation(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Add location"
            className="flex-1 text-sm text-gray-900 bg-transparent border-none outline-none placeholder:text-gray-400 px-0 py-1"
          />
        </div>

        {/* Description */}
        <div className="flex items-start gap-2.5">
          <Icon icon={FileEdit} size={16} className="text-gray-400 shrink-0 mt-1.5" />
          <textarea
            value={pendingEvent?.description || ''}
            onChange={(e) => updatePendingEventDescription(e.target.value)}
            placeholder="Add description"
            className="flex-1 text-sm text-gray-900 bg-transparent border-none outline-none placeholder:text-gray-400 px-0 py-1 resize-none"
            rows={2}
          />
        </div>
      </div>
    </motion.div>,
    document.body
  );
}
