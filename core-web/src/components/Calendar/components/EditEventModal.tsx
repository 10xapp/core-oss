import { useState, useRef, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { motion } from 'motion/react';
import { XMarkIcon } from '@heroicons/react/24/outline';
import { HugeiconsIcon } from '@hugeicons/react';
import { PencilEdit01Icon, Video01Icon } from '@hugeicons-pro/core-stroke-standard';
import { updateCalendarEvent } from '../../../api/client';
import TimePickerInput from './TimePickerInput';
import type { CalendarEvent } from '../../../api/client';
import { useCalendarStore } from '../../../stores/calendarStore';

interface EditEventModalProps {
  event: CalendarEvent;
  onClose: () => void;
  onUpdated: () => void;
}

// Parse ISO date string to local date/time values
const parseToLocal = (isoString: string) => {
  const date = new Date(isoString);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  return {
    date: `${year}-${month}-${day}`,
    time: `${hours}:${minutes}`
  };
};

// Validate a meeting link URL and return it only if it uses a safe scheme.
// Returns the sanitized absolute URL string if safe, otherwise null.
const getSafeMeetingLink = (rawLink: string): string | null => {
  const trimmed = rawLink.trim();
  if (!trimmed) {
    return null;
  }

  try {
    const url = new URL(trimmed, window.location.origin);
    // Only allow http and https — this rejects javascript:, data:, vbscript:, etc.
    if (url.protocol === 'http:' || url.protocol === 'https:') {
      return url.href;
    }
    return null;
  } catch {
    return null;
  }
};

// Check if a validated URL points to Google Meet by hostname.
const isGoogleMeetUrl = (safeUrl: string): boolean => {
  try {
    return new URL(safeUrl).hostname === 'meet.google.com';
  } catch {
    return false;
  }
};

export default function EditEventModal({
  event,
  onClose,
  onUpdated
}: EditEventModalProps) {
  const updateEventInStore = useCalendarStore((state) => state.updateEvent);
  const formRef = useRef<HTMLFormElement>(null);

  const startLocal = parseToLocal(event.start_time);
  const endLocal = parseToLocal(event.end_time);

  // Handle Cmd/Ctrl+Enter to submit from anywhere (including textarea)
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') {
        e.preventDefault();
        formRef.current?.requestSubmit();
      }
    };
    document.addEventListener('keydown', handleKeyDown);
    return () => document.removeEventListener('keydown', handleKeyDown);
  }, []);

  const [title, setTitle] = useState(event.title);
  const [startDate, setStartDate] = useState(startLocal.date);
  const [endDate, setEndDate] = useState(endLocal.date);
  const [startTime, setStartTime] = useState(startLocal.time);
  const [endTime, setEndTime] = useState(endLocal.time);
  const [location, setLocation] = useState(event.location || '');
  const [description, setDescription] = useState(event.description || '');
  const [meetingLink, setMeetingLink] = useState(event.meeting_link || '');
  const [isAllDay, setIsAllDay] = useState(event.all_day);
  const [isEditingLink, setIsEditingLink] = useState(!event.meeting_link);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;

    setIsLoading(true);
    setError('');

    // Capture current store state BEFORE optimistic update for potential revert
    const currentEvent = useCalendarStore.getState().events.find(ev => ev.id === event.id);
    const originalState = currentEvent ? { ...currentEvent } : null;

    const startDateTime = isAllDay
      ? `${startDate}T00:00:00`
      : `${startDate}T${startTime}:00`;
    const endDateTime = isAllDay
      ? `${endDate}T23:59:59`
      : `${endDate}T${endTime}:00`;

    const updates = {
      title: title.trim(),
      description: description.trim() || undefined,
      start_time: startDateTime,
      end_time: endDateTime,
      all_day: isAllDay,
      location: location.trim() || undefined,
      meeting_link: meetingLink.trim() || undefined
    };

    // Optimistically update the store immediately
    updateEventInStore(event.id, updates);
    onUpdated();

    // Then sync with server in background
    // Spread all existing event fields to prevent data loss (attendees, recurrence, etc.)
    try {
      await updateCalendarEvent(event.id, {
        ...currentEvent,
        ...updates
      });
    } catch (err) {
      // Revert to captured state (not stale props) on failure
      if (originalState) {
        updateEventInStore(event.id, originalState);
      }
      console.error('Failed to update event:', err);
    }
  };

  const inputStyles = "w-full bg-white border border-border-gray rounded-lg px-3 py-2.5 text-xs text-gray-900 placeholder:text-text-tertiary outline-none focus:border-text-tertiary transition-colors";

  return createPortal(
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.15 }}
      className="fixed inset-0 z-9999 flex items-center justify-center bg-black/30"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 10 }}
        transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
        className="bg-white rounded-lg overflow-hidden"
        style={{
          width: '100%',
          maxWidth: 600,
          margin: '0 16px',
          boxShadow: '0 24px 48px -12px rgba(0, 0, 0, 0.15)',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-start justify-between px-6 pt-6 pb-0">
          <h2 className="text-lg font-medium text-gray-900">Edit Event</h2>
          <button
            onClick={onClose}
            className="p-1.5 -mr-1 text-gray-300 hover:text-gray-500 rounded-lg transition-colors"
          >
            <XMarkIcon className="w-4.5 h-4.5 stroke-2" />
          </button>
        </div>

        <form ref={formRef} onSubmit={handleSubmit} className="px-6 pb-6 pt-5 space-y-5">
          {error && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600">
              {error}
            </div>
          )}

          <div>
            <label className="block text-xs text-text-secondary mb-1.5">Title</label>
            <input
              type="text"
              value={title}
              onChange={(e) => setTitle(e.target.value)}
              className="w-full bg-white border border-border-gray rounded-lg px-3 py-2.5 text-sm text-gray-900 placeholder:text-text-tertiary outline-none focus:border-text-tertiary transition-colors"
              placeholder="Event title"
              autoFocus
              required
            />
          </div>

          <div className="grid grid-cols-1 sm:grid-cols-2 gap-5">
            <div className="space-y-4">
              <div>
                <label className="block text-xs text-text-secondary mb-1.5">Meeting Link</label>
                {(() => {
                  const safeMeetingLink = getSafeMeetingLink(meetingLink);
                  if (safeMeetingLink && isGoogleMeetUrl(safeMeetingLink)) {
                    return (
                      <a
                        href={safeMeetingLink}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-2 px-3 py-2.5 bg-blue-50 border border-blue-200 rounded-lg text-xs text-blue-600 font-medium hover:bg-blue-100 transition-colors"
                      >
                        <HugeiconsIcon icon={Video01Icon} size={14} className="text-blue-600 shrink-0" />
                        Join with Google Meet
                      </a>
                    );
                  }
                  if (safeMeetingLink && !isEditingLink) {
                    return (
                      <a
                        href={safeMeetingLink}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-2 px-3 py-2.5 bg-neutral-50 border border-border-subtle rounded-lg text-xs text-text-secondary hover:bg-neutral-100 transition-colors"
                      >
                        <HugeiconsIcon icon={PencilEdit01Icon} size={14} className="text-text-secondary shrink-0" />
                        <span className="truncate">{meetingLink.trim()}</span>
                      </a>
                    );
                  }
                  return (
                    <input
                      type="url"
                      value={meetingLink}
                      onChange={(e) => setMeetingLink(e.target.value)}
                      onBlur={() => { if (meetingLink.trim()) setIsEditingLink(false); }}
                      className={inputStyles}
                      placeholder="Add link"
                      autoFocus={isEditingLink && !!event.meeting_link}
                    />
                  );
                })()}
              </div>

              <div>
                <label className="block text-xs text-text-secondary mb-1.5">Start</label>
                <div className="flex gap-2">
                  <input
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className={inputStyles}
                    required
                  />
                  {!isAllDay && (
                    <div className="w-32 shrink-0">
                      <TimePickerInput
                        value={startTime}
                        onChange={setStartTime}
                        className={inputStyles}
                      />
                    </div>
                  )}
                </div>
              </div>
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-xs text-text-secondary mb-1.5">Location</label>
                <input
                  type="text"
                  value={location}
                  onChange={(e) => setLocation(e.target.value)}
                  className={inputStyles}
                  placeholder="Add location"
                />
              </div>

              <div>
                <label className="block text-xs text-text-secondary mb-1.5">End</label>
                <div className="flex gap-2">
                  {!isAllDay && (
                    <div className="w-32 shrink-0">
                      <TimePickerInput
                        value={endTime}
                        onChange={setEndTime}
                        className={inputStyles}
                      />
                    </div>
                  )}
                  <input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className={inputStyles}
                    required
                  />
                </div>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="editAllDay"
              checked={isAllDay}
              onChange={(e) => setIsAllDay(e.target.checked)}
              className="w-4 h-4 rounded border-border-gray text-gray-900 focus:ring-gray-900/20"
            />
            <label htmlFor="editAllDay" className="text-xs text-text-secondary">
              All day event
            </label>
          </div>

          <div>
            <label className="block text-xs text-text-secondary mb-1.5">Description</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={4}
              className={`${inputStyles} resize-none min-h-[100px]`}
              placeholder="Add description..."
            />
          </div>

          <div className="flex justify-end gap-3 pt-4 border-t border-border-light">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 text-xs font-medium text-text-secondary hover:text-gray-900 hover:bg-bg-gray rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading || !title.trim()}
              className="px-4 py-2 bg-black text-white rounded-lg text-xs font-medium disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {isLoading ? 'Saving...' : 'Save Changes'}
            </button>
          </div>
        </form>
      </motion.div>
    </motion.div>,
    document.body
  );
}
