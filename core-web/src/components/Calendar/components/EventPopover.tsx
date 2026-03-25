import { useState, useEffect, useRef, useMemo } from 'react';
import { createPortal } from 'react-dom';
import { motion, AnimatePresence } from 'motion/react';
import { HugeiconsIcon } from '@hugeicons/react';
import { Calendar03Icon, Location01Icon, Video01Icon, Delete02Icon, PencilEdit01Icon } from '@hugeicons-pro/core-stroke-standard';
import type { CalendarEvent } from '../../../api/client';
import { updateCalendarEvent } from '../../../api/client';
import { useCalendarStore } from '../../../stores/calendarStore';
import { useWorkspaceStore } from '../../../stores/workspaceStore';
import { useProjectMembers } from '../../../hooks/queries/useProjects';
import { avatarGradient } from '../../../utils/avatarGradient';
import EditEventModal from './EditEventModal';
import DatePicker from '../../ui/DatePicker';

interface EventPopoverProps {
  event: CalendarEvent;
  triggerRect: DOMRect;
  onClose: () => void;
  onUpdated: () => void;
  onDeleted: () => void;
}

const POPOVER_WIDTH = 340;
const POPOVER_GAP = 12;

type EditingField = 'title' | 'location' | 'link' | 'description' | 'date' | null;

export default function EventPopover({
  event,
  triggerRect,
  onClose,
  onUpdated,
  onDeleted
}: EventPopoverProps) {
  console.log('[EventPopover] Rendering with event:', {
    eventId: event.id,
    eventTitle: event.title,
    triggerRect: { top: triggerRect.top, left: triggerRect.left, right: triggerRect.right, bottom: triggerRect.bottom }
  });

  const deleteEvent = useCalendarStore((state) => state.deleteEvent);
  const updateEventInStore = useCalendarStore((state) => state.updateEvent);
  const respondToEvent = useCalendarStore((state) => state.respondToEvent);

  // Get workspace members for attendee profile pictures
  const activeWorkspaceId = useWorkspaceStore((state) => state.activeWorkspaceId);
  const { data: members = [] } = useProjectMembers(activeWorkspaceId);

  // Create a map of email -> member for quick lookup
  const membersByEmail = useMemo(() => {
    const map = new Map<string, typeof members[0]>();
    for (const member of members) {
      if (member.email) {
        map.set(member.email.toLowerCase(), member);
      }
    }
    return map;
  }, [members]);

  // Subscribe to store changes for this specific event
  // This ensures we always display the latest data (including optimistic updates)
  const storeEvent = useCalendarStore((state) => {
    const found = state.events.find(e => e.id === event.id);
    return found || event;
  });

  // Check if the event still exists in the store (temp events get removed after API call)
  const eventExistsInStore = useCalendarStore((state) =>
    state.events.some(e => e.id === event.id)
  );

  // Close popover if temp event is removed (replaced with real event or failed)
  useEffect(() => {
    if (event.id.startsWith('temp-') && !eventExistsInStore) {
      onClose();
    }
  }, [event.id, eventExistsInStore, onClose]);

  // Sync form state when store event changes (e.g., optimistic updates)
  // This updates the displayed values without resetting user edits in progress
  const [editingField, setEditingField] = useState<EditingField>(null);
  const [editTitle, setEditTitle] = useState(storeEvent.title);
  const [editLocation, setEditLocation] = useState(storeEvent.location || '');
  const [editLink, setEditLink] = useState(storeEvent.meeting_link || '');
  const [editDescription, setEditDescription] = useState(storeEvent.description || '');
  const [editDate, setEditDate] = useState(getLocalDateValue(storeEvent.start_time));
  const [editEndDate, setEditEndDate] = useState(getLocalDateValue(storeEvent.end_time));
  const [editStartTime, setEditStartTime] = useState(getLocalTimeValue(storeEvent.start_time));
  const [editEndTime, setEditEndTime] = useState(getLocalTimeValue(storeEvent.end_time));
  const [editAllDay, setEditAllDay] = useState(storeEvent.all_day);
  const [saveError, setSaveError] = useState('');
  const [isSaving, setIsSaving] = useState(false);

  // Sync form state when storeEvent changes, but only if not actively editing that field
  // This allows optimistic updates to show while preserving user edits in progress
  useEffect(() => {
    if (!editingField) {
      setEditTitle(storeEvent.title);
      setEditLocation(storeEvent.location || '');
      setEditLink(storeEvent.meeting_link || '');
      setEditDescription(storeEvent.description || '');
      setEditDate(getLocalDateValue(storeEvent.start_time));
      setEditEndDate(getLocalDateValue(storeEvent.end_time));
      setEditStartTime(getLocalTimeValue(storeEvent.start_time));
      setEditEndTime(getLocalTimeValue(storeEvent.end_time));
      setEditAllDay(storeEvent.all_day);
    }
  }, [storeEvent, editingField]);

  // Modal state
  const [showEditModal, setShowEditModal] = useState(false);

  // Dynamic height state
  const [contentHeight, setContentHeight] = useState<number>(0);
  const contentRef = useRef<HTMLDivElement>(null);
  const popoverRef = useRef<HTMLDivElement>(null);

  // Measure content height for smooth transitions
  useEffect(() => {
    if (contentRef.current) {
      const height = contentRef.current.scrollHeight;
      setContentHeight(height);
    }
  }, [editingField, storeEvent, editTitle, editLocation, editLink, editDescription, editAllDay]);

  // Calculate position manually based on triggerRect
  // For wide triggers (like in day view), try multiple positions to find best fit
  const EDGE_MARGIN = 20;

  // Try positioning on the left of the trigger first (best for day view with wide events)
  let left = triggerRect.left - POPOVER_WIDTH - POPOVER_GAP;

  // Check if it fits on the left
  const fitsOnLeft = left >= EDGE_MARGIN;

  if (!fitsOnLeft) {
    // If doesn't fit on left, try positioning on the right
    left = triggerRect.right + POPOVER_GAP;
    const fitsOnRight = left + POPOVER_WIDTH <= window.innerWidth - EDGE_MARGIN;

    if (!fitsOnRight) {
      // If doesn't fit on right either, center it on screen
      left = (window.innerWidth - POPOVER_WIDTH) / 2;
    }
  }

  // Final bounds check to ensure it's always visible
  if (left + POPOVER_WIDTH > window.innerWidth - EDGE_MARGIN) {
    left = window.innerWidth - POPOVER_WIDTH - EDGE_MARGIN;
  }

  if (left < EDGE_MARGIN) {
    left = EDGE_MARGIN;
  }

  // Calculate top position - only shift up if popover would overflow bottom
  const popoverHeight = 350; // Approximate popover height
  const bottomOverflow = triggerRect.top + popoverHeight - window.innerHeight + 20;
  const top = bottomOverflow > 0
    ? Math.max(20, triggerRect.top - bottomOverflow)
    : triggerRect.top;

  const floatingStyles: React.CSSProperties = {
    position: 'fixed',
    top,
    left,
    zIndex: 9999,
  };

  // Log the actual positioning (after bounds checking)
  const triggerWidth = triggerRect.right - triggerRect.left;
  console.log('[EventPopover] Final positioning:', {
    left,
    top,
    width: POPOVER_WIDTH,
    triggerWidth,
    strategy: left < triggerRect.left ? 'left of trigger' : left > triggerRect.right ? 'right of trigger' : 'centered on screen',
    windowWidth: window.innerWidth,
    windowHeight: window.innerHeight,
    willRenderFrom: `${left}px to ${left + POPOVER_WIDTH}px horizontally`,
    triggerRect: { left: triggerRect.left, right: triggerRect.right }
  });

  // Lock scrolling while popover is open
  useEffect(() => {
    const scrollContainer = document.getElementById('calendar-scroll-container');
    if (!scrollContainer) return;

    const preventScroll = (e: WheelEvent) => {
      e.preventDefault();
    };

    scrollContainer.addEventListener('wheel', preventScroll, { passive: false });
    return () => {
      scrollContainer.removeEventListener('wheel', preventScroll);
    };
  }, []);

  // Close on outside click (but not when editing or modal is open)
  useEffect(() => {
    const handleClickOutside = (e: MouseEvent) => {
      if (showEditModal || editingField) return;
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        // Stop propagation to prevent the click from reaching underlying time slots
        e.stopPropagation();
        onClose();
      }
    };

    const handleEscape = (e: KeyboardEvent) => {
      if (showEditModal) return;
      if (e.key === 'Escape') {
        if (editingField) {
          setEditingField(null);
        } else {
          onClose();
        }
      }
    };

    const timer = setTimeout(() => {
      document.addEventListener('mousedown', handleClickOutside);
    }, 0);
    document.addEventListener('keydown', handleEscape);

    return () => {
      clearTimeout(timer);
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [onClose, showEditModal, editingField]);

  // Utility functions for date/time
  function getLocalDateValue(isoString: string): string {
    const date = new Date(isoString);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  function getLocalTimeValue(isoString: string): string {
    const date = new Date(isoString);
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${hours}:${minutes}`;
  }

  function formatDisplayDate(isoString: string): string {
    const date = new Date(isoString);
    return date.toLocaleDateString('en-US', {
      weekday: 'long',
      month: 'long',
      day: 'numeric'
    });
  }

  function formatDisplayTime(isoString: string): string {
    const date = new Date(isoString);
    return date.toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  }

  // Check if event is still being created (has temp ID)
  const isTempEvent = storeEvent.id.startsWith('temp-');

  // Save handler
  const handleSaveField = async (field: EditingField, value: any, endDateValue?: string) => {
    if (!field) return;

    // Don't allow saving if event is still being created
    if (isTempEvent) {
      setSaveError('Please wait for event to finish saving');
      return;
    }

    setSaveError('');
    setIsSaving(true);

    // Get current event for optimistic update
    const currentEvent = useCalendarStore.getState().events.find(e => e.id === event.id);
    const originalState = currentEvent ? { ...currentEvent } : null;

    const updates: Partial<CalendarEvent> = {};

    try {
      // Validate and prepare updates
      switch (field) {
        case 'title': {
          const trimmed = value.trim();
          if (!trimmed) {
            setSaveError('Title cannot be empty');
            setIsSaving(false);
            return;
          }
          updates.title = trimmed;
          break;
        }
        case 'location':
          updates.location = value.trim() || undefined;
          break;
        case 'link':
          updates.meeting_link = value.trim() || undefined;
          break;
        case 'description':
          updates.description = value.trim() || undefined;
          break;
        case 'date': {
          if (!value) {
            setSaveError('Start date is required');
            setIsSaving(false);
            return;
          }
          const finalEndDate = endDateValue || value;
          if (!finalEndDate) {
            setSaveError('End date is required');
            setIsSaving(false);
            return;
          }
          // Validate that end date is not before start date
          if (finalEndDate < value) {
            setSaveError('End date cannot be before start date');
            setIsSaving(false);
            return;
          }
          // Create ISO strings with timezone
          if (editAllDay) {
            updates.start_time = `${value}T00:00:00`;
            updates.end_time = `${finalEndDate}T23:59:59`;
          } else {
            if (!editStartTime || !editEndTime) {
              setSaveError('Start and end times are required');
              setIsSaving(false);
              return;
            }
            updates.start_time = `${value}T${editStartTime}:00`;
            updates.end_time = `${finalEndDate}T${editEndTime}:00`;
          }
          break;
        }
      }

      // Optimistic update
      updateEventInStore(event.id, updates);
      setEditingField(null);

      // API call in background
      await updateCalendarEvent(event.id, {
        ...currentEvent,
        ...updates
      });

      onUpdated();
    } catch (err) {
      // Revert on error
      if (originalState) {
        updateEventInStore(event.id, originalState);
      }
      setSaveError(`Failed to save ${field}`);
      console.error(`Failed to save ${field}:`, err);
    } finally {
      setIsSaving(false);
    }
  };

  // Event handlers for editing
  const handleTitleBlur = async () => {
    await handleSaveField('title', editTitle);
  };

  const handleLocationBlur = async () => {
    await handleSaveField('location', editLocation);
  };

  const handleLinkBlur = async () => {
    await handleSaveField('link', editLink);
  };

  const handleDescriptionBlur = async () => {
    await handleSaveField('description', editDescription);
  };

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement | HTMLTextAreaElement>) => {
    if (e.key === 'Escape') {
      setEditingField(null);
    } else if (e.key === 'Enter' && e.currentTarget.tagName !== 'TEXTAREA') {
      e.preventDefault();
      handleSaveField(editingField, e.currentTarget.value);
    } else if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && e.currentTarget.tagName === 'TEXTAREA') {
      e.preventDefault();
      handleSaveField(editingField, e.currentTarget.value);
    }
  };

  const handleEditModalClose = () => {
    setShowEditModal(false);
  };

  const handleEditSuccess = () => {
    setShowEditModal(false);
    onUpdated();
  };

  const handleDelete = async () => {
    setSaveError('');
    onDeleted();

    try {
      await deleteEvent(event.id);
    } catch (err) {
      console.error('Delete failed, event restored:', err);
    }
  };

  return createPortal(
    <AnimatePresence>
      <motion.div
        ref={popoverRef}
        initial={{
          opacity: 0,
          scale: 0.95
        }}
        animate={{
          opacity: 1,
          scale: 1
        }}
        exit={{
          opacity: 0,
          scale: 0.95
        }}
        transition={{
          duration: 0.15,
          ease: [0.4, 0, 0.2, 1]
        }}
        onAnimationComplete={() => {
          console.log('[EventPopover] Animation complete, popoverRef.current:', popoverRef.current);
          if (popoverRef.current) {
            const rect = popoverRef.current.getBoundingClientRect();
            console.log('[EventPopover] Popover element in DOM at rect:', { top: rect.top, left: rect.left, width: rect.width, height: rect.height });
          }
        }}
        style={{
          ...floatingStyles,
          width: POPOVER_WIDTH,
          zIndex: 9999,
        }}
        className="bg-white rounded-xl overflow-hidden border border-border-gray shadow-lg"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Content with dynamic height */}
        <div
          style={{
            maxHeight: contentHeight || 'auto',
            transition: 'max-height 0.2s ease-out',
            overflow: 'hidden',
          }}
        >
          <div ref={contentRef} className="relative">
            {/* Temp event indicator */}
            {isTempEvent && (
              <div className="mx-4 mt-4 p-2.5 bg-blue-50 border border-blue-200 rounded-lg text-xs text-blue-600">
                Saving event...
              </div>
            )}
            {/* Error state */}
            {saveError && !isTempEvent && (
              <div className="mx-4 mt-4 p-2.5 bg-red-50 border border-red-200 rounded-lg text-xs text-red-600">
                {saveError}
              </div>
            )}

            <div className="p-4">
              {/* Header with actions */}
              <div className="flex items-start justify-between gap-3 mb-3">
                {editingField === 'title' ? (
                  <input
                    type="text"
                    value={editTitle}
                    onChange={(e) => setEditTitle(e.target.value)}
                    onBlur={handleTitleBlur}
                    onKeyDown={handleKeyDown}
                    autoFocus
                    className="flex-1 text-base font-semibold bg-gray-50 border border-gray-200 rounded px-2 py-1 text-gray-900 focus:outline-none focus:ring-2 focus:ring-gray-900/10"
                    disabled={isSaving}
                  />
                ) : (
                  <h3
                    onClick={() => setEditingField('title')}
                    className="text-base font-semibold text-gray-900 leading-snug cursor-pointer hover:bg-gray-100 hover:rounded px-1 flex-1"
                  >
                    {storeEvent.title}
                  </h3>
                )}
                <div className="flex items-center gap-0.5 shrink-0">
                  <button
                    onClick={() => setShowEditModal(true)}
                    className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-md transition-colors"
                    title="Edit all fields"
                  >
                    <HugeiconsIcon icon={PencilEdit01Icon} size={16} strokeWidth={2} />
                  </button>
                  <button
                    onClick={handleDelete}
                    className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-md transition-colors"
                    title="Delete"
                    disabled={isSaving}
                  >
                    <HugeiconsIcon icon={Delete02Icon} size={16} strokeWidth={2} />
                  </button>
                </div>
              </div>

              {/* Date and time */}
              <div className="flex items-start gap-2.5 mb-3">
                <HugeiconsIcon icon={Calendar03Icon} size={16} className="text-gray-400 mt-0.5 shrink-0" />
                <div className="flex-1">
                  {editingField === 'date' ? (
                    <div className="space-y-3 bg-gray-50 p-3 rounded-lg border border-gray-200">
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="block text-xs font-medium text-gray-700 mb-1.5">Start Date</label>
                          <DatePicker
                            value={editDate}
                            onChange={(newDate) => {
                              setEditDate(newDate);
                              // Auto-update end date if start date is later
                              if (newDate > editEndDate) {
                                setEditEndDate(newDate);
                              }
                            }}
                            showQuickActions={false}
                            showClearButton={false}
                            showRelativeDate={false}
                            showIcon={false}
                            buttonClassName="w-full text-sm py-2 justify-start bg-white border border-gray-200 rounded-lg hover:bg-white"
                          />
                        </div>
                        <div>
                          <label className="block text-xs font-medium text-gray-700 mb-1.5">End Date</label>
                          <DatePicker
                            value={editEndDate}
                            onChange={setEditEndDate}
                            showQuickActions={false}
                            showClearButton={false}
                            showRelativeDate={false}
                            showIcon={false}
                            buttonClassName="w-full text-sm py-2 justify-start bg-white border border-gray-200 rounded-lg hover:bg-white"
                            align="right"
                          />
                        </div>
                      </div>

                      {!editAllDay && (
                        <div className="grid grid-cols-2 gap-2">
                          <div>
                            <label className="block text-xs font-medium text-gray-700 mb-1.5">Start Time</label>
                            <input
                              type="time"
                              value={editStartTime}
                              onChange={(e) => setEditStartTime(e.target.value)}
                              className="w-full px-3 py-2 text-sm bg-white border border-gray-200 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500"
                              disabled={isSaving}
                            />
                          </div>
                          <div>
                            <label className="block text-xs font-medium text-gray-700 mb-1.5">End Time</label>
                            <input
                              type="time"
                              value={editEndTime}
                              onChange={(e) => setEditEndTime(e.target.value)}
                              className="w-full px-3 py-2 text-sm bg-white border border-gray-200 rounded-lg text-gray-900 focus:outline-none focus:ring-2 focus:ring-blue-500"
                              disabled={isSaving}
                            />
                          </div>
                        </div>
                      )}

                      <div className="flex items-center gap-2 pt-1">
                        <input
                          type="checkbox"
                          id="allDayEdit"
                          checked={editAllDay}
                          onChange={(e) => setEditAllDay(e.target.checked)}
                          className="w-4 h-4 rounded border-gray-300 text-gray-900 focus:ring-gray-900/20"
                        />
                        <label htmlFor="allDayEdit" className="text-sm text-gray-700">
                          All day event
                        </label>
                      </div>

                      <div className="flex gap-2 justify-end pt-1">
                        <button
                          onClick={() => setEditingField(null)}
                          className="px-3 py-1 text-xs text-gray-600 hover:text-gray-900 hover:bg-gray-200 rounded transition-colors"
                        >
                          Cancel
                        </button>
                        <button
                          onClick={() => handleSaveField('date', editDate, editEndDate)}
                          disabled={isSaving}
                          className="px-3 py-1 text-xs bg-gray-900 text-white rounded hover:bg-gray-800 disabled:opacity-50 transition-colors"
                        >
                          {isSaving ? 'Saving...' : 'Save'}
                        </button>
                      </div>
                    </div>
                  ) : (
                    <div
                      onClick={() => setEditingField('date')}
                      className="cursor-pointer hover:bg-gray-100 hover:rounded px-1 py-0.5 -mx-1 inline-block"
                    >
                      {(() => {
                        const startDate = getLocalDateValue(storeEvent.start_time);
                        const endDate = getLocalDateValue(storeEvent.end_time);
                        const isMultiDay = startDate !== endDate;

                        if (isMultiDay) {
                          return (
                            <>
                              <p className="text-sm text-gray-900">{formatDisplayDate(storeEvent.start_time)} – {formatDisplayDate(storeEvent.end_time)}</p>
                              {storeEvent.all_day ? (
                                <p className="text-xs text-gray-500">All day</p>
                              ) : (
                                <p className="text-xs text-gray-500">
                                  {formatDisplayTime(storeEvent.start_time)} – {formatDisplayTime(storeEvent.end_time)}
                                </p>
                              )}
                            </>
                          );
                        } else {
                          return (
                            <>
                              <p className="text-sm text-gray-900">{formatDisplayDate(storeEvent.start_time)}</p>
                              {storeEvent.all_day ? (
                                <p className="text-xs text-gray-500">All day</p>
                              ) : (
                                <p className="text-xs text-gray-500">
                                  {formatDisplayTime(storeEvent.start_time)} – {formatDisplayTime(storeEvent.end_time)}
                                </p>
                              )}
                            </>
                          );
                        }
                      })()}
                    </div>
                  )}
                </div>
              </div>

              {/* Meeting Link field - positioned after time */}
              {storeEvent?.meeting_link ? (
                <div className="flex items-start gap-2.5 mb-3">
                  <HugeiconsIcon icon={Video01Icon} size={16} className={`mt-0.5 shrink-0 ${storeEvent.meeting_link.includes('meet.google.com') ? 'text-blue-600' : 'text-gray-400'}`} />
                  <div className="flex-1">
                    {storeEvent.meeting_link.includes('meet.google.com') ? (
                      <a
                        href={storeEvent.meeting_link}
                        target="_blank"
                        rel="noopener noreferrer"
                        onClick={(e) => e.stopPropagation()}
                        className="inline-flex items-center gap-1.5 text-sm text-blue-600 font-medium hover:text-blue-700 hover:underline"
                      >
                        Join with Google Meet
                      </a>
                    ) : (
                      <a
                        href={storeEvent.meeting_link}
                        target="_blank"
                        rel="noopener noreferrer"
                        onClick={(e) => e.stopPropagation()}
                        className="text-sm text-blue-600 hover:underline break-all"
                      >
                        {storeEvent.meeting_link}
                      </a>
                    )}
                  </div>
                </div>
              ) : editingField === 'link' ? (
                <div className="flex items-start gap-2.5 mb-3">
                  <HugeiconsIcon icon={Video01Icon} size={16} className="text-gray-400 mt-0.5 shrink-0" />
                  <div className="flex-1">
                    <input
                      type="url"
                      value={editLink}
                      onChange={(e) => setEditLink(e.target.value)}
                      onBlur={handleLinkBlur}
                      onKeyDown={handleKeyDown}
                      autoFocus
                      placeholder="https://..."
                      className="w-full px-2 py-1 text-sm bg-gray-50 border border-gray-200 rounded text-gray-900 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-900/10"
                      disabled={isSaving}
                    />
                  </div>
                </div>
              ) : null}

              {/* Location */}
              {storeEvent.location || editingField === 'location' ? (
                <div className="flex items-start gap-2.5 mb-3">
                  <HugeiconsIcon icon={Location01Icon} size={16} className="text-gray-400 mt-0.5 shrink-0" />
                  <div className="flex-1">
                    {editingField === 'location' ? (
                      <input
                        type="text"
                        value={editLocation}
                        onChange={(e) => setEditLocation(e.target.value)}
                        onBlur={handleLocationBlur}
                        onKeyDown={handleKeyDown}
                        autoFocus
                        placeholder="Add location"
                        className="w-full px-2 py-1 text-sm bg-gray-50 border border-gray-200 rounded text-gray-900 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-900/10"
                        disabled={isSaving}
                      />
                    ) : (
                      <p
                        onClick={() => setEditingField('location')}
                        className="text-sm text-gray-700 cursor-pointer hover:bg-gray-100 hover:rounded px-1"
                      >
                        {storeEvent.location}
                      </p>
                    )}
                  </div>
                </div>
              ) : (
                <div className="flex items-start gap-2.5 mb-3">
                  <HugeiconsIcon icon={Location01Icon} size={16} className="text-gray-400 mt-0.5 shrink-0" />
                  <p
                    onClick={() => setEditingField('location')}
                    className="text-sm text-gray-400 cursor-pointer hover:text-gray-600 hover:bg-gray-100 hover:rounded px-1"
                  >
                    Add location
                  </p>
                </div>
              )}

              {/* Description */}
              {storeEvent.description || editingField === 'description' ? (
                <div className="pt-3 mt-3 border-t border-gray-100">
                  {editingField === 'description' ? (
                    <textarea
                      value={editDescription}
                      onChange={(e) => setEditDescription(e.target.value)}
                      onBlur={handleDescriptionBlur}
                      onKeyDown={handleKeyDown}
                      autoFocus
                      placeholder="Add description"
                      rows={3}
                      className="w-full px-2 py-1 text-sm bg-gray-50 border border-gray-200 rounded text-gray-900 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-900/10 resize-none"
                      disabled={isSaving}
                    />
                  ) : (
                    <p
                      onClick={() => setEditingField('description')}
                      className="text-sm text-gray-600 whitespace-pre-wrap leading-relaxed cursor-pointer hover:bg-gray-100 hover:rounded px-1"
                    >
                      {storeEvent.description}
                    </p>
                  )}
                </div>
              ) : (
                <div className="pt-3 mt-3 border-t border-gray-100">
                  <p
                    onClick={() => setEditingField('description')}
                    className="text-sm text-gray-400 cursor-pointer hover:text-gray-600 hover:bg-gray-100 hover:rounded px-1 py-0.5 inline-block"
                  >
                    Add description
                  </p>
                </div>
              )}

              {/* Attendees - read only */}
              {storeEvent.attendees && storeEvent.attendees.length > 0 && (
                <div className="pt-3 mt-3 border-t border-gray-100">
                  <p className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                    Attendees
                  </p>
                  <div className="space-y-1.5">
                    {storeEvent.attendees.map((attendee, i) => {
                      const member = membersByEmail.get(attendee.email.toLowerCase());
                      const displayName = attendee.display_name || member?.name || attendee.email;
                      const initials = displayName
                        .split(' ')
                        .map((n) => n[0])
                        .join('')
                        .toUpperCase()
                        .slice(0, 2) || '?';

                      return (
                        <div key={i} className="flex items-center gap-2">
                          <div
                            className="w-5 h-5 rounded-full flex items-center justify-center shrink-0 overflow-hidden"
                            style={!member?.avatar_url ? { background: avatarGradient(displayName) } : undefined}
                          >
                            {member?.avatar_url ? (
                              <img
                                src={member.avatar_url}
                                alt={displayName}
                                className="w-full h-full object-cover"
                              />
                            ) : (
                              <span className="text-[8px] font-medium text-white">
                                {initials}
                              </span>
                            )}
                          </div>
                          <span className="text-sm text-gray-700">
                            {displayName}
                            {attendee.response_status && (
                              <span className="text-gray-400 text-xs ml-1.5">({attendee.response_status})</span>
                            )}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* RSVP buttons - show if user is an attendee (not organizer) */}
              {(() => {
                if (storeEvent.is_organizer) return null;

                const accountEmail = storeEvent.account_email?.toLowerCase();
                const userAttendee = storeEvent.attendees?.find(
                  (att) => att.email.toLowerCase() === accountEmail
                );
                const currentStatus = userAttendee?.response_status;
                const canRespond = userAttendee && currentStatus !== undefined;

                if (!canRespond) return null;

                const handleRSVP = async (status: 'accepted' | 'declined' | 'tentative') => {
                  try {
                    await respondToEvent(storeEvent.id, status);
                  } catch (err) {
                    console.error('Failed to respond to event:', err);
                  }
                };

                return (
                  <div className="pt-3 mt-3 border-t border-gray-100">
                    <p className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">
                      Your Response
                    </p>
                    <div className="flex gap-2">
                      <button
                        onClick={() => handleRSVP('accepted')}
                        disabled={isSaving}
                        className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                          currentStatus === 'accepted'
                            ? 'bg-green-100 text-green-700 border border-green-300'
                            : 'bg-gray-100 text-gray-600 hover:bg-green-50 hover:text-green-700'
                        }`}
                      >
                        Yes
                      </button>
                      <button
                        onClick={() => handleRSVP('tentative')}
                        disabled={isSaving}
                        className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                          currentStatus === 'tentative'
                            ? 'bg-yellow-100 text-yellow-700 border border-yellow-300'
                            : 'bg-gray-100 text-gray-600 hover:bg-yellow-50 hover:text-yellow-700'
                        }`}
                      >
                        Maybe
                      </button>
                      <button
                        onClick={() => handleRSVP('declined')}
                        disabled={isSaving}
                        className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                          currentStatus === 'declined'
                            ? 'bg-red-100 text-red-700 border border-red-300'
                            : 'bg-gray-100 text-gray-600 hover:bg-red-50 hover:text-red-700'
                        }`}
                      >
                        No
                      </button>
                    </div>
                  </div>
                );
              })()}
            </div>
          </div>
        </div>
      </motion.div>
      {showEditModal && (
        <EditEventModal
          event={event}
          onClose={handleEditModalClose}
          onUpdated={handleEditSuccess}
        />
      )}
    </AnimatePresence>,
    document.body
  );
}
