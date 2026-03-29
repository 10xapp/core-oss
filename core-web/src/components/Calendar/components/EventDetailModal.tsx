import { useState } from 'react';
import { createPortal } from 'react-dom';
import { X, Trash2, Pencil, MapPin, Calendar, Video } from 'lucide-react';
import { Icon } from '../../ui/Icon';
import type { CalendarEvent } from '../../../api/client';
import { updateCalendarEvent, deleteCalendarEvent } from '../../../api/client';

interface EventDetailModalProps {
  event: CalendarEvent;
  onClose: () => void;
  onUpdated: () => void;
  onDeleted: () => void;
}

function getSafeHref(link: string): string | null {
  try {
    const url = new URL(link, window.location.origin);
    if (url.protocol === 'http:' || url.protocol === 'https:') {
      return url.href;
    }
    return null;
  } catch {
    return null;
  }
}

function isGoogleMeetLink(meetingLink?: string | null): boolean {
  if (!meetingLink) {
    return false;
  }
  try {
    const url = new URL(meetingLink);
    return url.hostname === 'meet.google.com';
  } catch {
    return false;
  }
}

export default function EventDetailModal({
  event,
  onClose,
  onUpdated,
  onDeleted
}: EventDetailModalProps) {
  const [isEditing, setIsEditing] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [isDeleting, setIsDeleting] = useState(false);
  const [error, setError] = useState('');

  // Edit state
  const [title, setTitle] = useState(event.title);
  const [date, setDate] = useState(event.start_time.split('T')[0]);
  const [startTime, setStartTime] = useState(event.start_time.split('T')[1]?.slice(0, 5) || '09:00');
  const [endTime, setEndTime] = useState(event.end_time.split('T')[1]?.slice(0, 5) || '10:00');
  const [location, setLocation] = useState(event.location || '');
  const [description, setDescription] = useState(event.description || '');
  const [isAllDay, setIsAllDay] = useState(event.all_day);

  const formatDisplayDate = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleDateString('en-US', {
      weekday: 'long',
      year: 'numeric',
      month: 'long',
      day: 'numeric'
    });
  };

  const formatDisplayTime = (dateStr: string) => {
    const date = new Date(dateStr);
    return date.toLocaleTimeString('en-US', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true
    });
  };

  const handleSave = async () => {
    if (!title.trim()) return;

    setIsLoading(true);
    setError('');

    try {
      const startDateTime = isAllDay
        ? `${date}T00:00:00`
        : `${date}T${startTime}:00`;
      const endDateTime = isAllDay
        ? `${date}T23:59:59`
        : `${date}T${endTime}:00`;

      await updateCalendarEvent(event.id, {
        title: title.trim(),
        description: description.trim() || undefined,
        start_time: startDateTime,
        end_time: endDateTime,
        all_day: isAllDay,
        location: location.trim() || undefined
      });
      onUpdated();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to update event');
    } finally {
      setIsLoading(false);
    }
  };

  const handleDelete = async () => {
    setIsDeleting(true);
    setError('');

    try {
      await deleteCalendarEvent(event.id);
      onDeleted();
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete event');
      setIsDeleting(false);
    }
  };

  const inputStyles = "w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-900 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-900/10 focus:border-gray-400 transition-colors";

  return createPortal(
    <div
      style={{
        position: 'fixed',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        backgroundColor: 'rgba(0, 0, 0, 0.5)',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        zIndex: 9999,
      }}
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div
        style={{
          backgroundColor: 'white',
          borderRadius: 16,
          width: '100%',
          maxWidth: 400,
          margin: '0 16px',
          boxShadow: '0 25px 50px -12px rgba(0, 0, 0, 0.25)',
          overflow: 'hidden',
        }}
        onClick={(e) => e.stopPropagation()}
      >
        <div className="flex items-center justify-between px-6 py-4 border-b border-gray-100">
          <h2 className="text-lg font-semibold text-gray-900">
            {isEditing ? 'Edit Event' : 'Event Details'}
          </h2>
          <div className="flex items-center gap-1">
            {!isEditing && (
              <>
                <button
                  onClick={() => setIsEditing(true)}
                  className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
                  title="Edit event"
                >
                  <Icon icon={Pencil} size={20} />
                </button>
                <button
                  onClick={handleDelete}
                  disabled={isDeleting}
                  className="p-1.5 text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-lg transition-colors disabled:opacity-50"
                  title="Delete event"
                >
                  <Icon icon={Trash2} size={20} />
                </button>
              </>
            )}
            <button
              onClick={onClose}
              className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
            >
              <Icon icon={X} size={20} />
            </button>
          </div>
        </div>

        {error && (
          <div className="mx-6 mt-4 p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600">
            {error}
          </div>
        )}

        {isEditing ? (
          <form onSubmit={(e) => { e.preventDefault(); handleSave(); }} className="p-6 space-y-5">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">Title</label>
              <input
                type="text"
                value={title}
                onChange={(e) => setTitle(e.target.value)}
                className={inputStyles}
                placeholder="Event title"
                autoFocus
                required
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">Date</label>
              <input
                type="date"
                value={date}
                onChange={(e) => setDate(e.target.value)}
                className={inputStyles}
                required
              />
            </div>

            <div className="flex items-center gap-2">
              <input
                type="checkbox"
                id="editAllDay"
                checked={isAllDay}
                onChange={(e) => setIsAllDay(e.target.checked)}
                className="w-4 h-4 rounded border-gray-300 text-gray-900 focus:ring-gray-900/20"
              />
              <label htmlFor="editAllDay" className="text-sm text-gray-600">
                All day
              </label>
            </div>

            {!isAllDay && (
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">Start</label>
                  <input
                    type="time"
                    value={startTime}
                    onChange={(e) => setStartTime(e.target.value)}
                    className={inputStyles}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1.5">End</label>
                  <input
                    type="time"
                    value={endTime}
                    onChange={(e) => setEndTime(e.target.value)}
                    className={inputStyles}
                  />
                </div>
              </div>
            )}

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">Location</label>
              <input
                type="text"
                value={location}
                onChange={(e) => setLocation(e.target.value)}
                className={inputStyles}
                placeholder="Add location"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1.5">Description</label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={3}
                className={`${inputStyles} resize-none`}
                placeholder="Add description"
              />
            </div>

            <div className="flex justify-end gap-3 pt-2">
              <button
                type="button"
                onClick={() => setIsEditing(false)}
                className="px-4 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={isLoading || !title.trim()}
                className="px-5 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
              >
                {isLoading ? 'Saving...' : 'Save Changes'}
              </button>
            </div>
          </form>
        ) : (
          <div className="p-6 space-y-4">
            <h3 className="text-xl font-semibold text-gray-900">{event.title}</h3>

            <div className="space-y-3">
              <div className="flex items-start gap-3">
                <Icon icon={Calendar} size={20} className="text-gray-400 mt-0.5 flex-shrink-0" />
                <div>
                  <p className="text-sm text-gray-900">{formatDisplayDate(event.start_time)}</p>
                  {!event.all_day && (
                    <p className="text-sm text-gray-500">
                      {formatDisplayTime(event.start_time)} - {formatDisplayTime(event.end_time)}
                    </p>
                  )}
                  {event.all_day && (
                    <p className="text-sm text-gray-500">All day</p>
                  )}
                </div>
              </div>

              {event.location && (
                <div className="flex items-start gap-3">
                  <Icon icon={MapPin} size={20} className="text-gray-400 mt-0.5 flex-shrink-0" />
                  <p className="text-sm text-gray-900">{event.location}</p>
                </div>
              )}

              {event.meeting_link && getSafeHref(event.meeting_link) && (
                <div className="flex items-start gap-3">
                  <Icon icon={Video} size={20} className={`mt-0.5 shrink-0 ${isGoogleMeetLink(event.meeting_link) ? 'text-blue-600' : 'text-gray-400'}`} />
                  <a
                    href={getSafeHref(event.meeting_link)!}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={`text-sm hover:underline ${isGoogleMeetLink(event.meeting_link) ? 'text-blue-600 font-medium hover:text-blue-700' : 'text-blue-600 hover:text-blue-700'}`}
                  >
                    {isGoogleMeetLink(event.meeting_link) ? 'Join with Google Meet' : 'Join Meeting'}
                  </a>
                </div>
              )}

              {event.description && (
                <div className="pt-2 border-t border-gray-100">
                  <p className="text-sm text-gray-600 whitespace-pre-wrap">{event.description}</p>
                </div>
              )}

              {event.attendees && event.attendees.length > 0 && (
                <div className="pt-2 border-t border-gray-100">
                  <p className="text-sm font-medium text-gray-700 mb-2">Attendees</p>
                  <div className="space-y-1">
                    {event.attendees.map((attendee, i) => (
                      <p key={i} className="text-sm text-gray-600">
                        {attendee.display_name || attendee.email}
                        {attendee.response_status && (
                          <span className="text-gray-400 ml-2">({attendee.response_status})</span>
                        )}
                      </p>
                    ))}
                  </div>
                </div>
              )}
            </div>

            <div className="flex justify-end pt-4">
              <button
                onClick={onClose}
                className="px-4 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
              >
                Close
              </button>
            </div>
          </div>
        )}
      </div>
    </div>,
    document.body
  );
}
