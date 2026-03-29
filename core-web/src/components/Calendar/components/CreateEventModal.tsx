import { useState } from 'react';
import { createPortal } from 'react-dom';
import { motion } from 'motion/react';
import { X, Video } from 'lucide-react';
import { Icon } from '../../ui/Icon';
import { createCalendarEvent } from '../../../api/client';
import { useCalendarStore } from '../../../stores/calendarStore';
import DatePicker from '../../ui/DatePicker';

interface CreateEventModalProps {
  onClose: () => void;
  onCreated: () => void;
  initialDate: Date | null;
  initialHour?: number;
}

export default function CreateEventModal({
  onClose,
  onCreated,
  initialDate,
  initialHour
}: CreateEventModalProps) {
  const { addEvent, removeEvent, replaceEvent } = useCalendarStore();

  const getInitialTime = () => {
    if (initialHour !== undefined) {
      return `${initialHour.toString().padStart(2, '0')}:00`;
    }
    return '09:00';
  };

  const getInitialEndTime = () => {
    if (initialHour !== undefined) {
      return `${(initialHour + 1).toString().padStart(2, '0')}:00`;
    }
    return '10:00';
  };

  const [title, setTitle] = useState('');
  const [date, setDate] = useState(
    (initialDate || new Date()).toISOString().split('T')[0]
  );
  const [startTime, setStartTime] = useState(getInitialTime());
  const [endTime, setEndTime] = useState(getInitialEndTime());
  const [location, setLocation] = useState('');
  const [description, setDescription] = useState('');
  const [isAllDay, setIsAllDay] = useState(false);
  const [addGoogleMeet, setAddGoogleMeet] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!title.trim()) return;

    setIsLoading(true);
    setError('');

    const startDateTime = isAllDay
      ? `${date}T00:00:00`
      : `${date}T${startTime}:00`;
    const endDateTime = isAllDay
      ? `${date}T23:59:59`
      : `${date}T${endTime}:00`;

    // Create optimistic event with temporary ID
    const tempId = `temp-${Date.now()}`;
    const optimisticEvent = {
      id: tempId,
      title: title.trim(),
      description: description.trim() || undefined,
      start_time: startDateTime,
      end_time: endDateTime,
      all_day: isAllDay,
      location: location.trim() || undefined,
      status: 'confirmed' as const,
    };

    // Add optimistic event and close modal immediately
    addEvent(optimisticEvent);
    onCreated();

    try {
      const createdEvent = await createCalendarEvent({
        title: title.trim(),
        description: description.trim() || undefined,
        start_time: startDateTime,
        end_time: endDateTime,
        all_day: isAllDay,
        location: location.trim() || undefined,
        add_google_meet: addGoogleMeet || undefined,
      });
      // Replace optimistic event with the real one from server
      replaceEvent(tempId, createdEvent);
    } catch (err) {
      // Remove optimistic event on failure
      removeEvent(tempId);
      console.error('Failed to create event:', err);
    }
  };

  const inputStyles = "w-full px-3 py-2 bg-gray-50 border border-gray-200 rounded-lg text-sm text-gray-900 placeholder:text-gray-400 focus:outline-none focus:ring-2 focus:ring-gray-900/10 focus:border-gray-400 transition-colors";

  return createPortal(
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.15 }}
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
      <motion.div
        initial={{ opacity: 0, scale: 0.95, y: 10 }}
        animate={{ opacity: 1, scale: 1, y: 0 }}
        exit={{ opacity: 0, scale: 0.95, y: 10 }}
        transition={{ duration: 0.15, ease: [0.4, 0, 0.2, 1] }}
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
          <h2 className="text-lg font-semibold text-gray-900">New Event</h2>
          <button
            onClick={onClose}
            className="p-1.5 text-gray-400 hover:text-gray-600 hover:bg-gray-100 rounded-lg transition-colors"
          >
            <Icon icon={X} size={20} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="p-6 space-y-5">
          {error && (
            <div className="p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-600">
              {error}
            </div>
          )}

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
            <DatePicker
              value={date}
              onChange={setDate}
              showClearButton={false}
              showRelativeDate={false}
              buttonClassName="w-full justify-start"
            />
          </div>

          <div className="flex items-center gap-2">
            <input
              type="checkbox"
              id="allDay"
              checked={isAllDay}
              onChange={(e) => setIsAllDay(e.target.checked)}
              className="w-4 h-4 rounded border-gray-300 text-gray-900 focus:ring-gray-900/20"
            />
            <label htmlFor="allDay" className="text-sm text-gray-600">
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

          <button
            type="button"
            onClick={() => setAddGoogleMeet(!addGoogleMeet)}
            className={`flex items-center gap-2.5 w-full text-left px-3 py-2.5 rounded-lg border transition-colors ${
              addGoogleMeet
                ? 'border-blue-200 bg-blue-50'
                : 'border-gray-200 bg-gray-50 hover:bg-gray-100'
            }`}
          >
            <Icon icon={Video} size={16} className={addGoogleMeet ? 'text-blue-600' : 'text-gray-400'} />
            <span className={`text-sm ${addGoogleMeet ? 'text-blue-600 font-medium' : 'text-gray-500'}`}>
              {addGoogleMeet ? 'Google Meet video conferencing' : 'Add Google Meet video conferencing'}
            </span>
          </button>

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
              onClick={onClose}
              className="px-4 py-2 text-sm font-medium text-gray-600 hover:text-gray-900 hover:bg-gray-100 rounded-lg transition-colors"
            >
              Cancel
            </button>
            <button
              type="submit"
              disabled={isLoading || !title.trim()}
              className="px-5 py-2 bg-gray-900 text-white rounded-lg text-sm font-medium hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {isLoading ? 'Creating...' : 'Create Event'}
            </button>
          </div>
        </form>
      </motion.div>
    </motion.div>,
    document.body
  );
}
