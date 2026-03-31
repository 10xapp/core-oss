import { useRef, useEffect, useMemo, useCallback } from 'react';
import type { CalendarEvent } from '../../../api/client';
import { formatHour, isToday, isSameDay } from '../utils/dateHelpers';
import { calculateEventLayouts } from '../utils/eventLayout';
import { useCalendarStore } from '../../../stores/calendarStore';
import CurrentTimeIndicator from './CurrentTimeIndicator';
import DraggableEventBlock from './DraggableEventBlock';
import DroppableTimeSlot from './DroppableTimeSlot';
import AllDayEventsSection from './AllDayEventsSection';
import NewEventBlock from './NewEventBlock';

interface TimelineGridProps {
  date: Date;
  onEventClick: (event: CalendarEvent, element: HTMLDivElement) => void;
  onTimeSlotClick: (date: Date, hour: number, triggerRect: DOMRect) => void;
  hourHeight?: number;
  timeColumnWidth?: number;
}

const HOURS = Array.from({ length: 24 }, (_, i) => i);

export default function TimelineGrid({
  date,
  onEventClick,
  onTimeSlotClick,
  hourHeight = 60,
  timeColumnWidth = 53
}: TimelineGridProps) {
  console.log('[TimelineGrid] Rendered with onEventClick:', typeof onEventClick);

  const scrollRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const isDraggingToCreateRef = useRef(false);
  const dragStartYRef = useRef(0);
  const startHourRef = useRef(0);
  const startMinuteRef = useRef(0);

  // Use the store's indexed selectors for O(1) event lookups
  // Subscribe to events and selectedAccountIds so useMemo re-runs when either changes
  const events = useCalendarStore((state) => state.events);
  const selectedAccountIds = useCalendarStore((state) => state.selectedAccountIds);
  const getTimedEventsForDate = useCalendarStore((state) => state.getTimedEventsForDate);
  const getAllDayEventsForDate = useCalendarStore((state) => state.getAllDayEventsForDate);
  const pendingEvent = useCalendarStore((state) => state.pendingEvent);
  const startDraggingToCreate = useCalendarStore((state) => state.startDraggingToCreate);
  const updateDragToCreate = useCalendarStore((state) => state.updateDragToCreate);
  const cancelCreatingEvent = useCalendarStore((state) => state.cancelCreatingEvent);
  const finishDragToCreate = useCalendarStore((state) => state.finishDragToCreate);

  const timedEvents = useMemo(
    () => getTimedEventsForDate(date),
    [getTimedEventsForDate, date, events, selectedAccountIds]
  );

  const allDayEvents = useMemo(
    () => getAllDayEventsForDate(date),
    [getAllDayEventsForDate, date, events, selectedAccountIds]
  );

  const eventLayouts = useMemo(
    () => calculateEventLayouts(timedEvents, date),
    [timedEvents, date]
  );

  const containerWidth = containerRef.current?.offsetWidth || 400;

  // Auto-scroll to 2 hours before current time on mount
  useEffect(() => {
    if (scrollRef.current && isToday(date)) {
      const currentHour = new Date().getHours();
      const scrollToHour = Math.max(0, currentHour - 2);
      scrollRef.current.scrollTop = scrollToHour * hourHeight;
    }
  }, [date, hourHeight]);

  const handleTimeSlotClick = (hour: number, e: React.MouseEvent<HTMLButtonElement>) => {
    const triggerRect = e.currentTarget.getBoundingClientRect();
    onTimeSlotClick(date, hour, triggerRect);
  };

  const calculateTimeFromPosition = useCallback((clientY: number) => {
    if (!scrollRef.current || !containerRef.current) return { hour: 0, minute: 0 };

    const scrollTop = scrollRef.current.scrollTop;
    const scrollRect = scrollRef.current.getBoundingClientRect();

    // Position within the scrollable container
    const positionInScroll = clientY - scrollRect.top + scrollTop;

    // Total minutes from top of timeline
    // hourHeight pixels = 60 minutes, so: minutes = (pixels / (pixels/minute)) = pixels / (hourHeight/60)
    const totalMinutes = positionInScroll / (hourHeight / 60);

    // Snap to 15-minute increments
    const snappedMinutes = Math.round(totalMinutes / 15) * 15;

    const hour = Math.floor(snappedMinutes / 60) % 24;
    const minute = snappedMinutes % 60;

    return { hour, minute };
  }, [hourHeight]);

  const handleDragToCreateStart = useCallback((hour: number, minute: number, e: React.PointerEvent) => {
    // Only start drag-to-create on left click and if not already creating an event
    if (e.button !== 0 || pendingEvent?.isDragging) return;

    isDraggingToCreateRef.current = true;
    dragStartYRef.current = e.clientY;
    startHourRef.current = hour;
    startMinuteRef.current = minute;

    let hasMovedEnough = false;

    const handlePointerMove = (moveEvent: PointerEvent) => {
      const deltaY = Math.abs(moveEvent.clientY - dragStartYRef.current);

      // Require at least 10px drag distance to start drag-to-create
      if (deltaY >= 10 && !hasMovedEnough) {
        hasMovedEnough = true;
        // Start creating the event with the start time
        const state = useCalendarStore.getState();
        if (!state.pendingEvent?.isDragging) {
          state.startDraggingToCreate(date, startHourRef.current, startMinuteRef.current, new DOMRect());
        }
      }

      // Update the end time as user drags
      if (hasMovedEnough) {
        const { hour: endHour, minute: endMinute } = calculateTimeFromPosition(moveEvent.clientY);
        updateDragToCreate(endHour, endMinute);
      }
    };

    const handlePointerUp = () => {
      isDraggingToCreateRef.current = false;
      document.removeEventListener('pointermove', handlePointerMove);
      document.removeEventListener('pointerup', handlePointerUp);

      if (hasMovedEnough) {
        finishDragToCreate();
      } else {
        cancelCreatingEvent();
      }
    };

    document.addEventListener('pointermove', handlePointerMove);
    document.addEventListener('pointerup', handlePointerUp);
  }, [date, calculateTimeFromPosition, updateDragToCreate, startDraggingToCreate, cancelCreatingEvent, finishDragToCreate, pendingEvent?.isDragging]);

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      {/* All-day events */}
      <AllDayEventsSection events={allDayEvents} onEventClick={onEventClick} />

      {/* Scrollable timeline */}
      <div
        ref={scrollRef}
        id="calendar-scroll-container"
        className="flex-1 overflow-y-auto overflow-x-hidden"
        style={{ backgroundColor: '#FCFCFC' }}
      >
        <div
          ref={containerRef}
          className="relative"
          style={{ height: 24 * hourHeight }}
        >
          {/* Hour grid with 4 invisible 15-minute drop zones per hour */}
          {HOURS.map(hour => (
            <div
              key={hour}
              className="absolute w-full flex"
              style={{
                top: hour * hourHeight,
                height: hourHeight
              }}
            >
              {/* Time label */}
              <div
                className="shrink-0 pr-2 text-right text-text-secondary"
                style={{
                  width: timeColumnWidth - 6,
                  display: 'flex',
                  alignItems: 'flex-start',
                  justifyContent: 'flex-end',
                  fontSize: 11,
                  paddingTop: 2,
                  lineHeight: 1,
                  transform: 'translateY(-8px)'
                }}
              >
                {hour === 0 ? '' : formatHour(hour)}
              </div>

              {/* Grid line - positioned absolutely at the exact top of the hour block, spanning only the day columns */}
              <div
                style={{
                  position: 'absolute',
                  top: 0,
                  left: timeColumnWidth,
                  right: 0,
                  borderTop: '0.5px solid rgba(209, 213, 221, 0.6)',
                  pointerEvents: 'none'
                }}
              />

              {/* 4 invisible quarter-hour drop zones stacked in flex column */}
              <div className="flex-1 flex flex-col" style={{ marginLeft: 8 }}>
                {[0, 15, 30, 45].map(minute => (
                  <DroppableTimeSlot
                    key={minute}
                    id={`day-slot-${date.toISOString()}-${hour}-${minute}`}
                    date={date}
                    hour={hour}
                    minute={minute}
                    onClick={(e) => handleTimeSlotClick(hour, e)}
                    className="flex-1"
                    onPointerDown={(e) => {
                      handleDragToCreateStart(hour, minute, e);
                    }}
                  />
                ))}
              </div>
            </div>
          ))}

          {/* Current time indicator */}
          {isToday(date) && (
            <CurrentTimeIndicator
              hourHeight={hourHeight}
              timeColumnWidth={timeColumnWidth}
            />
          )}

          {/* Event blocks */}
          {timedEvents.map(event => {
            const layout = eventLayouts.get(event.id);
            if (!layout) return null;

            return (
              <DraggableEventBlock
                key={event.id}
                event={event}
                layout={layout}
                containerWidth={containerWidth}
                onClick={onEventClick}
                hourHeight={hourHeight}
                timeColumnWidth={timeColumnWidth}
                viewDate={date}
              />
            );
          })}

          {/* New event block (when creating inline) */}
          {pendingEvent && isSameDay(pendingEvent.date, date) && (
            <NewEventBlock
              date={pendingEvent.date}
              hour={pendingEvent.hour}
              minute={pendingEvent.minute}
              endDate={pendingEvent.endDate}
              endHour={pendingEvent.endHour}
              endMinute={pendingEvent.endMinute}
              containerWidth={containerWidth}
              hourHeight={hourHeight}
              timeColumnWidth={timeColumnWidth}
              title={pendingEvent.title}
              isDragging={pendingEvent.isDragging === true}
            />
          )}
        </div>
      </div>
    </div>
  );
}
