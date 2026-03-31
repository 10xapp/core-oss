import { useRef, useEffect, useMemo, useState, useCallback } from "react";
import type { CalendarEvent } from "../../../api/client";
import { getAccountPalette } from "../../../utils/accountColors";
import {
  formatHour,
  isToday,
  getWeekDays,
  startOfWeek,
  isSameDay,
} from "../utils/dateHelpers";
import { calculateEventLayouts } from "../utils/eventLayout";
import { useCalendarStore } from "../../../stores/calendarStore";
import DraggableEventBlock from "./DraggableEventBlock";
import DroppableTimeSlot from "./DroppableTimeSlot";
import NewEventBlock from "./NewEventBlock";

interface WeekViewProps {
  selectedDate: Date;
  onEventClick: (event: CalendarEvent, element: HTMLDivElement) => void;
  onTimeSlotClick: (date: Date, hour: number, triggerRect: DOMRect) => void;
  onDateSelect: (date: Date) => void;
}

const HOURS = Array.from({ length: 24 }, (_, i) => i);
const DAY_NAMES = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
];
const HOUR_HEIGHT = 60;
const TIME_COLUMN_WIDTH = 53;
const TIME_LABEL_WIDTH = TIME_COLUMN_WIDTH - 8; // Actual width of time label column

export default function WeekView({
  selectedDate,
  onEventClick,
  onTimeSlotClick,
  onDateSelect,
}: WeekViewProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const isDraggingToCreateRef = useRef(false);
  const dragStartYRef = useRef(0);
  const dragDayIndexRef = useRef(0);
  const startHourRef = useRef(0);
  const startMinuteRef = useRef(0);
  const [containerWidth, setContainerWidth] = useState(0);

  // Use the store's indexed selectors for O(1) event lookups
  // Subscribe to events and selectedAccountIds so useMemo re-runs when either changes
  const events = useCalendarStore((state) => state.events);
  const selectedAccountIds = useCalendarStore((state) => state.selectedAccountIds);
  const getAllDayEventsForDate = useCalendarStore(
    (state) => state.getAllDayEventsForDate,
  );
  const getTimedEventsForDate = useCalendarStore(
    (state) => state.getTimedEventsForDate,
  );
  const pendingEvent = useCalendarStore((state) => state.pendingEvent);
  const startDraggingToCreate = useCalendarStore(
    (state) => state.startDraggingToCreate,
  );
  const startCreatingAllDayEvent = useCalendarStore(
    (state) => state.startCreatingAllDayEvent,
  );
  const updateDragToCreate = useCalendarStore(
    (state) => state.updateDragToCreate,
  );
  const cancelCreatingEvent = useCalendarStore(
    (state) => state.cancelCreatingEvent,
  );
  const finishDragToCreate = useCalendarStore(
    (state) => state.finishDragToCreate,
  );

  const weekStart = useMemo(() => startOfWeek(selectedDate), [selectedDate]);
  const weekDays = useMemo(() => getWeekDays(weekStart), [weekStart]);

  // Track container width from scrollRef (not containerRef) to account for scrollbar
  // The header uses flex layout inside scrollRef, so we need to measure the same element
  // to ensure dayColumnWidth matches the actual flex column widths
  useEffect(() => {
    if (scrollRef.current) {
      setContainerWidth(scrollRef.current.clientWidth);

      const resizeObserver = new ResizeObserver((entries) => {
        for (const entry of entries) {
          // Use clientWidth to exclude scrollbar width
          setContainerWidth((entry.target as HTMLElement).clientWidth);
        }
      });

      resizeObserver.observe(scrollRef.current);
      return () => resizeObserver.disconnect();
    }
  }, []);

  // Auto-scroll to 2 hours before current time on mount
  useEffect(() => {
    if (scrollRef.current) {
      const currentHour = new Date().getHours();
      const scrollToHour = Math.max(0, currentHour - 2);
      scrollRef.current.scrollTop = scrollToHour * HOUR_HEIGHT;
    }
  }, []);

  // Get all-day events for week header
  // Include `events` and `selectedAccountIds` in deps so memo invalidates when either changes
  // Note: Multi-day all-day events are automatically indexed to all spanned days via buildEventsByDateIndex
  // and will appear in each day's column, which is the current expected behavior
  const allDayEventsByDay = useMemo(() => {
    return weekDays.map((day) => getAllDayEventsForDate(day));
  }, [weekDays, getAllDayEventsForDate, events, selectedAccountIds]);

  // Memoize timed events and their layouts for each day
  // This prevents recalculating layouts on every render
  const timedEventsByDay = useMemo(() => {
    return weekDays.map((day) => getTimedEventsForDate(day));
  }, [weekDays, getTimedEventsForDate, events, selectedAccountIds]);

  const layoutsByDay = useMemo(() => {
    return timedEventsByDay.map((events, dayIndex) =>
      calculateEventLayouts(events, weekDays[dayIndex]),
    );
  }, [timedEventsByDay, weekDays]);

  const calculateTimeFromPosition = useCallback((clientY: number) => {
    if (!scrollRef.current || !containerRef.current)
      return { hour: 0, minute: 0, dayIndex: 0 };

    const scrollTop = scrollRef.current.scrollTop;
    const scrollRect = scrollRef.current.getBoundingClientRect();

    // Position within the scrollable container
    const positionInScroll = clientY - scrollRect.top + scrollTop;

    // Total minutes from top of timeline
    // HOUR_HEIGHT pixels = 60 minutes, so: minutes = (pixels / (pixels/minute)) = pixels / (HOUR_HEIGHT/60)
    const totalMinutes = positionInScroll / (HOUR_HEIGHT / 60);

    // Snap to 15-minute increments
    const snappedMinutes = Math.round(totalMinutes / 15) * 15;

    const hour = Math.floor(snappedMinutes / 60) % 24;
    const minute = snappedMinutes % 60;

    return { hour, minute, dayIndex: 0 };
  }, []);

  const handleDragToCreateStart = useCallback(
    (hour: number, minute: number, dayIndex: number, e: React.PointerEvent) => {
      // Only start drag-to-create on left click and if not already creating an event
      if (e.button !== 0 || pendingEvent?.isDragging) return;

      isDraggingToCreateRef.current = true;
      dragStartYRef.current = e.clientY;
      dragDayIndexRef.current = dayIndex;
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
            const date = weekDays[dragDayIndexRef.current];
            state.startDraggingToCreate(
              date,
              startHourRef.current,
              startMinuteRef.current,
              new DOMRect(),
            );
          }
        }

        // Update the end time as user drags
        if (hasMovedEnough) {
          const { hour: endHour, minute: endMinute } =
            calculateTimeFromPosition(moveEvent.clientY);
          updateDragToCreate(endHour, endMinute);
        }
      };

      const handlePointerUp = () => {
        isDraggingToCreateRef.current = false;
        document.removeEventListener("pointermove", handlePointerMove);
        document.removeEventListener("pointerup", handlePointerUp);

        if (hasMovedEnough) {
          finishDragToCreate();
        } else {
          cancelCreatingEvent();
        }
      };

      document.addEventListener("pointermove", handlePointerMove);
      document.addEventListener("pointerup", handlePointerUp);
    },
    [
      weekDays,
      calculateTimeFromPosition,
      updateDragToCreate,
      startDraggingToCreate,
      cancelCreatingEvent,
      finishDragToCreate,
      pendingEvent?.isDragging,
    ],
  );

  return (
    <div ref={containerRef} className="flex-1 flex flex-col overflow-hidden">
      {/* Scrollable timeline - header is sticky inside to share the same width */}
      <div
        ref={scrollRef}
        id="calendar-scroll-container"
        className="flex-1 overflow-y-auto"
        style={{ backgroundColor: "#FCFCFC" }}
      >
        {/* Week header with day names and dates - sticky at top */}
        <div className="sticky top-0 z-30 border-b border-gray-200 bg-white">
          <div className="flex">
            {/* Time column spacer - matches time label width in grid */}
            <div style={{ width: TIME_COLUMN_WIDTH - 8 }} />

            {/* Day headers */}
            {weekDays.map((day, index) => {
              const dayIsToday = isToday(day);
              const isSelected = isSameDay(day, selectedDate);

              return (
                <button
                  key={index}
                  onClick={() => onDateSelect(day)}
                  className={`flex-1 py-4 flex flex-col items-center hover:bg-gray-50 transition-colors ${
                    isSelected && !dayIsToday ? "bg-gray-50" : ""
                  }`}
                >
                  <span className="text-sm text-gray-500">
                    {DAY_NAMES[index]}
                  </span>
                  <span
                    className={`
                      mt-1 text-lg font-medium
                      ${dayIsToday ? "text-slate-500" : "text-gray-900"}
                    `}
                  >
                    {day.getDate()}
                  </span>
                </button>
              );
            })}
          </div>

          {/* All-day events row - always visible */}
          <div className="flex min-h-8">
            <div
              style={{ width: TIME_COLUMN_WIDTH - 8 }}
              className="shrink-0 flex items-center justify-end pr-2"
            >
              <span className="text-xs text-gray-400">All</span>
            </div>
            {weekDays.map((day, dayIndex) => {
              const dayAllDayEvents = allDayEventsByDay[dayIndex];
              const isSelectedDay = isSameDay(day, selectedDate);
              const dayIsToday = isToday(day);
              return (
                <div
                  key={dayIndex}
                  className={`flex-1 border-l border-gray-100 px-1 py-1 flex flex-col gap-0.5 cursor-pointer hover:bg-gray-50 transition-colors ${
                    isSelectedDay && !dayIsToday ? "bg-gray-50" : ""
                  }`}
                  onClick={(e) => {
                    // Only trigger if clicking the container itself, not an event
                    if (
                      e.target === e.currentTarget ||
                      (e.target as HTMLElement).classList.contains(
                        "all-day-slot",
                      )
                    ) {
                      startCreatingAllDayEvent(
                        day,
                        e.currentTarget.getBoundingClientRect(),
                      );
                    }
                  }}
                >
                  {dayAllDayEvents.slice(0, 2).map((event) => {
                    const palette = getAccountPalette(event.account_email);
                    return (
                      <button
                        key={event.id}
                        onClick={(e) => {
                          e.stopPropagation();
                          onEventClick(
                            event,
                            e.currentTarget as unknown as HTMLDivElement,
                          );
                        }}
                        className="text-left px-1.5 py-0.5 rounded text-xs font-medium truncate transition-opacity hover:opacity-80"
                        style={{
                          backgroundColor: palette.bg,
                          color: palette.title,
                        }}
                      >
                        {event.title}
                      </button>
                    );
                  })}
                  {dayAllDayEvents.length > 2 && (
                    <span className="text-xs text-gray-400 px-1">
                      +{dayAllDayEvents.length - 2}
                    </span>
                  )}
                  {/* Clickable area when no events */}
                  {dayAllDayEvents.length === 0 && (
                    <div className="all-day-slot flex-1 min-h-4" />
                  )}
                </div>
              );
            })}
          </div>
        </div>

        <div className="relative" style={{ height: 24 * HOUR_HEIGHT }}>
          {/* Background shading for selected day — uses flex layout to match header/grid columns */}
          <div className="absolute inset-0 flex pointer-events-none">
            <div className="shrink-0" style={{ width: TIME_COLUMN_WIDTH - 8 }} />
            {weekDays.map((day, index) => {
              const isSelected = isSameDay(day, selectedDate);
              const dayIsToday = isToday(day);
              return (
                <div
                  key={`bg-${index}`}
                  className={`flex-1 ${isSelected && !dayIsToday ? "bg-gray-50" : ""}`}
                />
              );
            })}
          </div>

          {/* Hour grid with 4 invisible 15-minute drop zones per hour */}
          {HOURS.map((hour) => (
            <div
              key={hour}
              className="absolute w-full flex"
              style={{
                top: hour * HOUR_HEIGHT,
                height: HOUR_HEIGHT,
              }}
            >
              {/* Time label */}
              <div
                className="shrink-0 pr-2 text-right text-text-secondary"
                style={{
                  width: TIME_COLUMN_WIDTH - 8,
                  display: "flex",
                  alignItems: "flex-start",
                  justifyContent: "flex-end",
                  fontSize: 11,
                  paddingTop: 2,
                  lineHeight: 1,
                  transform: "translateY(-8px)",
                }}
              >
                {hour === 0 ? "" : formatHour(hour)}
              </div>

              {/* Grid line - positioned absolutely at the exact top of the hour block, spanning only the day columns */}
              <div
                style={{
                  position: "absolute",
                  top: 0,
                  left: TIME_LABEL_WIDTH,
                  right: 0,
                  borderTop: "0.5px solid rgba(209, 213, 221, 0.6)",
                  pointerEvents: "none",
                }}
              />

              {/* Day columns with 4 invisible quarter-hour drop zones each */}
              <div className="flex-1 flex flex-col">
                {[0, 15, 30, 45].map((minute) => (
                  <div key={minute} className="flex-1 flex">
                    {weekDays.map((d, dayIndex) => (
                      <DroppableTimeSlot
                        key={dayIndex}
                        id={`week-slot-${d.toISOString()}-${hour}-${minute}`}
                        date={d}
                        hour={hour}
                        minute={minute}
                        onClick={(e) =>
                          onTimeSlotClick(
                            d,
                            hour,
                            e.currentTarget.getBoundingClientRect(),
                          )
                        }
                        onPointerDown={(e) => {
                          handleDragToCreateStart(hour, minute, dayIndex, e);
                        }}
                        className="flex-1"
                        style={{
                          borderLeft: "0.5px solid rgba(209, 213, 221, 0.6)",
                          borderTop:
                            minute === 0
                              ? "0.5px solid rgba(209, 213, 221, 0.6)"
                              : undefined,
                        }}
                      />
                    ))}
                  </div>
                ))}
              </div>
            </div>
          ))}

          {/* Event blocks and current time indicator - flex overlay matching grid columns */}
          <div className="absolute inset-0 flex pointer-events-none">
            <div className="shrink-0" style={{ width: TIME_LABEL_WIDTH }} />
            {weekDays.map((day, dayIndex) => {
              const dayEvents = timedEventsByDay[dayIndex];
              const layouts = layoutsByDay[dayIndex];
              const dayIsToday = isToday(day);

              return (
                <div key={dayIndex} className="flex-1 relative">
                  {/* Current time indicator for today */}
                  {dayIsToday && (
                    <CurrentTimeIndicatorWeek hourHeight={HOUR_HEIGHT} />
                  )}

                  {/* Event blocks */}
                  {dayEvents.map((event) => {
                    const layout = layouts.get(event.id);
                    if (!layout) return null;

                    return (
                      <DraggableEventBlock
                        key={event.id}
                        event={event}
                        layout={layout}
                        containerWidth={containerWidth}
                        dayIndex={dayIndex}
                        onClick={onEventClick}
                        isWeekView
                        hourHeight={HOUR_HEIGHT}
                        timeColumnWidth={TIME_COLUMN_WIDTH}
                        viewDate={weekDays[dayIndex]}
                      />
                    );
                  })}

                  {/* New event block (when creating inline) */}
                  {pendingEvent && isSameDay(pendingEvent.date, day) && (
                    <NewEventBlock
                      key="pending-event"
                      date={pendingEvent.date}
                      hour={pendingEvent.hour}
                      minute={pendingEvent.minute}
                      endHour={pendingEvent.endHour}
                      endMinute={pendingEvent.endMinute}
                      containerWidth={containerWidth}
                      isWeekView
                      hourHeight={HOUR_HEIGHT}
                      timeColumnWidth={TIME_COLUMN_WIDTH}
                      title={pendingEvent.title}
                      isDragging={pendingEvent.isDragging === true}
                    />
                  )}
                </div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}

// Current time indicator for week view (rendered inside the day column flex container)
function CurrentTimeIndicatorWeek({
  hourHeight,
}: {
  hourHeight: number;
}) {
  const [currentTime, setCurrentTime] = useState(new Date());

  useEffect(() => {
    const timer = setInterval(() => setCurrentTime(new Date()), 60000);
    return () => clearInterval(timer);
  }, []);

  const hours = currentTime.getHours();
  const minutes = currentTime.getMinutes();
  const topPosition = (hours * 60 + minutes) * (hourHeight / 60);
  const circleSize = 8;

  return (
    <div
      className="absolute pointer-events-none z-[110]"
      style={{
        top: topPosition,
        left: -circleSize / 2,
        right: 0,
      }}
    >
      {/* Circle centered on the left edge of today's column */}
      <div
        className="absolute bg-orange-500 rounded-full"
        style={{
          width: circleSize,
          height: circleSize,
          top: -circleSize / 2,
          left: 0,
        }}
      />
      {/* Line from circle to end of today's column */}
      <div
        className="absolute bg-orange-500"
        style={{ height: 2, left: circleSize, right: 0, top: -1 }}
      />
    </div>
  );
}
