import { useMemo, useRef, useEffect, useState } from "react";
import type { CalendarEvent } from "../../../api/client";
import {
  isToday,
  isSameDay,
  startOfWeek,
  addDays,
  formatTime,
  startOfMonth,
  addMonths,
} from "../utils/dateHelpers";
import { useCalendarStore } from "../../../stores/calendarStore";
import { getAccountPalette, type AccountColorPalette } from "../../../utils/accountColors";
import { getUserResponseStatus } from "../../../stores/calendarStore";

interface MonthViewProps {
  selectedDate: Date;
  onDateSelect: (date: Date) => void;
  onEventClick?: (event: CalendarEvent, element: HTMLDivElement) => void;
  onVisibleMonthChange?: (date: Date) => void;
}

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MAX_VISIBLE_EVENTS = 4;
const MONTHS_BEFORE = 24;
const MONTHS_AFTER = 24;
const ROW_HEIGHT = 140;

function getAllDayEventPalette(event: CalendarEvent): AccountColorPalette {
  // Use account-based colors for all events
  return getAccountPalette(event.account_email);
}

function getTimedEventDotColor(event: CalendarEvent): string {
  // Use account accent color for timed event dots
  const palette = getAccountPalette(event.account_email);
  return palette.accent;
}

function formatEventTime(dateString: string): string {
  const time = formatTime(dateString);
  return time.replace(" ", "");
}

// Generate a flat list of unique weeks with month context
function generateWeeksWithMonths(
  baseMonth: Date,
  monthsBefore: number,
  monthsAfter: number,
) {
  const startMonth = addMonths(baseMonth, -monthsBefore);
  const endMonth = addMonths(baseMonth, monthsAfter);

  const firstMonthStart = startOfMonth(startMonth);
  let currentWeekStart = startOfWeek(firstMonthStart);
  const lastMonthEnd = new Date(
    endMonth.getFullYear(),
    endMonth.getMonth() + 1,
    0,
  );

  const weeks: { weekStart: Date; days: Date[]; displayMonth: Date }[] = [];
  const monthFirstWeekIndices = new Map<string, number>();

  while (currentWeekStart <= lastMonthEnd) {
    const days: Date[] = [];
    for (let i = 0; i < 7; i++) {
      days.push(addDays(currentWeekStart, i));
    }

    let displayMonth: Date;
    const firstOfMonthInWeek = days.find((d) => d.getDate() === 1);
    if (firstOfMonthInWeek) {
      displayMonth = startOfMonth(firstOfMonthInWeek);
      const monthKey = `${displayMonth.getFullYear()}-${displayMonth.getMonth()}`;
      if (!monthFirstWeekIndices.has(monthKey)) {
        monthFirstWeekIndices.set(monthKey, weeks.length);
      }
    } else {
      const monthCounts = new Map<string, { count: number; month: Date }>();
      for (const day of days) {
        const key = `${day.getFullYear()}-${day.getMonth()}`;
        const existing = monthCounts.get(key);
        if (existing) {
          existing.count++;
        } else {
          monthCounts.set(key, { count: 1, month: startOfMonth(day) });
        }
      }
      let maxCount = 0;
      displayMonth = startOfMonth(days[0]);
      for (const { count, month } of monthCounts.values()) {
        if (count > maxCount) {
          maxCount = count;
          displayMonth = month;
        }
      }
    }

    weeks.push({ weekStart: currentWeekStart, days, displayMonth });
    currentWeekStart = addDays(currentWeekStart, 7);
  }

  return { weeks, monthFirstWeekIndices };
}

export default function MonthView({
  selectedDate,
  onDateSelect,
  onEventClick,
  onVisibleMonthChange,
}: MonthViewProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const monthMarkerRefs = useRef<Map<string, HTMLDivElement>>(new Map());

  const [visibleMonth, setVisibleMonth] = useState<Date>(
    startOfMonth(selectedDate),
  );

  // Use the store's indexed selector for O(1) event lookups
  // Subscribe to events and selectedAccountIds to re-render when either changes
  useCalendarStore((state) => state.events);
  useCalendarStore((state) => state.selectedAccountIds);
  const getEventsForDate = useCalendarStore((state) => state.getEventsForDate);

  const baseMonth = useMemo(() => startOfMonth(new Date()), []);

  const { weeks, monthFirstWeekIndices } = useMemo(
    () => generateWeeksWithMonths(baseMonth, MONTHS_BEFORE, MONTHS_AFTER),
    [baseMonth],
  );

  // Use IntersectionObserver to detect visible month for graying out days
  useEffect(() => {
    const scrollEl = scrollRef.current;
    if (!scrollEl) return;

    const observer = new IntersectionObserver(
      (entries) => {
        // Find the entry that's most visible at the top
        let topEntry: IntersectionObserverEntry | null = null;
        let topPosition = Infinity;

        for (const entry of entries) {
          if (entry.isIntersecting) {
            const rect = entry.boundingClientRect;
            if (rect.top < topPosition && rect.top >= -rect.height / 2) {
              topPosition = rect.top;
              topEntry = entry;
            }
          }
        }

        if (topEntry) {
          const monthKey = (topEntry.target as HTMLElement).dataset.month;
          if (monthKey) {
            const [year, month] = monthKey.split("-").map(Number);
            const newMonth = new Date(year, month, 1);

            if (newMonth.getTime() !== visibleMonth.getTime()) {
              setVisibleMonth(newMonth);
              onVisibleMonthChange?.(newMonth);
            }
          }
        }
      },
      {
        root: scrollEl,
        rootMargin: "-10% 0px -80% 0px",
        threshold: [0, 0.5, 1],
      },
    );

    // Observe all month markers
    monthMarkerRefs.current.forEach((el) => {
      observer.observe(el);
    });

    return () => observer.disconnect();
  }, [visibleMonth, onVisibleMonthChange]);

  // Initial scroll to selected date
  useEffect(() => {
    const monthKey = `${startOfMonth(selectedDate).getFullYear()}-${startOfMonth(selectedDate).getMonth()}`;
    const markerEl = monthMarkerRefs.current.get(monthKey);
    if (markerEl) {
      markerEl.scrollIntoView({ behavior: "instant", block: "start" });
    }
  }, []);

  const handleEventClick = (
    e: React.MouseEvent<HTMLButtonElement>,
    event: CalendarEvent,
  ) => {
    e.stopPropagation();
    onEventClick?.(event, e.currentTarget as unknown as HTMLDivElement);
  };

  const startCreatingAllDayEvent = useCalendarStore(
    (state) => state.startCreatingAllDayEvent,
  );

  const handleDayClick = (date: Date, e: React.MouseEvent<HTMLDivElement>) => {
    onDateSelect(date);
    // Use click coordinates for precise popover positioning near the cursor
    const clickRect = new DOMRect(e.clientX, e.clientY, 0, 0);
    startCreatingAllDayEvent(date, clickRect);
  };

  // Get the month key for a week if it's a boundary
  const getMonthKeyForWeek = (weekIndex: number): string | null => {
    for (const [monthKey, index] of monthFirstWeekIndices) {
      if (index === weekIndex) return monthKey;
    }
    return null;
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden bg-white">
      {/* Day headers */}
      <div className="shrink-0 grid grid-cols-7 border-b border-gray-100">
        {DAY_NAMES.map((day) => (
          <div
            key={day}
            className="py-3 text-center text-sm text-gray-500 font-medium border-l border-gray-100 first:border-l-0"
          >
            {day}
          </div>
        ))}
      </div>

      {/* Scrollable weeks */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto">
        {weeks.map((week, weekIndex) => {
          const monthKey = getMonthKeyForWeek(weekIndex);
          const isBoundary = monthKey !== null;

          return (
            <div
              key={week.weekStart.toISOString()}
              ref={
                isBoundary
                  ? (el) => {
                      if (el) monthMarkerRefs.current.set(monthKey, el);
                      else monthMarkerRefs.current.delete(monthKey);
                    }
                  : undefined
              }
              data-month={monthKey || undefined}
              className="grid grid-cols-7 border-b border-gray-100"
              style={{ height: ROW_HEIGHT }}
            >
              {week.days.map((date, dayIndex) => {
                const dayIsToday = isToday(date);
                const isCurrentMonth =
                  date.getMonth() === visibleMonth.getMonth() &&
                  date.getFullYear() === visibleMonth.getFullYear();
                const isSelected = isSameDay(date, selectedDate);
                const dayEvents = getEventsForDate(date);
                const visibleEvents = dayEvents.slice(0, MAX_VISIBLE_EVENTS);
                const hiddenCount = dayEvents.length - MAX_VISIBLE_EVENTS;

                return (
                  <div
                    key={dayIndex}
                    data-day-cell
                    onClick={(e) => handleDayClick(date, e)}
                    className={`
                    border-l border-gray-100 first:border-l-0 p-1 cursor-pointer
                    hover:bg-gray-50 transition-colors flex flex-col overflow-hidden
                    ${!isCurrentMonth ? "bg-gray-50/50" : ""}
                    ${isSelected && !dayIsToday ? "bg-gray-50" : ""}
                  `}
                  >
                    <div className="flex items-center justify-end mb-1">
                      <span
                        className={`
                        w-7 h-7 flex items-center justify-center rounded-full text-sm
                        ${
                          dayIsToday
                            ? "text-white font-semibold"
                            : isCurrentMonth
                              ? "text-gray-900"
                              : "text-gray-400"
                        }
                      `}
                        style={
                          dayIsToday
                            ? { backgroundColor: "#3E5266" }
                            : undefined
                        }
                      >
                        {date.getDate()}
                      </span>
                    </div>

                    <div className="flex-1 flex flex-col gap-1 overflow-hidden">
                      {visibleEvents.map((event) => {
                        const rsvp = getUserResponseStatus(event);
                        const isOutlined = rsvp === 'tentative' || rsvp === 'needsAction' || rsvp === 'declined';
                        const isDeclined = rsvp === 'declined';

                        if (event.all_day) {
                          const palette = getAllDayEventPalette(event);
                          return (
                            <button
                              key={event.id}
                              onClick={(e) => handleEventClick(e, event)}
                              className="w-full h-5 text-left px-1.5 rounded text-xs truncate transition-opacity hover:opacity-80 flex items-center"
                              style={{
                                backgroundColor: isOutlined ? '#FFFFFF' : palette.bg,
                                color: palette.title,
                                border: isOutlined ? `1px solid ${palette.accent}40` : undefined,
                                textDecoration: isDeclined ? 'line-through' : undefined,
                              }}
                            >
                              {event.title}
                            </button>
                          );
                        }

                        const dotColor = getTimedEventDotColor(event);
                        return (
                          <button
                            key={event.id}
                            onClick={(e) => handleEventClick(e, event)}
                            className="w-full h-5 text-left flex items-center gap-1 px-1 rounded text-xs truncate hover:bg-gray-100 transition-colors"
                          >
                            <span
                              className="w-2 h-2 rounded-full shrink-0"
                              style={{
                                backgroundColor: isOutlined ? 'transparent' : dotColor,
                                border: isOutlined ? `1.5px solid ${dotColor}` : undefined,
                              }}
                            />
                            <span className="text-gray-500 shrink-0" style={{ textDecoration: isDeclined ? 'line-through' : undefined }}>
                              {formatEventTime(event.start_time)}
                            </span>
                            <span className="text-gray-700 truncate" style={{ textDecoration: isDeclined ? 'line-through' : undefined }}>
                              {event.title}
                            </span>
                          </button>
                        );
                      })}

                      {hiddenCount > 0 && (
                        <div className="h-5 text-xs text-gray-400 px-1 flex items-center">
                          +{hiddenCount} more
                        </div>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          );
        })}
      </div>
    </div>
  );
}
