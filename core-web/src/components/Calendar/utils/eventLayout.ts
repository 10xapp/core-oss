import type { CalendarEvent } from '../../../api/client';
import type { EventLayoutInfo } from '../types/calendar.types';
import { getMinutesFromMidnight, getDurationMinutes, isSameDay, parseDateFromISO, parseDateForAllDayEvent } from './dateHelpers';

const MIN_EVENT_DURATION = 30; // Minimum display duration in minutes
const INDENT_PER_LEVEL = 25; // Pixels per overlap level for non-dividing events
const CLOSE_START_THRESHOLD = 59; // Minutes - events starting within this split width

interface EventWithTiming {
  event: CalendarEvent;
  startMinutes: number;
  endMinutes: number;
}

// Get display info for a multi-day event on a specific view date
export function getEventDisplayInfo(event: CalendarEvent, viewDate: Date) {
  const startDate = parseDateFromISO(event.start_time);
  const endDate = parseDateFromISO(event.end_time);

  const startDay = new Date(startDate.getFullYear(), startDate.getMonth(), startDate.getDate());
  const endDay = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate());
  const currentDay = new Date(viewDate.getFullYear(), viewDate.getMonth(), viewDate.getDate());

  const isFirstDay = currentDay.getTime() === startDay.getTime();
  const isLastDay = currentDay.getTime() === endDay.getTime();
  const isMultiDay = startDay.getTime() !== endDay.getTime();

  // Calculate display times for this specific day
  let displayStartTime, displayEndTime;

  if (isFirstDay) {
    displayStartTime = event.start_time;
  } else {
    // Middle or last day: start at midnight
    const year = currentDay.getFullYear();
    const month = String(currentDay.getMonth() + 1).padStart(2, '0');
    const day = String(currentDay.getDate()).padStart(2, '0');
    displayStartTime = `${year}-${month}-${day}T00:00:00`;
  }

  if (isLastDay) {
    displayEndTime = event.end_time;
  } else {
    // First or middle day: end at 23:59
    const year = currentDay.getFullYear();
    const month = String(currentDay.getMonth() + 1).padStart(2, '0');
    const day = String(currentDay.getDate()).padStart(2, '0');
    displayEndTime = `${year}-${month}-${day}T23:59:59`;
  }

  return {
    isMultiDay,
    isFirstDay,
    isLastDay,
    isMiddleDay: !isFirstDay && !isLastDay,
    displayStartTime,
    displayEndTime
  };
}

// Get events for a specific date (excluding all-day events for timeline)
export function getTimedEventsForDate(events: CalendarEvent[], date: Date): CalendarEvent[] {
  return events.filter(event => {
    if (event.all_day) return false;
    const eventDate = parseDateFromISO(event.start_time);
    return isSameDay(eventDate, date);
  });
}

// Get all-day events for a specific date
export function getAllDayEventsForDate(events: CalendarEvent[], date: Date): CalendarEvent[] {
  return events.filter(event => {
    if (!event.all_day) return false;
    // Use parseDateForAllDayEvent to avoid timezone shifts
    const eventDate = parseDateForAllDayEvent(event.start_time);
    return isSameDay(eventDate, date);
  });
}

// Calculate layout information for overlapping events
// For multi-day events, uses display times for the specific viewDate
export function calculateEventLayouts(events: CalendarEvent[], viewDate?: Date): Map<string, EventLayoutInfo> {
  const layouts = new Map<string, EventLayoutInfo>();

  if (events.length === 0) return layouts;

  // Convert events to timing info and sort by start time
  // For multi-day events, use display times for the current view date
  const timedEvents: EventWithTiming[] = events
    .map(event => {
      let startMinutes, endMinutes;

      if (viewDate) {
        const displayInfo = getEventDisplayInfo(event, viewDate);
        startMinutes = getMinutesFromMidnight(displayInfo.displayStartTime);
        endMinutes = getMinutesFromMidnight(displayInfo.displayEndTime);
      } else {
        // Fallback for backward compatibility (when viewDate not provided)
        startMinutes = getMinutesFromMidnight(event.start_time);
        endMinutes = getMinutesFromMidnight(event.start_time) +
          Math.max(getDurationMinutes(event.start_time, event.end_time), MIN_EVENT_DURATION);
      }

      return {
        event,
        startMinutes,
        endMinutes: Math.max(endMinutes, startMinutes + MIN_EVENT_DURATION)
      };
    })
    .sort((a, b) => a.startMinutes - b.startMinutes);

  // Group overlapping events
  const groups: EventWithTiming[][] = [];

  for (const timedEvent of timedEvents) {
    let foundGroup = false;

    for (const group of groups) {
      const overlaps = group.some(existing =>
        timedEvent.startMinutes < existing.endMinutes &&
        timedEvent.endMinutes > existing.startMinutes
      );

      if (overlaps) {
        group.push(timedEvent);
        foundGroup = true;
        break;
      }
    }

    if (!foundGroup) {
      groups.push([timedEvent]);
    }
  }

  // Process each group to assign columns
  for (const group of groups) {
    if (group.length === 1) {
      // Single event - full width, no overlap
      layouts.set(group[0].event.id, {
        columnIndex: 0,
        totalColumns: 1,
        shouldDivideWidth: false,
        indentLevel: 0
      });
      continue;
    }

    // Sort group by start time
    group.sort((a, b) => a.startMinutes - b.startMinutes);

    // Classify events as dividing or non-dividing
    const dividingEvents: EventWithTiming[] = [];
    const nonDividingEvents: EventWithTiming[] = [];

    for (let i = 0; i < group.length; i++) {
      const current = group[i];
      let isDividing = false;

      // Check if this event starts within CLOSE_START_THRESHOLD minutes of any earlier event
      for (let j = 0; j < i; j++) {
        const earlier = group[j];
        const startDiff = current.startMinutes - earlier.startMinutes;
        if (startDiff <= CLOSE_START_THRESHOLD && current.startMinutes < earlier.endMinutes) {
          isDividing = true;
          break;
        }
      }

      // First event is always dividing if there are overlaps
      if (i === 0 && group.length > 1) {
        // Check if any later event starts close to this one
        const hasCloseFollower = group.slice(1).some(later =>
          later.startMinutes - current.startMinutes <= CLOSE_START_THRESHOLD &&
          later.startMinutes < current.endMinutes
        );
        isDividing = hasCloseFollower;
      }

      if (isDividing) {
        dividingEvents.push(current);
      } else {
        nonDividingEvents.push(current);
      }
    }

    // Assign columns to dividing events
    const totalDividingColumns = Math.max(dividingEvents.length, 1);
    dividingEvents.forEach((timedEvent, index) => {
      layouts.set(timedEvent.event.id, {
        columnIndex: index,
        totalColumns: totalDividingColumns,
        shouldDivideWidth: true,
        indentLevel: 0
      });
    });

    // Assign indent levels to non-dividing events
    for (const timedEvent of nonDividingEvents) {
      // Count how many earlier events this overlaps with
      let indentLevel = 0;
      for (const earlier of group) {
        if (earlier === timedEvent) break;
        if (timedEvent.startMinutes < earlier.endMinutes) {
          indentLevel++;
        }
      }

      layouts.set(timedEvent.event.id, {
        columnIndex: 0,
        totalColumns: 1,
        shouldDivideWidth: false,
        indentLevel
      });
    }
  }

  return layouts;
}

// Calculate position and size for an event block
// For multi-day events, viewDate should be provided to get correct display times for that day
export function calculateEventPosition(
  event: CalendarEvent,
  layout: EventLayoutInfo,
  containerWidth: number,
  hourHeight: number = 60,
  timeColumnWidth: number = 53, // 45px + 8px padding
  viewDate?: Date
): {
  top: number;
  left: number;
  width: number;
  height: number;
} {
  let startMinutes: number;
  let endMinutes: number;

  if (viewDate) {
    // Use display times for multi-day event support
    const displayInfo = getEventDisplayInfo(event, viewDate);
    startMinutes = getMinutesFromMidnight(displayInfo.displayStartTime);
    endMinutes = getMinutesFromMidnight(displayInfo.displayEndTime);
  } else {
    // Fallback for backward compatibility
    startMinutes = getMinutesFromMidnight(event.start_time);
    endMinutes = getMinutesFromMidnight(event.end_time);
  }

  const duration = Math.max(endMinutes - startMinutes, MIN_EVENT_DURATION);

  const gridLeftMargin = 8; // Matches the marginLeft on DroppableTimeSlot
  const rightPadding = 16; // Padding on right only
  const availableWidth = containerWidth - timeColumnWidth - gridLeftMargin - rightPadding;

  let left: number;
  let width: number;

  if (layout.shouldDivideWidth) {
    const columnWidth = availableWidth / layout.totalColumns;
    left = timeColumnWidth + gridLeftMargin + (layout.columnIndex * columnWidth);
    width = columnWidth - 2; // 2px gap between columns
  } else {
    const indent = layout.indentLevel * INDENT_PER_LEVEL;
    left = timeColumnWidth + gridLeftMargin + indent;
    width = availableWidth - indent;
  }

  return {
    top: (startMinutes / 60) * hourHeight, // Align with grid line
    left,
    width: Math.max(width, 50), // Minimum 50px width
    height: Math.max((duration / 60) * hourHeight - 2, 26) // Small bottom margin, min 26px
  };
}

// Calculate position for week view (multiple columns)
// For multi-day events, viewDate should be provided to get correct display times for that day
export function calculateWeekEventPosition(
  event: CalendarEvent,
  layout: EventLayoutInfo,
  dayColumnWidth: number,
  _dayIndex: number,
  hourHeight: number = 60,
  _timeColumnWidth: number = 53,
  viewDate?: Date
): {
  top: number;
  left: number;
  width: number;
  height: number;
} {
  let startMinutes: number;
  let endMinutes: number;

  if (viewDate) {
    // Use display times for multi-day event support
    const displayInfo = getEventDisplayInfo(event, viewDate);
    startMinutes = getMinutesFromMidnight(displayInfo.displayStartTime);
    endMinutes = getMinutesFromMidnight(displayInfo.displayEndTime);
  } else {
    // Fallback for backward compatibility
    startMinutes = getMinutesFromMidnight(event.start_time);
    endMinutes = getMinutesFromMidnight(event.end_time);
  }

  const duration = Math.max(endMinutes - startMinutes, MIN_EVENT_DURATION);

  // Left is relative to the day column container (events are rendered inside flex columns)
  let left: number;
  let width: number;

  const rightPadding = 8; // Padding on right only

  if (layout.shouldDivideWidth) {
    const availableWidth = dayColumnWidth - rightPadding;
    const eventWidth = availableWidth / layout.totalColumns;
    left = layout.columnIndex * eventWidth;
    width = eventWidth - 2; // 2px gap between columns
  } else {
    const indent = Math.min(layout.indentLevel * 8, dayColumnWidth * 0.3); // Smaller indent for week view
    left = indent;
    width = dayColumnWidth - rightPadding - indent;
  }

  return {
    top: (startMinutes / 60) * hourHeight,
    left,
    width: Math.max(width, 20),
    height: Math.max((duration / 60) * hourHeight - 2, 20)
  };
}
