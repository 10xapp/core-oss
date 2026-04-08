import { useState, useCallback, useEffect, useRef, useMemo } from "react";
import { useDraggable } from "@dnd-kit/core";
import type { CalendarEvent } from "../../../api/client";
import type { EventLayoutInfo } from "../types/calendar.types";
import { formatTimeRangeCompact } from "../utils/dateHelpers";
import {
  calculateEventPosition,
  calculateWeekEventPosition,
  getEventDisplayInfo,
} from "../utils/eventLayout";
import { useCalendarStore, getUserResponseStatus } from "../../../stores/calendarStore";
import { getAccountPalette } from "../../../utils/accountColors";
import cn from "classnames";

interface DraggableEventBlockProps {
  event: CalendarEvent;
  layout: EventLayoutInfo;
  containerWidth: number;
  dayIndex?: number;
  onClick: (event: CalendarEvent, element: HTMLDivElement) => void;
  isWeekView?: boolean;
  hourHeight?: number;
  timeColumnWidth?: number;
  viewDate?: Date;
}

export default function DraggableEventBlock({
  event,
  layout,
  containerWidth,
  dayIndex = 0,
  onClick,
  isWeekView = false,
  hourHeight = 60,
  timeColumnWidth = 53,
  viewDate,
}: DraggableEventBlockProps) {
  // Calculate position early to include width in drag data
  const timeLabelWidth = timeColumnWidth - 8;
  const position = isWeekView
    ? calculateWeekEventPosition(
        event,
        layout,
        (containerWidth - timeLabelWidth) / 7,
        dayIndex,
        hourHeight,
        timeLabelWidth,
        viewDate,
      )
    : calculateEventPosition(
        event,
        layout,
        containerWidth,
        hourHeight,
        timeColumnWidth,
        viewDate,
      );

  const { attributes, listeners, setNodeRef, isDragging } = useDraggable({
    id: event.id,
    data: {
      event,
      type: "event",
      width: position.width,
      height: position.height,
    },
  });

  const resizeEvent = useCalendarStore((state) => state.resizeEvent);
  const [isResizing, setIsResizing] = useState<"top" | "bottom" | null>(null);
  const [resizeOffset, setResizeOffset] = useState(0);
  const startYRef = useRef(0);

  // Parse event times to minutes from midnight (in local timezone)
  const getEventMinutes = useCallback(() => {
    const parseToLocalMinutes = (dateString: string) => {
      const isUTC =
        dateString.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(dateString);
      if (isUTC) {
        const date = new Date(dateString);
        return date.getHours() * 60 + date.getMinutes();
      } else {
        const timeMatch = dateString.match(/T(\d{2}):(\d{2})/);
        if (timeMatch) {
          return parseInt(timeMatch[1], 10) * 60 + parseInt(timeMatch[2], 10);
        }
        return 0;
      }
    };

    return {
      start: parseToLocalMinutes(event.start_time),
      end: parseToLocalMinutes(event.end_time),
    };
  }, [event.start_time, event.end_time]);

  // Handle resize start
  const handleResizeStart = useCallback(
    (edge: "top" | "bottom", e: React.PointerEvent) => {
      e.stopPropagation();
      e.preventDefault();
      setIsResizing(edge);
      startYRef.current = e.clientY;
    },
    [],
  );

  // Handle resize move
  useEffect(() => {
    if (!isResizing) return;

    const handlePointerMove = (e: PointerEvent) => {
      const deltaY = e.clientY - startYRef.current;
      // Snap to 15-minute increments (hourHeight / 4)
      const snapIncrement = hourHeight / 4;
      const snappedDelta = Math.round(deltaY / snapIncrement) * snapIncrement;
      setResizeOffset(snappedDelta);
    };

    const handlePointerUp = () => {
      const { start, end } = getEventMinutes();
      // Convert pixel offset to minutes (hourHeight pixels = 60 minutes)
      const minutesDelta = Math.round((resizeOffset / hourHeight) * 60);

      let newStart = start;
      let newEnd = end;

      if (isResizing === "top") {
        newStart = Math.max(0, Math.min(start + minutesDelta, end - 15));
      } else if (isResizing === "bottom") {
        // Clamp to end of day (23:59 = 1439 minutes) to prevent resizing past midnight
        newEnd = Math.min(1439, Math.max(end + minutesDelta, start + 15));
      }

      // Only update if there was an actual change
      if (newStart !== start || newEnd !== end) {
        resizeEvent(event.id, newStart, newEnd);
      }

      setIsResizing(null);
      setResizeOffset(0);
    };

    document.addEventListener("pointermove", handlePointerMove);
    document.addEventListener("pointerup", handlePointerUp);
    return () => {
      document.removeEventListener("pointermove", handlePointerMove);
      document.removeEventListener("pointerup", handlePointerUp);
    };
  }, [
    isResizing,
    resizeOffset,
    hourHeight,
    event.id,
    getEventMinutes,
    resizeEvent,
  ]);

  // Calculate preview times during resize
  const getPreviewTimes = useCallback(() => {
    if (!isResizing || resizeOffset === 0) return null;

    const { start, end } = getEventMinutes();
    const minutesDelta = Math.round((resizeOffset / hourHeight) * 60);

    let newStart = start;
    let newEnd = end;

    if (isResizing === "top") {
      newStart = Math.max(0, Math.min(start + minutesDelta, end - 15));
    } else {
      newEnd = Math.min(24 * 60, Math.max(end + minutesDelta, start + 15));
    }

    const formatMinutes = (mins: number) => {
      const h = Math.floor(mins / 60);
      const m = mins % 60;
      const period = h >= 12 ? "PM" : "AM";
      const displayH = h === 0 ? 12 : h > 12 ? h - 12 : h;
      return `${displayH}:${String(m).padStart(2, "0")} ${period}`;
    };

    return `${formatMinutes(newStart)} - ${formatMinutes(newEnd)}`;
  }, [isResizing, resizeOffset, hourHeight, getEventMinutes]);

  // Apply resize offset to position
  let adjustedTop = position.top;
  let adjustedHeight = position.height;
  if (isResizing === "top") {
    adjustedTop = position.top + resizeOffset;
    adjustedHeight = position.height - resizeOffset;
  } else if (isResizing === "bottom") {
    adjustedHeight = position.height + resizeOffset;
  }

  // Show time for events >= 45 minutes in week view, >= 1 hour in day view
  // Day view: only show if enough space (don't overlap with title)
  const isVeryShortEvent = adjustedHeight < 25;
  const isShortEvent = adjustedHeight < 40;
  const timeThreshold = isWeekView ? 35 : 50;
  const showTime = adjustedHeight >= timeThreshold && !isVeryShortEvent;

  // Get display info for multi-day events
  const displayInfo = viewDate ? getEventDisplayInfo(event, viewDate) : null;

  // Get account-based color palette and RSVP status
  const palette = useMemo(() => getAccountPalette(event.account_email), [event.account_email]);
  const rsvpStatus = useMemo(() => getUserResponseStatus(event), [event.attendees, event.account_email]);
  const isOutlined = rsvpStatus === 'tentative' || rsvpStatus === 'needsAction' || rsvpStatus === 'declined';
  const isDeclined = rsvpStatus === 'declined';

  // When dragging, hide the original element since DragOverlay shows the preview
  const style = {
    top: adjustedTop,
    left: position.left,
    width: position.width,
    height: adjustedHeight,
    opacity: isDragging ? 0 : 1,
    cursor: "default",
    zIndex: isResizing ? 100 : 10,
    transition: isResizing ? "none" : "opacity 0.2s",
  };

  // Don't apply drag listeners while resizing
  const dragListeners = isResizing ? {} : listeners;

  // For short events, reduce padding so title fits
  const getPadding = () => {
    if (isWeekView) return "6px 8px";
    if (isVeryShortEvent) return "1px 6px";
    if (isShortEvent) return "4px 8px";
    return "10px 8px";
  };

  return (
    <div
      ref={setNodeRef}
      {...attributes}
      data-event-id={event.id}
      onClick={(e) => {
        console.log("[DraggableEventBlock] onClick fired", {
          eventId: event.id,
          eventTitle: event.title,
          isDragging,
          isResizing,
          eventType: e.type,
          target: e.target,
          currentTarget: e.currentTarget,
        });
        if (!isDragging && !isResizing) {
          console.log(
            "[DraggableEventBlock] Calling onClick callback for event:",
            event.title,
          );
          e.stopPropagation();
          onClick(event, e.currentTarget as HTMLDivElement);
        } else {
          console.log(
            "[DraggableEventBlock] onClick blocked - isDragging:",
            isDragging,
            "isResizing:",
            isResizing,
          );
        }
      }}
      {...dragListeners}
      className={cn(
        "absolute pointer-events-auto text-left transition-opacity focus:outline-none focus:ring-2 select-none rounded-md group overflow-hidden",
        displayInfo?.isMultiDay && "multi-day-event",
        displayInfo?.isFirstDay && "multi-day-first",
        displayInfo?.isMiddleDay && "multi-day-middle",
        displayInfo?.isLastDay && "multi-day-last",
      )}
      style={{
        ...style,
        backgroundColor: isOutlined ? 'var(--color-bg-white)' : palette.bg,
        border: isOutlined ? `1px solid ${palette.accent}40` : undefined,
        // @ts-expect-error CSS custom property for focus ring
        '--tw-ring-color': `${palette.accent}66`,
      }}
    >
      {/* Top resize handle - only visible on hover */}
      <div
        onPointerDown={(e) => {
          e.stopPropagation();
          handleResizeStart("top", e);
        }}
        className="absolute top-0 left-0 right-0 h-2 cursor-ns-resize z-10 opacity-0 group-hover:opacity-100"
      />

      <div
        className={cn(
          "relative h-full flex",
          isVeryShortEvent ? "items-center gap-1.5" : "gap-2.5"
        )}
        style={{
          padding: getPadding(),
        }}
      >
        {/* Left accent bar */}
        <div
          className="shrink-0 rounded-full self-stretch"
          style={{
            width: isVeryShortEvent ? 3 : 4,
            backgroundColor: rsvpStatus === 'needsAction' ? 'transparent' : palette.accent,
            border: rsvpStatus === 'needsAction' ? `1.5px dashed ${palette.accent}` : undefined,
          }}
        />

        {/* Left continuation indicator for multi-day events */}
        {displayInfo?.isMultiDay && !displayInfo?.isFirstDay && (
          <div className="absolute left-0 top-1/2 transform -translate-y-1/2 opacity-40 text-sm">
            ←
          </div>
        )}

        <div className="flex-1 overflow-hidden min-w-0">
          <p
            className="font-medium truncate"
            style={{
              fontSize: isVeryShortEvent ? 11 : 12,
              lineHeight: 1.2,
              color: palette.title,
              textDecoration: isDeclined ? 'line-through' : undefined,
            }}
          >
            {event.title}
          </p>
          {showTime && (
            <p
              className="truncate"
              style={{
                fontSize: 10,
                marginTop: 1,
                color: palette.time,
                textDecoration: isDeclined ? 'line-through' : undefined,
              }}
            >
              {getPreviewTimes() ||
                formatTimeRangeCompact(
                  displayInfo?.displayStartTime || event.start_time,
                  displayInfo?.displayEndTime || event.end_time,
                )}
            </p>
          )}
        </div>

        {/* Right continuation indicator for multi-day events */}
        {displayInfo?.isMultiDay && !displayInfo?.isLastDay && (
          <div className="absolute right-0 top-1/2 transform -translate-y-1/2 opacity-40 text-sm">
            →
          </div>
        )}
      </div>

      {/* Bottom resize handle - only visible on hover */}
      <div
        onPointerDown={(e) => {
          e.stopPropagation();
          handleResizeStart("bottom", e);
        }}
        className="absolute bottom-0 left-0 right-0 h-2 cursor-ns-resize z-10 opacity-0 group-hover:opacity-100"
      />
    </div>
  );
}

// Static event block for drag overlay
export function EventBlockOverlay({
  event,
  isWeekView = false,
  height,
  width,
  offsetMinutes = 0,
}: {
  event: CalendarEvent;
  isWeekView?: boolean;
  height?: number;
  width?: number;
  offsetMinutes?: number;
}) {
  // Show time for events >= 45 minutes in week view, >= 1 hour in day view
  // Day view: only show if enough space (don't overlap with title)
  const timeThreshold = isWeekView ? 35 : 50;
  const isVeryShort = height && height < 25;
  const showTime = !height || (height >= timeThreshold && !isVeryShort);
  const palette = useMemo(() => getAccountPalette(event.account_email), [event.account_email]);
  const rsvpStatus = useMemo(() => getUserResponseStatus(event), [event.attendees, event.account_email]);
  const isOutlined = rsvpStatus === 'tentative' || rsvpStatus === 'needsAction' || rsvpStatus === 'declined';
  const isDeclined = rsvpStatus === 'declined';

  // Calculate adjusted times based on offset
  const parseTime = (dateString: string) => {
    const date = new Date(dateString);
    return date.getHours() * 60 + date.getMinutes();
  };

  const startMins = parseTime(event.start_time);
  const endMins = parseTime(event.end_time);
  const newStartMins = startMins + offsetMinutes;
  const newEndMins = endMins + offsetMinutes;

  // Format minutes to time string (e.g., "2:30 PM")
  const formatMinutes = (mins: number) => {
    const hours = Math.floor(mins / 60) % 24;
    const minutes = mins % 60;
    const period = hours >= 12 ? "PM" : "AM";
    const displayHours = hours === 0 ? 12 : hours > 12 ? hours - 12 : hours;
    return `${displayHours}:${String(minutes).padStart(2, "0")} ${period}`;
  };

  const displayTime =
    offsetMinutes !== 0
      ? `${formatMinutes(newStartMins)} - ${formatMinutes(newEndMins)}`
      : formatTimeRangeCompact(event.start_time, event.end_time);

  return (
    <div
      className="rounded-md overflow-hidden border-2"
      style={{
        width: width || (isWeekView ? 120 : 200),
        height: height || "auto",
        minHeight: 30,
        boxShadow: "0 4px 6px -1px rgba(0, 0, 0, 0.1)",
        backgroundColor: isOutlined ? '#FFFFFFcc' : `${palette.bg}cc`,
        borderColor: palette.accent,
      }}
    >
      <div
        className="h-full flex gap-2.5"
        style={{
          padding: isWeekView ? "6px 8px" : "10px 8px",
        }}
      >
        {/* Left accent bar */}
        <div className="w-1 shrink-0 rounded-full" style={{
          backgroundColor: rsvpStatus === 'needsAction' ? 'transparent' : palette.accent,
          border: rsvpStatus === 'needsAction' ? `1.5px dashed ${palette.accent}` : undefined,
        }} />
        <div className="flex-1 overflow-hidden min-w-0">
          <p
            className="font-medium truncate"
            style={{
              fontSize: 12,
              lineHeight: 1.3,
              color: palette.title,
              textDecoration: isDeclined ? 'line-through' : undefined,
            }}
          >
            {event.title}
          </p>
          {showTime && (
            <p
              className="truncate font-medium"
              style={{
                fontSize: 10,
                marginTop: 1,
                color: palette.time,
                textDecoration: isDeclined ? 'line-through' : undefined,
              }}
            >
              {displayTime}
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
