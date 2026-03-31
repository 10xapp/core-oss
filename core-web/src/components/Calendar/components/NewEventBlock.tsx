import { useEffect, useRef } from "react";
import { motion } from "motion/react";
import { formatTimeRangeCompact } from "../utils/dateHelpers";
import { useCalendarStore } from "../../../stores/calendarStore";

/** Spring aligned with 15-min vertical snap (hourHeight/4 px); tuned for quick follow without losing the smooth spring */
const DRAG_CREATE_SPRING = {
  type: "spring" as const,
  stiffness: 780,
  damping: 42,
  mass: 0.55,
};

interface NewEventBlockProps {
  date: Date;
  hour: number;
  minute?: number;
  endDate?: Date;
  endHour?: number;
  endMinute?: number;
  containerWidth: number;
  isWeekView?: boolean;
  hourHeight?: number;
  timeColumnWidth?: number;
  title?: string;
  /** True while pointer is down for drag-to-create — enables magnetic spring on size/position */
  isDragging?: boolean;
}

export default function NewEventBlock({
  date,
  hour,
  minute = 0,
  endDate,
  endHour,
  endMinute = 0,
  containerWidth,
  isWeekView = false,
  hourHeight = 60,
  timeColumnWidth = 53,
  title = "",
  isDragging = false,
}: NewEventBlockProps) {
  const blockRef = useRef<HTMLDivElement>(null);
  const updatePendingEventRect = useCalendarStore(
    (state) => state.updatePendingEventRect,
  );
  // Calculate position similar to DraggableEventBlock
  const timeLabelWidth = timeColumnWidth - 8;

  // Determine end time (for drag-to-create, use endHour/endMinute, otherwise default to 1 hour)
  const finalEndHour = endHour !== undefined ? endHour : hour + 1;
  const finalEndMinute = endMinute !== undefined ? endMinute : 0;

  // Calculate total duration in minutes, accounting for day wraparound
  const startTotalMinutes = hour * 60 + minute;
  let endTotalMinutes = finalEndHour * 60 + finalEndMinute;

  // Check if end time wraps to next day (same logic as time display)
  const endTimeWrapsToNextDay =
    finalEndHour > 23 ||
    finalEndHour < hour ||
    (finalEndHour === hour && finalEndMinute < minute);

  // If it wraps to next day, add 24 hours to the end time for duration calculation
  if (endTimeWrapsToNextDay && !endDate) {
    endTotalMinutes += 24 * 60;
  }

  const durationMinutes = Math.max(15, endTotalMinutes - startTotalMinutes); // Minimum 15 minutes

  let position: { top: number; left: number; width: number; height: number };

  if (isWeekView) {
    const dayColumnWidth = (containerWidth - timeLabelWidth) / 7;
    position = {
      top: ((hour * 60 + minute) / 60) * hourHeight,
      left: 2,
      width: dayColumnWidth - 4,
      height: (durationMinutes / 60) * hourHeight - 2,
    };
  } else {
    position = {
      top: ((hour * 60 + minute) / 60) * hourHeight,
      left: timeColumnWidth,
      width: containerWidth - timeColumnWidth - 8,
      height: (durationMinutes / 60) * hourHeight - 2,
    };
  }

  // Format time for display - match the save logic
  const startTime = new Date(date);
  startTime.setHours(hour, minute, 0, 0);

  // Use explicit endDate if provided, otherwise check if time wraps to next day
  const endTime = new Date(endDate || date);
  let actualEndHour = finalEndHour;

  // Check if end time wraps to next day (when no explicit endDate is set)
  if (!endDate) {
    const endTimeWrapsToNextDay =
      finalEndHour > 23 ||
      finalEndHour < hour ||
      (finalEndHour === hour && finalEndMinute < minute);

    if (endTimeWrapsToNextDay && finalEndHour <= 23) {
      // Only adjust date if hour is valid (not already > 23)
      endTime.setDate(endTime.getDate() + 1);
    }
    if (finalEndHour > 23) {
      actualEndHour = finalEndHour - 24;
    }
  }

  endTime.setHours(actualEndHour, finalEndMinute, 0, 0);
  const timeDisplay = formatTimeRangeCompact(
    startTime.toISOString(),
    endTime.toISOString(),
  );

  // Show time for events >= 45 minutes in week view, >= 1 hour in day view
  // Day view: only show if enough space (don't overlap with title)
  const isVeryShortEvent = position.height < 25;
  const isShortEvent = position.height < 40;
  const timeThreshold = isWeekView ? 35 : 50;
  const showTime = position.height >= timeThreshold && !isVeryShortEvent;

  // Update the triggerRect in the store so the popover positions correctly
  useEffect(() => {
    if (blockRef.current) {
      const rect = blockRef.current.getBoundingClientRect();
      updatePendingEventRect(rect);
    }
  }, [
    updatePendingEventRect,
    position.top,
    position.left,
    position.width,
    position.height,
  ]);

  const getPadding = () => {
    if (isWeekView) return "6px 8px";
    if (isVeryShortEvent) return "1px 6px";
    if (isShortEvent) return "4px 8px";
    return "10px 8px";
  };

  return (
    <motion.div
      ref={blockRef}
      initial={false}
      animate={{
        top: position.top,
        left: position.left,
        width: position.width,
        height: position.height,
      }}
      transition={isDragging ? DRAG_CREATE_SPRING : { duration: 0 }}
      onAnimationComplete={() => {
        if (blockRef.current) {
          updatePendingEventRect(blockRef.current.getBoundingClientRect());
        }
      }}
      className="absolute pointer-events-auto text-left bg-[#D6EFF8] rounded-md ring-2 ring-[#35A9DD] overflow-hidden"
      style={{ zIndex: 50 }}
    >
      <div
        className={`h-full flex ${isVeryShortEvent ? 'items-center gap-1.5' : 'gap-2.5'}`}
        style={{
          padding: getPadding(),
        }}
      >
        <div
          className="shrink-0 rounded-full self-stretch"
          style={{
            width: isVeryShortEvent ? 3 : 4,
            backgroundColor: '#35A9DD',
          }}
        />
        <div className="flex-1 overflow-hidden min-w-0">
          <p
            className="font-medium truncate"
            style={{
              fontSize: isVeryShortEvent ? 11 : 12,
              lineHeight: 1.2,
              color: '#19556E',
            }}
          >
            {title || "New event"}
          </p>
          {showTime && (
            <p
              className="truncate"
              style={{
                fontSize: 10,
                marginTop: 1,
                color: '#2680A5',
              }}
            >
              {timeDisplay}
            </p>
          )}
        </div>
      </div>
    </motion.div>
  );
}
