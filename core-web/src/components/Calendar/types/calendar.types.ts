import type { CalendarEvent } from '../../../api/client';

export type ViewMode = 'day' | 'week' | 'month' | 'year';

export interface EventLayoutInfo {
  columnIndex: number;
  totalColumns: number;
  shouldDivideWidth: boolean;
  indentLevel: number;
}

export interface TimeSlot {
  hour: number;
  date: Date;
}

export interface CalendarViewProps {
  events: CalendarEvent[];
  selectedDate: Date;
  onDateSelect: (date: Date) => void;
  onEventClick: (event: CalendarEvent) => void;
  onTimeSlotClick: (slot: TimeSlot) => void;
}

export interface DayViewProps extends CalendarViewProps {
  currentDayIndex: number;
  onDayIndexChange: (index: number) => void;
}

export interface WeekViewProps extends CalendarViewProps {
  currentWeekIndex: number;
  onWeekIndexChange: (index: number) => void;
}

export interface MonthViewProps extends CalendarViewProps {}

export interface EventBlockProps {
  event: CalendarEvent;
  layout: EventLayoutInfo;
  containerWidth: number;
  onClick: (event: CalendarEvent) => void;
  isWeekView?: boolean;
}

export { type CalendarEvent };
