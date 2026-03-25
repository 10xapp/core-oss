import type { CalendarEvent } from "../../../api/client";
import { getAccountPalette } from "../../../utils/accountColors";
import { getUserResponseStatus } from "../../../stores/calendarStore";

interface AllDayEventsSectionProps {
  events: CalendarEvent[];
  onEventClick: (event: CalendarEvent, element: HTMLDivElement) => void;
  maxVisible?: number;
}

export default function AllDayEventsSection({
  events,
  onEventClick,
  maxVisible = 3,
}: AllDayEventsSectionProps) {
  if (events.length === 0) return null;

  const visibleEvents = events.slice(0, maxVisible);
  const remainingCount = events.length - maxVisible;

  return (
    <div className="px-5 py-2 border-b border-border-gray bg-white">
      <div className="flex flex-wrap gap-1.5">
        {visibleEvents.map((event) => {
          const palette = getAccountPalette(event.account_email);
          const rsvp = getUserResponseStatus(event);
          const isOutlined = rsvp === 'tentative' || rsvp === 'needsAction' || rsvp === 'declined';
          const isDeclined = rsvp === 'declined';
          return (
            <button
              key={event.id}
              onClick={(e) =>
                onEventClick(event, e.currentTarget as unknown as HTMLDivElement)
              }
              className="px-3 py-1.5 rounded-md text-sm font-medium transition-opacity hover:opacity-80 focus:outline-none focus:ring-2"
              style={{
                backgroundColor: isOutlined ? '#FFFFFF' : palette.bg,
                color: palette.title,
                border: isOutlined ? `1px solid ${palette.accent}40` : undefined,
                textDecoration: isDeclined ? 'line-through' : undefined,
                // @ts-expect-error CSS custom property for focus ring
                '--tw-ring-color': `${palette.accent}66`,
              }}
            >
              {event.title}
            </button>
          );
        })}
        {remainingCount > 0 && (
          <span className="px-2 py-1.5 text-sm text-text-secondary">
            +{remainingCount} more
          </span>
        )}
      </div>
    </div>
  );
}
