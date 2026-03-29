import { useEffect, useCallback, useState, useRef } from "react";
import { Plus, LoaderCircle } from "lucide-react";
import { Icon } from "../ui/Icon";
import { AnimatePresence, motion } from "motion/react";
import {
  DndContext,
  DragOverlay,
  pointerWithin,
  useSensor,
  useSensors,
  PointerSensor,
  useDndMonitor,
} from "@dnd-kit/core";
import { useSearchParams } from "react-router-dom";
// Removed restrictToVerticalAxis to allow cross-day dragging
import type { CalendarEvent } from "../../api/client";
import { useCalendarStore } from "../../stores/calendarStore";
import CalendarHeader from "./components/CalendarHeader";
import ViewModeSelector from "./components/ViewModeSelector";
import DayView from "./components/DayView";
import WeekView from "./components/WeekView";
import MonthView from "./components/MonthView";
import YearView from "./components/YearView";
import MiniMonthCalendar from "./components/MiniMonthCalendar";
import EventPopover from "./components/EventPopover";
import NewEventPopover from "./components/NewEventPopover";
import { EventBlockOverlay } from "./components/DraggableEventBlock";
import { eventCreationFlags } from "./utils/eventCreationFlags";
import { getAccountAccentColor, registerAccountOrder } from "../../utils/accountColors";
import { SIDEBAR } from "../../lib/sidebar";
import { HeaderButtons } from "../MiniAppHeader/MiniAppHeader";

// Keep sync functionality available via keyboard shortcut (S key)

// Custom modifier to snap to 15-minute increments (allows horizontal movement for cross-day dragging)
function snapToFifteenMinutes({ transform }: any) {
  const hourHeight = 60; // pixels per hour
  const snapIncrement = hourHeight / 4; // 15 minutes = 1/4 hour
  return {
    ...transform,
    // Keep x as-is to allow horizontal dragging between days
    y: Math.round(transform.y / snapIncrement) * snapIncrement,
  };
}

// Content component that uses useDndMonitor (must be inside DndContext)
function CalendarContent({
  selectedDate,
  viewMode,
  swipeableDays,
  dayIndex,
  isLoading,
  error,
  isSyncing,
  setViewMode,
  setSelectedDate,
  setDayIndex,
  handleEventClick,
  handleTimeSlotClick,
  handleSwitchToDayView,
  handleSwitchToMonthView,
  handlePrevious,
  handleNext,
  handleToday,
  selectedEventData,
  setSelectedEventData,
  popoverCloseTimeRef,
  eventCreationFlags,
  setDragOffset,
  lastDragOffsetRef,
}: any) {
  // Monitor drag movement to track offset - must be inside DndContext
  useDndMonitor({
    onDragMove(event) {
      if (event.delta) {
        setDragOffset(event.delta);
        lastDragOffsetRef.current = event.delta;
      }
    },
    onDragEnd() {
      setDragOffset(null);
    },
    onDragCancel() {
      setDragOffset(null);
    },
  });

  return (
    <div className="flex-1 flex flex-col h-full overflow-hidden bg-white">
      {/* Header: matches mini app h-12 pattern */}
      <div className="h-12 flex items-center justify-between pl-4 pr-2 shrink-0 border-b border-border-gray">
        {/* Left side: date header with navigation */}
        <div className="flex items-center gap-1">
          <CalendarHeader
            date={selectedDate}
            viewMode={viewMode}
            onPrevious={handlePrevious}
            onNext={handleNext}
            onToday={handleToday}
          />
          {isSyncing && (
            <Icon
              icon={LoaderCircle}
              size={14}
              className="animate-spin text-gray-400 ml-1.5"
            />
          )}
        </div>

        {/* Right side: view mode selector + header buttons */}
        <div className="flex items-center gap-2">
          <ViewModeSelector value={viewMode} onChange={setViewMode} />
          <HeaderButtons />
        </div>
      </div>

      {/* Main content area */}
      {isLoading ? (
        <div className="flex-1 flex items-center justify-center text-text-secondary">
          <Icon
            icon={LoaderCircle}
            size={24}
            className="animate-spin mr-2"
          />
          Loading calendar...
        </div>
      ) : error ? (
        <div className="flex-1 flex items-center justify-center text-red-500">
          {error}
        </div>
      ) : (
        <AnimatePresence mode="wait">
          <motion.div
            key={viewMode}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="flex-1 flex flex-col overflow-hidden"
          >
            {viewMode === "day" && (
              <DayView
                selectedDate={selectedDate}
                onEventClick={handleEventClick}
                onTimeSlotClick={handleTimeSlotClick}
                swipeableDays={swipeableDays}
                currentDayIndex={dayIndex}
                onDayIndexChange={setDayIndex}
                onDateChange={setSelectedDate}
              />
            )}
            {viewMode === "week" && (
              <WeekView
                selectedDate={selectedDate}
                onEventClick={handleEventClick}
                onTimeSlotClick={handleTimeSlotClick}
                onDateSelect={handleSwitchToDayView}
              />
            )}
            {viewMode === "month" && (
              <MonthView
                selectedDate={selectedDate}
                onDateSelect={setSelectedDate}
                onEventClick={handleEventClick}
                onVisibleMonthChange={setSelectedDate}
              />
            )}
            {viewMode === "year" && (
              <YearView
                selectedDate={selectedDate}
                onMonthSelect={handleSwitchToMonthView}
                onDaySelect={handleSwitchToDayView}
              />
            )}
          </motion.div>
        </AnimatePresence>
      )}

      {/* New Event Popover (inline creation) */}
      <NewEventPopover />

      {/* Event Popover */}
      {(() => {
        console.log(
          "[CalendarContent] Checking if should render EventPopover - selectedEventData:",
          selectedEventData
            ? {
                eventId: selectedEventData.event.id,
                eventTitle: selectedEventData.event.title,
              }
            : null,
        );
        return (
          selectedEventData && (
            <EventPopover
              event={selectedEventData.event}
              triggerRect={selectedEventData.rect}
              onClose={() => {
                console.log("[CalendarContent] EventPopover onClose called");
                popoverCloseTimeRef.current = Date.now();
                eventCreationFlags.popoverIsClosing = true;
                setSelectedEventData(null);
                setTimeout(() => {
                  eventCreationFlags.popoverIsClosing = false;
                }, 100);
              }}
              onUpdated={() => {
                console.log("[CalendarContent] EventPopover onUpdated called");
                setSelectedEventData(null);
              }}
              onDeleted={() => {
                console.log("[CalendarContent] EventPopover onDeleted called");
                setSelectedEventData(null);
              }}
            />
          )
        );
      })()}

      {/* Drag overlay - moved outside content div */}
    </div>
  );
}

export default function CalendarView() {
  // Get state and actions from store
  const {
    viewMode,
    setViewMode,
    selectedDate,
    setSelectedDate,
    events,
    dayIndex,
    setDayIndex,
    swipeableDays,
    isLoading,
    isSyncing,
    error,
    pendingEvent,
    navigate,
    goToToday,
    syncEvents,
    startCreatingEvent,
    cancelCreatingEvent,
    rescheduleEvent,
    accountsStatus,
    selectedAccountIds,
    setSelectedAccounts,
    accountSelectionInitialized,
    toggleAccountSelection,
  } = useCalendarStore();
  const [searchParams] = useSearchParams();
  const handledNotificationTargetRef = useRef<string | null>(null);

  // Drag state
  const [activeEvent, setActiveEvent] = useState<CalendarEvent | null>(null);
  const [activeEventWidth, setActiveEventWidth] = useState<number | null>(null);
  const [activeEventHeight, setActiveEventHeight] = useState<number | null>(
    null,
  );
  const [dragOffset, setDragOffset] = useState<{ x: number; y: number } | null>(
    null,
  );
  const lastDragOffsetRef = useRef<{ x: number; y: number } | null>(null);

  // Event popover state
  const [selectedEventData, setSelectedEventData] = useState<{
    event: CalendarEvent;
    rect: DOMRect;
  } | null>(null);
  const popoverCloseTimeRef = useRef(0);
  const pendingEventCloseTimeRef = useRef(0);

  // Track when pending event is closed
  useEffect(() => {
    if (!pendingEvent) {
      pendingEventCloseTimeRef.current = Date.now();
    }
  }, [pendingEvent]);

  // Log whenever selectedEventData changes
  useEffect(() => {
    console.log(
      "[CalendarView] selectedEventData changed:",
      selectedEventData
        ? {
            eventId: selectedEventData.event.id,
            eventTitle: selectedEventData.event.title,
          }
        : null,
    );
  }, [selectedEventData]);

  // Dismiss popovers when view mode changes
  useEffect(() => {
    setSelectedEventData(null);
    cancelCreatingEvent();
  }, [viewMode, cancelCreatingEvent]);

  useEffect(() => {
    const targetEventId = searchParams.get("event_id");
    const targetDate = searchParams.get("date");
    const targetAccountEmail = searchParams.get("account_email");
    const focusToken = searchParams.get("focus");

    if (!targetEventId && !targetDate) {
      handledNotificationTargetRef.current = null;
      return;
    }

    const targetKey = `${targetEventId ?? ""}|${targetDate ?? ""}|${targetAccountEmail ?? ""}|${focusToken ?? ""}`;
    if (handledNotificationTargetRef.current === targetKey) {
      return;
    }

    // Mark as handled immediately to prevent re-entry from state changes below
    handledNotificationTargetRef.current = targetKey;

    if (targetDate) {
      const nextDate = new Date(targetDate);
      if (!Number.isNaN(nextDate.getTime())) {
        setSelectedDate(nextDate);
      }
    }

    if (
      targetAccountEmail
      && accountSelectionInitialized
      && !selectedAccountIds.includes(targetAccountEmail)
    ) {
      setSelectedAccounts([...selectedAccountIds, targetAccountEmail]);
      return;
    }

    if (!targetEventId) {
      return;
    }

    const targetEvent = events.find((event) => event.id === targetEventId);
    if (!targetEvent) {
      return;
    }

    if (
      targetEvent.account_email
      && accountSelectionInitialized
      && !selectedAccountIds.includes(targetEvent.account_email)
    ) {
      setSelectedAccounts([...selectedAccountIds, targetEvent.account_email]);
      return;
    }

    setViewMode("day");
    const animationFrame = window.requestAnimationFrame(() => {
      const targetElement = document.querySelector(`[data-event-id="${targetEventId}"]`);
      if (!(targetElement instanceof HTMLElement)) {
        return;
      }

      setSelectedEventData({
        event: targetEvent,
        rect: targetElement.getBoundingClientRect(),
      });
    });

    return () => window.cancelAnimationFrame(animationFrame);
  }, [
    accountSelectionInitialized,
    events,
    searchParams,
    selectedAccountIds,
    setSelectedAccounts,
    setSelectedDate,
    setSelectedEventData,
    setViewMode,
  ]);

  // Configure drag sensors with distance threshold to prevent accidental drags on click
  const sensors = useSensors(
    useSensor(PointerSensor, {
      activationConstraint: {
        distance: 8, // Require 8px of movement before drag starts
      },
    }),
  );

  // Register account order for consistent color assignment (first account = blue, etc.)
  useEffect(() => {
    if (accountsStatus.length > 0) {
      registerAccountOrder(accountsStatus);
    }
  }, [accountsStatus]);

  // Poll for new events every 5 minutes
  useEffect(() => {
    const intervalId = setInterval(
      () => {
        syncEvents();
      },
      5 * 60 * 1000,
    ); // 5 minutes

    return () => clearInterval(intervalId);
  }, [syncEvents]);

  // Event handlers
  const handleEventClick = useCallback(
    (event: CalendarEvent, element: HTMLDivElement) => {
      console.log("[CalendarView.handleEventClick] Called with event:", {
        eventId: event.id,
        eventTitle: event.title,
        element,
        currentSelectedEvent: selectedEventData?.event.id,
        viewMode,
      });

      // Ignore clicks on the SAME event if popover just closed (prevents reopen on click-outside)
      // But allow clicks on DIFFERENT events to always work
      const isSameEvent = selectedEventData?.event.id === event.id;
      const timeSinceClose = Date.now() - popoverCloseTimeRef.current;
      console.log(
        "[CalendarView.handleEventClick] isSameEvent:",
        isSameEvent,
        "timeSinceClose:",
        timeSinceClose,
      );

      if (isSameEvent && timeSinceClose < 100) {
        console.log(
          "[CalendarView.handleEventClick] Ignoring - same event and popover just closed",
        );
        return;
      }

      // Toggle: close if clicking the same event, otherwise open the new event's popover
      if (isSameEvent) {
        console.log(
          "[CalendarView.handleEventClick] Closing popover for same event",
        );
        setSelectedEventData(null);
      } else {
        const rect = element.getBoundingClientRect();
        console.log(
          "[CalendarView.handleEventClick] Opening popover with rect:",
          rect,
        );
        console.log(
          "[CalendarView.handleEventClick] Setting selectedEventData:",
          { event, rect },
        );
        setSelectedEventData({ event, rect });
      }
    },
    [selectedEventData?.event.id, viewMode],
  );

  const handleTimeSlotClick = useCallback(
    (date: Date, hour: number, triggerRect: DOMRect) => {
      // Don't create events if popover is closing (prevents race condition with mousedown/click events)
      if (eventCreationFlags.popoverIsClosing) {
        return;
      }

      // Don't create events if pending event is closing (prevents creating new event when dismissing draft)
      if (eventCreationFlags.pendingEventIsClosing) {
        return;
      }

      // If an event popover is open, just close it (don't create a new event)
      if (selectedEventData) {
        setSelectedEventData(null);
        return;
      }
      // If already creating an event, dismiss it
      if (pendingEvent) {
        eventCreationFlags.pendingEventIsClosing = true;
        cancelCreatingEvent();
        setTimeout(() => {
          eventCreationFlags.pendingEventIsClosing = false;
        }, 100);
        return;
      }
      startCreatingEvent(date, hour, triggerRect);
    },
    [startCreatingEvent, cancelCreatingEvent, selectedEventData, pendingEvent],
  );

  const handleNewEvent = useCallback(
    (e?: React.MouseEvent<HTMLButtonElement>) => {
      // Use button position if available, otherwise center of screen
      const triggerRect = e
        ? e.currentTarget.getBoundingClientRect()
        : new DOMRect(
            window.innerWidth / 2 - 100,
            window.innerHeight / 3,
            200,
            60,
          );
      const currentHour = new Date().getHours();
      startCreatingEvent(selectedDate, currentHour, triggerRect);
    },
    [startCreatingEvent, selectedDate],
  );

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Don't handle shortcuts when typing in input fields or creating event
      if (
        e.target instanceof HTMLInputElement ||
        e.target instanceof HTMLTextAreaElement ||
        pendingEvent
      ) {
        return;
      }

      switch (e.key.toLowerCase()) {
        case "d":
          setViewMode("day");
          break;
        case "w":
          setViewMode("week");
          break;
        case "m":
          setViewMode("month");
          break;
        case "y":
          setViewMode("year");
          break;
        case "t":
          goToToday();
          break;
        case "arrowleft":
          navigate(-1);
          break;
        case "arrowright":
          navigate(1);
          break;
        case "n":
          if (!e.metaKey && !e.ctrlKey) {
            handleNewEvent();
          }
          break;
        case "s":
          if (!e.metaKey && !e.ctrlKey) {
            syncEvents();
          }
          break;
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [
    setViewMode,
    goToToday,
    navigate,
    handleNewEvent,
    pendingEvent,
    syncEvents,
  ]);

  const handleSwitchToDayView = useCallback(
    (date: Date) => {
      setSelectedDate(date);
      setViewMode("day");
    },
    [setSelectedDate, setViewMode],
  );

  const handleSwitchToMonthView = useCallback(
    (date: Date) => {
      setSelectedDate(date);
      setViewMode("month");
    },
    [setSelectedDate, setViewMode],
  );

  const handleSelectDate = useCallback(
    (date: Date) => {
      setSelectedDate(date);
    },
    [setSelectedDate],
  );

  // Navigation handlers
  const handlePrevious = useCallback(() => navigate(-1), [navigate]);
  const handleNext = useCallback(() => navigate(1), [navigate]);
  const handleToday = useCallback(() => goToToday(), [goToToday]);

  // Drag handlers
  const handleDragStart = useCallback(
    (event: {
      active: {
        data: {
          current?: {
            type?: string;
            event?: CalendarEvent;
            width?: number;
            height?: number;
          };
        };
      };
    }) => {
      const { active } = event;
      if (active.data.current?.type === "event") {
        setActiveEvent(active.data.current.event as CalendarEvent);
        setActiveEventWidth(active.data.current.width || null);
        setActiveEventHeight(active.data.current.height || null);
      }
    },
    [],
  );

  const handleDragEnd = useCallback(
    (event: {
      active: { data: { current?: { event?: CalendarEvent } } };
      over: {
        data: {
          current?: {
            type?: string;
            date?: Date;
            hour?: number;
            minute?: number;
          };
        };
      } | null;
    }) => {
      const { active, over } = event;
      setActiveEvent(null);
      setActiveEventWidth(null);
      setActiveEventHeight(null);

      if (!over || !active.data.current?.event) return;

      const droppedEvent = active.data.current.event;
      const dropData = over.data.current;

      if (
        dropData?.type === "timeSlot" &&
        dropData.date &&
        dropData.hour !== undefined
      ) {
        // Parse the event's original start time to get original hour and minute
        const parseTime = (dateString: string) => {
          const isUTC =
            dateString.endsWith("Z") || /[+-]\d{2}:\d{2}$/.test(dateString);
          if (isUTC) {
            const date = new Date(dateString);
            return { hours: date.getHours(), minutes: date.getMinutes() };
          }
          const match = dateString.match(/T(\d{2}):(\d{2})/);
          if (match) {
            return {
              hours: parseInt(match[1], 10),
              minutes: parseInt(match[2], 10),
            };
          }
          return { hours: 0, minutes: 0 };
        };

        const originalTime = parseTime(droppedEvent.start_time);
        const originalMinutes = originalTime.hours * 60 + originalTime.minutes;

        // Calculate new time based on drag offset, not the drop zone under cursor
        const hourHeight = 60; // pixels per hour
        const dragOffsetMinutes = lastDragOffsetRef.current
          ? Math.round((lastDragOffsetRef.current.y / hourHeight) * 60)
          : 0;

        const newTotalMinutes = originalMinutes + dragOffsetMinutes;
        const newHours = Math.floor(newTotalMinutes / 60) % 24;
        const newMinutes = newTotalMinutes % 60;

        // Reset the ref after using it
        lastDragOffsetRef.current = null;

        rescheduleEvent(droppedEvent.id, dropData.date, newHours, newMinutes);
      }
    },
    [rescheduleEvent],
  );

  return (
    <div className="flex-1 flex h-full overflow-hidden">
      {/* Main content container - light bg with rounded corners */}
      <div className="flex-1 flex overflow-hidden bg-bg-mini-app">
        {/* Sidebar */}
        <div className={`w-[212px] shrink-0 flex flex-col overflow-hidden ${SIDEBAR.bg} border-r border-black/5`}>
          {/* Header */}
          <div className="h-12 flex items-center justify-between pl-4 pr-2 shrink-0">
            <h2 className="text-base font-semibold text-text-body">Calendar</h2>
            <button
              onClick={(e) => handleNewEvent(e)}
              className="p-1 rounded bg-white border border-black/10 hover:border-black/20 text-text-secondary hover:text-text-body transition-colors"
              title="New event"
            >
              <Icon icon={Plus} size={16} />
            </button>
          </div>

          <div className="flex-1 overflow-y-auto pt-0">
            <div className="px-3">
              <MiniMonthCalendar
                selectedDate={selectedDate}
                onSelectDate={handleSelectDate}
              />
            </div>

            <div className="pl-4 pr-2 mt-4">
              <div className="text-xs font-medium text-text-tertiary uppercase tracking-wide mb-2">
                Accounts
              </div>
              <div className="space-y-0.5">
                {accountsStatus.length === 0 ? (
                  <div className="text-xs text-text-tertiary">No accounts</div>
                ) : (
                  accountsStatus.map((account) => {
                    // When selection hasn't been initialized, all accounts are implicitly selected (unified view)
                    // When initialized, show actual selection state
                    const isSelected =
                      !accountSelectionInitialized ||
                      selectedAccountIds.includes(account.id) ||
                      selectedAccountIds.includes(account.email);
                    return (
                      <button
                        key={account.id}
                        type="button"
                        onClick={() =>
                          toggleAccountSelection(account.id, account.email)
                        }
                        className="w-full flex items-center gap-2 py-1.5 text-sm transition-colors text-text-secondary hover:text-text-body"
                      >
                        <span
                          className="w-3 h-3 rounded-full border shrink-0"
                          style={{
                            backgroundColor: isSelected ? getAccountAccentColor(account.email) : 'transparent',
                            borderColor: isSelected ? getAccountAccentColor(account.email) : '#D1D5DB',
                          }}
                        />
                        <span className="truncate">{account.email}</span>
                      </button>
                    );
                  })
                )}
              </div>
            </div>
          </div>
        </div>

        {/* Main content */}
        <div className="flex-1 min-w-0 flex overflow-hidden bg-white rounded-r-lg relative">
          <div className="flex-1 min-w-0 flex flex-col overflow-hidden">
        <DndContext
          sensors={sensors}
          onDragStart={handleDragStart}
          onDragEnd={handleDragEnd}
          collisionDetection={pointerWithin}
          modifiers={[snapToFifteenMinutes]}
        >
          <CalendarContent
            selectedDate={selectedDate}
            viewMode={viewMode}
            swipeableDays={swipeableDays}
            dayIndex={dayIndex}
            isLoading={isLoading}
            error={error}
            isSyncing={isSyncing}
            setViewMode={setViewMode}
            setSelectedDate={setSelectedDate}
            setDayIndex={setDayIndex}
            handleEventClick={handleEventClick}
            handleTimeSlotClick={handleTimeSlotClick}
            handleSwitchToDayView={handleSwitchToDayView}
            handleSwitchToMonthView={handleSwitchToMonthView}
            handlePrevious={handlePrevious}
            handleNext={handleNext}
            handleToday={handleToday}
            selectedEventData={selectedEventData}
            setSelectedEventData={setSelectedEventData}
            popoverCloseTimeRef={popoverCloseTimeRef}
            eventCreationFlags={eventCreationFlags}
            setDragOffset={setDragOffset}
            lastDragOffsetRef={lastDragOffsetRef}
          />

          {/* Drag overlay */}
          <DragOverlay>
            {activeEvent &&
              (() => {
                // Calculate event height based on duration (or use tracked height if available)
                const hourHeight = 60;
                let height = activeEventHeight;
                if (!height) {
                  const parseTime = (dateString: string) => {
                    const isUTC =
                      dateString.endsWith("Z") ||
                      /[+-]\d{2}:\d{2}$/.test(dateString);
                    if (isUTC) {
                      const date = new Date(dateString);
                      return date.getHours() * 60 + date.getMinutes();
                    }
                    const match = dateString.match(/T(\d{2}):(\d{2})/);
                    return match
                      ? parseInt(match[1]) * 60 + parseInt(match[2])
                      : 0;
                  };
                  const startMins = parseTime(activeEvent.start_time);
                  const endMins = parseTime(activeEvent.end_time);
                  const durationMins = endMins - startMins;
                  height = (durationMins / 60) * hourHeight;
                }

                // Calculate time offset from drag delta
                const offsetMinutes = dragOffset
                  ? Math.round((dragOffset.y / hourHeight) * 60)
                  : 0;

                // Use actual event width if available, otherwise use defaults
                const width =
                  activeEventWidth || (viewMode === "week" ? 140 : 200);

                return (
                  <EventBlockOverlay
                    event={activeEvent}
                    isWeekView={viewMode === "week"}
                    height={height}
                    width={width}
                    offsetMinutes={offsetMinutes}
                  />
                );
              })()}
          </DragOverlay>
        </DndContext>
          </div>
        </div>
      </div>
    </div>
  );
}
