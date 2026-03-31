import { create } from "zustand";
import { persist } from "zustand/middleware";
import {
  getCalendarEvents,
  syncCalendar,
  updateCalendarEvent,
  deleteCalendarEvent,
  respondToCalendarEvent,
  type CalendarEvent,
  type CalendarResponseStatus,
} from "../api/client";
import { registerAccountOrder } from "../utils/accountColors";
import {
  startOfDay,
  startOfWeek,
  addDays,
  addWeeks,
  addMonths,
  addYears,
  generateSwipeableDays,
  generateSwipeableWeeks,
  getTodayDayIndex,
  getCurrentWeekIndex,
} from "../components/Calendar/utils/dateHelpers";
import type { ViewMode } from "../components/Calendar/types/calendar.types";

interface CalendarState {
  // View state
  viewMode: ViewMode;
  selectedDate: Date;

  // Navigation indices (for swipeable views)
  dayIndex: number;
  weekIndex: number;

  // Pre-generated navigation arrays
  swipeableDays: Date[];
  swipeableWeeks: Date[];

  // Events data
  events: CalendarEvent[];
  eventsByDate: Map<string, CalendarEvent[]>; // "YYYY-MM-DD" → events for that date
  accountsStatus: { id: string; email: string; provider: string }[];
  selectedAccountIds: string[]; // Empty = all accounts
  accountSelectionInitialized: boolean;
  isLoading: boolean;
  isRevalidating: boolean;
  isSyncing: boolean;
  error: string | null;
  lastFetched: number | null;

  // Selectors
  getEventsForDate: (date: Date) => CalendarEvent[];
  getAllDayEventsForDate: (date: Date) => CalendarEvent[];
  getTimedEventsForDate: (date: Date) => CalendarEvent[];

  // Modal state (legacy - to be removed)
  showCreateModal: boolean;
  createModalInitialDate: Date | null;
  createModalInitialHour: number | undefined;

  // Inline event creation state
  pendingEvent: {
    date: Date;
    hour: number;
    minute?: number; // Start minute (0, 15, 30, 45)
    endDate?: Date; // End date (allows multi-day events)
    endHour?: number; // For drag-to-create
    endMinute?: number; // For drag-to-create
    triggerRect: DOMRect;
    title: string;
    isDragging?: boolean; // True while user is dragging to set duration
    isAllDay?: boolean; // True when creating an all-day event
    description?: string;
    location?: string;
    meeting_link?: string;
    add_google_meet?: boolean;
  } | null;

  // Actions
  setViewMode: (mode: ViewMode) => void;
  setSelectedDate: (date: Date) => void;
  setDayIndex: (index: number) => void;
  setWeekIndex: (index: number) => void;
  navigate: (direction: 1 | -1) => void;
  goToToday: () => void;
  fetchEvents: () => Promise<void>;
  syncEvents: () => Promise<void>;
  setSelectedAccounts: (accountIds: string[]) => void;
  toggleAccountSelection: (accountId?: string, accountEmail?: string) => void;

  // Modal actions (legacy - to be removed)
  openCreateModal: (date?: Date, hour?: number) => void;
  closeCreateModal: () => void;

  // Inline event creation actions
  startCreatingEvent: (date: Date, hour: number, triggerRect: DOMRect) => void;
  startCreatingAllDayEvent: (date: Date, triggerRect: DOMRect) => void;
  updatePendingEventTitle: (title: string) => void;
  updatePendingEventTime: (hour: number, minute?: number) => void;
  updatePendingEventEndTime: (endHour: number, endMinute?: number) => void;
  updatePendingEventEndDate: (endDate: Date) => void;
  updatePendingEventAllDay: (isAllDay: boolean) => void;
  updatePendingEventDescription: (description: string) => void;
  updatePendingEventLocation: (location: string) => void;
  updatePendingEventLink: (link: string) => void;
  togglePendingEventGoogleMeet: (enabled: boolean) => void;
  updatePendingEventRect: (rect: DOMRect) => void;
  cancelCreatingEvent: () => void;
  confirmPendingEvent: () => Promise<void>;

  // Drag-to-create actions
  startDraggingToCreate: (
    date: Date,
    hour: number,
    minute: number,
    triggerRect: DOMRect,
  ) => void;
  updateDragToCreate: (endHour: number, endMinute: number) => void;
  /** Call when pointer-up ends drag-to-create so UI can stop spring-animating the preview */
  finishDragToCreate: () => void;

  // Event actions
  addEvent: (event: CalendarEvent) => void;
  updateEvent: (eventId: string, updates: Partial<CalendarEvent>) => void;
  replaceEvent: (tempId: string, newEvent: CalendarEvent) => void;
  removeEvent: (eventId: string) => void;
  deleteEvent: (eventId: string) => Promise<void>;
  respondToEvent: (eventId: string, responseStatus: CalendarResponseStatus) => Promise<void>;
  rescheduleEvent: (
    eventId: string,
    newDate: Date,
    newHour: number,
    newMinute?: number,
  ) => Promise<void>;
  resizeEvent: (
    eventId: string,
    newStartMinutes: number,
    newEndMinutes: number,
  ) => Promise<void>;
  refreshEvents: () => Promise<void>;
  preload: () => void;
}


// Convert any date/time to a local date key "YYYY-MM-DD"
// This ensures UTC times are properly converted to local timezone
function getLocalDateKey(dateOrIsoString: Date | string): string {
  if (!dateOrIsoString) {
    console.error(
      "getLocalDateKey received undefined or null:",
      dateOrIsoString,
    );
    throw new Error("Invalid date provided to getLocalDateKey");
  }
  const d =
    typeof dateOrIsoString === "string"
      ? new Date(dateOrIsoString)
      : dateOrIsoString;
  if (isNaN(d.getTime())) {
    console.error("getLocalDateKey received invalid date:", dateOrIsoString);
    throw new Error("Invalid date format provided to getLocalDateKey");
  }
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, "0")}-${String(d.getDate()).padStart(2, "0")}`;
}

// Get the current user's RSVP response status for an event
export function getUserResponseStatus(event: CalendarEvent): string | null {
  if (!event.attendees || !event.account_email) return null;
  const userAttendee = event.attendees.find(
    (a) => a.email.toLowerCase() === event.account_email?.toLowerCase(),
  );
  return userAttendee?.response_status ?? null;
}

// Parse date for indexing - for all-day events, extract date directly from string
// to avoid timezone shifts (all-day events represent calendar dates, not moments in time)
function parseDateForIndexing(dateString: string, isAllDay: boolean): Date {
  if (isAllDay) {
    // Extract date directly from ISO string without timezone conversion
    const dateMatch = dateString.match(/^(\d{4})-(\d{2})-(\d{2})/);
    if (dateMatch) {
      return new Date(
        parseInt(dateMatch[1], 10),
        parseInt(dateMatch[2], 10) - 1,
        parseInt(dateMatch[3], 10),
      );
    }
  }
  // For timed events, use normal Date parsing (converts to local timezone)
  const date = new Date(dateString);
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

// Build a date index from events for O(1) lookups
// Key format: "YYYY-MM-DD" (local timezone) → CalendarEvent[]
// For multi-day events, event is indexed on every date it spans
function buildEventsByDateIndex(
  events: CalendarEvent[],
): Map<string, CalendarEvent[]> {
  const index = new Map<string, CalendarEvent[]>();

  for (const event of events) {
    // Parse start and end dates - handle all-day events specially to avoid timezone shifts
    const isAllDay = event.all_day === true;
    const currentDate = parseDateForIndexing(event.start_time, isAllDay);
    const endDayOnly = parseDateForIndexing(event.end_time, isAllDay);

    // Add event to every date it spans
    while (currentDate <= endDayOnly) {
      const dateKey = getLocalDateKey(currentDate);

      const existing = index.get(dateKey);
      if (existing) {
        existing.push(event);
      } else {
        index.set(dateKey, [event]);
      }

      // Move to next day
      currentDate.setDate(currentDate.getDate() + 1);
    }
  }

  // Sort events within each date by start time
  for (const [, dateEvents] of index) {
    dateEvents.sort((a, b) => a.start_time.localeCompare(b.start_time));
  }

  return index;
}

// Helper to update both events array and index together
function setEventsWithIndex(events: CalendarEvent[]) {
  return {
    events,
    eventsByDate: buildEventsByDateIndex(events),
  };
}

export const useCalendarStore = create<CalendarState>()(
  persist(
    (set, get) => ({
      // Initial state
      viewMode: "day",
      selectedDate: startOfDay(new Date()),
      dayIndex: getTodayDayIndex(),
      weekIndex: getCurrentWeekIndex(),
      swipeableDays: generateSwipeableDays(),
      swipeableWeeks: generateSwipeableWeeks(),
      events: [],
      eventsByDate: new Map(),
      accountsStatus: [],
      selectedAccountIds: [],
      accountSelectionInitialized: false,
      isLoading: false,
      setSelectedAccounts: (accountIds) => {
        set({
          selectedAccountIds: accountIds,
          accountSelectionInitialized: true,
        });
        // No fetchEvents() needed - client-side filtering handles display instantly
      },
      toggleAccountSelection: (accountId, accountEmail) => {
        // Prefer email for consistency with event.account_email filtering
        const key = accountEmail || accountId;
        if (!key) return;
        const { selectedAccountIds } = get();
        // Check for both the key and any existing entry for this account
        const isSelected = selectedAccountIds.includes(key) ||
          (accountId && selectedAccountIds.includes(accountId)) ||
          (accountEmail && selectedAccountIds.includes(accountEmail));

        // Remove both possible keys to handle legacy data
        const next = isSelected
          ? selectedAccountIds.filter((id) => id !== key && id !== accountId && id !== accountEmail)
          : [...selectedAccountIds, key];

        set({ selectedAccountIds: next, accountSelectionInitialized: true });
        // No fetchEvents() needed - client-side filtering handles display instantly
      },
      isRevalidating: false,
      isSyncing: false,
      error: null,
      lastFetched: null,
      showCreateModal: false,
      createModalInitialDate: null,
      createModalInitialHour: undefined,
      pendingEvent: null,

      // Selectors for efficient event lookups
      getEventsForDate: (date: Date) => {
        const state = get();
        const dateKey = getLocalDateKey(date);
        const events = state.eventsByDate.get(dateKey) || [];

        // Filter events based on selected accounts
        // If not initialized, show all events (unified view)
        if (!state.accountSelectionInitialized) {
          return events;
        }

        // If initialized but no accounts selected, show no events
        if (state.selectedAccountIds.length === 0) {
          return [];
        }

        // Filter to only show events from selected accounts
        // selectedAccountIds can contain either account IDs or email addresses
        const selectedEmails = new Set<string>();
        for (const id of state.selectedAccountIds) {
          // Skip null/undefined values (can happen from corrupted localStorage)
          if (!id) continue;

          // If it's an email, add directly
          if (id.includes("@")) {
            selectedEmails.add(id.toLowerCase());
          } else {
            // It's an ID, find the corresponding email
            const account = state.accountsStatus.find((acc) => acc.id === id);
            if (account?.email) {
              selectedEmails.add(account.email.toLowerCase());
            }
          }
        }

        // Check if all accounts are selected (show all events including those without account_email)
        const allAccountsSelected = state.accountsStatus.every(
          (acc) => selectedEmails.has(acc.email.toLowerCase())
        );

        return events.filter((event) => {
          // Events without account_email are only shown if all accounts are selected
          if (!event.account_email) return allAccountsSelected;
          return selectedEmails.has(event.account_email.toLowerCase());
        });
      },

      getAllDayEventsForDate: (date: Date) => {
        const events = get().getEventsForDate(date);
        // Include events explicitly marked as all_day, or events spanning 23+ hours
        return events.filter((e) => {
          if (e.all_day === true) return true;
          // Check if event spans most of the day (23+ hours = 1380+ minutes)
          const start = new Date(e.start_time);
          const end = new Date(e.end_time);
          const durationMinutes = (end.getTime() - start.getTime()) / 60000;
          return durationMinutes >= 1380;
        });
      },

      getTimedEventsForDate: (date: Date) => {
        const events = get().getEventsForDate(date);
        // Exclude all-day events and events spanning 23+ hours
        return events.filter((e) => {
          if (e.all_day === true) return false;
          const start = new Date(e.start_time);
          const end = new Date(e.end_time);
          const durationMinutes = (end.getTime() - start.getTime()) / 60000;
          return durationMinutes < 1380;
        });
      },

      setViewMode: (mode) => set({ viewMode: mode }),

      setSelectedDate: (date) => {
        const newDate = startOfDay(date);
        const state = get();

        // Update day index to match the selected date
        const today = startOfDay(new Date());
        const diffDays = Math.round(
          (newDate.getTime() - today.getTime()) / (1000 * 60 * 60 * 24),
        );
        const newDayIndex = getTodayDayIndex() + diffDays;
        const clampedDayIndex = Math.max(
          0,
          Math.min(newDayIndex, state.swipeableDays.length - 1),
        );

        // Update week index to match the selected date
        const currentWeekStart = startOfWeek(today);
        const selectedWeekStart = startOfWeek(newDate);
        const diffWeeks = Math.round(
          (selectedWeekStart.getTime() - currentWeekStart.getTime()) /
            (1000 * 60 * 60 * 24 * 7),
        );
        const newWeekIndex = getCurrentWeekIndex() + diffWeeks;
        const clampedWeekIndex = Math.max(
          0,
          Math.min(newWeekIndex, state.swipeableWeeks.length - 1),
        );

        set({
          selectedDate: newDate,
          dayIndex: clampedDayIndex,
          weekIndex: clampedWeekIndex,
        });
      },

      setDayIndex: (index) => {
        const state = get();
        const clampedIndex = Math.max(
          0,
          Math.min(index, state.swipeableDays.length - 1),
        );
        const newDate = state.swipeableDays[clampedIndex];
        set({
          dayIndex: clampedIndex,
          selectedDate: newDate,
        });
      },

      setWeekIndex: (index) => {
        const state = get();
        const clampedIndex = Math.max(
          0,
          Math.min(index, state.swipeableWeeks.length - 1),
        );
        set({ weekIndex: clampedIndex });
      },

      navigate: (direction) => {
        const state = get();
        const {
          viewMode,
          selectedDate,
          dayIndex,
          weekIndex,
          swipeableDays,
          swipeableWeeks,
        } = state;

        switch (viewMode) {
          case "day": {
            const newDayIndex = Math.max(
              0,
              Math.min(dayIndex + direction, swipeableDays.length - 1),
            );
            set({
              selectedDate: addDays(selectedDate, direction),
              dayIndex: newDayIndex,
            });
            break;
          }
          case "week": {
            const newWeekIndex = Math.max(
              0,
              Math.min(weekIndex + direction, swipeableWeeks.length - 1),
            );
            set({
              selectedDate: addWeeks(selectedDate, direction),
              weekIndex: newWeekIndex,
            });
            break;
          }
          case "month": {
            set({ selectedDate: addMonths(selectedDate, direction) });
            break;
          }
          case "year": {
            set({ selectedDate: addYears(selectedDate, direction) });
            break;
          }
        }
      },

      goToToday: () => {
        const today = startOfDay(new Date());
        set({
          selectedDate: today,
          dayIndex: getTodayDayIndex(),
          weekIndex: getCurrentWeekIndex(),
        });
      },

      fetchEvents: async () => {
        const { events: cachedEvents, lastFetched } = get();
        const hasCachedData = cachedEvents.length > 0 && lastFetched;

        // If we have cached data, show it immediately and revalidate in background
        if (hasCachedData) {
          set({ isRevalidating: true, error: null });
        } else {
          set({ isLoading: true, error: null });
        }

        try {
          // Always fetch all events - client-side filtering handles account selection
          const result = await getCalendarEvents();
          const events = result.events || [];
          const incomingAccounts =
            result.accounts_status || get().accountsStatus;
          const shouldInitSelection =
            !get().accountSelectionInitialized && incomingAccounts.length > 0;

          // Register account order BEFORE setting state so colors are correct on first render
          if (incomingAccounts.length > 0) {
            registerAccountOrder(incomingAccounts);
          }

          set({
            ...setEventsWithIndex(events),
            accountsStatus: incomingAccounts,
            // Prefer emails for selectedAccountIds to match event.account_email filtering
            selectedAccountIds: shouldInitSelection
              ? incomingAccounts.map((acc) => acc.email || acc.id)
              : get().selectedAccountIds,
            accountSelectionInitialized:
              get().accountSelectionInitialized || shouldInitSelection,
            isLoading: false,
            isRevalidating: false,
            lastFetched: Date.now(),
          });
        } catch (err) {
          // On error, keep cached data if available
          set({
            error:
              err instanceof Error ? err.message : "Failed to fetch events",
            isLoading: false,
            isRevalidating: false,
          });
        }
      },

      syncEvents: async () => {
        set({ isSyncing: true });
        try {
          await syncCalendar();
          await get().refreshEvents();
        } catch (err) {
          console.error("Sync failed:", err);
        } finally {
          set({ isSyncing: false });
        }
      },

      preload: () => {
        const STALE = 5 * 60 * 1000;
        const { events, lastFetched } = get();
        if (
          events.length === 0 ||
          !lastFetched ||
          Date.now() - lastFetched > STALE
        ) {
          get().fetchEvents();
        }
      },

      openCreateModal: (date, hour) => {
        const state = get();
        set({
          showCreateModal: true,
          createModalInitialDate: date || state.selectedDate,
          createModalInitialHour: hour,
        });
      },

      closeCreateModal: () => {
        set({
          showCreateModal: false,
          createModalInitialDate: null,
          createModalInitialHour: undefined,
        });
      },

      startCreatingEvent: (date, hour, triggerRect) => {
        set({
          pendingEvent: {
            date,
            hour,
            minute: 0,
            endHour: hour + 1,
            endMinute: 0,
            triggerRect,
            title: "",
          },
        });
      },

      startCreatingAllDayEvent: (date, triggerRect) => {
        set({
          pendingEvent: {
            date,
            hour: 0,
            triggerRect,
            title: "",
            isAllDay: true,
          },
        });
      },

      updatePendingEventTitle: (title) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, title } });
        }
      },

      updatePendingEventTime: (hour, minute = 0) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, hour, minute } });
        }
      },

      updatePendingEventEndTime: (endHour, endMinute = 0) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, endHour, endMinute } });
        }
      },

      updatePendingEventEndDate: (endDate) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, endDate } });
        }
      },

      updatePendingEventAllDay: (isAllDay) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, isAllDay } });
        }
      },

      updatePendingEventDescription: (description) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, description } });
        }
      },

      updatePendingEventLocation: (location) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, location } });
        }
      },

      updatePendingEventLink: (link) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, meeting_link: link } });
        }
      },

      togglePendingEventGoogleMeet: (enabled) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, add_google_meet: enabled, meeting_link: enabled ? undefined : pending.meeting_link } });
        }
      },

      updatePendingEventRect: (triggerRect) => {
        const pending = get().pendingEvent;
        if (pending) {
          set({ pendingEvent: { ...pending, triggerRect } });
        }
      },

      cancelCreatingEvent: () => {
        set({ pendingEvent: null });
      },

      confirmPendingEvent: async () => {
        const pending = get().pendingEvent;
        if (!pending || !pending.title.trim()) {
          set({ pendingEvent: null });
          return;
        }

        const { date, hour, title, isAllDay } = pending;

        // Ensure date is a valid Date object
        const dateObj = date instanceof Date ? date : new Date(date);

        // Format times
        const year = dateObj.getFullYear();
        const month = String(dateObj.getMonth() + 1).padStart(2, "0");
        const day = String(dateObj.getDate()).padStart(2, "0");

        let start_time: string;
        let end_time: string;

        if (isAllDay) {
          // All-day event: start at midnight, end at 23:59:59
          start_time = `${year}-${month}-${day}T00:00:00`;
          end_time = `${year}-${month}-${day}T23:59:59`;
        } else {
          // Timed event: 1-hour duration
          const startHourStr = String(hour).padStart(2, "0");
          const endHour = hour + 1;
          const endHourStr = String(endHour).padStart(2, "0");
          start_time = `${year}-${month}-${day}T${startHourStr}:00:00`;
          end_time = `${year}-${month}-${day}T${endHourStr}:00:00`;
        }

        // Create optimistic event with temporary ID
        const tempId = `temp-${Date.now()}`;
        const optimisticEvent = {
          id: tempId,
          title: title.trim(),
          start_time,
          end_time,
          all_day: isAllDay || false,
          status: "confirmed" as const,
        };

        // Add optimistic event and clear pending state
        // Use get() to access addEvent to avoid stale closure issues
        get().addEvent(optimisticEvent);
        set({ pendingEvent: null });

        try {
          const { createCalendarEvent } = await import("../api/client");
          const createdEvent = await createCalendarEvent({
            title: title.trim(),
            start_time,
            end_time,
            all_day: isAllDay || false,
          });
          // Replace optimistic event with the real one from server
          get().replaceEvent(tempId, createdEvent);
        } catch (err) {
          // Remove optimistic event on failure
          get().removeEvent(tempId);
          console.error("Failed to create event:", err);
        }
      },

      startDraggingToCreate: (date, hour, minute, triggerRect) => {
        // Start with 15-minute duration
        let endHour = hour;
        let endMinute = minute + 15;

        // Handle minute overflow (e.g., 8:45 + 15 min = 9:00)
        if (endMinute >= 60) {
          endHour += Math.floor(endMinute / 60);
          endMinute = endMinute % 60;
        }

        // Clamp to valid hour range
        endHour = Math.min(endHour, 23);

        set({
          pendingEvent: {
            date,
            hour,
            minute,
            endHour,
            endMinute,
            triggerRect,
            title: "",
            isDragging: true,
          },
        });
      },

      updateDragToCreate: (endHour, endMinute) => {
        const pending = get().pendingEvent;
        if (pending && pending.isDragging) {
          set({
            pendingEvent: {
              ...pending,
              endHour,
              endMinute,
            },
          });
        }
      },

      finishDragToCreate: () => {
        const pending = get().pendingEvent;
        if (pending?.isDragging) {
          set({ pendingEvent: { ...pending, isDragging: false } });
        }
      },

      addEvent: (event) => {
        const state = get();
        const newEvents = [...state.events, event];
        set(setEventsWithIndex(newEvents));
      },

      updateEvent: (eventId, updates) => {
        const state = get();
        const newEvents = state.events.map((e) =>
          e.id === eventId ? { ...e, ...updates } : e,
        );
        set(setEventsWithIndex(newEvents));
      },

      replaceEvent: (tempId, newEvent) => {
        // Validate the event has required fields
        if (!newEvent.start_time || !newEvent.end_time) {
          console.error(
            "replaceEvent received event with missing date fields:",
            newEvent,
          );
          throw new Error("Event missing start_time or end_time");
        }

        const state = get();
        const tempEvent = state.events.find((e) => e.id === tempId);

        let newEvents: CalendarEvent[];
        if (tempEvent) {
          // Replace the temp event with the real one
          // Preserve fields from temp event that server doesn't return (e.g., meeting_link)
          const mergedEvent = {
            ...newEvent,
            meeting_link: newEvent.meeting_link || tempEvent.meeting_link,
            description: newEvent.description || tempEvent.description,
            location: newEvent.location || tempEvent.location,
          };
          newEvents = state.events.map((e) =>
            e.id === tempId ? mergedEvent : e,
          );
        } else {
          // Temp event was removed (maybe by a refresh), just add the new event
          // But first check if the new event already exists (avoid duplicates)
          const eventExists = state.events.some((e) => e.id === newEvent.id);
          if (eventExists) {
            return; // Event already in the list, nothing to do
          }
          newEvents = [...state.events, newEvent];
        }
        set(setEventsWithIndex(newEvents));
      },

      removeEvent: (eventId) => {
        const state = get();
        const newEvents = state.events.filter((e) => e.id !== eventId);
        set(setEventsWithIndex(newEvents));
      },

      deleteEvent: async (eventId) => {
        const eventToDelete = get().events.find((e) => e.id === eventId);
        if (!eventToDelete) return;

        // Optimistically remove the event immediately
        const newEvents = get().events.filter((e) => e.id !== eventId);
        set(setEventsWithIndex(newEvents));

        try {
          await deleteCalendarEvent(eventId);
        } catch (err) {
          // Revert on failure - add the event back
          console.error("Failed to delete event:", err);
          const currentEvents = get().events;
          set(setEventsWithIndex([...currentEvents, eventToDelete]));
          throw err; // Re-throw so caller can handle (show error message)
        }
      },

      respondToEvent: async (eventId, responseStatus) => {
        const event = get().events.find((e) => e.id === eventId);
        if (!event) return;

        // Store original attendees for rollback
        const originalAttendees = event.attendees;

        // Optimistically update the attendee's response status
        // We update any attendee matching the event's account_email
        const accountEmail = event.account_email?.toLowerCase();
        if (accountEmail && event.attendees) {
          const updatedAttendees = event.attendees.map((att) =>
            att.email.toLowerCase() === accountEmail
              ? { ...att, response_status: responseStatus }
              : att
          );
          const newEvents = get().events.map((e) =>
            e.id === eventId ? { ...e, attendees: updatedAttendees } : e
          );
          set(setEventsWithIndex(newEvents));
        }

        try {
          await respondToCalendarEvent(eventId, responseStatus);
        } catch (err) {
          // Revert on failure
          console.error("Failed to respond to event:", err);
          const revertedEvents = get().events.map((e) =>
            e.id === eventId ? { ...e, attendees: originalAttendees } : e
          );
          set(setEventsWithIndex(revertedEvents));
          throw err;
        }
      },

      refreshEvents: async () => {
        // Silent refresh without loading state
        // Preserves optimistic events (temp-*) until they're confirmed or removed
        try {
          const state = get();
          // Always fetch all events - client-side filtering handles account selection
          const result = await getCalendarEvents();
          const serverEvents = result.events || [];
          const incomingAccounts =
            result.accounts_status || state.accountsStatus;
          const shouldInitSelection =
            !state.accountSelectionInitialized && incomingAccounts.length > 0;

          // Register account order BEFORE setting state so colors are correct
          if (incomingAccounts.length > 0) {
            registerAccountOrder(incomingAccounts);
          }

          // Keep any optimistic events that haven't been confirmed yet
          const optimisticEvents = state.events.filter((e) =>
            e.id.startsWith("temp-"),
          );
          const serverEventIds = new Set(serverEvents.map((e) => e.id));

          // Merge: server events + optimistic events not yet on server
          const mergedEvents = [
            ...serverEvents,
            ...optimisticEvents.filter((e) => !serverEventIds.has(e.id)),
          ];

          set({
            ...setEventsWithIndex(mergedEvents),
            accountsStatus: incomingAccounts,
            // Prefer emails for selectedAccountIds to match event.account_email filtering
            selectedAccountIds: shouldInitSelection
              ? incomingAccounts.map((acc) => acc.email || acc.id)
              : state.selectedAccountIds,
            accountSelectionInitialized:
              state.accountSelectionInitialized || shouldInitSelection,
            lastFetched: Date.now(),
          });
        } catch (err) {
          console.error("Failed to refresh events:", err);
        }
      },

      rescheduleEvent: async (eventId, newDate, newHour, newMinute = 0) => {
        const event = get().events.find((e) => e.id === eventId);
        if (!event) return;

        // Capture original event for potential revert
        const originalEvent = { ...event };

        // Parse duration from original event times (in minutes)
        // Use regex to avoid timezone conversion issues
        const startMatch = event.start_time.match(/T(\d{2}):(\d{2})/);
        const endMatch = event.end_time.match(/T(\d{2}):(\d{2})/);
        let durationMinutes = 60; // Default 1 hour
        if (startMatch && endMatch) {
          const startMinutes =
            parseInt(startMatch[1]) * 60 + parseInt(startMatch[2]);
          const endMinutes = parseInt(endMatch[1]) * 60 + parseInt(endMatch[2]);
          durationMinutes = endMinutes - startMinutes;
          if (durationMinutes <= 0) durationMinutes = 60; // Handle edge cases
        }

        // Format new times as local ISO strings (no timezone conversion)
        const year = newDate.getFullYear();
        const month = String(newDate.getMonth() + 1).padStart(2, "0");
        const day = String(newDate.getDate()).padStart(2, "0");

        // Snap minute to 15-minute interval (0, 15, 30, 45)
        const snappedMinute = Math.round(newMinute / 15) * 15;
        const startHourStr = String(newHour).padStart(2, "0");
        const startMinuteStr = String(snappedMinute).padStart(2, "0");

        const endTotalMinutes = newHour * 60 + snappedMinute + durationMinutes;
        let endHour = Math.floor(endTotalMinutes / 60);
        const endMinute = endTotalMinutes % 60;

        // Handle multi-day events: if end hour exceeds 23, move to next day
        const endDate = new Date(newDate);
        if (endHour > 23) {
          endDate.setDate(endDate.getDate() + Math.floor(endHour / 24));
          endHour = endHour % 24;
        }

        const endHourStr = String(endHour).padStart(2, "0");
        const endMinuteStr = String(endMinute).padStart(2, "0");

        // Format end date
        const endYear = endDate.getFullYear();
        const endMonth = String(endDate.getMonth() + 1).padStart(2, "0");
        const endDay = String(endDate.getDate()).padStart(2, "0");

        const start_time = `${year}-${month}-${day}T${startHourStr}:${startMinuteStr}:00`;
        const end_time = `${endYear}-${endMonth}-${endDay}T${endHourStr}:${endMinuteStr}:00`;

        // Optimistically update the UI
        const updatedEvent = { ...event, start_time, end_time };
        const currentEvents = get().events;
        const optimisticEvents = currentEvents.map((e) =>
          e.id === eventId ? updatedEvent : e,
        );
        set(setEventsWithIndex(optimisticEvents));

        try {
          // Send all event fields to prevent data loss with PUT semantics
          await updateCalendarEvent(eventId, {
            ...event,
            start_time,
            end_time,
          });
        } catch (err) {
          // Revert on error - restore original event in current state (not stale captured state)
          console.error("Failed to reschedule event:", err);
          const revertEvents = get().events.map((e) =>
            e.id === eventId ? originalEvent : e,
          );
          set(setEventsWithIndex(revertEvents));
        }
      },

      resizeEvent: async (eventId, newStartMinutes, newEndMinutes) => {
        const event = get().events.find((e) => e.id === eventId);
        if (!event) return;

        // Capture original event for potential revert
        const originalEvent = { ...event };

        // Parse the date from the original event
        const dateMatch = event.start_time.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (!dateMatch) return;
        const [, year, month, day] = dateMatch;

        // Format new times
        const startHour = Math.floor(newStartMinutes / 60);
        const startMinute = newStartMinutes % 60;
        const endHour = Math.floor(newEndMinutes / 60);
        const endMinute = newEndMinutes % 60;

        const start_time = `${year}-${month}-${day}T${String(startHour).padStart(2, "0")}:${String(startMinute).padStart(2, "0")}:00`;
        const end_time = `${year}-${month}-${day}T${String(endHour).padStart(2, "0")}:${String(endMinute).padStart(2, "0")}:00`;

        // Optimistically update the UI
        const updatedEvent = { ...event, start_time, end_time };
        const currentEvents = get().events;
        const optimisticEvents = currentEvents.map((e) =>
          e.id === eventId ? updatedEvent : e,
        );
        set(setEventsWithIndex(optimisticEvents));

        try {
          // Send all event fields to prevent data loss with PUT semantics
          await updateCalendarEvent(eventId, {
            ...event,
            start_time,
            end_time,
          });
        } catch (err) {
          // Revert on error
          console.error("Failed to resize event:", err);
          const revertEvents = get().events.map((e) =>
            e.id === eventId ? originalEvent : e,
          );
          set(setEventsWithIndex(revertEvents));
        }
      },
    }),
    {
      name: "core-calendar-storage-v2",
      partialize: (state) => ({
        // Persist UI preferences and events data for instant load
        // Note: selectedDate is NOT persisted - calendar always starts at today
        viewMode: state.viewMode,
        events: state.events,
        lastFetched: state.lastFetched,
        selectedAccountIds: state.selectedAccountIds,
        accountsStatus: state.accountsStatus,
        accountSelectionInitialized: state.accountSelectionInitialized,
      }),
      merge: (persisted, current) => {
        const persistedState = persisted as {
          viewMode?: ViewMode;
          events?: CalendarEvent[];
          lastFetched?: number | null;
          selectedAccountIds?: string[];
          accountsStatus?: { id: string; email: string; provider: string }[];
          accountSelectionInitialized?: boolean;
        };
        const events = persistedState?.events || [];
        const accountsStatus = persistedState?.accountsStatus || [];

        // Register account order from persisted state so colors are correct on first render
        if (accountsStatus.length > 0) {
          registerAccountOrder(accountsStatus);
        }

        return {
          ...current,
          viewMode: persistedState?.viewMode || current.viewMode,
          // selectedDate always starts at today (not persisted)
          events,
          eventsByDate: buildEventsByDateIndex(events),
          lastFetched: persistedState?.lastFetched || null,
          selectedAccountIds: (persistedState?.selectedAccountIds || []).filter(
            (id): id is string => !!id,
          ),
          accountsStatus,
          accountSelectionInitialized:
            persistedState?.accountSelectionInitialized || false,
        };
      },
    },
  ),
);
