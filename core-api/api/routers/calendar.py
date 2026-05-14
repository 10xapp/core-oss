"""
Calendar router - HTTP endpoints for calendar operations

Supports unified multi-account calendar view:
- GET /events returns all events (unified view) by default
- GET /events?account_id=X returns events from specific account only
- GET /events?account_ids=X,Y,Z returns events from multiple specific accounts

Response includes:
- events: List of events with account metadata (account_email, account_provider)
- unified: Boolean indicating if showing all accounts or filtered
- accounts_status: List of all connected calendar accounts
"""
from fastapi import APIRouter, HTTPException, status, Depends, Query
from typing import Optional, List, Literal
from pydantic import BaseModel
from api.services.calendar import (
    get_all_events,
    get_today_events,
    create_event,
    update_event,
    delete_event,
    respond_to_event,
)
from api.dependencies import get_current_user_jwt, get_current_user_id
from api.exceptions import handle_api_exception
import logging

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/calendar", tags=["calendar"])


# ============================================================================
# Request/Response Models
# ============================================================================

class AccountStatus(BaseModel):
    """Status of a connected calendar account."""
    connection_id: str
    email: Optional[str] = None
    provider: Optional[str] = None
    avatar: Optional[str] = None
    last_synced: Optional[str] = None
    has_events: Optional[bool] = None

    class Config:
        extra = "allow"


class AttendeeItem(BaseModel):
    """A single attendee."""
    email: str
    display_name: Optional[str] = None
    response_status: Optional[str] = None


class CalendarEventItem(BaseModel):
    """A single calendar event."""
    id: str
    title: Optional[str] = None
    description: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    all_day: Optional[bool] = None
    location: Optional[str] = None
    account_email: Optional[str] = None
    account_provider: Optional[str] = None
    google_event_id: Optional[str] = None
    # Attendee and meeting link fields
    attendees: Optional[List[AttendeeItem]] = None
    organizer_email: Optional[str] = None
    is_organizer: Optional[bool] = None
    html_link: Optional[str] = None
    meeting_link: Optional[str] = None  # Google Meet / video conference link
    # Recurrence fields
    recurrence: Optional[List[str]] = None
    recurring_event_id: Optional[str] = None

    class Config:
        extra = "allow"


class CalendarEventsListResponse(BaseModel):
    """Response model for calendar events list."""
    events: List[CalendarEventItem]
    unified: bool = True
    accounts_status: Optional[List[AccountStatus]] = None

    class Config:
        extra = "allow"


class CreateEventRequest(BaseModel):
    """Request model for creating a calendar event."""
    title: str
    description: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    all_day: Optional[bool] = False
    location: Optional[str] = None
    account_id: Optional[str] = None
    # Recurrence rules (Google expects RRULE/EXDATE lines)
    recurrence: Optional[List[str]] = None
    # Attendees (list of email addresses)
    attendees: Optional[List[str]] = None
    # Send email invitations to attendees (Google Calendar)
    notify_attendees: Optional[bool] = False
    # Auto-generate a Google Meet video conference link
    add_google_meet: Optional[bool] = False

    class Config:
        extra = "allow"


class UpdateEventRequest(BaseModel):
    """Request model for updating a calendar event."""
    title: Optional[str] = None
    description: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    all_day: Optional[bool] = None
    location: Optional[str] = None
    # Update recurrence rules (pass-through). If provided, applies per scope.
    recurrence: Optional[List[str]] = None
    # Scope of update: instance (default), all, following (following may be limited)
    scope: Optional[Literal['instance', 'all', 'following']] = None
    # For scope='following', cutoff start time (ISO). If omitted, uses current instance start.
    cutoff_start: Optional[str] = None
    # Attendees (list of email addresses)
    attendees: Optional[List[str]] = None
    # Send email notifications to attendees (Google Calendar)
    notify_attendees: Optional[bool] = False

    class Config:
        extra = "allow"


class CalendarEventResponse(BaseModel):
    """Response model for a single calendar event operation."""
    id: Optional[str] = None
    title: Optional[str] = None
    synced_to_google: Optional[bool] = None

    class Config:
        extra = "allow"


class CalendarDeleteResponse(BaseModel):
    """Response model for delete event."""
    success: bool
    synced_to_google: Optional[bool] = None
    already_deleted: Optional[bool] = None

    class Config:
        extra = "allow"


class CalendarSyncResponse(BaseModel):
    """Response model for calendar sync."""
    status: Optional[str] = None
    synced: Optional[int] = None
    message: Optional[str] = None
    user_id: Optional[str] = None
    new_events: Optional[int] = None
    updated_events: Optional[int] = None
    deleted_events: Optional[int] = None
    total_events: Optional[int] = None
    total_fetched: Optional[int] = None
    jobs_enqueued: Optional[int] = None
    streams_marked: Optional[int] = None

    class Config:
        extra = "allow"


class RSVPRequest(BaseModel):
    """Request model for responding to a calendar invite."""
    response_status: Literal['accepted', 'declined', 'tentative']


class RSVPResponse(BaseModel):
    """Response model for RSVP operation."""
    id: str
    response_status: str
    synced_to_google: bool = False

    class Config:
        extra = "allow"


# ============================================================================
# Endpoints
# ============================================================================

@router.get("/events", response_model=CalendarEventsListResponse)
async def get_all_events_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
    account_id: Optional[str] = Query(None, description="Filter by single account (ext_connection_id). Backward compatible."),
    account_ids: Optional[str] = Query(None, description="Comma-separated list of account IDs. Takes precedence over account_id.")
):
    """
    Get all calendar events for a specific user.

    Query params:
    - account_id: Filter by single account (backward compatible)
    - account_ids: Comma-separated list of account IDs for multi-account filtering.
                   Takes precedence over account_id if both provided.

    Unified View Behavior:
    - No params: Returns events from ALL accounts (unified view)
    - account_id: Returns events from single account
    - account_ids: Returns events from specified accounts

    Response includes account metadata (account_email, account_provider) for each event.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        # Parse account_ids from comma-separated string
        account_id_list: Optional[List[str]] = None
        if account_ids:
            account_id_list = [acc_id.strip() for acc_id in account_ids.split(",") if acc_id.strip()]
            # Set to None if empty after filtering (e.g., input was ",,,")
            if not account_id_list:
                account_id_list = None
        elif account_id:
            # Backwards compatibility: single account_id becomes a list of one
            account_id_list = [account_id]

        logger.info(f"📅 [CALENDAR] User {user_id} fetching calendar events (account_ids={account_id_list})")
        result = get_all_events(user_id, user_jwt, account_ids=account_id_list)

        # Check for service-level errors and convert to HTTP error
        if result.get("source") == "error":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("error", "Failed to fetch events from calendar service")
            )

        event_count = len(result.get('events', []))
        logger.info(f"📅 [CALENDAR] Found {event_count} events for user {user_id} (unified={result.get('unified', True)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch events", logger)


@router.get("/events/today", response_model=CalendarEventsListResponse)
async def get_today_events_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
    account_id: Optional[str] = Query(None, description="Filter by single account (ext_connection_id). Backward compatible."),
    account_ids: Optional[str] = Query(None, description="Comma-separated list of account IDs. Takes precedence over account_id.")
):
    """
    Get calendar events for today for a specific user.

    Query params:
    - account_id: Filter by single account (backward compatible)
    - account_ids: Comma-separated list of account IDs for multi-account filtering.
                   Takes precedence over account_id if both provided.

    Unified View Behavior:
    - No params: Returns today's events from ALL accounts (unified view)
    - account_id: Returns today's events from single account
    - account_ids: Returns today's events from specified accounts

    Response includes account metadata (account_email, account_provider) for each event.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        # Parse account_ids from comma-separated string
        account_id_list: Optional[List[str]] = None
        if account_ids:
            account_id_list = [acc_id.strip() for acc_id in account_ids.split(",") if acc_id.strip()]
            # Set to None if empty after filtering (e.g., input was ",,,")
            if not account_id_list:
                account_id_list = None
        elif account_id:
            # Backwards compatibility: single account_id becomes a list of one
            account_id_list = [account_id]

        logger.info(f"📅 [CALENDAR] User {user_id} fetching today's events (account_ids={account_id_list})")
        result = get_today_events(user_id, user_jwt, account_ids=account_id_list)

        # Check for service-level errors and convert to HTTP error
        if result.get("source") == "error":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=result.get("error", "Failed to fetch today's events from calendar service")
            )

        event_count = len(result.get('events', []))
        logger.info(f"📅 [CALENDAR] Found {event_count} events for today for user {user_id} (unified={result.get('unified', True)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to fetch today's events", logger)


@router.post("/events", response_model=CalendarEventResponse)
async def create_event_endpoint(
    event_data: CreateEventRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Create a new calendar event (syncs to Google Calendar if connected).
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📅 Creating event for user {user_id}")
        result = create_event(user_id, event_data.model_dump(exclude_none=True), user_jwt)
        logger.info(f"✅ Event created (Google sync: {result.get('synced_to_google', False)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to create event", logger)


@router.put("/events/{event_id}", response_model=CalendarEventResponse)
async def update_event_endpoint(
    event_id: str,
    event_data: UpdateEventRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Update an existing calendar event (syncs to Google Calendar if connected).
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📅 Updating event {event_id} for user {user_id}")
        result = update_event(event_id, event_data.model_dump(exclude_none=True), user_id, user_jwt)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Event not found"
            )
        logger.info(f"✅ Event updated (Google sync: {result.get('synced_to_google', False)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to update event", logger)


@router.delete("/events/{event_id}", response_model=CalendarDeleteResponse)
async def delete_event_endpoint(
    event_id: str,
    scope: Optional[Literal['instance', 'all']] = Query(
        'instance',
        description=(
            "Deletion scope: instance (single occurrence) or all (entire recurring series). "
            "Only applicable for recurring events."
        )
    ),
    notify_attendees: Optional[bool] = Query(False, description="Send cancellation emails to attendees (Google Calendar)"),
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Delete a calendar event (syncs to Google Calendar if connected).
    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📅 Deleting event {event_id} for user {user_id}")
        result = delete_event(event_id, user_id, user_jwt, scope=scope, notify_attendees=bool(notify_attendees))
        if not result.get('success'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Event not found"
            )
        if result.get('already_deleted'):
            logger.info(f"✅ Event {event_id} was already deleted (idempotent)")
        else:
            logger.info(f"✅ Event deleted (Google sync: {result.get('synced_to_google', False)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to delete event", logger)


@router.post("/events/{event_id}/rsvp", response_model=RSVPResponse)
async def respond_to_event_endpoint(
    event_id: str,
    rsvp_data: RSVPRequest,
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Respond to a calendar event invitation (accept/decline/tentative).

    Updates the user's response status in Google Calendar.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        logger.info(f"📅 RSVP '{rsvp_data.response_status}' for event {event_id} by user {user_id}")
        result = respond_to_event(event_id, rsvp_data.response_status, user_id, user_jwt)
        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Event not found or you are not an attendee"
            )
        logger.info(f"✅ RSVP updated (Google sync: {result.get('synced_to_google', False)})")
        return result
    except Exception as e:
        handle_api_exception(e, "Failed to respond to event", logger)


@router.post("/sync", response_model=CalendarSyncResponse, status_code=200)
async def sync_google_calendar_endpoint(
    user_jwt: str = Depends(get_current_user_jwt),
    user_id: str = Depends(get_current_user_id),
):
    """
    Sync calendar events from connected providers.

    Marks per-connection calendar streams dirty and returns 202 immediately.

    Sync execution happens on the background worker plane.

    Requires: Authorization header with user's Supabase JWT
    """
    try:
        from lib.supabase_client import get_authenticated_supabase_client
        from lib.supabase_client import get_service_role_client
        from fastapi.responses import JSONResponse
        from api.services.syncs.sync_dispatcher import MANUAL_SYNC_PRIORITY, mark_stream_dirty
        from api.services.syncs.sync_state_store import SYNC_KIND_CALENDAR

        auth_supabase = get_authenticated_supabase_client(user_jwt)
        service_supabase = get_service_role_client()
        connections_result = auth_supabase.table('ext_connections')\
            .select('id, provider')\
            .eq('user_id', user_id)\
            .eq('is_active', True)\
            .in_('provider', ['google', 'microsoft'])\
            .execute()

        connections = connections_result.data or []
        streams_marked = 0

        for conn in connections:
            cid = conn.get('id')
            if not cid:
                continue
            if conn.get('provider') == 'google':
                if mark_stream_dirty(
                    service_supabase,
                    cid,
                    "google",
                    SYNC_KIND_CALENDAR,
                    priority=MANUAL_SYNC_PRIORITY,
                    metadata={"source": "manual-calendar-sync"},
                ):
                    streams_marked += 1
            elif conn.get('provider') == 'microsoft':
                if mark_stream_dirty(
                    service_supabase,
                    cid,
                    "microsoft",
                    SYNC_KIND_CALENDAR,
                    priority=MANUAL_SYNC_PRIORITY,
                    metadata={"source": "manual-calendar-sync"},
                ):
                    streams_marked += 1

        if streams_marked <= 0:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Calendar sync is temporarily unavailable. Please try again shortly.",
            )

        logger.info(f"✅ Marked {streams_marked} calendar streams dirty for user {user_id[:8]}...")
        return JSONResponse(
            status_code=202,
            content={
                "status": "accepted",
                "jobs_enqueued": streams_marked,
                "streams_marked": streams_marked,
            }
        )
    except Exception as e:
        handle_api_exception(e, "Failed to sync calendar", logger)
