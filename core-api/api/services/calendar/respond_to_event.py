"""
Calendar event RSVP service - respond to calendar invites

Allows users to accept, decline, or tentatively accept calendar invitations.
"""
from typing import Dict, Any, Optional, Literal
from datetime import datetime, timezone
from lib.supabase_client import get_authenticated_supabase_client
from api.services.calendar.google_api_helpers import get_google_calendar_service_for_account
import logging

logger = logging.getLogger(__name__)

ResponseStatus = Literal['accepted', 'declined', 'tentative']


def respond_to_event(
    event_id: str,
    response_status: ResponseStatus,
    user_id: str,
    user_jwt: str,
) -> Optional[Dict[str, Any]]:
    """
    Respond to a calendar event invitation (accept/decline/tentative).

    Updates the user's response status in both Google Calendar and local DB.
    """
    auth_supabase = get_authenticated_supabase_client(user_jwt)

    # Get the event with connection info
    event_result = auth_supabase.table('calendar_events')\
        .select('id, external_id, ext_connection_id, raw_item')\
        .eq('id', event_id)\
        .eq('user_id', user_id)\
        .single()\
        .execute()

    if not event_result.data:
        logger.warning(f"Event {event_id} not found for user {user_id}")
        return None

    event = event_result.data
    external_id = event.get('external_id')
    connection_id = event.get('ext_connection_id')

    if not external_id or not connection_id:
        logger.warning(f"Event {event_id} has no external calendar link")
        return None

    # Get Google Calendar service for this account
    service, _ = get_google_calendar_service_for_account(user_id, user_jwt, connection_id)
    if not service:
        logger.error(f"Failed to get Google Calendar service for connection {connection_id}")
        return None

    # Get the current event from Google to find our attendee entry
    google_event = service.events().get(
        calendarId='primary',
        eventId=external_id
    ).execute()

    attendees = google_event.get('attendees', [])
    user_attendee_index = None
    user_email = None

    # Look up the user's email from the connection
    connection_result = auth_supabase.table('ext_connections')\
        .select('metadata')\
        .eq('id', connection_id)\
        .single()\
        .execute()

    if connection_result.data:
        metadata = connection_result.data.get('metadata') or {}
        user_email = metadata.get('email')

    # Prefer Google's "self" flag (always accurate), fall back to email match
    for i, attendee in enumerate(attendees):
        if attendee.get('self'):
            user_attendee_index = i
            if not user_email:
                user_email = attendee.get('email')
            break

    if user_attendee_index is None and user_email:
        for i, attendee in enumerate(attendees):
            if attendee.get('email', '').lower() == user_email.lower():
                user_attendee_index = i
                break

    if user_attendee_index is None:
        logger.warning(
            f"User {user_email} not found in attendees for event {external_id} "
            f"(attendee count: {len(attendees)}, "
            f"attendee emails: {[a.get('email') for a in attendees]})"
        )
        return None

    # Update our response status and send to Google Calendar.
    # Use the user's email as calendarId so Google knows we're updating
    # our own RSVP (not trying to modify as the organizer).
    attendees[user_attendee_index]['responseStatus'] = response_status
    attendee_email = attendees[user_attendee_index].get('email', user_email)

    updated = service.events().patch(
        calendarId=attendee_email,
        eventId=external_id,
        body={'attendees': attendees},
    ).execute()
    logger.info(
        f"Updated RSVP to '{response_status}' for event {external_id}, "
        f"Google returned status: {updated.get('status')}"
    )

    # Update the raw_item in our database to reflect the change
    raw_item = event.get('raw_item') or {}
    if 'attendees' in raw_item and user_email:
        for attendee in raw_item['attendees']:
            if attendee.get('email', '').lower() == user_email.lower():
                attendee['responseStatus'] = response_status
                break

    auth_supabase.table('calendar_events')\
        .update({
            'raw_item': raw_item,
            'synced_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat(),
        })\
        .eq('id', event_id)\
        .execute()

    return {
        'id': event_id,
        'response_status': response_status,
        'synced_to_google': True,
    }
